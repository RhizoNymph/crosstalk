//! [`PgFingerprintIndex`].
//!
//! - `postings (fingerprint, span, span_offset)`: one row per posting, the
//!   primary key making inserts idempotent; by span for eviction.
//! - `observations (observation, at)` and `observed (fingerprint,
//!   observation, at)`: one observation per `observe` call, one `observed`
//!   row per distinct fingerprint in it; `frequency(f, now)` counts the
//!   `observed` rows of `f` with `at >= now - retention`.
//!
//! Every write (`insert`, `observe`, `evict`) is one transaction that first
//! deletes the observations outside the retention period at its `now`,
//! as the reference does. `insert` and `lookup` refuse a call holding a
//! fingerprint of a shard this node does not own before touching the
//! database (`provenance.index.wrong-shard-rejected`). No column holds text
//! (`provenance.index.no-text`).

use std::collections::BTreeSet;

use crosstalk_spec::derived::provenance::fingerprint::{
    Fingerprint, FingerprintHit, PositionedFingerprint,
};
use crosstalk_spec::derived::provenance::span::OriginatedSpan;
use crosstalk_spec::ids::SpanId;
use crosstalk_spec::interfaces::l4_provenance::{FingerprintIndex, IndexError};
use crosstalk_spec::support::Timestamp;
use sqlx::{PgConnection, PgPool, Row};

use crate::config::IndexSettings;
use crate::pg::{fingerprint_from, fingerprint_i64, horizon, id_bytes, id_from, time_i64};

fn store_error(reason: impl std::fmt::Display) -> IndexError {
    IndexError::Store {
        reason: reason.to_string(),
    }
}

/// The fingerprint index in Postgres. Clones share the pool.
#[derive(Debug, Clone)]
pub struct PgFingerprintIndex {
    pool: PgPool,
    settings: IndexSettings,
}

impl PgFingerprintIndex {
    /// An index over `pool`, whose database has L4's migrations applied.
    pub fn new(pool: PgPool, settings: IndexSettings) -> Self {
        Self { pool, settings }
    }

    pub fn settings(&self) -> &IndexSettings {
        &self.settings
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    fn check_shards<'a>(
        &self,
        fingerprints: impl IntoIterator<Item = &'a PositionedFingerprint>,
    ) -> Result<(), IndexError> {
        match fingerprints
            .into_iter()
            .find(|positioned| !self.settings.owns(positioned.fingerprint))
        {
            Some(positioned) => Err(IndexError::WrongShard {
                fingerprint: positioned.fingerprint,
            }),
            None => Ok(()),
        }
    }

    fn horizon(&self, now: Timestamp) -> i64 {
        horizon(now, self.settings.retention_micros())
    }

    fn cutoff(&self) -> i64 {
        i64::try_from(self.settings.cutoff()).unwrap_or(i64::MAX)
    }

    /// Delete every posting and observation (tests that reuse one
    /// database).
    pub async fn clear(&self) -> Result<(), IndexError> {
        sqlx::query("TRUNCATE provenance.postings, provenance.observed, provenance.observations")
            .execute(&self.pool)
            .await
            .map_err(store_error)?;
        Ok(())
    }

    /// How many postings and observed fingerprints the index holds.
    pub async fn row_counts(&self) -> Result<(u64, u64), IndexError> {
        let row = sqlx::query(
            "SELECT (SELECT count(*) FROM provenance.postings) AS postings, \
             (SELECT count(*) FROM provenance.observed) AS observed",
        )
        .fetch_one(&self.pool)
        .await
        .map_err(store_error)?;
        let postings: i64 = row.try_get("postings").map_err(store_error)?;
        let observed: i64 = row.try_get("observed").map_err(store_error)?;
        Ok((
            u64::try_from(postings).unwrap_or(0),
            u64::try_from(observed).unwrap_or(0),
        ))
    }
}

async fn age_out(conn: &mut PgConnection, horizon: i64) -> Result<(), IndexError> {
    sqlx::query("DELETE FROM provenance.observations WHERE at < $1")
        .bind(horizon)
        .execute(&mut *conn)
        .await
        .map_err(store_error)?;
    Ok(())
}

fn columns(fingerprints: &[PositionedFingerprint]) -> (Vec<i64>, Vec<i64>) {
    fingerprints
        .iter()
        .map(|positioned| {
            (
                fingerprint_i64(positioned.fingerprint),
                i64::from(positioned.offset),
            )
        })
        .unzip()
}

impl FingerprintIndex for PgFingerprintIndex {
    async fn insert(
        &mut self,
        span: &OriginatedSpan,
        fingerprints: &[PositionedFingerprint],
        now: Timestamp,
    ) -> Result<(), IndexError> {
        self.check_shards(fingerprints)?;
        let horizon = self.horizon(now);
        let (values, offsets) = columns(fingerprints);
        let mut tx = self.pool.begin().await.map_err(store_error)?;
        age_out(&mut tx, horizon).await?;
        sqlx::query(
            "INSERT INTO provenance.postings (fingerprint, span, span_offset) \
             SELECT q.fp, $3, q.off FROM unnest($1::bigint[], $2::bigint[]) AS q(fp, off) \
             WHERE (SELECT count(*) FROM provenance.observed o \
                    WHERE o.fingerprint = q.fp AND o.at >= $4) <= $5 \
             ON CONFLICT DO NOTHING",
        )
        .bind(values)
        .bind(offsets)
        .bind(id_bytes(span.span().id))
        .bind(horizon)
        .bind(self.cutoff())
        .execute(&mut *tx)
        .await
        .map_err(store_error)?;
        tx.commit().await.map_err(store_error)?;
        Ok(())
    }

    async fn lookup(
        &self,
        fingerprints: &[PositionedFingerprint],
        now: Timestamp,
    ) -> Result<Vec<FingerprintHit>, IndexError> {
        self.check_shards(fingerprints)?;
        if fingerprints.is_empty() {
            return Ok(Vec::new());
        }
        let (values, offsets) = columns(fingerprints);
        let rows = sqlx::query(
            "SELECT q.fp, p.span, p.span_offset, q.off \
             FROM unnest($1::bigint[], $2::bigint[]) WITH ORDINALITY AS q(fp, off, ord) \
             JOIN provenance.postings p ON p.fingerprint = q.fp \
             WHERE (SELECT count(*) FROM provenance.observed o \
                    WHERE o.fingerprint = q.fp AND o.at >= $3) <= $4 \
             ORDER BY q.ord, p.span, p.span_offset",
        )
        .bind(values)
        .bind(offsets)
        .bind(self.horizon(now))
        .bind(self.cutoff())
        .fetch_all(&self.pool)
        .await
        .map_err(store_error)?;
        rows.iter()
            .map(|row| {
                let fingerprint: i64 = row.try_get("fp").map_err(store_error)?;
                let span: Vec<u8> = row.try_get("span").map_err(store_error)?;
                let span_offset: i64 = row.try_get("span_offset").map_err(store_error)?;
                let query_offset: i64 = row.try_get("off").map_err(store_error)?;
                Ok(FingerprintHit {
                    fingerprint: fingerprint_from(fingerprint),
                    span: id_from(&span).map_err(store_error)?,
                    span_offset: u32::try_from(span_offset).map_err(store_error)?,
                    query_offset: u32::try_from(query_offset).map_err(store_error)?,
                })
            })
            .collect()
    }

    async fn frequency(&self, fingerprint: Fingerprint, now: Timestamp) -> Result<u64, IndexError> {
        let count: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM provenance.observed WHERE fingerprint = $1 AND at >= $2",
        )
        .bind(fingerprint_i64(fingerprint))
        .bind(self.horizon(now))
        .fetch_one(&self.pool)
        .await
        .map_err(store_error)?;
        Ok(u64::try_from(count).unwrap_or(0))
    }

    async fn observe(
        &mut self,
        fingerprints: &[Fingerprint],
        at: Timestamp,
        now: Timestamp,
    ) -> Result<(), IndexError> {
        let horizon = self.horizon(now);
        let mut tx = self.pool.begin().await.map_err(store_error)?;
        age_out(&mut tx, horizon).await?;
        if self.settings.counts(at, now) {
            let at = time_i64(at).map_err(store_error)?;
            let observation: i64 = sqlx::query_scalar(
                "INSERT INTO provenance.observations (at) VALUES ($1) RETURNING observation",
            )
            .bind(at)
            .fetch_one(&mut *tx)
            .await
            .map_err(store_error)?;
            let distinct: BTreeSet<i64> =
                fingerprints.iter().copied().map(fingerprint_i64).collect();
            let distinct: Vec<i64> = distinct.into_iter().collect();
            sqlx::query(
                "INSERT INTO provenance.observed (fingerprint, observation, at) \
                 SELECT fp, $2, $3 FROM unnest($1::bigint[]) AS q(fp)",
            )
            .bind(distinct)
            .bind(observation)
            .bind(at)
            .execute(&mut *tx)
            .await
            .map_err(store_error)?;
        }
        tx.commit().await.map_err(store_error)?;
        Ok(())
    }

    async fn evict(&mut self, spans: &[SpanId], now: Timestamp) -> Result<(), IndexError> {
        let horizon = self.horizon(now);
        let keys: Vec<Vec<u8>> = spans.iter().copied().map(id_bytes).collect();
        let mut tx = self.pool.begin().await.map_err(store_error)?;
        age_out(&mut tx, horizon).await?;
        let evicted = sqlx::query("DELETE FROM provenance.postings WHERE span = ANY($1::bytea[])")
            .bind(keys)
            .execute(&mut *tx)
            .await
            .map_err(store_error)?;
        tx.commit().await.map_err(store_error)?;
        tracing::debug!(
            spans = spans.len(),
            postings = evicted.rows_affected(),
            "spans evicted from the fingerprint index"
        );
        Ok(())
    }
}
