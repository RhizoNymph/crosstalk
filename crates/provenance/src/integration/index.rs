//! `PgFingerprintIndex` against Postgres.

use std::time::Duration;

use crosstalk_memory::model::HarnessConfig;
use crosstalk_memory::provenance::model::check_fingerprint_index;
use crosstalk_spec::derived::provenance::fingerprint::{
    Fingerprint, FingerprintHit, PositionedFingerprint,
};
use crosstalk_spec::derived::provenance::span::{OriginatedSpan, Span, SpanLocation, SpanState};
use crosstalk_spec::ids::SpanId;
use crosstalk_spec::interfaces::l4_provenance::{FingerprintIndex, IndexError};
use crosstalk_spec::observed::message::PartRef;
use crosstalk_spec::support::{ByteRange, Timestamp};
use crosstalk_testkit::ids::Ids;
use sqlx::Row;
use tokio::sync::OnceCell;

use super::{database, lazy_pool, lazy_pool_with};
use crate::config::IndexSettings;
use crate::index::PgFingerprintIndex;

fn span(ids: &mut Ids) -> OriginatedSpan {
    OriginatedSpan::new(Span {
        id: ids.span(),
        location: SpanLocation {
            part: PartRef {
                message: ids.message(),
                index: 0,
            },
            range: ByteRange::new(0, 40).expect("range"),
        },
        agent: ids.agent(),
        exchange: ids.exchange(),
        state: SpanState::Originated,
    })
    .expect("originated")
}

fn positioned(values: &[u64]) -> Vec<PositionedFingerprint> {
    values
        .iter()
        .enumerate()
        .map(|(offset, value)| PositionedFingerprint {
            fingerprint: Fingerprint(*value),
            offset: u32::try_from(offset).expect("small"),
        })
        .collect()
}

const T: Timestamp = Timestamp::from_micros(1_000_000);

fn settings(cutoff: u64) -> IndexSettings {
    IndexSettings::single_node(cutoff, Duration::from_secs(60)).expect("settings")
}

async fn postings_of(index: &PgFingerprintIndex, fingerprint: u64) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM provenance.postings WHERE fingerprint = $1")
        .bind(crate::pg::fingerprint_i64(Fingerprint(fingerprint)))
        .fetch_one(index.pool())
        .await
        .expect("count")
}

pub async fn insert_skips_fingerprints_above_cutoff() {
    let Some(db) = database("pg_insert_skips_fingerprints_above_cutoff").await else {
        return;
    };
    let mut index = PgFingerprintIndex::new(lazy_pool(&db), settings(2));
    for _ in 0..3 {
        index
            .observe(&[Fingerprint(11)], T, T)
            .await
            .expect("observe");
    }
    let mut ids = Ids::new();
    index
        .insert(&span(&mut ids), &positioned(&[11, 12]), T)
        .await
        .expect("insert");
    assert_eq!(
        postings_of(&index, 11).await,
        0,
        "a boilerplate fingerprint got a posting"
    );
    assert_eq!(postings_of(&index, 12).await, 1);
    index.pool().close().await;
    db.close().await.expect("close");
}

pub async fn lookup_ignores_fingerprints_above_cutoff() {
    let Some(db) = database("pg_lookup_ignores_fingerprints_above_cutoff").await else {
        return;
    };
    let mut index = PgFingerprintIndex::new(lazy_pool(&db), settings(2));
    let mut ids = Ids::new();
    let indexed = span(&mut ids);
    index
        .insert(&indexed, &positioned(&[21, 22]), T)
        .await
        .expect("insert");
    let hits = index.lookup(&positioned(&[21]), T).await.expect("lookup");
    assert_eq!(hits.len(), 1);
    for _ in 0..3 {
        index
            .observe(&[Fingerprint(21)], T, T)
            .await
            .expect("observe");
    }
    let hits = index
        .lookup(&positioned(&[21, 22]), T)
        .await
        .expect("lookup");
    assert_eq!(
        hits,
        vec![FingerprintHit {
            fingerprint: Fingerprint(22),
            span: indexed.span().id,
            span_offset: 1,
            query_offset: 1,
        }]
    );
    index.pool().close().await;
    db.close().await.expect("close");
}

pub async fn observations_age_out() {
    let Some(db) = database("pg_observations_age_out").await else {
        return;
    };
    let mut index = PgFingerprintIndex::new(lazy_pool(&db), settings(2));
    index
        .observe(&[Fingerprint(1), Fingerprint(2)], T, T)
        .await
        .expect("observe");
    assert_eq!(index.row_counts().await.expect("counts").1, 2);
    let later = Timestamp::from_micros(T.as_micros() + 61_000_000);
    assert_eq!(
        index
            .frequency(Fingerprint(1), later)
            .await
            .expect("frequency"),
        0
    );
    index.evict(&[], later).await.expect("evict");
    assert_eq!(index.row_counts().await.expect("counts"), (0, 0));
    let rows: i64 = sqlx::query_scalar("SELECT count(*) FROM provenance.observations")
        .fetch_one(index.pool())
        .await
        .expect("count");
    assert_eq!(rows, 0, "observation rows outlived retention");
    index.pool().close().await;
    db.close().await.expect("close");
}

pub async fn lookup_after_evict_has_no_hits() {
    let Some(db) = database("pg_lookup_after_evict_has_no_hits").await else {
        return;
    };
    let mut index = PgFingerprintIndex::new(lazy_pool(&db), settings(5));
    let mut ids = Ids::new();
    let evicted = span(&mut ids);
    let kept = span(&mut ids);
    index
        .insert(&evicted, &positioned(&[31, 32]), T)
        .await
        .expect("insert");
    index
        .insert(&kept, &positioned(&[32]), T)
        .await
        .expect("insert");
    index.evict(&[evicted.span().id], T).await.expect("evict");
    let hits = index
        .lookup(&positioned(&[31, 32]), T)
        .await
        .expect("lookup");
    assert!(hits.iter().all(|hit| hit.span == kept.span().id));
    assert_eq!(hits.len(), 1);
    index.pool().close().await;
    db.close().await.expect("close");
}

pub async fn schema_has_no_text_columns() {
    let Some(db) = database("pg_index_schema_has_no_text_columns").await else {
        return;
    };
    let rows = sqlx::query(
        "SELECT table_name, column_name, data_type FROM information_schema.columns \
         WHERE table_schema = 'provenance' \
         AND table_name IN ('postings', 'observations', 'observed') ORDER BY 1, 2",
    )
    .fetch_all(db.pool())
    .await
    .expect("columns");
    assert!(!rows.is_empty());
    for row in &rows {
        let table: String = row.get("table_name");
        let column: String = row.get("column_name");
        let kind: String = row.get("data_type");
        assert!(
            matches!(kind.as_str(), "bigint" | "integer" | "bytea"),
            "{table}.{column} is {kind}"
        );
    }
    db.close().await.expect("close");
}

/// The Postgres index, emptied before its first call (each harness case
/// starts from an empty index on the shared test database).
struct Fresh {
    index: PgFingerprintIndex,
    ready: OnceCell<()>,
}

impl Fresh {
    async fn ready(&self) -> Result<(), IndexError> {
        self.ready
            .get_or_try_init(|| self.index.clear())
            .await
            .map(|_| ())
    }
}

impl FingerprintIndex for Fresh {
    async fn insert(
        &mut self,
        span: &OriginatedSpan,
        fingerprints: &[PositionedFingerprint],
        now: Timestamp,
    ) -> Result<(), IndexError> {
        self.ready().await?;
        self.index.insert(span, fingerprints, now).await
    }

    async fn lookup(
        &self,
        fingerprints: &[PositionedFingerprint],
        now: Timestamp,
    ) -> Result<Vec<FingerprintHit>, IndexError> {
        self.ready().await?;
        self.index.lookup(fingerprints, now).await
    }

    async fn frequency(&self, fingerprint: Fingerprint, now: Timestamp) -> Result<u64, IndexError> {
        self.ready().await?;
        self.index.frequency(fingerprint, now).await
    }

    async fn observe(
        &mut self,
        fingerprints: &[Fingerprint],
        at: Timestamp,
        now: Timestamp,
    ) -> Result<(), IndexError> {
        self.ready().await?;
        self.index.observe(fingerprints, at, now).await
    }

    async fn evict(&mut self, spans: &[SpanId], now: Timestamp) -> Result<(), IndexError> {
        self.ready().await?;
        self.index.evict(spans, now).await
    }
}

pub async fn matches_the_reference_model() {
    let Some(db) = database("pg_frequency_matches_observation_model").await else {
        return;
    };
    let options = db.url().connect_options().clone();
    let result = tokio::task::spawn_blocking(move || {
        check_fingerprint_index(
            HarnessConfig {
                cases: 12,
                max_ops: 24,
            },
            |config| {
                let settings = IndexSettings::sharded(
                    config.cutoff(),
                    config.retention(),
                    config.shards(),
                    config.owned().clone(),
                )
                .expect("the harness's settings are valid");
                Fresh {
                    index: PgFingerprintIndex::new(lazy_pool_with(options.clone()), settings),
                    ready: OnceCell::new(),
                }
            },
        )
    })
    .await
    .expect("the harness thread");
    db.close().await.expect("close");
    if let Err(mismatch) = result {
        panic!("{mismatch}");
    }
}
