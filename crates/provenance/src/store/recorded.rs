//! The span-index records of spans no scan committed, on Postgres: the
//! `provenance.recorded_spans` table (`migrations/0004_recorded_spans.sql`)
//! behind `PgProvenanceStore`'s [`ProvenanceStore::record_indexed`] and
//! [`ProvenanceStore::recorded_indexed`].
//!
//! [`ProvenanceStore::record_indexed`]: super::ProvenanceStore::record_indexed
//! [`ProvenanceStore::recorded_indexed`]: super::ProvenanceStore::recorded_indexed

use std::collections::BTreeMap;

use crosstalk_spec::derived::provenance::span::SpanLocation;
use crosstalk_spec::ids::SpanId;
use crosstalk_spec::interfaces::l4_provenance::IndexedSpan;
use crosstalk_spec::observed::message::PartRef;
use crosstalk_spec::support::ByteRange;
use sqlx::postgres::PgRow;
use sqlx::{PgPool, Row};

use super::ProvenanceStoreError;
use crate::pg::{hash_bytes, hash_from, id_bytes, id_from, u32_from};

fn corrupt(reason: &str) -> ProvenanceStoreError {
    ProvenanceStoreError::Corrupt {
        reason: reason.to_owned(),
    }
}

/// Keep `span` under `id` unless a scan committed a span under `id`
/// (`provenance.spans`) or it is already kept: one statement, so the check
/// and the write see one snapshot.
pub(super) async fn record(
    pool: &PgPool,
    id: SpanId,
    span: &IndexedSpan,
) -> Result<(), ProvenanceStoreError> {
    sqlx::query(
        "INSERT INTO provenance.recorded_spans \
         (span, agent, exchange, message, part, range_start, range_end) \
         SELECT $1, $2, $3, $4, $5, $6, $7 \
         WHERE NOT EXISTS (SELECT 1 FROM provenance.spans WHERE span = $1) \
         ON CONFLICT (span) DO NOTHING",
    )
    .bind(id_bytes(id))
    .bind(id_bytes(span.author))
    .bind(id_bytes(span.exchange))
    .bind(hash_bytes(span.location.part.message))
    .bind(i32::from(span.location.part.index))
    .bind(i64::from(span.location.range.start()))
    .bind(i64::from(span.location.range.end()))
    .execute(pool)
    .await?;
    Ok(())
}

/// The kept records among `ids`.
pub(super) async fn read(
    pool: &PgPool,
    ids: &[SpanId],
) -> Result<BTreeMap<SpanId, IndexedSpan>, ProvenanceStoreError> {
    let keys: Vec<Vec<u8>> = ids.iter().copied().map(id_bytes).collect();
    let rows = sqlx::query(
        "SELECT span, agent, exchange, message, part, range_start, range_end \
         FROM provenance.recorded_spans WHERE span = ANY($1::bytea[])",
    )
    .bind(keys)
    .fetch_all(pool)
    .await?;
    rows.iter().map(recorded_from).collect()
}

fn recorded_from(row: &PgRow) -> Result<(SpanId, IndexedSpan), ProvenanceStoreError> {
    let id: Vec<u8> = row.try_get("span")?;
    let agent: Vec<u8> = row.try_get("agent")?;
    let exchange: Vec<u8> = row.try_get("exchange")?;
    let message: Vec<u8> = row.try_get("message")?;
    let part: i32 = row.try_get("part")?;
    let start: i64 = row.try_get("range_start")?;
    let end: i64 = row.try_get("range_end")?;
    let range = ByteRange::new(u32_from(start, "a range")?, u32_from(end, "a range")?)
        .map_err(|_| corrupt("an empty range"))?;
    let index = u16::try_from(part).map_err(|_| corrupt("a part index"))?;
    Ok((
        id_from(&id)?,
        IndexedSpan {
            exchange: id_from(&exchange)?,
            author: id_from(&agent)?,
            location: SpanLocation {
                part: PartRef {
                    message: hash_from(&message)?,
                    index,
                },
                range,
            },
        },
    ))
}
