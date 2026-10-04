//! [`PgProvenanceStore`]: L4's records in the `provenance` schema
//! (`migrations/0001_provenance.sql`).
//!
//! Tables: `exchanges` (status, by id; by start time for pruning),
//! `exchange_requests` (the request's hashes until pruned),
//! `scanned_messages` (per-message scans, by message and by exchange),
//! `spans` (by id, by exchange in output order, by message and part, and the
//! live spans by indexing time), `matches` (by id, by reader message, by
//! origin span, by reader exchange). Each write is one transaction; span
//! states change only through `SpanState::advance`.

use crosstalk_spec::derived::provenance::matching::{Carrier, ContentMatch, MatchKind};
use crosstalk_spec::derived::provenance::span::{
    RelaySource, Span, SpanEvent, SpanLocation, SpanState,
};
use crosstalk_spec::ids::{ExchangeId, MessageHash, SpanId};
use crosstalk_spec::observed::message::PartRef;
use crosstalk_spec::support::{ByteRange, Timestamp};
use crosstalk_store::{Layer, Migrations, StoreError};
use sqlx::postgres::PgRow;
use sqlx::{PgConnection, PgPool, Row};

use super::{
    Committed, ExchangeRecord, MessageScan, ProvenanceStore, ProvenanceStoreError, ScanCommit,
    ScanFailure, ScanStatus, ScannedAs, SpanRecord, StoredMatch,
};
use crate::pg::{
    Failure, OutOfRange, failure, hash_bytes, hash_from, horizon, id_bytes, id_from, time_from,
    time_i64, u32_from,
};

/// L4's migrations, embedded.
pub static MIGRATIONS: sqlx::migrate::Migrator = sqlx::migrate!("./migrations");

/// Run L4's migrations in the `provenance` schema.
pub async fn migrate(pool: &PgPool) -> Result<(), StoreError> {
    crosstalk_store::migrate(pool, Layer::Provenance, Migrations::Embedded(&MIGRATIONS)).await
}

impl From<sqlx::Error> for ProvenanceStoreError {
    fn from(error: sqlx::Error) -> Self {
        match failure(&error) {
            Failure::Transient(failure) => Self::Unavailable {
                reason: failure.to_string(),
            },
            Failure::Refused(failure) => Self::Rejected {
                reason: failure.to_string(),
            },
        }
    }
}

impl From<OutOfRange> for ProvenanceStoreError {
    fn from(error: OutOfRange) -> Self {
        Self::Corrupt {
            reason: error.to_string(),
        }
    }
}

fn corrupt(reason: impl Into<String>) -> ProvenanceStoreError {
    ProvenanceStoreError::Corrupt {
        reason: reason.into(),
    }
}

/// L4's records in Postgres. Clones share the pool.
#[derive(Debug, Clone)]
pub struct PgProvenanceStore {
    pool: PgPool,
}

impl PgProvenanceStore {
    /// A store over `pool`, whose database has L4's migrations applied.
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }
}

fn scanned_as_text(scanned_as: ScannedAs) -> &'static str {
    match scanned_as {
        ScannedAs::Input => "input",
        ScannedAs::System => "system",
        ScannedAs::Output => "output",
    }
}

fn scanned_as_from(text: &str) -> Result<ScannedAs, ProvenanceStoreError> {
    match text {
        "input" => Ok(ScannedAs::Input),
        "system" => Ok(ScannedAs::System),
        "output" => Ok(ScannedAs::Output),
        other => Err(corrupt(format!("scanned_as {other:?}"))),
    }
}

fn status_from(row: &PgRow) -> Result<ScanStatus, ProvenanceStoreError> {
    let status: String = row.try_get("status")?;
    let at: Option<i64> = row.try_get("status_at")?;
    let at = || -> Result<Timestamp, ProvenanceStoreError> {
        Ok(time_from(at.ok_or_else(|| corrupt("status time missing"))?)?)
    };
    match status.as_str() {
        "pending" => Ok(ScanStatus::Pending),
        "scanned" => Ok(ScanStatus::Scanned { at: at()? }),
        "indexed" => Ok(ScanStatus::Indexed { at: at()? }),
        "failed" => {
            let kind: Option<String> = row.try_get("failure_kind")?;
            let message: Option<Vec<u8>> = row.try_get("failure_message")?;
            let reason: Option<String> = row.try_get("failure_reason")?;
            let message = || -> Result<MessageHash, ProvenanceStoreError> {
                Ok(hash_from(
                    message
                        .as_deref()
                        .ok_or_else(|| corrupt("failure message missing"))?,
                )?)
            };
            let failure = match kind.as_deref() {
                Some("body_missing") => ScanFailure::BodyMissing(message()?),
                Some("body_undecodable") => ScanFailure::BodyUndecodable(message()?),
                Some("inconsistent") => ScanFailure::Inconsistent {
                    reason: reason.unwrap_or_default(),
                },
                other => return Err(corrupt(format!("failure kind {other:?}"))),
            };
            Ok(ScanStatus::Failed { at: at()?, failure })
        }
        other => Err(corrupt(format!("status {other:?}"))),
    }
}

/// A span state as its columns: state, relay span, relay message,
/// indexed_at, first_hit_at, hits, expired_at.
struct StateColumns {
    state: &'static str,
    relay_span: Option<Vec<u8>>,
    relay_message: Option<Vec<u8>>,
    indexed_at: Option<i64>,
    first_hit_at: Option<i64>,
    hits: Option<i64>,
    expired_at: Option<i64>,
}

fn state_columns(
    state: &SpanState,
    indexed_before: Option<i64>,
) -> Result<StateColumns, ProvenanceStoreError> {
    let mut columns = StateColumns {
        state: "",
        relay_span: None,
        relay_message: None,
        indexed_at: None,
        first_hit_at: None,
        hits: None,
        expired_at: None,
    };
    match state {
        SpanState::Extracted => return Err(corrupt("an extracted span is never stored")),
        SpanState::Common => columns.state = "common",
        SpanState::Relayed { source } => {
            columns.state = "relayed";
            match source {
                RelaySource::Span(span) => columns.relay_span = Some(id_bytes(*span)),
                RelaySource::Input(message) => columns.relay_message = Some(hash_bytes(*message)),
            }
        }
        SpanState::Originated => columns.state = "originated",
        SpanState::Indexed { at } => {
            columns.state = "indexed";
            columns.indexed_at = Some(time_i64(*at)?);
        }
        SpanState::Propagated {
            indexed_at,
            first_hit_at,
            hits,
        } => {
            columns.state = "propagated";
            columns.indexed_at = Some(time_i64(*indexed_at)?);
            columns.first_hit_at = Some(time_i64(*first_hit_at)?);
            columns.hits = Some(i64::from(hits.get()));
        }
        SpanState::Expired { at } => {
            columns.state = "expired";
            columns.indexed_at = indexed_before;
            columns.expired_at = Some(time_i64(*at)?);
        }
    }
    Ok(columns)
}

fn state_from(row: &PgRow) -> Result<SpanState, ProvenanceStoreError> {
    let state: String = row.try_get("state")?;
    let time = |column: &str| -> Result<Timestamp, ProvenanceStoreError> {
        let value: Option<i64> = row.try_get(column)?;
        Ok(time_from(
            value.ok_or_else(|| corrupt(format!("{column} missing")))?,
        )?)
    };
    match state.as_str() {
        "common" => Ok(SpanState::Common),
        "relayed" => {
            let span: Option<Vec<u8>> = row.try_get("relay_span")?;
            let message: Option<Vec<u8>> = row.try_get("relay_message")?;
            let source = match (span, message) {
                (Some(span), None) => RelaySource::Span(id_from(&span)?),
                (None, Some(message)) => RelaySource::Input(hash_from(&message)?),
                _ => return Err(corrupt("relay source")),
            };
            Ok(SpanState::Relayed { source })
        }
        "originated" => Ok(SpanState::Originated),
        "indexed" => Ok(SpanState::Indexed {
            at: time("indexed_at")?,
        }),
        "propagated" => {
            let hits: Option<i64> = row.try_get("hits")?;
            let hits = hits
                .and_then(|hits| u32::try_from(hits).ok())
                .and_then(std::num::NonZeroU32::new)
                .ok_or_else(|| corrupt("hits"))?;
            Ok(SpanState::Propagated {
                indexed_at: time("indexed_at")?,
                first_hit_at: time("first_hit_at")?,
                hits,
            })
        }
        "expired" => Ok(SpanState::Expired {
            at: time("expired_at")?,
        }),
        other => Err(corrupt(format!("span state {other:?}"))),
    }
}

/// The span columns every span read selects.
macro_rules! span_columns {
    () => {
        "span, agent, exchange, message, part, range_start, range_end, ordinal, state, \
         relay_span, relay_message, indexed_at, first_hit_at, hits, expired_at, index_seq"
    };
}

fn span_from(row: &PgRow) -> Result<SpanRecord, ProvenanceStoreError> {
    let id: Vec<u8> = row.try_get("span")?;
    let agent: Vec<u8> = row.try_get("agent")?;
    let exchange: Vec<u8> = row.try_get("exchange")?;
    let message: Vec<u8> = row.try_get("message")?;
    let part: i32 = row.try_get("part")?;
    let start: i64 = row.try_get("range_start")?;
    let end: i64 = row.try_get("range_end")?;
    let ordinal: i32 = row.try_get("ordinal")?;
    let index_seq: Option<i64> = row.try_get("index_seq")?;
    let range = ByteRange::new(u32_from(start, "a range")?, u32_from(end, "a range")?)
        .map_err(|_| corrupt("an empty range"))?;
    let part = u16::try_from(part).map_err(|_| corrupt("a part index"))?;
    Ok(SpanRecord {
        span: Span {
            id: id_from(&id)?,
            location: SpanLocation {
                part: PartRef {
                    message: hash_from(&message)?,
                    index: part,
                },
                range,
            },
            agent: id_from(&agent)?,
            exchange: id_from(&exchange)?,
            state: state_from(row)?,
        },
        ordinal: u32::try_from(ordinal).map_err(|_| corrupt("an ordinal"))?,
        index_seq: index_seq.and_then(|seq| u64::try_from(seq).ok()),
    })
}

/// The match columns every match read selects.
macro_rules! match_columns {
    () => {
        "id, reader_exchange, ordinal, at, origin, origin_agent, reader, read_message, \
         read_part, read_start, read_end, carrier, kind, matched_bytes"
    };
}

fn match_from(row: &PgRow) -> Result<StoredMatch, ProvenanceStoreError> {
    let id: Vec<u8> = row.try_get("id")?;
    let exchange: Vec<u8> = row.try_get("reader_exchange")?;
    let ordinal: i32 = row.try_get("ordinal")?;
    let at: i64 = row.try_get("at")?;
    let origin: Vec<u8> = row.try_get("origin")?;
    let origin_agent: Vec<u8> = row.try_get("origin_agent")?;
    let reader: Vec<u8> = row.try_get("reader")?;
    let message: Vec<u8> = row.try_get("read_message")?;
    let part: i32 = row.try_get("read_part")?;
    let start: i64 = row.try_get("read_start")?;
    let end: i64 = row.try_get("read_end")?;
    let carrier: String = row.try_get("carrier")?;
    let kind: String = row.try_get("kind")?;
    let matched: i64 = row.try_get("matched_bytes")?;
    let carrier: Carrier =
        serde_json::from_str(&carrier).map_err(|error| corrupt(format!("carrier: {error}")))?;
    let kind: MatchKind =
        serde_json::from_str(&kind).map_err(|error| corrupt(format!("match kind: {error}")))?;
    let range = ByteRange::new(u32_from(start, "a range")?, u32_from(end, "a range")?)
        .map_err(|_| corrupt("an empty range"))?;
    let read_at = SpanLocation {
        part: PartRef {
            message: hash_from(&message)?,
            index: u16::try_from(part).map_err(|_| corrupt("a part index"))?,
        },
        range,
    };
    let matched = std::num::NonZeroU32::new(u32_from(matched, "matched bytes")?)
        .ok_or_else(|| corrupt("zero matched bytes"))?;
    let content = ContentMatch::new(
        id_from(&origin)?,
        id_from(&origin_agent)?,
        id_from(&reader)?,
        id_from(&exchange)?,
        read_at,
        carrier,
        kind,
        matched,
    )
    .map_err(|error| corrupt(format!("content match: {error:?}")))?;
    Ok(StoredMatch {
        id: id_from(&id)?,
        ordinal: u32::try_from(ordinal).map_err(|_| corrupt("an ordinal"))?,
        at: time_from(at)?,
        content,
    })
}

async fn insert_span(
    conn: &mut PgConnection,
    span: &Span,
    ordinal: usize,
) -> Result<(), ProvenanceStoreError> {
    let columns = state_columns(&span.state, None)?;
    sqlx::query(
        "INSERT INTO provenance.spans (span, agent, exchange, message, part, range_start, \
         range_end, ordinal, state, relay_span, relay_message, indexed_at, first_hit_at, hits, \
         expired_at) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15) \
         ON CONFLICT (span) DO NOTHING",
    )
    .bind(id_bytes(span.id))
    .bind(id_bytes(span.agent))
    .bind(id_bytes(span.exchange))
    .bind(hash_bytes(span.location.part.message))
    .bind(i32::from(span.location.part.index))
    .bind(i64::from(span.location.range.start()))
    .bind(i64::from(span.location.range.end()))
    .bind(i32::try_from(ordinal).map_err(|_| corrupt("an ordinal"))?)
    .bind(columns.state)
    .bind(columns.relay_span)
    .bind(columns.relay_message)
    .bind(columns.indexed_at)
    .bind(columns.first_hit_at)
    .bind(columns.hits)
    .bind(columns.expired_at)
    .execute(&mut *conn)
    .await?;
    Ok(())
}

/// Advance a stored span by `event` (row locked), writing the new state.
/// A refused transition is returned, changing nothing.
async fn advance_span(
    conn: &mut PgConnection,
    id: SpanId,
    event: SpanEvent,
    index_seq: bool,
) -> Result<Option<SpanState>, ProvenanceStoreError> {
    let query = concat!(
        "SELECT ",
        span_columns!(),
        " FROM provenance.spans WHERE span = $1 FOR UPDATE"
    );
    let Some(row) = sqlx::query(query)
        .bind(id_bytes(id))
        .fetch_optional(&mut *conn)
        .await?
    else {
        return Ok(None);
    };
    let record = span_from(&row)?;
    let indexed_before: Option<i64> = row.try_get("indexed_at")?;
    let next = record
        .span
        .state
        .advance(event)
        .map_err(ProvenanceStoreError::Transition)?;
    let columns = state_columns(&next, indexed_before)?;
    let update = if index_seq {
        "UPDATE provenance.spans SET state = $2, relay_span = $3, relay_message = $4, \
         indexed_at = $5, first_hit_at = $6, hits = $7, expired_at = $8, \
         index_seq = nextval('provenance.index_seq') WHERE span = $1"
    } else {
        "UPDATE provenance.spans SET state = $2, relay_span = $3, relay_message = $4, \
         indexed_at = $5, first_hit_at = $6, hits = $7, expired_at = $8 WHERE span = $1"
    };
    sqlx::query(update)
        .bind(id_bytes(id))
        .bind(columns.state)
        .bind(columns.relay_span)
        .bind(columns.relay_message)
        .bind(columns.indexed_at)
        .bind(columns.first_hit_at)
        .bind(columns.hits)
        .bind(columns.expired_at)
        .execute(&mut *conn)
        .await?;
    Ok(Some(next))
}

async fn insert_match(
    conn: &mut PgConnection,
    stored: &StoredMatch,
) -> Result<(), ProvenanceStoreError> {
    let content = &stored.content;
    let read_at = content.read_at();
    let carrier = serde_json::to_string(content.carrier())
        .map_err(|error| corrupt(format!("carrier: {error}")))?;
    let kind = serde_json::to_string(content.kind())
        .map_err(|error| corrupt(format!("match kind: {error}")))?;
    sqlx::query(
        "INSERT INTO provenance.matches (id, reader_exchange, ordinal, at, origin, origin_agent, \
         reader, read_message, read_part, read_start, read_end, carrier, kind, matched_bytes) \
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14) \
         ON CONFLICT (id) DO NOTHING",
    )
    .bind(id_bytes(stored.id))
    .bind(id_bytes(content.reader_exchange()))
    .bind(i32::try_from(stored.ordinal).map_err(|_| corrupt("an ordinal"))?)
    .bind(time_i64(stored.at)?)
    .bind(id_bytes(content.origin()))
    .bind(id_bytes(content.origin_agent()))
    .bind(id_bytes(content.reader()))
    .bind(hash_bytes(read_at.part.message))
    .bind(i32::from(read_at.part.index))
    .bind(i64::from(read_at.range.start()))
    .bind(i64::from(read_at.range.end()))
    .bind(carrier)
    .bind(kind)
    .bind(i64::from(content.matched_bytes().get()))
    .execute(&mut *conn)
    .await?;
    Ok(())
}

async fn locked_status(
    conn: &mut PgConnection,
    exchange: ExchangeId,
) -> Result<Option<ScanStatus>, ProvenanceStoreError> {
    let row = sqlx::query(
        "SELECT status, status_at, failure_kind, failure_message, failure_reason \
         FROM provenance.exchanges WHERE exchange = $1 FOR UPDATE",
    )
    .bind(id_bytes(exchange))
    .fetch_optional(&mut *conn)
    .await?;
    row.map(|row| status_from(&row)).transpose()
}

impl ProvenanceStore for PgProvenanceStore {
    async fn record_exchange(&mut self, record: ExchangeRecord) -> Result<(), ProvenanceStoreError> {
        let mut tx = self.pool.begin().await?;
        let inserted = sqlx::query(
            "INSERT INTO provenance.exchanges (exchange, started_at, output, status) \
             VALUES ($1, $2, $3, 'pending') ON CONFLICT (exchange) DO NOTHING",
        )
        .bind(id_bytes(record.id))
        .bind(time_i64(record.started_at)?)
        .bind(record.output.map(hash_bytes))
        .execute(&mut *tx)
        .await?;
        if inserted.rows_affected() > 0 {
            let request: Vec<Vec<u8>> = record.request.iter().copied().map(hash_bytes).collect();
            sqlx::query(
                "INSERT INTO provenance.exchange_requests (exchange, request) VALUES ($1, $2)",
            )
            .bind(id_bytes(record.id))
            .bind(request)
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        Ok(())
    }

    async fn exchange(
        &self,
        id: ExchangeId,
    ) -> Result<Option<(ExchangeRecord, ScanStatus)>, ProvenanceStoreError> {
        let row = sqlx::query(
            "SELECT e.exchange, e.started_at, e.output, e.status, e.status_at, e.failure_kind, \
             e.failure_message, e.failure_reason, r.request \
             FROM provenance.exchanges e \
             LEFT JOIN provenance.exchange_requests r ON r.exchange = e.exchange \
             WHERE e.exchange = $1",
        )
        .bind(id_bytes(id))
        .fetch_optional(&self.pool)
        .await?;
        let Some(row) = row else {
            return Ok(None);
        };
        let started_at: i64 = row.try_get("started_at")?;
        let output: Option<Vec<u8>> = row.try_get("output")?;
        let request: Option<Vec<Vec<u8>>> = row.try_get("request")?;
        let request = request
            .unwrap_or_default()
            .iter()
            .map(|hash| hash_from(hash))
            .collect::<Result<Vec<_>, _>>()?;
        let record = ExchangeRecord {
            id,
            started_at: time_from(started_at)?,
            request,
            output: output.as_deref().map(hash_from).transpose()?,
        };
        Ok(Some((record, status_from(&row)?)))
    }

    async fn spans(&self, ids: &[SpanId]) -> Result<Vec<SpanRecord>, ProvenanceStoreError> {
        let keys: Vec<Vec<u8>> = ids.iter().copied().map(id_bytes).collect();
        let query = concat!(
            "SELECT ",
            span_columns!(),
            " FROM provenance.spans WHERE span = ANY($1::bytea[])"
        );
        let rows = sqlx::query(query).bind(keys).fetch_all(&self.pool).await?;
        let mut found = std::collections::HashMap::with_capacity(rows.len());
        for row in &rows {
            let record = span_from(row)?;
            found.insert(record.span.id, record);
        }
        Ok(ids.iter().filter_map(|id| found.get(id).cloned()).collect())
    }

    async fn span(&self, id: SpanId) -> Result<Option<SpanRecord>, ProvenanceStoreError> {
        let query = concat!(
            "SELECT ",
            span_columns!(),
            " FROM provenance.spans WHERE span = $1"
        );
        let row = sqlx::query(query)
            .bind(id_bytes(id))
            .fetch_optional(&self.pool)
            .await?;
        row.map(|row| span_from(&row)).transpose()
    }

    async fn exchange_spans(
        &self,
        exchange: ExchangeId,
    ) -> Result<Vec<SpanRecord>, ProvenanceStoreError> {
        let query = concat!(
            "SELECT ",
            span_columns!(),
            " FROM provenance.spans WHERE exchange = $1 ORDER BY ordinal"
        );
        let rows = sqlx::query(query)
            .bind(id_bytes(exchange))
            .fetch_all(&self.pool)
            .await?;
        rows.iter().map(span_from).collect()
    }

    async fn exchange_matches(
        &self,
        exchange: ExchangeId,
    ) -> Result<Vec<StoredMatch>, ProvenanceStoreError> {
        let query = concat!(
            "SELECT ",
            match_columns!(),
            " FROM provenance.matches WHERE reader_exchange = $1 ORDER BY ordinal"
        );
        let rows = sqlx::query(query)
            .bind(id_bytes(exchange))
            .fetch_all(&self.pool)
            .await?;
        rows.iter().map(match_from).collect()
    }

    async fn matches_in_message(
        &self,
        message: MessageHash,
    ) -> Result<Vec<StoredMatch>, ProvenanceStoreError> {
        let query = concat!(
            "SELECT ",
            match_columns!(),
            " FROM provenance.matches WHERE read_message = $1 \
             ORDER BY read_part, read_start, read_end, id"
        );
        let rows = sqlx::query(query)
            .bind(hash_bytes(message))
            .fetch_all(&self.pool)
            .await?;
        rows.iter().map(match_from).collect()
    }

    async fn matches_of_span(&self, span: SpanId) -> Result<Vec<StoredMatch>, ProvenanceStoreError> {
        let query = concat!(
            "SELECT ",
            match_columns!(),
            " FROM provenance.matches WHERE origin = $1 ORDER BY at, id"
        );
        let rows = sqlx::query(query)
            .bind(id_bytes(span))
            .fetch_all(&self.pool)
            .await?;
        rows.iter().map(match_from).collect()
    }

    async fn message_scans(
        &self,
        message: MessageHash,
    ) -> Result<Vec<MessageScan>, ProvenanceStoreError> {
        let rows = sqlx::query(
            "SELECT s.exchange, s.scanned_as, e.status, e.status_at, e.failure_kind, \
             e.failure_message, e.failure_reason \
             FROM provenance.scanned_messages s \
             JOIN provenance.exchanges e ON e.exchange = s.exchange \
             WHERE s.message = $1 ORDER BY s.exchange, s.scanned_as",
        )
        .bind(hash_bytes(message))
        .fetch_all(&self.pool)
        .await?;
        let mut scans = rows
            .iter()
            .map(|row| {
                let exchange: Vec<u8> = row.try_get("exchange")?;
                let scanned_as: String = row.try_get("scanned_as")?;
                Ok(MessageScan {
                    message,
                    exchange: id_from(&exchange)?,
                    scanned_as: scanned_as_from(&scanned_as)?,
                    status: status_from(row)?,
                })
            })
            .collect::<Result<Vec<_>, ProvenanceStoreError>>()?;
        scans.sort_by_key(|scan| (scan.exchange, scan.scanned_as));
        Ok(scans)
    }

    async fn index_watermark(&self) -> Result<u64, ProvenanceStoreError> {
        let max: Option<i64> = sqlx::query_scalar("SELECT max(index_seq) FROM provenance.spans")
            .fetch_one(&self.pool)
            .await?;
        Ok(max.and_then(|max| u64::try_from(max).ok()).unwrap_or(0))
    }

    async fn commit_scan(&mut self, commit: ScanCommit) -> Result<Committed, ProvenanceStoreError> {
        let mut tx = self.pool.begin().await?;
        match locked_status(&mut tx, commit.exchange).await? {
            None => {
                return Err(ProvenanceStoreError::UnknownExchange {
                    exchange: commit.exchange,
                });
            }
            Some(ScanStatus::Pending) => {}
            Some(_) => return Ok(Committed::AlreadyScanned),
        }
        for (ordinal, span) in commit.spans.iter().enumerate() {
            insert_span(&mut tx, span, ordinal).await?;
        }
        for stored in &commit.matches {
            match advance_span(
                &mut tx,
                stored.content.origin(),
                SpanEvent::Hit { at: commit.at },
                false,
            )
            .await
            {
                Ok(_) => {}
                Err(ProvenanceStoreError::Transition(refused)) => {
                    tracing::debug!(span = ?stored.content.origin(), refused = ?refused, "hit not recorded on the origin span");
                }
                Err(error) => return Err(error),
            }
            insert_match(&mut tx, stored).await?;
        }
        for (message, scanned_as) in &commit.messages {
            sqlx::query(
                "INSERT INTO provenance.scanned_messages (message, exchange, scanned_as) \
                 VALUES ($1, $2, $3) ON CONFLICT DO NOTHING",
            )
            .bind(hash_bytes(*message))
            .bind(id_bytes(commit.exchange))
            .bind(scanned_as_text(*scanned_as))
            .execute(&mut *tx)
            .await?;
        }
        sqlx::query(
            "UPDATE provenance.exchanges SET status = 'scanned', status_at = $2 \
             WHERE exchange = $1",
        )
        .bind(id_bytes(commit.exchange))
        .bind(time_i64(commit.at)?)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(Committed::Written)
    }

    async fn mark_indexed(
        &mut self,
        exchange: ExchangeId,
        at: Timestamp,
    ) -> Result<(), ProvenanceStoreError> {
        let mut tx = self.pool.begin().await?;
        match locked_status(&mut tx, exchange).await? {
            None => return Err(ProvenanceStoreError::UnknownExchange { exchange }),
            Some(ScanStatus::Scanned { .. }) => {}
            Some(_) => return Ok(()),
        }
        let originated: Vec<Vec<u8>> = sqlx::query_scalar(
            "SELECT span FROM provenance.spans WHERE exchange = $1 AND state = 'originated' \
             ORDER BY ordinal",
        )
        .bind(id_bytes(exchange))
        .fetch_all(&mut *tx)
        .await?;
        for span in originated {
            advance_span(&mut tx, id_from(&span)?, SpanEvent::Index { at }, true).await?;
        }
        sqlx::query(
            "UPDATE provenance.exchanges SET status = 'indexed', status_at = $2 \
             WHERE exchange = $1",
        )
        .bind(id_bytes(exchange))
        .bind(time_i64(at)?)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(())
    }

    async fn mark_failed(
        &mut self,
        exchange: ExchangeId,
        at: Timestamp,
        failure: ScanFailure,
    ) -> Result<(), ProvenanceStoreError> {
        let (kind, message, reason) = match failure {
            ScanFailure::BodyMissing(message) => ("body_missing", Some(hash_bytes(message)), None),
            ScanFailure::BodyUndecodable(message) => {
                ("body_undecodable", Some(hash_bytes(message)), None)
            }
            ScanFailure::Inconsistent { reason } => ("inconsistent", None, Some(reason)),
        };
        let mut tx = self.pool.begin().await?;
        match locked_status(&mut tx, exchange).await? {
            None => return Err(ProvenanceStoreError::UnknownExchange { exchange }),
            Some(ScanStatus::Pending) => {}
            Some(_) => return Ok(()),
        }
        sqlx::query(
            "UPDATE provenance.exchanges SET status = 'failed', status_at = $2, \
             failure_kind = $3, failure_message = $4, failure_reason = $5 WHERE exchange = $1",
        )
        .bind(id_bytes(exchange))
        .bind(time_i64(at)?)
        .bind(kind)
        .bind(message)
        .bind(reason)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(())
    }

    async fn expiring(
        &self,
        now: Timestamp,
        retention_micros: u64,
        limit: usize,
    ) -> Result<Vec<SpanId>, ProvenanceStoreError> {
        let rows: Vec<Vec<u8>> = sqlx::query_scalar(
            "SELECT span FROM provenance.spans \
             WHERE state IN ('indexed', 'propagated') AND indexed_at < $1 \
             ORDER BY indexed_at, span LIMIT $2",
        )
        .bind(horizon(now, retention_micros))
        .bind(i64::try_from(limit).unwrap_or(i64::MAX))
        .fetch_all(&self.pool)
        .await?;
        rows.iter()
            .map(|span| id_from(span).map_err(ProvenanceStoreError::from))
            .collect()
    }

    async fn expire(&mut self, spans: &[SpanId], at: Timestamp) -> Result<(), ProvenanceStoreError> {
        let mut tx = self.pool.begin().await?;
        for span in spans {
            match advance_span(&mut tx, *span, SpanEvent::Expire { at }, false).await {
                Ok(_) => {}
                Err(ProvenanceStoreError::Transition(refused)) => {
                    tracing::debug!(span = ?span, refused = ?refused, "span not expired");
                }
                Err(error) => return Err(error),
            }
        }
        tx.commit().await?;
        Ok(())
    }

    async fn prune(&mut self, before: Timestamp) -> Result<(), ProvenanceStoreError> {
        sqlx::query(
            "DELETE FROM provenance.exchange_requests r USING provenance.exchanges e \
             WHERE r.exchange = e.exchange AND e.started_at < $1",
        )
        .bind(time_i64(before)?)
        .execute(&self.pool)
        .await?;
        Ok(())
    }
}
