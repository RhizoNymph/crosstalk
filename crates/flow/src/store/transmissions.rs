//! `PgTransmissionStore`: `TransmissionStore` and `TransmissionVerdicts`
//! over `flow.transmissions` and the verdict log beside each transmission
//! (`flow.verdicts`).
//!
//! - `save` replaces the stored transmission and never touches its log
//!   (`flow.transmission-store.save-keeps-verdicts`).
//! - `set` runs in one serializable transaction that locks the
//!   transmission's row (`FOR UPDATE`), so appends to one log are
//!   serialised and the judgeable check reads the state the append is made
//!   against; it never writes the transmission (`flow.verdict.state-untouched`),
//!   and on `Appended` it stages `VerdictSet` and `Changed::Verdict` in the
//!   outbox in the same transaction (`flow.verdict.set-event-once`).
//! - `quality` tallies every stored transmission opened in the window with
//!   its current verdict, read in one snapshot, agents resolved through the
//!   store's `AgentDirectory` at the read.
//!
//! The `state`, `route`, `channel_id` and `opened_at` columns are derived
//! from the saved transmission and index the transmission list.
//!
//! `flow.transmission_matches` keys every content match a transmission
//! holds (`MatchKey`), rewritten in `save`'s transaction, and answers
//! `holding`.

use crosstalk_spec::aggregates::quality::DetectionQuality;
use crosstalk_spec::derived::flow::transmission::{Route, Transmission, TransmissionState};
use crosstalk_spec::derived::flow::verdict::{
    InvalidVerdictRecord, TransmissionVerdict, Verdict, VerdictLog, VerdictRecorded,
    VerdictRevision,
};
use crosstalk_spec::events::BusEvent;
use crosstalk_spec::events::changed::Changed;
use crosstalk_spec::events::detect::DetectEvent;
use std::collections::{BTreeMap, BTreeSet};

use crosstalk_spec::ids::{ExchangeId, OperatorId, SpanId, TransmissionId};
use crosstalk_spec::interfaces::l3_reconstruction::AgentDirectory;
use crosstalk_spec::interfaces::l5_flow::transmissions::{
    MatchKey, TransmissionStore, TransmissionStoreError,
};
use crosstalk_spec::interfaces::l5_flow::verdicts::{TransmissionVerdicts, VerdictError};
use crosstalk_spec::support::{TimeWindow, Timestamp};
use crosstalk_store::{SerializableRetry, TxError, retry_serializable};
use sqlx::{PgConnection, PgPool};

use super::codec::{from_json, id_text, json, micros};
use super::error::{Fault, failed, finished};
use super::outbox::{EventSink, Relay, stage};

/// The transmission store on Postgres. Clones share the pool.
pub struct PgTransmissionStore<D, S> {
    pool: PgPool,
    retry: SerializableRetry,
    relay: Relay<S>,
    agents: D,
}

impl<D: Clone, S> Clone for PgTransmissionStore<D, S> {
    fn clone(&self) -> Self {
        Self {
            pool: self.pool.clone(),
            retry: self.retry,
            relay: self.relay.clone(),
            agents: self.agents.clone(),
        }
    }
}

impl<D, S> std::fmt::Debug for PgTransmissionStore<D, S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PgTransmissionStore")
            .field("retry", &self.retry)
            .finish_non_exhaustive()
    }
}

impl<D, S: EventSink> PgTransmissionStore<D, S> {
    /// The store on `pool` (whose flow migrations have run), resolving
    /// agents through `agents` and relaying its events to `sink`.
    pub fn new(pool: PgPool, agents: D, sink: S) -> Self {
        Self {
            relay: Relay::new(pool.clone(), sink),
            pool,
            retry: SerializableRetry::default(),
            agents,
        }
    }

    /// The same store with another serializable retry policy.
    pub fn with_retry(mut self, retry: SerializableRetry) -> Self {
        self.retry = retry;
        self
    }

    /// The outbox relay.
    pub fn relay(&self) -> &Relay<S> {
        &self.relay
    }
}

/// The `state` column of a transmission state.
pub(crate) fn state_column(state: &TransmissionState) -> &'static str {
    match state {
        TransmissionState::Detected => "detected",
        TransmissionState::AwaitingContent { .. } => "awaiting_content",
        TransmissionState::Suspected { .. } => "suspected",
        TransmissionState::Confirmed(_) => "confirmed",
        TransmissionState::Classified { .. } => "classified",
        TransmissionState::Aggregated { .. } => "aggregated",
        TransmissionState::Discarded { .. } => "discarded",
    }
}

/// The `route` and `channel_id` columns of a route.
fn route_columns(route: &Route) -> (&'static str, Option<String>) {
    match route {
        Route::Channel(channel) => ("channel", Some(id_text(*channel))),
        Route::Delegation(_) => ("delegation", None),
        Route::Direct(_) => ("direct", None),
        Route::Unobserved => ("unobserved", None),
    }
}

fn verdict_column(verdict: Option<Verdict>) -> Option<&'static str> {
    verdict.map(|verdict| match verdict {
        Verdict::Genuine => "genuine",
        Verdict::FalseDetection => "false_detection",
    })
}

fn verdict_of(column: Option<&str>) -> Result<Option<Verdict>, Fault> {
    match column {
        None => Ok(None),
        Some("genuine") => Ok(Some(Verdict::Genuine)),
        Some("false_detection") => Ok(Some(Verdict::FalseDetection)),
        Some(other) => Err(Fault::corrupt("verdicts.verdict", other)),
    }
}

/// The stored transmission, locked for the transaction when `lock`.
async fn stored(
    conn: &mut PgConnection,
    id: TransmissionId,
    lock: bool,
) -> Result<Option<Transmission>, Fault> {
    let sql = if lock {
        "SELECT transmission FROM flow.transmissions WHERE id = $1 FOR UPDATE"
    } else {
        "SELECT transmission FROM flow.transmissions WHERE id = $1"
    };
    let row: Option<(String,)> = sqlx::query_as(sql)
        .bind(id_text(id))
        .fetch_optional(&mut *conn)
        .await?;
    row.map(|(text,)| from_json("transmissions.transmission", &text))
        .transpose()
        .map_err(Fault::from)
}

/// The transmission's verdict log, oldest record first.
async fn log_of(conn: &mut PgConnection, id: TransmissionId) -> Result<VerdictLog, Fault> {
    let rows: Vec<(i32, String)> = sqlx::query_as(
        "SELECT revision, record FROM flow.verdicts WHERE transmission_id = $1 ORDER BY revision",
    )
    .bind(id_text(id))
    .fetch_all(&mut *conn)
    .await?;
    log_from(id, rows)
}

/// A verdict log from its stored (revision, record) rows, oldest first.
fn log_from(id: TransmissionId, rows: Vec<(i32, String)>) -> Result<VerdictLog, Fault> {
    let mut records = Vec::with_capacity(rows.len());
    for (revision, record) in rows {
        let revision = u32::try_from(revision)
            .ok()
            .and_then(std::num::NonZeroU32::new)
            .map(VerdictRevision::new)
            .ok_or_else(|| Fault::corrupt("verdicts.revision", revision))?;
        let record: TransmissionVerdict = from_json("verdicts.record", &record)?;
        records.push((revision, record));
    }
    VerdictLog::from_records(id, records).map_err(|error| Fault::corrupt("verdict log", error))
}

/// `TransmissionVerdicts::set`, in its transaction.
async fn set_verdict(
    conn: &mut PgConnection,
    id: TransmissionId,
    verdict: Option<Verdict>,
    by: OperatorId,
    at: Timestamp,
    note: Option<String>,
) -> Result<(VerdictRecorded, Vec<BusEvent>), TxError<VerdictError>> {
    let Some(transmission) = stored(conn, id, true).await? else {
        return Err(TxError::Abort(VerdictError::UnknownTransmission(id)));
    };
    let record = TransmissionVerdict::new(&transmission, verdict, by, at, note)
        .map_err(|_| TxError::Abort(VerdictError::NotJudgeable(id)))?;
    let mut log = log_of(conn, id).await?;
    let recorded = log.record(record.clone()).map_err(|error| match error {
        InvalidVerdictRecord::OtherTransmission | InvalidVerdictRecord::RevisionsExhausted => {
            TxError::Abort(VerdictError::Store {
                reason: format!("verdict log refused the record: {error:?}"),
            })
        }
    })?;
    let VerdictRecorded::Appended(revision) = recorded else {
        return Ok((recorded, Vec::new()));
    };
    let revision_column = i32::try_from(revision.get().get())
        .map_err(|_| Fault::corrupt("verdicts.revision", revision))?;
    sqlx::query(
        "INSERT INTO flow.verdicts (transmission_id, revision, verdict, record) \
         VALUES ($1, $2, $3, $4)",
    )
    .bind(id_text(id))
    .bind(revision_column)
    .bind(verdict_column(verdict))
    .bind(json("verdict record", &record).map_err(Fault::from)?)
    .execute(&mut *conn)
    .await?;
    let events = vec![
        BusEvent::Detect(DetectEvent::VerdictSet {
            transmission: id,
            verdict,
            revision,
            by,
            at,
        }),
        BusEvent::Changed(Changed::Verdict(id)),
    ];
    stage(conn, &events).await?;
    Ok((recorded, events))
}

impl<D: Send + Sync, S: EventSink> TransmissionStore for PgTransmissionStore<D, S> {
    async fn save(&mut self, transmission: Transmission) -> Result<(), TransmissionStoreError> {
        let (route, channel) = route_columns(&transmission.route);
        let opened = micros("transmissions.opened_at", transmission.opened_at).map_err(failed)?;
        let body = json("transmission", &transmission).map_err(failed)?;
        let keys = match_rows(&transmission).map_err(failed)?;
        let id = id_text(transmission.id);
        let mut tx = self.pool.begin().await.map_err(failed)?;
        sqlx::query(
            "INSERT INTO flow.transmissions (id, state, route, channel_id, opened_at, transmission) \
             VALUES ($1, $2, $3, $4, $5, $6) \
             ON CONFLICT (id) DO UPDATE SET state = EXCLUDED.state, route = EXCLUDED.route, \
                 channel_id = EXCLUDED.channel_id, opened_at = EXCLUDED.opened_at, \
                 transmission = EXCLUDED.transmission",
        )
        .bind(&id)
        .bind(state_column(&transmission.state))
        .bind(route)
        .bind(channel)
        .bind(opened)
        .bind(body)
        .execute(&mut *tx)
        .await
        .map_err(failed)?;
        sqlx::query("DELETE FROM flow.transmission_matches WHERE transmission_id = $1")
            .bind(&id)
            .execute(&mut *tx)
            .await
            .map_err(failed)?;
        if !keys.origin.is_empty() {
            sqlx::query(
                "INSERT INTO flow.transmission_matches \
                     (origin, reader_exchange, message, part, range_start, range_end, transmission_id) \
                 SELECT k.origin, k.reader_exchange, k.message, k.part, k.range_start, k.range_end, $7 \
                 FROM UNNEST($1::text[], $2::text[], $3::text[], $4::int4[], $5::int8[], $6::int8[]) \
                     AS k(origin, reader_exchange, message, part, range_start, range_end) \
                 ON CONFLICT (origin, reader_exchange, message, part, range_start, range_end) \
                 DO UPDATE SET transmission_id = EXCLUDED.transmission_id",
            )
            .bind(&keys.origin)
            .bind(&keys.reader_exchange)
            .bind(&keys.message)
            .bind(&keys.part)
            .bind(&keys.range_start)
            .bind(&keys.range_end)
            .bind(&id)
            .execute(&mut *tx)
            .await
            .map_err(failed)?;
        }
        tx.commit().await.map_err(failed)?;
        Ok(())
    }

    async fn transmission(
        &self,
        id: TransmissionId,
    ) -> Result<Option<Transmission>, TransmissionStoreError> {
        let mut conn = self.pool.acquire().await.map_err(failed)?;
        stored(&mut conn, id, false).await.map_err(failed)
    }

    // TODO(flow-store): list over `flow.transmissions` (opened_at window,
    // state column, channel_id resolved through the supersession table),
    // keyset-paged on id. Live and eval read the memory store today.
    async fn list(
        &self,
        _query: &crosstalk_spec::interfaces::l5_flow::transmissions::TransmissionQuery,
        _page: &crosstalk_spec::paging::PageRequest<crosstalk_spec::paging::TransmissionList>,
    ) -> Result<
        crosstalk_spec::paging::Page<Transmission, crosstalk_spec::paging::TransmissionList>,
        TransmissionStoreError,
    > {
        Err(TransmissionStoreError::Store {
            reason: "TransmissionStore::list is not implemented on Postgres yet".to_owned(),
        })
    }

    async fn holding(
        &self,
        matches: &BTreeSet<MatchKey>,
    ) -> Result<BTreeMap<MatchKey, TransmissionId>, TransmissionStoreError> {
        if matches.is_empty() {
            return Ok(BTreeMap::new());
        }
        let keys = KeyColumns::of(matches.iter()).map_err(failed)?;
        let rows: Vec<(String, String, String, i32, i64, i64, String)> = sqlx::query_as(
            "SELECT m.origin, m.reader_exchange, m.message, m.part, m.range_start, m.range_end, \
                    m.transmission_id \
             FROM UNNEST($1::text[], $2::text[], $3::text[], $4::int4[], $5::int8[], $6::int8[]) \
                 AS k(origin, reader_exchange, message, part, range_start, range_end) \
             JOIN flow.transmission_matches m USING \
                 (origin, reader_exchange, message, part, range_start, range_end)",
        )
        .bind(&keys.origin)
        .bind(&keys.reader_exchange)
        .bind(&keys.message)
        .bind(&keys.part)
        .bind(&keys.range_start)
        .bind(&keys.range_end)
        .fetch_all(&self.pool)
        .await
        .map_err(failed)?;
        let mut held = BTreeMap::new();
        for (origin, reader_exchange, message, part, start, end, transmission) in rows {
            let key =
                key_of(&origin, &reader_exchange, &message, part, start, end).map_err(failed)?;
            let id = TransmissionId::from_ulid_text(&transmission).map_err(|_| {
                failed(Fault::corrupt(
                    "transmission_matches.transmission_id",
                    transmission,
                ))
            })?;
            held.insert(key, id);
        }
        Ok(held)
    }
}

/// `MatchKey`s as the parallel arrays `UNNEST` reads.
#[derive(Debug, Default)]
struct KeyColumns {
    origin: Vec<String>,
    reader_exchange: Vec<String>,
    message: Vec<String>,
    part: Vec<i32>,
    range_start: Vec<i64>,
    range_end: Vec<i64>,
}

impl KeyColumns {
    fn of<'a>(keys: impl Iterator<Item = &'a MatchKey>) -> Result<Self, Fault> {
        let mut columns = Self::default();
        for key in keys {
            columns.origin.push(id_text(key.origin));
            columns.reader_exchange.push(id_text(key.reader_exchange));
            columns
                .message
                .push(key.read_at.part.message.digest().to_hex());
            columns.part.push(i32::from(key.read_at.part.index));
            columns
                .range_start
                .push(i64::from(key.read_at.range.start()));
            columns.range_end.push(i64::from(key.read_at.range.end()));
        }
        Ok(columns)
    }
}

/// The keys of every content match `transmission` holds; none for a state
/// without content.
fn match_rows(transmission: &Transmission) -> Result<KeyColumns, Fault> {
    let keys: BTreeSet<MatchKey> = transmission
        .state
        .confirmed()
        .map(|confirmed| confirmed.content().iter().map(MatchKey::of).collect())
        .unwrap_or_default();
    KeyColumns::of(keys.iter())
}

/// A `MatchKey` read back from its columns.
fn key_of(
    origin: &str,
    reader_exchange: &str,
    message: &str,
    part: i32,
    start: i64,
    end: i64,
) -> Result<MatchKey, Fault> {
    use crosstalk_spec::derived::provenance::span::SpanLocation;
    use crosstalk_spec::ids::MessageHash;
    use crosstalk_spec::observed::message::PartRef;
    use crosstalk_spec::support::{Blake3, ByteRange};

    let origin = SpanId::from_ulid_text(origin)
        .map_err(|_| Fault::corrupt("transmission_matches.origin", origin))?;
    let reader_exchange = ExchangeId::from_ulid_text(reader_exchange)
        .map_err(|_| Fault::corrupt("transmission_matches.reader_exchange", reader_exchange))?;
    let digest = Blake3::from_hex(message)
        .map_err(|_| Fault::corrupt("transmission_matches.message", message))?;
    let index =
        u16::try_from(part).map_err(|_| Fault::corrupt("transmission_matches.part", part))?;
    let start = u32::try_from(start)
        .map_err(|_| Fault::corrupt("transmission_matches.range_start", start))?;
    let end =
        u32::try_from(end).map_err(|_| Fault::corrupt("transmission_matches.range_end", end))?;
    let range = ByteRange::new(start, end)
        .map_err(|_| Fault::corrupt("transmission_matches.range", end))?;
    Ok(MatchKey {
        origin,
        reader_exchange,
        read_at: SpanLocation {
            part: PartRef {
                message: MessageHash::from_digest(digest),
                index,
            },
            range,
        },
    })
}

impl<D: AgentDirectory + Send + Sync, S: EventSink> TransmissionVerdicts
    for PgTransmissionStore<D, S>
{
    async fn set(
        &mut self,
        transmission: TransmissionId,
        verdict: Option<Verdict>,
        by: OperatorId,
        at: Timestamp,
        note: Option<String>,
    ) -> Result<VerdictRecorded, VerdictError> {
        let (recorded, events) = retry_serializable(&self.pool, &self.retry, |conn| {
            let note = note.clone();
            Box::pin(async move { set_verdict(conn, transmission, verdict, by, at, note).await })
        })
        .await
        .map_err(finished)?;
        if !events.is_empty() {
            self.relay.after_commit().await;
        }
        Ok(recorded)
    }

    async fn log(&self, transmission: TransmissionId) -> Result<VerdictLog, VerdictError> {
        // One statement: the transmission's existence and its records.
        let row: Option<(Vec<i32>, Vec<String>)> = sqlx::query_as(
            "SELECT ARRAY(SELECT v.revision FROM flow.verdicts v \
                          WHERE v.transmission_id = t.id ORDER BY v.revision), \
                    ARRAY(SELECT v.record FROM flow.verdicts v \
                          WHERE v.transmission_id = t.id ORDER BY v.revision) \
             FROM flow.transmissions t WHERE t.id = $1",
        )
        .bind(id_text(transmission))
        .fetch_optional(&self.pool)
        .await
        .map_err(failed)?;
        let Some((revisions, records)) = row else {
            return Err(VerdictError::UnknownTransmission(transmission));
        };
        log_from(transmission, revisions.into_iter().zip(records).collect()).map_err(failed)
    }

    async fn quality(&self, window: TimeWindow) -> Result<DetectionQuality, VerdictError> {
        let start = micros("window.start", window.start()).map_err(failed)?;
        let end = micros("window.end", window.end()).map_err(failed)?;
        let rows: Vec<(String, Option<String>)> = sqlx::query_as(
            "SELECT t.transmission, v.verdict FROM flow.transmissions t \
             LEFT JOIN LATERAL (SELECT verdict FROM flow.verdicts \
                                WHERE transmission_id = t.id \
                                ORDER BY revision DESC LIMIT 1) v ON true \
             WHERE t.opened_at >= $1 AND t.opened_at < $2",
        )
        .bind(start)
        .bind(end)
        .fetch_all(&self.pool)
        .await
        .map_err(failed)?;
        let mut judged = Vec::with_capacity(rows.len());
        for (transmission, verdict) in rows {
            let transmission: Transmission =
                from_json("transmissions.transmission", &transmission).map_err(failed)?;
            judged.push((
                transmission,
                verdict_of(verdict.as_deref()).map_err(failed)?,
            ));
        }
        let agents = &self.agents;
        Ok(DetectionQuality::tally(
            window,
            judged
                .iter()
                .map(|(transmission, verdict)| (transmission, *verdict)),
            |agent| agents.canonical(agent),
        ))
    }
}
