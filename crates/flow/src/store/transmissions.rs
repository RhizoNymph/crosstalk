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

use crosstalk_spec::aggregates::quality::DetectionQuality;
use crosstalk_spec::derived::flow::transmission::{Route, Transmission, TransmissionState};
use crosstalk_spec::derived::flow::verdict::{
    InvalidVerdictRecord, TransmissionVerdict, Verdict, VerdictLog, VerdictRecorded,
    VerdictRevision,
};
use crosstalk_spec::events::BusEvent;
use crosstalk_spec::events::changed::Changed;
use crosstalk_spec::events::detect::DetectEvent;
use crosstalk_spec::ids::{OperatorId, TransmissionId};
use crosstalk_spec::interfaces::l3_reconstruction::AgentDirectory;
use crosstalk_spec::interfaces::l5_flow::transmissions::{
    TransmissionStore, TransmissionStoreError,
};
use crosstalk_spec::interfaces::l5_flow::verdicts::{TransmissionVerdicts, VerdictError};
use crosstalk_spec::support::{TimeWindow, Timestamp};
use crosstalk_store::{SerializableRetry, TxError, retry_serializable};
use sqlx::{PgConnection, PgPool};

use super::codec::{from_json, id_text, json, micros};
use super::error::{Fault, failed, finished};
use super::outbox::{EventSink, Relay, stage};

/// The transmission store on Postgres. Clones share the pool.
#[derive(Clone)]
pub struct PgTransmissionStore<D> {
    pool: PgPool,
    retry: SerializableRetry,
    relay: Relay,
    agents: D,
}

impl<D> std::fmt::Debug for PgTransmissionStore<D> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PgTransmissionStore")
            .field("retry", &self.retry)
            .finish_non_exhaustive()
    }
}

impl<D> PgTransmissionStore<D> {
    /// The store on `pool` (whose flow migrations have run), resolving
    /// agents through `agents` and relaying its events to `sink`.
    pub fn new(pool: PgPool, agents: D, sink: EventSink) -> Self {
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
    pub fn relay(&self) -> &Relay {
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

impl<D: Send + Sync> TransmissionStore for PgTransmissionStore<D> {
    async fn save(&mut self, transmission: Transmission) -> Result<(), TransmissionStoreError> {
        let (route, channel) = route_columns(&transmission.route);
        sqlx::query(
            "INSERT INTO flow.transmissions (id, state, route, channel_id, opened_at, transmission) \
             VALUES ($1, $2, $3, $4, $5, $6) \
             ON CONFLICT (id) DO UPDATE SET state = EXCLUDED.state, route = EXCLUDED.route, \
                 channel_id = EXCLUDED.channel_id, opened_at = EXCLUDED.opened_at, \
                 transmission = EXCLUDED.transmission",
        )
        .bind(id_text(transmission.id))
        .bind(state_column(&transmission.state))
        .bind(route)
        .bind(channel)
        .bind(micros("transmissions.opened_at", transmission.opened_at).map_err(failed)?)
        .bind(json("transmission", &transmission).map_err(failed)?)
        .execute(&self.pool)
        .await
        .map_err(failed)?;
        Ok(())
    }

    async fn transmission(
        &self,
        id: TransmissionId,
    ) -> Result<Option<Transmission>, TransmissionStoreError> {
        let mut conn = self.pool.acquire().await.map_err(failed)?;
        stored(&mut conn, id, false).await.map_err(failed)
    }
}

impl<D: AgentDirectory + Send + Sync> TransmissionVerdicts for PgTransmissionStore<D> {
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
