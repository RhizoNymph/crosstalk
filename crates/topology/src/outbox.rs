//! The store's commit path to the bus: a transactional outbox and its
//! relay.
//!
//! ```text
//! store write txn ── INSERT topology.outbox (event | traffic window) ──▶ COMMIT
//!        │ after commit: Wake::poke (capacity-1 channel: wake-ups coalesce)
//!        ▼
//! OutboxRelay::run ── drain, batch by batch, under the drain lock:
//!   txn A (stamp):   unstamped traffic rows ─▶ one row holding their hull
//!                    every unstamped row ─▶ envelope_id, at (OutboxIds: clock + ULID generator)
//!                    COMMIT                                  ── the ids are now fixed
//!   txn B (publish): stamped rows in seq order ─▶ Announce(Envelope { id, at, event }) ─▶ bus
//!                    DELETE the rows published               ── COMMIT
//! ```
//!
//! An event is in the outbox exactly when the change that decided it
//! committed, so the relay publishes it only after queries can see the
//! change. **Stable ids** (`topology.outbox.stable-envelope-id`): every row
//! gets its envelope id and time once, in a committed transaction before its
//! first publish. A drain that fails or is interrupted after publishing and
//! before deleting leaves the rows stamped, and the next drain republishes
//! them under the same ids, which a durable bus deduplicates
//! (`transport.publish.idempotent-on-id`). So each staged event lands in
//! the bus log once.
//!
//! One drain runs at a time across processes: both transactions take a
//! transaction-scoped advisory lock, and the publish transaction holds it
//! while it publishes. Rows are stamped and published in seq order, so
//! events reach the bus in commit order with increasing ids, and every
//! `WatermarkAdvanced` is later than the one before it.
//!
//! **Traffic.** Every committed change to an edge or access bucket writes
//! a traffic row naming the bucket's window. The stamp transaction
//! coalesces a batch's unstamped traffic rows into the last of them, whose
//! window becomes their hull, and stamps it: one notification, under one
//! id, covers every change of the batch. It is published in its row's seq
//! place (after every event committed before the batch's last traffic row).
//! A stamped traffic row is never merged again, so a republish carries the
//! same window under the same id.

use std::sync::Arc;
use std::time::Duration;

use crosstalk_spec::events::{BusEvent, Envelope};
use crosstalk_spec::ids::{EventId, SeededRandom, UlidGenerator};
use crosstalk_spec::interfaces::l2_transport::{BusError, EventBus};
use crosstalk_spec::support::{Clock, TimeWindow, Timestamp};
use sqlx::{PgConnection, PgPool, Postgres, Row, Transaction};
use tokio::sync::mpsc;

use crate::codec;
use crate::store::DbError;

/// The advisory lock key one drain holds at a time (`topology` in ASCII).
const DRAIN_LOCK: i64 = 0x746f_706f_6c6f_6779;

/// How many outbox rows one drain transaction reads.
const BATCH: i64 = 512;

/// Publishes one envelope whose id and time the caller fixed.
///
/// The relay passes the row's stamped envelope; the consumer passes an
/// envelope whose id it derived from the delivery
/// (`transport.consumer.derived-envelope-ids`). Neither mints at publish
/// time, so a retry publishes the same envelope.
pub trait Announce: Send + Sync {
    fn announce(&self, envelope: Envelope) -> impl Future<Output = Result<(), BusError>> + Send;
}

/// [`Announce`] over the spec's `EventBus`.
pub struct BusAnnouncer<E> {
    bus: E,
}

impl<E> BusAnnouncer<E> {
    pub fn new(bus: E) -> Self {
        Self { bus }
    }

    pub fn bus(&self) -> &E {
        &self.bus
    }
}

impl<E: EventBus + Send + Sync> Announce for BusAnnouncer<E> {
    fn announce(&self, envelope: Envelope) -> impl Future<Output = Result<(), BusError>> + Send {
        self.bus.publish(envelope)
    }
}

impl<A: Announce> Announce for Arc<A> {
    fn announce(&self, envelope: Envelope) -> impl Future<Output = Result<(), BusError>> + Send {
        A::announce(self, envelope)
    }
}

/// Where the relay's stamps come from: the injected clock's reading and a
/// ULID generator at it. Owned by one relay, so the ids it stamps increase
/// in seq order.
pub struct OutboxIds {
    clock: Arc<dyn Clock>,
    ids: UlidGenerator<SeededRandom>,
}

impl OutboxIds {
    /// Stamps at `clock`'s readings. In a deployment, `entropy` comes from
    /// the OS (`SeededRandom::from_entropy`), so ids minted after a restart
    /// never repeat persisted ones (`surface.ids.unique-across-restart`);
    /// tests pass a fixed seed.
    pub fn new(clock: Arc<dyn Clock>, entropy: SeededRandom) -> Self {
        Self {
            ids: UlidGenerator::new(Arc::clone(&clock), entropy),
            clock,
        }
    }

    fn stamp(&mut self) -> Result<(EventId, Timestamp), StampError> {
        let at = self.clock.now();
        let id = self.ids.mint_at(at).map_err(|_| StampError::IdsExhausted)?;
        Ok((id, at))
    }
}

impl std::fmt::Debug for OutboxIds {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OutboxIds").finish_non_exhaustive()
    }
}

/// Why a row could not be stamped.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum StampError {
    #[error("no event id left to mint")]
    IdsExhausted,
}

/// **HOOK (follow-mode spec batch): `Changed::Traffic`.** The notification
/// a coalesced traffic window becomes. The follow-mode batch adds
/// `Changed::Traffic(TimeWindow)`; once it lands, this returns
/// `Some(BusEvent::Changed(Changed::Traffic(window)))`. Until then traffic
/// rows are coalesced, stamped and deleted, and nothing is published for
/// them.
pub fn traffic_notification(window: TimeWindow) -> Option<BusEvent> {
    let _ = window;
    None
}

/// Why a drain stopped. Rows it did not publish stay in the outbox, with
/// whatever stamps they got.
#[derive(Debug, thiserror::Error)]
pub enum RelayError {
    #[error("outbox: {0}")]
    Store(#[from] DbError),
    #[error("outbox row {seq}: {source}")]
    Stamp {
        seq: i64,
        #[source]
        source: StampError,
    },
    #[error("outbox row {seq}: publish failed: {error:?}")]
    Announce { seq: i64, error: BusError },
}

impl From<sqlx::Error> for RelayError {
    fn from(error: sqlx::Error) -> Self {
        Self::Store(DbError::from(error))
    }
}

/// Write `event` to the outbox in the caller's transaction, unstamped.
pub(crate) async fn enqueue(conn: &mut PgConnection, event: &BusEvent) -> Result<(), DbError> {
    let text = serde_json::to_string(event)
        .map_err(|error| DbError::inconsistent(format!("event does not encode: {error}")))?;
    sqlx::query("INSERT INTO topology.outbox (event) VALUES ($1)")
        .bind(text)
        .execute(conn)
        .await?;
    Ok(())
}

/// The store's side of the relay's wake-up: a capacity-1 channel, so a
/// burst of commits wakes the relay once.
#[derive(Debug, Clone)]
pub struct Wake(mpsc::Sender<()>);

impl Wake {
    /// Tell the relay something committed. Never blocks: a pending wake-up
    /// already covers this commit, and with no relay running the rows wait
    /// in the outbox.
    pub fn poke(&self) {
        let _ = self.0.try_send(());
    }
}

/// Publishes what the store commits to its outbox.
#[derive(Debug)]
pub struct OutboxRelay {
    pool: PgPool,
    wake: mpsc::Receiver<()>,
}

/// What one outbox row carries, decoded.
enum Payload {
    Event(Box<BusEvent>),
    Traffic(TimeWindow),
}

impl OutboxRelay {
    pub(crate) fn new(pool: PgPool) -> (Wake, Self) {
        let (sender, wake) = mpsc::channel(1);
        (Wake(sender), Self { pool, wake })
    }

    /// Drain after every wake-up and at least every `poll` (rows another
    /// process committed, or rows a failed drain left), until every store
    /// sharing this relay's wake-up is dropped; then drain once more and
    /// return. Call it at start before the consumer subscribes: the first
    /// drain republishes, under their stamped ids, rows a previous process
    /// left.
    pub async fn run<A: Announce>(mut self, announcer: A, mut ids: OutboxIds, poll: Duration) {
        tracing::info!(poll_ms = poll.as_millis(), "topology outbox relay started");
        loop {
            let open = tokio::select! {
                woken = self.wake.recv() => woken.is_some(),
                () = tokio::time::sleep(poll) => true,
            };
            if let Err(error) = drain(&self.pool, &mut ids, &announcer).await {
                tracing::warn!(error = %error, "topology outbox drain failed; retrying on the next wake-up");
            }
            if !open {
                break;
            }
        }
        tracing::info!("topology outbox relay stopped");
    }
}

/// Stamp, publish and delete every outbox row, oldest first, batch by
/// batch. Returns how many rows it removed (traffic rows merged into
/// another included).
pub async fn drain<A: Announce>(
    pool: &PgPool,
    ids: &mut OutboxIds,
    announcer: &A,
) -> Result<u64, RelayError> {
    let mut total = 0;
    loop {
        let merged = stamp_batch(pool, ids).await?;
        let published = publish_batch(pool, announcer).await?;
        total += merged + published;
        if merged + published == 0 {
            return Ok(total);
        }
    }
}

/// Take the drain lock in `tx`.
async fn lock(tx: &mut Transaction<'_, Postgres>) -> Result<(), sqlx::Error> {
    sqlx::query("SELECT pg_advisory_xact_lock($1)")
        .bind(DRAIN_LOCK)
        .execute(&mut **tx)
        .await
        .map(|_| ())
}

/// Txn A: stamp the unstamped rows among the next batch, coalescing their
/// traffic rows first. Returns how many traffic rows it merged away.
pub(crate) async fn stamp_batch(pool: &PgPool, ids: &mut OutboxIds) -> Result<u64, RelayError> {
    let mut tx = pool.begin().await?;
    lock(&mut tx).await?;
    let rows = sqlx::query(
        "SELECT seq, event IS NULL AS traffic, traffic_start, traffic_end FROM topology.outbox \
         WHERE envelope_id IS NULL ORDER BY seq LIMIT $1",
    )
    .bind(BATCH)
    .fetch_all(&mut *tx)
    .await?;
    if rows.is_empty() {
        return Ok(0);
    }
    let mut events = Vec::new();
    let mut traffic: Vec<(i64, TimeWindow)> = Vec::new();
    for row in &rows {
        let seq: i64 = row.try_get("seq")?;
        if row.try_get::<bool, _>("traffic")? {
            let start: Option<i64> = row.try_get("traffic_start")?;
            let end: Option<i64> = row.try_get("traffic_end")?;
            traffic.push((seq, window(start, end)?));
        } else {
            events.push(seq);
        }
    }
    let mut merged = 0;
    let mut stamps = events;
    if let Some(&(carrier, last)) = traffic.last() {
        let hull = traffic
            .iter()
            .fold(last, |held, &(_, window)| cover(held, window));
        let others: Vec<i64> = traffic
            .iter()
            .map(|&(seq, _)| seq)
            .filter(|&seq| seq != carrier)
            .collect();
        merged = sqlx::query("DELETE FROM topology.outbox WHERE seq = ANY($1)")
            .bind(&others)
            .execute(&mut *tx)
            .await?
            .rows_affected();
        sqlx::query(
            "UPDATE topology.outbox SET traffic_start = $2, traffic_end = $3 WHERE seq = $1",
        )
        .bind(carrier)
        .bind(codec::time(hull.start()).map_err(DbError::from)?)
        .bind(codec::time(hull.end()).map_err(DbError::from)?)
        .execute(&mut *tx)
        .await?;
        stamps.push(carrier);
        stamps.sort_unstable();
    }
    for seq in stamps {
        let (id, at) = ids
            .stamp()
            .map_err(|source| RelayError::Stamp { seq, source })?;
        sqlx::query("UPDATE topology.outbox SET envelope_id = $2, at = $3 WHERE seq = $1")
            .bind(seq)
            .bind(codec::event(id))
            .bind(codec::time(at).map_err(DbError::from)?)
            .execute(&mut *tx)
            .await?;
    }
    tx.commit().await?;
    tracing::debug!(rows = rows.len(), merged, "topology outbox rows stamped");
    Ok(merged)
}

/// Txn B: publish the next batch of stamped rows in seq order under their
/// stamps, and delete the ones published. Returns how many it deleted.
pub(crate) async fn publish_batch<A: Announce>(
    pool: &PgPool,
    announcer: &A,
) -> Result<u64, RelayError> {
    let mut tx = pool.begin().await?;
    lock(&mut tx).await?;
    let rows = sqlx::query(
        "SELECT seq, envelope_id, at, event, traffic_start, traffic_end FROM topology.outbox \
         WHERE envelope_id IS NOT NULL ORDER BY seq LIMIT $1",
    )
    .bind(BATCH)
    .fetch_all(&mut *tx)
    .await?;
    let mut published_through = None;
    let mut failure = None;
    for row in &rows {
        let seq: i64 = row.try_get("seq")?;
        let id: String = row.try_get("envelope_id")?;
        let at: i64 = row.try_get("at")?;
        let id = codec::stored_event(&id).map_err(DbError::from)?;
        let at = codec::timestamp("envelope time", at).map_err(DbError::from)?;
        let event = match payload(
            row.try_get("event")?,
            row.try_get("traffic_start")?,
            row.try_get("traffic_end")?,
        )? {
            Payload::Event(event) => Some(*event),
            Payload::Traffic(window) => traffic_notification(window),
        };
        if let Some(event) = event
            && let Err(source) = announcer.announce(Envelope { id, at, event }).await
        {
            failure = Some(RelayError::Announce { seq, error: source });
            break;
        }
        published_through = Some(seq);
    }
    let removed = match published_through {
        Some(through) => {
            sqlx::query("DELETE FROM topology.outbox WHERE envelope_id IS NOT NULL AND seq <= $1")
                .bind(through)
                .execute(&mut *tx)
                .await?
                .rows_affected()
        }
        None => 0,
    };
    tx.commit().await?;
    match failure {
        Some(error) => Err(error),
        None => Ok(removed),
    }
}

fn window(start: Option<i64>, end: Option<i64>) -> Result<TimeWindow, DbError> {
    match (start, end) {
        (Some(start), Some(end)) => {
            let start = codec::timestamp("traffic start", start)?;
            let end = codec::timestamp("traffic end", end)?;
            TimeWindow::new(start, end)
                .map_err(|_| DbError::inconsistent("outbox traffic window is empty"))
        }
        _ => Err(DbError::inconsistent(
            "outbox row is neither an event nor traffic",
        )),
    }
}

fn payload(
    event: Option<String>,
    start: Option<i64>,
    end: Option<i64>,
) -> Result<Payload, DbError> {
    match (event, start, end) {
        (Some(text), None, None) => serde_json::from_str(&text)
            .map(|event| Payload::Event(Box::new(event)))
            .map_err(|error| {
                DbError::inconsistent(format!("outbox event does not decode: {error}"))
            }),
        (None, start, end) => window(start, end).map(Payload::Traffic),
        _ => Err(DbError::inconsistent(
            "outbox row is neither an event nor traffic",
        )),
    }
}

/// The smallest window covering both.
fn cover(a: TimeWindow, b: TimeWindow) -> TimeWindow {
    let start: Timestamp = a.start().min(b.start());
    let end: Timestamp = a.end().max(b.end());
    // Both inputs are non-empty, so start < end.
    TimeWindow::new(start, end).unwrap_or(a)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn window(start: u64, end: u64) -> TimeWindow {
        TimeWindow::new(Timestamp::from_micros(start), Timestamp::from_micros(end))
            .expect("non-empty")
    }

    #[test]
    fn traffic_windows_coalesce_to_their_hull() {
        assert_eq!(cover(window(10, 20), window(40, 50)), window(10, 50));
        assert_eq!(cover(window(40, 50), window(10, 20)), window(10, 50));
        assert_eq!(cover(window(10, 50), window(20, 30)), window(10, 50));
    }

    #[test]
    fn traffic_hook_publishes_nothing_until_the_spec_has_the_variant() {
        assert_eq!(traffic_notification(window(0, 10)), None);
    }
}
