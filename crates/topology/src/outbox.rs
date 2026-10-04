//! The store's commit path to the bus: a transactional outbox and its
//! relay.
//!
//! ```text
//! store write txn ── INSERT topology.outbox (event | traffic window) ──▶ COMMIT
//!        │ after commit: Wake::poke (capacity-1 channel: wake-ups coalesce)
//!        ▼
//! OutboxRelay::run ── drain: lock, read rows in seq order, publish, delete ──▶ Announce ──▶ bus
//!                       traffic rows coalesce into one window per batch
//!                       (Changed::Traffic, once the spec has it: HOOK below)
//! ```
//!
//! An event is in the outbox exactly when the change that decided it
//! committed, so the relay publishes it only after queries can see the
//! change, and a crash between commit and publish leaves it for the next
//! drain (delivery is at least once; consumers are idempotent). One drain
//! runs at a time across processes (a transaction-scoped advisory lock), so
//! events reach the bus in commit order and every `WatermarkAdvanced` is
//! later than the one before it.
//!
//! **Traffic.** Every committed change to an edge or access bucket writes
//! a traffic row naming the bucket's window. A drain coalesces every
//! traffic row it reads into one window covering them all (their hull), so
//! one notification covers every committed change of the batch, and it is
//! published after the batch's other events.

use std::sync::Arc;
use std::time::Duration;

use crosstalk_spec::events::{BusEvent, Envelope};
use crosstalk_spec::ids::{EventId, SeededRandom, UlidGenerator};
use crosstalk_spec::interfaces::l2_transport::{BusError, EventBus};
use crosstalk_spec::support::{Clock, TimeWindow, Timestamp};
use sqlx::{PgConnection, PgPool, Row};
use tokio::sync::{Mutex, mpsc};

use crate::codec;
use crate::store::DbError;

/// The advisory lock key one drain holds at a time (`topology` in ASCII).
const DRAIN_LOCK: i64 = 0x746f_706f_6c6f_6779;

/// How many outbox rows one drain transaction reads.
const BATCH: i64 = 512;

/// Why an event did not reach the bus.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum AnnounceError {
    #[error("no event id left to mint")]
    IdsExhausted,
    #[error("publish failed: {0:?}")]
    Bus(BusError),
}

/// Publishes one event: stamps it into an [`Envelope`] and hands it to the
/// bus.
pub trait Announce: Send + Sync {
    fn announce(&self, event: BusEvent) -> impl Future<Output = Result<(), AnnounceError>> + Send;
}

/// [`Announce`] over the spec's `EventBus`: envelope ids minted with the
/// spec's `UlidGenerator` at the clock's time, under one lock, so envelopes
/// reach the bus in id order.
pub struct BusAnnouncer<E> {
    bus: E,
    clock: Arc<dyn Clock>,
    ids: Mutex<UlidGenerator<SeededRandom>>,
}

impl<E> BusAnnouncer<E> {
    pub fn new(bus: E, clock: Arc<dyn Clock>, entropy: SeededRandom) -> Self {
        Self {
            bus,
            ids: Mutex::new(UlidGenerator::new(Arc::clone(&clock), entropy)),
            clock,
        }
    }

    pub fn bus(&self) -> &E {
        &self.bus
    }
}

impl<E: EventBus + Send + Sync> Announce for BusAnnouncer<E> {
    async fn announce(&self, event: BusEvent) -> Result<(), AnnounceError> {
        let mut ids = self.ids.lock().await;
        let at = self.clock.now();
        let id: EventId = ids.mint_at(at).map_err(|_| AnnounceError::IdsExhausted)?;
        self.bus
            .publish(Envelope { id, at, event })
            .await
            .map_err(AnnounceError::Bus)
    }
}

impl<A: Announce> Announce for Arc<A> {
    fn announce(&self, event: BusEvent) -> impl Future<Output = Result<(), AnnounceError>> + Send {
        A::announce(self, event)
    }
}

/// **HOOK (follow-mode spec batch): `Changed::Traffic`.** The notification
/// a coalesced traffic window becomes. The follow-mode batch adds
/// `Changed::Traffic(TimeWindow)`; once it lands, this returns
/// `Some(BusEvent::Changed(Changed::Traffic(window)))`. Until then traffic
/// rows are drained and coalesced, and nothing is published for them.
pub fn traffic_notification(window: TimeWindow) -> Option<BusEvent> {
    let _ = window;
    None
}

/// Why a drain stopped. Rows it did not publish stay in the outbox.
#[derive(Debug, thiserror::Error)]
pub enum RelayError {
    #[error("outbox: {0}")]
    Store(#[from] DbError),
    #[error("outbox event {seq}: {source}")]
    Announce {
        seq: i64,
        #[source]
        source: AnnounceError,
    },
}

impl From<sqlx::Error> for RelayError {
    fn from(error: sqlx::Error) -> Self {
        Self::Store(DbError::from(error))
    }
}

/// Write `event` to the outbox in the caller's transaction.
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

/// One outbox row, decoded.
enum OutboxRow {
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
    /// return.
    pub async fn run<A: Announce>(mut self, announcer: A, poll: Duration) {
        tracing::info!(poll_ms = poll.as_millis(), "topology outbox relay started");
        loop {
            let open = tokio::select! {
                woken = self.wake.recv() => woken.is_some(),
                () = tokio::time::sleep(poll) => true,
            };
            if let Err(error) = drain(&self.pool, &announcer).await {
                tracing::warn!(error = %error, "topology outbox drain failed; retrying on the next wake-up");
            }
            if !open {
                break;
            }
        }
        tracing::info!("topology outbox relay stopped");
    }
}

/// Publish and delete every outbox row, oldest first, batch by batch.
/// Returns how many rows it removed.
pub async fn drain<A: Announce>(pool: &PgPool, announcer: &A) -> Result<u64, RelayError> {
    let mut total = 0;
    loop {
        let removed = drain_batch(pool, announcer).await?;
        total += removed;
        if removed < BATCH.unsigned_abs() {
            return Ok(total);
        }
    }
}

async fn drain_batch<A: Announce>(pool: &PgPool, announcer: &A) -> Result<u64, RelayError> {
    let mut tx = pool.begin().await?;
    sqlx::query("SELECT pg_advisory_xact_lock($1)")
        .bind(DRAIN_LOCK)
        .execute(&mut *tx)
        .await?;
    let rows = sqlx::query(
        "SELECT seq, event, traffic_start, traffic_end FROM topology.outbox ORDER BY seq LIMIT $1",
    )
    .bind(BATCH)
    .fetch_all(&mut *tx)
    .await?;
    let mut decoded = Vec::with_capacity(rows.len());
    for row in &rows {
        let seq: i64 = row.try_get("seq")?;
        let event: Option<String> = row.try_get("event")?;
        let start: Option<i64> = row.try_get("traffic_start")?;
        let end: Option<i64> = row.try_get("traffic_end")?;
        decoded.push((seq, decode(event, start, end)?));
    }
    let Some(last) = decoded.last().map(|(seq, _)| *seq) else {
        return Ok(0);
    };
    let mut hull: Option<(i64, TimeWindow)> = None;
    let mut published_through = None;
    let mut failure = None;
    for (seq, row) in decoded {
        match row {
            OutboxRow::Event(event) => {
                if let Err(source) = announcer.announce(*event).await {
                    failure = Some(RelayError::Announce { seq, source });
                    break;
                }
                published_through = Some(seq);
            }
            OutboxRow::Traffic(window) => {
                hull = Some(match hull {
                    None => (seq, window),
                    Some((first, held)) => (first, cover(held, window)),
                });
                published_through = Some(seq);
            }
        }
    }
    if failure.is_none()
        && let Some((seq, window)) = hull
        && let Some(event) = traffic_notification(window)
        && let Err(source) = announcer.announce(event).await
    {
        // The events before the traffic rows are out; keep every traffic
        // row so the next drain covers them again.
        failure = Some(RelayError::Announce { seq, source });
        published_through = Some(seq - 1);
    }
    let removed = match published_through {
        Some(through) => sqlx::query("DELETE FROM topology.outbox WHERE seq <= $1")
            .bind(through.min(last))
            .execute(&mut *tx)
            .await?
            .rows_affected(),
        None => 0,
    };
    tx.commit().await?;
    match failure {
        Some(error) => Err(error),
        None => Ok(removed),
    }
}

fn decode(
    event: Option<String>,
    start: Option<i64>,
    end: Option<i64>,
) -> Result<OutboxRow, DbError> {
    match (event, start, end) {
        (Some(text), None, None) => serde_json::from_str(&text)
            .map(|event| OutboxRow::Event(Box::new(event)))
            .map_err(|error| {
                DbError::inconsistent(format!("outbox event does not decode: {error}"))
            }),
        (None, Some(start), Some(end)) => {
            let start = codec::timestamp("traffic start", start)?;
            let end = codec::timestamp("traffic end", end)?;
            TimeWindow::new(start, end)
                .map(OutboxRow::Traffic)
                .map_err(|_| DbError::inconsistent("outbox traffic window is empty"))
        }
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
