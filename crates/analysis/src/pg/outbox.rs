//! Where L6's Postgres stores publish the events of the decisions they take.
//!
//! A write appends its events to `analysis.outbox` in the transaction that
//! makes the change ([`append`]). Once it commits, the relay publishes them
//! ([`deliver`] for the write's own rows, [`flush`] for every row left):
//!
//! ```text
//! 1. txn: lock the rows (seq order, SKIP LOCKED); stamp every unstamped row
//!         with an envelope id and time from the sink (EventSink::stamp); COMMIT
//! 2. publish each row as Envelope { id, at, event }, in seq order
//! 3. delete the published rows
//! ```
//!
//! So each staged event is published under one envelope id, fixed in a
//! committed transaction before its first publish and never before the
//! transaction that staged it committed (`analysis.outbox.stable-envelope-id`).
//! A relay that fails or crashes after the stamp, after a publish or before
//! the delete leaves the rows stamped; the next relay republishes them
//! under the same ids, which the bus deduplicates
//! (`transport.publish.idempotent-on-id`). Events of concurrent writes may
//! be relayed out of commit order; readers re-query.
//!
//! [`BusSink`] is the wiring's sink: ids from a ULID generator at the
//! injected clock's reading (a store never reads a clock itself), each
//! envelope published on the spec's `EventBus` and awaited.

use std::sync::{Arc, Mutex, PoisonError};

use crosstalk_spec::events::{BusEvent, Envelope};
use crosstalk_spec::ids::EventId;
use crosstalk_spec::ids::mint::{RandomSource, SeededRandom, UlidExhausted, UlidGenerator};
use crosstalk_spec::interfaces::l2_transport::{BusError, EventBus};
use crosstalk_spec::support::{Clock, Timestamp};
use sqlx::{PgConnection, PgPool};

use super::codec::{CodecError, from_json, micros, timestamp, to_json};

/// Rows one relay round locks, stamps and publishes.
const RELAY_BATCH: i64 = 256;

/// Why a sink did not take an envelope or could not stamp one.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SinkError {
    #[error("the bus refused an event: {0:?}")]
    Bus(BusError),
    #[error("no envelope id is left: {0}")]
    Ids(UlidExhausted),
    #[error("the sink is closed")]
    Closed,
}

/// Stamps staged events with their envelope's id and time, and takes the
/// envelopes the relay publishes.
pub trait EventSink: Send + Sync + 'static {
    /// A fresh envelope id and time for one staged event. Called once per
    /// outbox row, inside the relay's stamping transaction.
    fn stamp(&self) -> Result<(EventId, Timestamp), SinkError>;

    /// Publish one stamped envelope. Called again with the same envelope
    /// when an earlier relay did not delete its row.
    fn publish(&self, envelope: Envelope) -> impl Future<Output = Result<(), SinkError>> + Send;
}

/// Mints envelope ids from a ULID generator at a clock's reading.
struct Stamper<R> {
    clock: Arc<dyn Clock>,
    ids: Mutex<UlidGenerator<R>>,
}

impl<R: RandomSource> Stamper<R> {
    fn stamp(&self) -> Result<(EventId, Timestamp), SinkError> {
        let at = self.clock.now();
        // A poisoned lock only means a minting call panicked; the
        // generator's last id is still valid.
        let mut ids = self.ids.lock().unwrap_or_else(PoisonError::into_inner);
        let id = ids.mint_at(at).map_err(SinkError::Ids)?;
        Ok((id, at))
    }
}

/// [`EventSink`] onto an [`EventBus`]: ids minted at the injected clock's
/// reading, each envelope published and awaited.
pub struct BusSink<E, R> {
    bus: Arc<E>,
    stamper: Stamper<R>,
}

impl<E, R> BusSink<E, R> {
    pub fn new(bus: Arc<E>, clock: Arc<dyn Clock>, ids: UlidGenerator<R>) -> Self {
        Self {
            bus,
            stamper: Stamper {
                clock,
                ids: Mutex::new(ids),
            },
        }
    }
}

impl<E, R> std::fmt::Debug for BusSink<E, R> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BusSink").finish_non_exhaustive()
    }
}

impl<E, R> EventSink for BusSink<E, R>
where
    E: EventBus + Send + Sync + 'static,
    R: RandomSource + 'static,
{
    fn stamp(&self) -> Result<(EventId, Timestamp), SinkError> {
        self.stamper.stamp()
    }

    async fn publish(&self, envelope: Envelope) -> Result<(), SinkError> {
        self.bus.publish(envelope).await.map_err(SinkError::Bus)
    }
}

/// An [`EventSink`] onto an unbounded channel of events, for tests and
/// in-process wiring that reads the events only. Its envelope ids are
/// minted at the epoch from a fixed seed.
#[derive(Clone)]
pub struct ChannelSink {
    sender: tokio::sync::mpsc::UnboundedSender<BusEvent>,
    stamper: Arc<Stamper<SeededRandom>>,
}

impl std::fmt::Debug for ChannelSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ChannelSink").finish_non_exhaustive()
    }
}

/// A clock that never moves, for sinks that stamp without one.
#[derive(Debug)]
struct Epoch;

impl Clock for Epoch {
    fn now(&self) -> Timestamp {
        Timestamp::from_micros(0)
    }
}

impl ChannelSink {
    pub fn new() -> (Self, tokio::sync::mpsc::UnboundedReceiver<BusEvent>) {
        let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
        let clock: Arc<dyn Clock> = Arc::new(Epoch);
        let stamper = Stamper {
            ids: Mutex::new(UlidGenerator::new(Arc::clone(&clock), SeededRandom::new(0))),
            clock,
        };
        (
            Self {
                sender,
                stamper: Arc::new(stamper),
            },
            receiver,
        )
    }
}

impl EventSink for ChannelSink {
    fn stamp(&self) -> Result<(EventId, Timestamp), SinkError> {
        self.stamper.stamp()
    }

    async fn publish(&self, envelope: Envelope) -> Result<(), SinkError> {
        self.sender
            .send(envelope.event)
            .map_err(|_| SinkError::Closed)
    }
}

/// Events appended in one transaction, ready to relay after it commits.
#[derive(Debug, Default)]
#[must_use = "deliver the events once the transaction commits"]
pub struct Pending {
    seqs: Vec<i64>,
}

impl Pending {
    pub fn is_empty(&self) -> bool {
        self.seqs.is_empty()
    }
}

/// Why the outbox could not be written, stamped or relayed.
#[derive(Debug, thiserror::Error)]
pub enum OutboxError {
    #[error(transparent)]
    Db(#[from] sqlx::Error),
    #[error(transparent)]
    Codec(#[from] CodecError),
    #[error("stamping: {0}")]
    Stamp(SinkError),
}

/// Append `events` to the outbox inside the caller's transaction, unstamped.
pub async fn append(
    conn: &mut PgConnection,
    events: Vec<BusEvent>,
) -> Result<Pending, OutboxError> {
    let mut seqs = Vec::with_capacity(events.len());
    for event in &events {
        let json = to_json("outbox event", event)?;
        let seq: i64 =
            sqlx::query_scalar("INSERT INTO analysis.outbox (event) VALUES ($1) RETURNING seq")
                .bind(json)
                .fetch_one(&mut *conn)
                .await?;
        seqs.push(seq);
    }
    Ok(Pending { seqs })
}

/// Relay a committed write's own rows. A failure is logged and leaves the
/// rows (stamped, if the stamp committed) for [`flush`]: the write itself
/// committed.
pub async fn deliver<S: EventSink>(pool: &PgPool, sink: &S, pending: Pending) {
    if pending.is_empty() {
        return;
    }
    let count = pending.seqs.len();
    let mut published = 0;
    loop {
        match relay(pool, sink, Some(&pending.seqs)).await {
            Ok(round) => {
                published += round.published;
                if let Some(error) = round.stopped {
                    tracing::warn!(events = count, published, error = %error, "publishing committed events stopped; left in the outbox");
                    return;
                }
                if round.locked < batch_len() {
                    return;
                }
            }
            Err(error) => {
                tracing::warn!(events = count, published, error = %error, "relaying committed events failed; left in the outbox");
                return;
            }
        }
    }
}

/// [`RELAY_BATCH`] as a length.
fn batch_len() -> usize {
    usize::try_from(RELAY_BATCH).unwrap_or(usize::MAX)
}

/// Publish every event left in the outbox, oldest first, each under the id
/// stamped on its row. Stops at the first event the sink refuses, leaving
/// it and every later one. Returns how many it published.
pub async fn flush<S: EventSink>(pool: &PgPool, sink: &S) -> Result<usize, OutboxError> {
    let mut published = 0;
    loop {
        let round = relay(pool, sink, None).await?;
        published += round.published;
        if let Some(error) = round.stopped {
            tracing::warn!(published, error = %error, "outbox flush stopped at an event the sink refused");
            return Ok(published);
        }
        if round.locked < batch_len() {
            return Ok(published);
        }
    }
}

/// What one relay round did.
#[derive(Debug)]
struct Round {
    /// Rows it locked and stamped (or found stamped).
    locked: usize,
    /// Rows it published and deleted.
    published: usize,
    /// The sink's refusal that stopped it, if one did.
    stopped: Option<SinkError>,
}

/// One relay round over `only` (or every row), up to [`RELAY_BATCH`] rows.
async fn relay<S: EventSink>(
    pool: &PgPool,
    sink: &S,
    only: Option<&[i64]>,
) -> Result<Round, OutboxError> {
    let rows = stamp(pool, sink, only).await?;
    let locked = rows.len();
    let mut published = Vec::with_capacity(rows.len());
    let mut stopped = None;
    for (seq, envelope) in rows {
        if let Err(error) = sink.publish(envelope).await {
            stopped = Some(error);
            break;
        }
        published.push(seq);
    }
    if !published.is_empty() {
        sqlx::query("DELETE FROM analysis.outbox WHERE seq = ANY($1)")
            .bind(&published)
            .execute(pool)
            .await?;
    }
    Ok(Round {
        locked,
        published: published.len(),
        stopped,
    })
}

/// Step 1 of a relay: lock up to a batch of rows in seq order (skipping
/// rows another relay holds), stamp the unstamped ones, and commit. Returns
/// every locked row as its envelope, in seq order.
pub(crate) async fn stamp<S: EventSink>(
    pool: &PgPool,
    sink: &S,
    only: Option<&[i64]>,
) -> Result<Vec<(i64, Envelope)>, OutboxError> {
    let mut tx = pool.begin().await?;
    let rows: Vec<(i64, String, Option<String>, Option<i64>)> = match only {
        Some(seqs) => {
            sqlx::query_as(
                "SELECT seq, event, envelope_id, at FROM analysis.outbox WHERE seq = ANY($1) \
                 ORDER BY seq LIMIT $2 FOR UPDATE SKIP LOCKED",
            )
            .bind(seqs)
            .bind(RELAY_BATCH)
            .fetch_all(&mut *tx)
            .await?
        }
        None => {
            sqlx::query_as(
                "SELECT seq, event, envelope_id, at FROM analysis.outbox \
                 ORDER BY seq LIMIT $1 FOR UPDATE SKIP LOCKED",
            )
            .bind(RELAY_BATCH)
            .fetch_all(&mut *tx)
            .await?
        }
    };
    let mut envelopes = Vec::with_capacity(rows.len());
    for (seq, json, id, at) in rows {
        let event: BusEvent = from_json("outbox event", &json)?;
        let (id, at) = match (id, at) {
            (Some(id), Some(at)) => (
                EventId::from_ulid_text(&id).map_err(|_| CodecError::Ulid {
                    what: "outbox envelope id",
                    text: id.clone(),
                })?,
                timestamp("outbox envelope time", at)?,
            ),
            _ => {
                let (id, at) = sink.stamp().map_err(OutboxError::Stamp)?;
                sqlx::query("UPDATE analysis.outbox SET envelope_id = $2, at = $3 WHERE seq = $1")
                    .bind(seq)
                    .bind(id.ulid_text())
                    .bind(micros("outbox envelope time", at)?)
                    .execute(&mut *tx)
                    .await?;
                (id, at)
            }
        };
        envelopes.push((seq, Envelope { id, at, event }));
    }
    tx.commit().await?;
    Ok(envelopes)
}
