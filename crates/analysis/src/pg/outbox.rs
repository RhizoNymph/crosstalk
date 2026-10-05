//! Where L6's Postgres stores publish the events of the decisions they take.
//!
//! A write appends its events to `analysis.outbox` in the transaction that
//! makes the change ([`append`]) and, once it commits, hands them to the
//! store's [`EventSink`] and deletes the rows ([`deliver`]). A sink failure
//! leaves the rows; [`flush`] publishes them later (the `alerts` consumer
//! flushes when it starts). So an event is published at least once, and
//! never before its change is visible to a read.
//!
//! [`BusSink`] is the wiring's sink: each event in its own envelope on the
//! spec's `EventBus`, its id minted from a ULID generator at the injected
//! clock's reading (a store never reads a clock itself).

use std::sync::{Arc, Mutex, PoisonError};

use crosstalk_spec::events::{BusEvent, Envelope};
use crosstalk_spec::ids::mint::{RandomSource, UlidExhausted, UlidGenerator};
use crosstalk_spec::interfaces::l2_transport::{BusError, EventBus};
use crosstalk_spec::support::Clock;
use sqlx::{PgConnection, PgPool};

use super::codec::{CodecError, from_json, to_json};

/// Why a sink did not take every event.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SinkError {
    #[error("the bus refused an event: {0:?}")]
    Bus(BusError),
    #[error("no envelope id is left: {0}")]
    Ids(UlidExhausted),
    #[error("the sink is closed")]
    Closed,
}

/// Takes the events a committed write publishes, in commit order.
pub trait EventSink: Send + Sync + 'static {
    fn publish(&self, events: Vec<BusEvent>) -> impl Future<Output = Result<(), SinkError>> + Send;
}

/// [`EventSink`] onto an [`EventBus`]: each event in its own envelope,
/// stamped with the injected clock's reading.
pub struct BusSink<E, R> {
    bus: Arc<E>,
    clock: Arc<dyn Clock>,
    ids: Mutex<UlidGenerator<R>>,
}

impl<E, R> BusSink<E, R> {
    pub fn new(bus: Arc<E>, clock: Arc<dyn Clock>, ids: UlidGenerator<R>) -> Self {
        Self {
            bus,
            clock,
            ids: Mutex::new(ids),
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
    async fn publish(&self, events: Vec<BusEvent>) -> Result<(), SinkError> {
        for event in events {
            let at = self.clock.now();
            let id = {
                // A poisoned lock only means a minting call panicked; the
                // generator's last id is still valid.
                let mut ids = self.ids.lock().unwrap_or_else(PoisonError::into_inner);
                ids.mint_at(at).map_err(SinkError::Ids)?
            };
            self.bus
                .publish(Envelope { id, at, event })
                .await
                .map_err(SinkError::Bus)?;
        }
        Ok(())
    }
}

/// An [`EventSink`] onto an unbounded channel, for tests and in-process
/// wiring that stamps envelopes itself.
#[derive(Debug, Clone)]
pub struct ChannelSink(tokio::sync::mpsc::UnboundedSender<BusEvent>);

impl ChannelSink {
    pub fn new() -> (Self, tokio::sync::mpsc::UnboundedReceiver<BusEvent>) {
        let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
        (Self(sender), receiver)
    }
}

impl EventSink for ChannelSink {
    async fn publish(&self, events: Vec<BusEvent>) -> Result<(), SinkError> {
        for event in events {
            self.0.send(event).map_err(|_| SinkError::Closed)?;
        }
        Ok(())
    }
}

/// Events appended in one transaction, ready to deliver after it commits.
#[derive(Debug, Default)]
#[must_use = "deliver the events once the transaction commits"]
pub struct Pending {
    seqs: Vec<i64>,
    events: Vec<BusEvent>,
}

impl Pending {
    pub fn is_empty(&self) -> bool {
        self.events.is_empty()
    }
}

/// Why the outbox could not be written or read.
#[derive(Debug, thiserror::Error)]
pub enum OutboxError {
    #[error(transparent)]
    Db(#[from] sqlx::Error),
    #[error(transparent)]
    Codec(#[from] CodecError),
}

/// Append `events` to the outbox inside the caller's transaction.
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
    Ok(Pending { seqs, events })
}

/// Hand committed events to `sink` and delete their rows. A failure is
/// logged and leaves the rows for [`flush`]: the write itself committed.
pub async fn deliver<S: EventSink>(pool: &PgPool, sink: &S, pending: Pending) {
    if pending.is_empty() {
        return;
    }
    let count = pending.events.len();
    if let Err(error) = sink.publish(pending.events).await {
        tracing::warn!(events = count, error = %error, "publishing committed events failed; left in the outbox");
        return;
    }
    if let Err(error) = sqlx::query("DELETE FROM analysis.outbox WHERE seq = ANY($1)")
        .bind(&pending.seqs)
        .execute(pool)
        .await
    {
        tracing::warn!(events = count, error = %error, "clearing published events failed; they may be published again");
    }
}

/// Publish every event left in the outbox, oldest first, and delete each
/// once the sink took it. Returns how many it published.
pub async fn flush<S: EventSink>(pool: &PgPool, sink: &S) -> Result<usize, OutboxError> {
    let rows: Vec<(i64, String)> =
        sqlx::query_as("SELECT seq, event FROM analysis.outbox ORDER BY seq")
            .fetch_all(pool)
            .await?;
    let mut published = 0;
    for (seq, json) in rows {
        let event: BusEvent = from_json("outbox event", &json)?;
        if let Err(error) = sink.publish(vec![event]).await {
            tracing::warn!(seq, error = %error, "outbox flush stopped at an event the sink refused");
            break;
        }
        sqlx::query("DELETE FROM analysis.outbox WHERE seq = $1")
            .bind(seq)
            .execute(pool)
            .await?;
        published += 1;
    }
    Ok(published)
}
