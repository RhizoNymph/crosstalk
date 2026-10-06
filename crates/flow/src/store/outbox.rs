//! The transactional outbox the flow stores publish through
//! (`flow.outbox.stable-envelope-id`, INV-1212).
//!
//! A write stages the events it decided (`stage`) in `flow.outbox` inside
//! its own serializable transaction, so they commit or roll back with the
//! change: a refused or rolled-back write publishes nothing, and a retried
//! transaction stages its events once (the failed attempts' rows rolled
//! back with them). Once the transaction commits, the store relays the
//! outbox ([`Relay::relay`]):
//!
//! 1. in a transaction of its own it takes the next rows (`FOR UPDATE SKIP
//!    LOCKED`, in `seq` order), stamps every row that has no envelope id
//!    yet with the sink's [`EventSink::stamp`] (migration `0003_restart`:
//!    `envelope_id`, `at`) and commits, so each row's id and time are fixed
//!    before its first publish;
//! 2. it publishes each row as an [`Envelope`] under its stamp, in `seq`
//!    order, awaiting the sink;
//! 3. it deletes the rows it published.
//!
//! A failure or a crash after step 1 leaves the rows stamped; the next
//! relay (any store's, or the one at start, before the flow group
//! subscribes) publishes them under the same ids, which a bus idempotent on
//! envelope ids (`PgBus`) holds once. A relay reads only committed rows, so
//! nothing is published before the transaction that staged it committed.
//!
//! Concurrent relays skip each other's locked rows in step 1, so a row is
//! stamped once. Two relays may publish one stamped row twice (one stopped
//! before deleting it), always under its one id. Events of two concurrent
//! writes may be relayed out of commit order; each event names what changed
//! and readers re-query, so no consumer depends on that order.

use std::sync::{Arc, Mutex, PoisonError};

use crosstalk_spec::events::{BusEvent, Envelope};
use crosstalk_spec::ids::{EventId, RandomSource, UlidExhausted, UlidGenerator};
use crosstalk_spec::interfaces::l2_transport::{BusError, EventBus};
use crosstalk_spec::support::{Clock, Timestamp};
use sqlx::{PgConnection, PgPool};
use tracing::warn;

use super::codec::{from_json, id_text, json, micros, parse_id, timestamp};
use super::error::{Fault, FlowStoreError};

/// How many staged events one relay pass takes.
const RELAY_BATCH: i64 = 256;

/// Why a sink did not stamp or publish an event.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SinkError {
    #[error("the bus refused an event: {0:?}")]
    Bus(BusError),
    #[error("no envelope id is left: {0}")]
    Ids(UlidExhausted),
}

/// The envelope id and time a staged event is published under, minted once
/// per outbox row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Stamp {
    pub id: EventId,
    pub at: Timestamp,
}

/// Stamps and publishes the events committed writes staged.
pub trait EventSink: Send + Sync + 'static {
    /// The stamp of one staged event. The relay calls it once per outbox
    /// row, inside the transaction that records the stamp; a row whose
    /// stamp committed is never stamped again.
    fn stamp(&self) -> Result<Stamp, SinkError>;

    /// Publish `envelope`; `Ok` once the bus holds it. The relay may
    /// publish an envelope again under the same id (it stopped before
    /// deleting the row); the bus deduplicates on the id.
    fn publish(&self, envelope: Envelope) -> impl Future<Output = Result<(), SinkError>> + Send;
}

/// [`EventSink`] onto an [`EventBus`]: stamps from a ULID generator at the
/// injected clock's reading (a store never reads a clock itself) and
/// publishes awaiting the bus.
pub struct BusSink<E, R> {
    bus: E,
    clock: Arc<dyn Clock>,
    ids: Mutex<UlidGenerator<R>>,
}

impl<E, R> BusSink<E, R> {
    /// A sink publishing on `bus`, minting envelope ids with `ids` at
    /// `clock`'s readings.
    pub fn new(bus: E, clock: Arc<dyn Clock>, ids: UlidGenerator<R>) -> Self {
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
    fn stamp(&self) -> Result<Stamp, SinkError> {
        let at = self.clock.now();
        // A poisoned lock only means a minting call panicked; the
        // generator's last id is still valid.
        let mut ids = self.ids.lock().unwrap_or_else(PoisonError::into_inner);
        let id = ids.mint_at(at).map_err(SinkError::Ids)?;
        Ok(Stamp { id, at })
    }

    async fn publish(&self, envelope: Envelope) -> Result<(), SinkError> {
        self.bus.publish(envelope).await.map_err(SinkError::Bus)
    }
}

/// A test sink: stamps from a counter at time zero and sends each
/// published event on a channel (the shape the store tests read events in).
#[cfg(test)]
#[derive(Debug)]
pub(crate) struct ChannelSink {
    sender: tokio::sync::mpsc::UnboundedSender<BusEvent>,
    next: std::sync::atomic::AtomicU64,
}

#[cfg(test)]
impl ChannelSink {
    pub(crate) fn new(sender: tokio::sync::mpsc::UnboundedSender<BusEvent>) -> Self {
        Self {
            sender,
            next: std::sync::atomic::AtomicU64::new(1),
        }
    }
}

#[cfg(test)]
impl EventSink for ChannelSink {
    fn stamp(&self) -> Result<Stamp, SinkError> {
        let n = self.next.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Ok(Stamp {
            id: EventId::from_ulid(u128::from(n)),
            at: Timestamp::from_micros(0),
        })
    }

    async fn publish(&self, envelope: Envelope) -> Result<(), SinkError> {
        self.sender
            .send(envelope.event)
            .map_err(|_| SinkError::Bus(BusError::Disconnected))
    }
}

/// Stage `events` in the outbox, inside the caller's transaction,
/// unstamped.
pub(crate) async fn stage(conn: &mut PgConnection, events: &[BusEvent]) -> Result<(), Fault> {
    for event in events {
        sqlx::query("INSERT INTO flow.outbox (event) VALUES ($1)")
            .bind(json("outbox event", event)?)
            .execute(&mut *conn)
            .await?;
    }
    Ok(())
}

/// Relays committed events from the outbox to a sink. Clones share the
/// sink.
pub struct Relay<S> {
    pool: PgPool,
    sink: Arc<S>,
}

impl<S> Clone for Relay<S> {
    fn clone(&self) -> Self {
        Self {
            pool: self.pool.clone(),
            sink: Arc::clone(&self.sink),
        }
    }
}

impl<S> std::fmt::Debug for Relay<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Relay").finish_non_exhaustive()
    }
}

/// An outbox row: its sequence, event and stamp columns.
type OutboxRow = (i64, String, Option<String>, Option<i64>);

/// A taken row, stamped.
struct Taken {
    seq: i64,
    envelope: Envelope,
}

impl<S: EventSink> Relay<S> {
    pub fn new(pool: PgPool, sink: S) -> Self {
        Self {
            pool,
            sink: Arc::new(sink),
        }
    }

    /// The sink the relay publishes to.
    pub fn sink(&self) -> &S {
        &self.sink
    }

    /// Stamp, publish and delete every staged event, oldest first, until
    /// the outbox is empty (run it at start, before the flow group
    /// subscribes). Returns how many were published. A sink failure stops
    /// the relay with the error after deleting what was published before
    /// it; the rest stay staged, stamped, for the next relay.
    pub async fn relay(&self) -> Result<usize, FlowStoreError> {
        let mut total = 0;
        loop {
            let taken = self.take().await?;
            if taken.is_empty() {
                return Ok(total);
            }
            let mut published = Vec::with_capacity(taken.len());
            let mut failed = None;
            for row in taken {
                let id = row.envelope.id;
                match self.sink.publish(row.envelope).await {
                    Ok(()) => published.push(row.seq),
                    Err(error) => {
                        warn!(seq = row.seq, envelope = %id.ulid_text(), error = %error, "outbox event not published; left stamped for the next relay");
                        failed = Some(error);
                        break;
                    }
                }
            }
            if !published.is_empty() {
                sqlx::query("DELETE FROM flow.outbox WHERE seq = ANY($1)")
                    .bind(&published)
                    .execute(&self.pool)
                    .await?;
            }
            total += published.len();
            if let Some(error) = failed {
                return Err(FlowStoreError::Sink(error));
            }
        }
    }

    /// Relay after a committed write: a failure leaves the events staged
    /// for the next relay, so the write still succeeded; it is logged.
    pub(crate) async fn after_commit(&self) {
        if let Err(error) = self.relay().await {
            warn!(error = %error, "outbox relay failed; the events stay staged for the next relay");
        }
    }

    /// Step 1: take the next batch, stamp the unstamped rows and commit the
    /// stamps.
    async fn take(&self) -> Result<Vec<Taken>, FlowStoreError> {
        let mut tx = self.pool.begin().await?;
        let rows: Vec<OutboxRow> = sqlx::query_as(
            "SELECT seq, event, envelope_id, at FROM flow.outbox ORDER BY seq \
             LIMIT $1 FOR UPDATE SKIP LOCKED",
        )
        .bind(RELAY_BATCH)
        .fetch_all(&mut *tx)
        .await?;
        let mut taken = Vec::with_capacity(rows.len());
        for (seq, event, id, at) in rows {
            let stamp = match (id, at) {
                (Some(id), Some(at)) => Stamp {
                    id: parse_id("outbox.envelope_id", &id)?,
                    at: timestamp("outbox.at", at)?,
                },
                (None, None) => {
                    let stamp = self.sink.stamp().map_err(FlowStoreError::Sink)?;
                    sqlx::query(
                        "UPDATE flow.outbox SET envelope_id = $2, at = $3 \
                         WHERE seq = $1 AND envelope_id IS NULL",
                    )
                    .bind(seq)
                    .bind(id_text(stamp.id))
                    .bind(micros("outbox.at", stamp.at)?)
                    .execute(&mut *tx)
                    .await?;
                    stamp
                }
                // The `outbox_stamped` constraint refuses a half stamp.
                _ => {
                    return Err(FlowStoreError::Corrupt {
                        what: "outbox row",
                        reason: format!("row {seq} is half stamped"),
                    });
                }
            };
            taken.push(Taken {
                seq,
                envelope: Envelope {
                    id: stamp.id,
                    at: stamp.at,
                    event: from_json("outbox event", &event)?,
                },
            });
        }
        tx.commit().await?;
        Ok(taken)
    }
}
