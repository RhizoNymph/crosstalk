//! Where L3's stores publish the events of the decisions they take.
//!
//! A store write appends its events to the transaction's outbox table.
//! Once the transaction commits, the store's relay (`agents::outbox`)
//! stamps each staged row with an envelope id and time from its
//! [`EventSink`] ([`EventSink::stamp`]) in a transaction of its own,
//! commits the stamps, publishes each row as an [`Envelope`] under its
//! stamp, and then deletes the published rows. A failure anywhere leaves
//! the rows, and `PgAgents::flush_outbox` relays them later under the ids
//! they were stamped with (`reconstruct.outbox.stable-envelope-id`,
//! INV-1211): an event is published at least once, never before its change
//! is visible, and every publish of it carries one envelope id, so a bus
//! that is idempotent on ids (`PgBus`) holds it once.
//!
//! [`BusSink`] is the wiring's sink: it stamps at the injected clock's
//! reading with a ULID generator (a store never reads a clock itself) and
//! publishes on the spec's `EventBus`, awaiting the bus.

use std::sync::{Arc, Mutex, PoisonError};

use crosstalk_spec::events::Envelope;
use crosstalk_spec::ids::EventId;
use crosstalk_spec::ids::mint::{RandomSource, UlidExhausted, UlidGenerator};
use crosstalk_spec::interfaces::l2_transport::{BusError, EventBus};
use crosstalk_spec::support::{Clock, Timestamp};

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
    /// row, inside the transaction that records the stamp, and never again
    /// for that row. Stamps are increasing.
    fn stamp(&self) -> Result<Stamp, SinkError>;

    /// Publish `envelope`; `Ok` once the bus holds it. The relay may
    /// publish an envelope again under the same id (it stopped before
    /// deleting the row); the bus deduplicates on the id.
    fn publish(&self, envelope: Envelope) -> impl Future<Output = Result<(), SinkError>> + Send;
}

/// [`EventSink`] onto an [`EventBus`]: stamps from a ULID generator at the
/// injected clock's reading, publishes awaiting the bus.
pub struct BusSink<E, R> {
    bus: Arc<E>,
    clock: Arc<dyn Clock>,
    ids: Mutex<UlidGenerator<R>>,
}

impl<E, R> BusSink<E, R> {
    /// A sink publishing on `bus`, minting envelope ids with `ids` at
    /// `clock`'s readings.
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
