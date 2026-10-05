//! Where L3's stores publish the events of the decisions they take.
//!
//! A store write appends its events to the transaction's outbox table and,
//! once the transaction commits, hands them to its [`EventSink`] and
//! deletes the outbox rows. A sink failure leaves the rows, and
//! `PgAgents::flush_outbox` publishes them later, so an event is published
//! at least once and never before its change is visible.
//!
//! [`BusSink`] is the wiring's sink: it puts each event in an envelope and
//! publishes it on the spec's `EventBus`. Its envelope ids come from a ULID
//! generator over the clock the wiring hands it (a store never reads a
//! clock itself).

use std::sync::{Arc, Mutex, PoisonError};

use crosstalk_spec::events::{BusEvent, Envelope};
use crosstalk_spec::ids::mint::{RandomSource, UlidExhausted, UlidGenerator};
use crosstalk_spec::interfaces::l2_transport::{BusError, EventBus};
use crosstalk_spec::support::Clock;

/// Why a sink did not take every event.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SinkError {
    #[error("the bus refused an event: {0:?}")]
    Bus(BusError),
    #[error("no envelope id is left: {0}")]
    Ids(UlidExhausted),
}

/// Takes the events a committed write publishes, in order.
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
