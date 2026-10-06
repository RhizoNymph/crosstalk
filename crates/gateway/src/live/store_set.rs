//! What a [`Live`](super::Live) process runs over: a [`LiveStoreSet`].
//!
//! | Set | Bus | Stores | Used when |
//! | --- | --- | --- | --- |
//! | [`MemorySet`] | `MpscBus` | `crosstalk-memory`'s reference stores, L3's `MemoryConversations`, L4's `MemoryProvenanceStore` | no `store` section (decision Q7), the eval harness, the e2e smoke, the UI |
//! | [`PgSet`](super::pg::PgSet) | `SpoolingBus` over `PgBus` | the Postgres bundle (`crosstalk_api::PgStores`) | a `store` section |
//!
//! The stage machinery ([`Stages`](super::Stages), the slot loop, settling,
//! the shutdown order) is the same for both; each set says what its bus
//! and stores are, and how a settle tells that nothing is left in flight
//! ([`Quiet`]).

use std::future::Future;

use crosstalk_api::HostedStores;
use crosstalk_spec::interfaces::l2_transport::{ConsumerGroup, EventBus, Subscription};
use crosstalk_transport::{MpscBus, MpscSubscription};
use tokio::sync::{mpsc, oneshot};

use super::relay::Flush;
use super::settle::SettleError;
use super::stage::{LayerStores, LiveStores, Slot};

/// The bus, stores and settling of one kind of live process.
pub trait LiveStoreSet: Send + Sync + Sized + 'static {
    /// The bus every stage, the pipeline and the surface share.
    type Bus: EventBus + Clone + Send + Sync + 'static;
    /// A consumer group's subscription, as the stages read it (the bus's
    /// own, or one served from it).
    type Sub: Subscription + Send + 'static;
    /// The stores the surface reads and the stages write.
    type Stores: HostedStores;
    /// What only the stages read (memory: L1, L3 and L4's layer stores;
    /// Postgres: the pool, the inner bus and the spool).
    type Layers: Clone + Send + Sync + 'static;
    /// How a settle and a shutdown see what is still in flight.
    type Quiet: Quiet;
    /// Whether a stage keeps deliveries unacked until a later step (the
    /// durable flow consumer acks after its checkpoint, which a `Drain`
    /// takes). A settle then drains the stages before it reads the groups.
    const DEFERRED_ACKS: bool;
}

/// What a settle and a shutdown ask of the bus and the store outboxes.
pub trait Quiet: Send + Sync + 'static {
    /// Whether any of `slots`' groups holds a delivery, or anything else
    /// the process will still publish waits (a spooled envelope, a store
    /// outbox row).
    fn busy(&self, slots: &[Slot]) -> impl Future<Output = Result<bool, SettleError>> + Send;

    /// Publish whatever the stores staged for a forwarder; how many events
    /// that was (0 where stores publish on their own).
    fn flush(&self) -> impl Future<Output = Result<u64, SettleError>> + Send;

    /// Wait until `group` holds nothing; `false` when its depth could not
    /// be read.
    fn wait_group_empty(&self, group: &ConsumerGroup) -> impl Future<Output = bool> + Send;

    /// Stop the bus: subscriptions then end.
    fn stop(&self) -> impl Future<Output = ()> + Send;
}

/// The in-memory set: the reference stores on an `MpscBus`, whose own
/// events the outbox forwarder publishes.
#[derive(Debug, Clone, Copy)]
pub struct MemorySet;

impl LiveStoreSet for MemorySet {
    type Bus = MpscBus;
    type Sub = MpscSubscription;
    type Stores = LiveStores;
    type Layers = LayerStores;
    type Quiet = MemoryQuiet;
    const DEFERRED_ACKS: bool = false;
}

/// [`Quiet`] for the memory set: the bus's group depths and the outbox
/// forwarder.
#[derive(Debug, Clone)]
pub struct MemoryQuiet {
    pub(crate) bus: MpscBus,
    pub(crate) flushes: mpsc::UnboundedSender<Flush>,
}

impl Quiet for MemoryQuiet {
    async fn busy(&self, slots: &[Slot]) -> Result<bool, SettleError> {
        super::settle::busy(&self.bus, slots).await
    }

    async fn flush(&self) -> Result<u64, SettleError> {
        let (done, flushed) = oneshot::channel();
        self.flushes
            .send(Flush(done))
            .map_err(|_| SettleError::OutboxStopped)?;
        flushed.await.map_err(|_| SettleError::OutboxStopped)
    }

    async fn wait_group_empty(&self, group: &ConsumerGroup) -> bool {
        matches!(super::settle::group_idle(&self.bus, group).await, Ok(()))
    }

    async fn stop(&self) {
        self.bus.shutdown().await;
    }
}
