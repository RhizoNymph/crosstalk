//! A consumer over the reference agent store, the in-memory conversation
//! store and a bus that records what is published.

use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use crosstalk_memory::reconstruct::MemoryAgents;
use crosstalk_memory::support::{IdSequence, Outbox};
use crosstalk_spec::events::ingest::{ConversationDelta, IngestEvent};
use crosstalk_spec::events::{BusEvent, Envelope, Subject};
use crosstalk_spec::ids::mint::SeededRandom;
use crosstalk_spec::ids::{EventId, ExchangeId};
use crosstalk_spec::interfaces::l2_transport::{
    BusError, ConsumerGroup, Delivery, DeliveryId, EventBus, RetryPolicy, Subscription,
};
use crosstalk_spec::observed::exchange::Exchange;
use crosstalk_transport::blob::MemoryBlobStore;
use tokio::sync::mpsc::UnboundedReceiver;

use super::support::{Scene, ulids};
use crate::consumer::{ConsumeError, ConsumerParts, Handled, ReconstructConsumer};
use crate::evidence::ChainEvidence;
use crate::ids::UlidSource;
use crate::thread::{ConversationThreader, MemoryConversations, ReadsMembers};

/// A bus that records every envelope published on it. Nothing subscribes.
#[derive(Debug, Clone, Default)]
pub(crate) struct RecordingBus(pub(crate) Arc<Mutex<Vec<Envelope>>>);

impl RecordingBus {
    pub(crate) fn envelopes(&self) -> Vec<Envelope> {
        self.0.lock().unwrap_or_else(PoisonError::into_inner).clone()
    }

    /// Every `ConversationDelta` published, with its envelope id.
    pub(crate) fn deltas(&self) -> Vec<(EventId, ConversationDelta)> {
        self.envelopes()
            .into_iter()
            .filter_map(|envelope| match envelope.event {
                BusEvent::Ingest(IngestEvent::ConversationDelta(delta)) => {
                    Some((envelope.id, delta))
                }
                _ => None,
            })
            .collect()
    }

    /// Every `AgentSeen` published.
    pub(crate) fn seen(&self) -> Vec<(EventId, BusEvent)> {
        self.envelopes()
            .into_iter()
            .filter(|envelope| {
                matches!(envelope.event, BusEvent::Ingest(IngestEvent::AgentSeen { .. }))
            })
            .map(|envelope| (envelope.id, envelope.event))
            .collect()
    }
}

/// A subscription that never delivers.
pub(crate) struct Silent;

impl Subscription for Silent {
    async fn next(&mut self) -> Option<Result<Delivery, BusError>> {
        None
    }

    async fn ack(&mut self, id: DeliveryId) -> Result<(), BusError> {
        Err(BusError::UnknownDelivery(id))
    }

    async fn nack(
        &mut self,
        id: DeliveryId,
        _retry_after: Duration,
        _reason: String,
    ) -> Result<(), BusError> {
        Err(BusError::UnknownDelivery(id))
    }
}

impl EventBus for RecordingBus {
    type Subscription = Silent;

    async fn publish(&self, envelope: Envelope) -> Result<(), BusError> {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(envelope);
        Ok(())
    }

    async fn subscribe(
        &self,
        _subjects: &[Subject],
        _group: ConsumerGroup,
        _retry: RetryPolicy,
    ) -> Result<Silent, BusError> {
        Ok(Silent)
    }
}

pub(crate) type Threader = ConversationThreader<
    MemoryConversations,
    MemoryBlobStore,
    ReadsMembers<MemoryAgents>,
    UlidSource<SeededRandom>,
>;

pub(crate) type Consumer = ReconstructConsumer<
    MemoryAgents,
    Threader,
    MemoryBlobStore,
    ChainEvidence,
    UlidSource<SeededRandom>,
    RecordingBus,
>;

/// The consumer and everything it writes to.
pub(crate) struct Rig {
    pub(crate) scene: Scene,
    pub(crate) agents: MemoryAgents,
    pub(crate) conversations: MemoryConversations,
    pub(crate) bus: RecordingBus,
    /// What the agent store published.
    pub(crate) store_events: UnboundedReceiver<BusEvent>,
    pub(crate) consumer: Consumer,
    envelopes: u64,
}

impl Rig {
    pub(crate) fn new() -> Self {
        let scene = Scene::new();
        let (outbox, store_events) = Outbox::channel();
        let agents = MemoryAgents::new(IdSequence::default(), outbox);
        let conversations = MemoryConversations::new();
        let bus = RecordingBus::default();
        let threader = ConversationThreader::new(
            conversations.clone(),
            Arc::clone(&scene.messages),
            ReadsMembers(agents.clone()),
            ulids(11),
        );
        let consumer = ReconstructConsumer::new(ConsumerParts {
            agents: agents.clone(),
            threader,
            messages: Arc::clone(&scene.messages),
            deriver: ChainEvidence::default(),
            agent_ids: ulids(13),
            bus: Arc::new(bus.clone()),
        });
        Self {
            scene,
            agents,
            conversations,
            bus,
            store_events,
            consumer,
            envelopes: 0,
        }
    }

    /// The `ExchangeCaptured` envelope of `exchange`.
    pub(crate) fn captured(&mut self, exchange: &Exchange) -> Envelope {
        self.envelopes += 1;
        Envelope {
            id: EventId::from_ulid(exchange.meta.id.as_ulid() ^ (0xFFFF << 64) ^ u128::from(self.envelopes)),
            at: exchange.meta.started_at,
            event: BusEvent::Ingest(IngestEvent::ExchangeCaptured(Box::new(exchange.clone()))),
        }
    }

    /// Deliver `exchange` once.
    pub(crate) async fn deliver(&mut self, exchange: &Exchange) -> Result<Handled, ConsumeError> {
        let envelope = self.captured(exchange);
        self.consumer.handle(&envelope).await
    }

    /// Everything the agent store published since the last call.
    pub(crate) fn store_events(&mut self) -> Vec<BusEvent> {
        crosstalk_memory::support::drain(&mut self.store_events)
    }

    /// The delta published for `exchange`.
    pub(crate) fn delta_of(&self, exchange: ExchangeId) -> Option<ConversationDelta> {
        self.bus
            .deltas()
            .into_iter()
            .map(|(_, delta)| delta)
            .find(|delta| delta.exchange == exchange)
    }
}
