//! The consumer over the `crosstalk-memory` reference stores, and the
//! evidence its tests feed it.

use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crosstalk_memory::flow::{MemoryChannels, MemoryVerdicts};
use crosstalk_memory::reconstruct::MemoryAgents;
use crosstalk_memory::support::{IdSequence, Outbox, drain};
use crosstalk_spec::derived::flow::access::Extraction;
use crosstalk_spec::derived::flow::channel::Channel;
use crosstalk_spec::derived::flow::resource::{Host, Locator};
use crosstalk_spec::derived::flow::transmission::Transmission;
use crosstalk_spec::derived::provenance::matching::ContentMatch;
use crosstalk_spec::events::detect::DetectEvent;
use crosstalk_spec::events::{BusEvent, Envelope, Subject};
use crosstalk_spec::ids::{AgentId, ChannelId, SeededRandom, SpanId};
use crosstalk_spec::interfaces::l2_transport::{
    BusError, ConsumerGroup, Delivery, DeliveryId, EventBus, RetryPolicy, Subscription,
};
use crosstalk_spec::interfaces::l3_reconstruction::lifecycle::{
    AgentLifecycle, AgentOrigin, NewAgent,
};
use crosstalk_spec::interfaces::l5_flow::channels::{ChannelReads, ChannelWithTraffic};
use crosstalk_spec::interfaces::l8_surface::channel_traffic::ChannelTransmissionFilter;
use crosstalk_spec::interfaces::l8_surface::lists::ChannelFilter;
use crosstalk_spec::observed::agent::IdentityEvidence;
use crosstalk_spec::observed::message::PartRef;
use crosstalk_spec::paging::{PageRequest, PageSize};
use crosstalk_spec::support::{Clock, NonEmpty, Timestamp};
use crosstalk_testkit::time::T0;
use tokio::sync::mpsc::UnboundedReceiver;

use crate::consumer::{FlowConsumer, FlowDeps, Observed, ReadResult, Settings, WriteCall};
use crate::correlate::tests::fixtures::{Scene, timing, tool_result};

pub(crate) type Registry = MemoryChannels<MemoryAgents>;
pub(crate) type Consumer<B> = FlowConsumer<Registry, MemoryVerdicts, MemoryAgents, B>;

/// The reference stores, as handles.
pub(crate) struct Stores {
    pub(crate) agents: MemoryAgents,
    pub(crate) registry: Registry,
    pub(crate) transmissions: MemoryVerdicts,
    /// What the registry published.
    pub(crate) registry_events: UnboundedReceiver<BusEvent>,
}

impl Stores {
    pub(crate) fn new() -> Self {
        let agents = MemoryAgents::new(IdSequence::new(1 << 90), Outbox::none());
        let (outbox, registry_events) = Outbox::channel();
        Self {
            registry: MemoryChannels::new(agents.clone(), IdSequence::default(), outbox),
            transmissions: MemoryVerdicts::new(Outbox::none()),
            agents,
            registry_events,
        }
    }

    /// Store `agent`, spawned by `parent`.
    pub(crate) async fn agent(&mut self, scene: &mut Scene, parent: Option<AgentId>) -> AgentId {
        let id = scene.agent();
        let credential = scene.ids.credential();
        let created = self
            .agents
            .create(NewAgent {
                id,
                evidence: NonEmpty::new(IdentityEvidence::StableCredential(credential)),
                parent,
                origin: AgentOrigin::Traffic { first_seen: T0 },
                label: None,
            })
            .await;
        assert_eq!(created, Ok(()));
        id
    }

    /// What the registry published since the last call.
    pub(crate) fn registry_events(&mut self) -> Vec<BusEvent> {
        drain(&mut self.registry_events)
    }

    /// Every channel the registry lists.
    pub(crate) async fn channels(&self) -> Vec<ChannelWithTraffic> {
        let page = self
            .registry
            .channels(&ChannelFilter::default(), &first_page())
            .await;
        match page {
            Ok(page) => page.into_parts().0,
            Err(error) => panic!("channels: {error:?}"),
        }
    }

    /// The transmissions routed through `channel`.
    pub(crate) async fn transmissions_of(&self, channel: ChannelId) -> Vec<Transmission> {
        let page = self
            .registry
            .transmissions(
                channel,
                &ChannelTransmissionFilter { confirmation: None },
                &first_page(),
            )
            .await;
        match page {
            Ok(page) => page.into_parts().0,
            Err(error) => panic!("transmissions: {error:?}"),
        }
    }

    /// The one channel stored, if exactly one is.
    pub(crate) async fn only_channel(&self) -> Option<Channel> {
        match self.channels().await.as_slice() {
            [only] => Some(only.channel().clone()),
            _ => None,
        }
    }
}

fn first_page<L>() -> PageRequest<L> {
    match PageSize::new(100) {
        Ok(size) => PageRequest { size, after: None },
        Err(error) => panic!("page size: {error:?}"),
    }
}

/// The consumer's settings for tests: the fixture timing, one-second ticks.
pub(crate) fn settings(shards: usize) -> Settings {
    Settings {
        timing: timing(),
        shards: NonZeroUsize::new(shards).unwrap_or(NonZeroUsize::MIN),
        tick_every: Duration::from_secs(1),
    }
}

/// A consumer over `stores`, publishing to `bus`.
pub(crate) fn consumer<B>(
    stores: &Stores,
    bus: B,
    clock: Arc<dyn Clock>,
    shards: usize,
) -> Consumer<B>
where
    B: EventBus + Send + Sync,
{
    FlowConsumer::new(
        settings(shards),
        FlowDeps {
            registry: stores.registry.clone(),
            transmissions: stores.transmissions.clone(),
            agents: stores.agents.clone(),
            bus,
            clock,
            entropy: SeededRandom::new(7),
        },
    )
}

/// A bus that records what is published and delivers nothing.
#[derive(Debug, Clone, Default)]
pub(crate) struct RecordingBus {
    published: Arc<Mutex<Vec<Envelope>>>,
}

impl RecordingBus {
    pub(crate) fn events(&self) -> Vec<BusEvent> {
        match self.published.lock() {
            Ok(published) => published
                .iter()
                .map(|envelope| envelope.event.clone())
                .collect(),
            Err(poisoned) => poisoned
                .into_inner()
                .iter()
                .map(|envelope| envelope.event.clone())
                .collect(),
        }
    }

    pub(crate) fn subjects(&self) -> Vec<Subject> {
        self.events().iter().map(BusEvent::subject).collect()
    }
}

/// A subscription that never delivers.
#[derive(Debug)]
pub(crate) struct Silent;

impl Subscription for Silent {
    async fn next(&mut self) -> Option<Result<Delivery, BusError>> {
        None
    }

    async fn ack(&mut self, _id: DeliveryId) -> Result<(), BusError> {
        Ok(())
    }

    async fn nack(
        &mut self,
        _id: DeliveryId,
        _retry_after: Duration,
        _reason: String,
    ) -> Result<(), BusError> {
        Ok(())
    }
}

impl EventBus for RecordingBus {
    type Subscription = Silent;

    async fn publish(&self, envelope: Envelope) -> Result<(), BusError> {
        match self.published.lock() {
            Ok(mut published) => published.push(envelope),
            Err(poisoned) => poisoned.into_inner().push(envelope),
        }
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

/// A wiki page: the URL the wiki tools read and write.
pub(crate) fn wiki_page(name: &str) -> Locator {
    Locator::Url {
        scheme: "https".to_owned(),
        host: Host("wiki.example".to_owned()),
        path: format!("/wiki/{name}"),
        query: None,
    }
}

/// A write by `agent` of `locator` at `at`, holding `spans`.
pub(crate) fn write(
    scene: &mut Scene,
    agent: AgentId,
    locator: &Locator,
    at: Timestamp,
    spans: Vec<SpanId>,
) -> Observed<WriteCall> {
    Observed {
        id: scene.ids.access(),
        agent,
        exchange: scene.exchange(),
        at,
        locator: locator.clone(),
        via: Extraction::Structured,
        op: WriteCall {
            call: PartRef {
                message: scene.ids.message(),
                index: 0,
            },
            spans,
        },
    }
}

/// A read by `agent` of `locator` at `at`.
pub(crate) fn read(
    scene: &mut Scene,
    agent: AgentId,
    locator: &Locator,
    at: Timestamp,
) -> Observed<ReadResult> {
    Observed {
        id: scene.ids.access(),
        agent,
        exchange: scene.exchange(),
        at,
        locator: locator.clone(),
        via: Extraction::Structured,
        op: ReadResult {
            result: PartRef {
                message: scene.ids.message(),
                index: 0,
            },
        },
    }
}

/// A match of `span` (by `from`) in `read`'s tool result.
pub(crate) fn found_in(
    scene: &mut Scene,
    read: &Observed<ReadResult>,
    from: AgentId,
    span: SpanId,
) -> ContentMatch {
    scene.matched(
        from,
        read.agent,
        read.exchange,
        span,
        read.op.result,
        tool_result(read.exchange),
    )
}

/// `ContentMatched` for `content`.
pub(crate) fn matched(content: &ContentMatch) -> BusEvent {
    BusEvent::Detect(DetectEvent::ContentMatched(content.clone()))
}

/// The transmissions announced confirmed, by id.
pub(crate) fn confirmations(events: &[BusEvent]) -> Vec<crosstalk_spec::ids::TransmissionId> {
    events
        .iter()
        .filter_map(|event| match event {
            BusEvent::Detect(DetectEvent::TransmissionConfirmed { transmission, .. }) => {
                Some(*transmission)
            }
            _ => None,
        })
        .collect()
}

/// Every `ChannelDiscovered` among `events`.
pub(crate) fn discoveries(events: &[BusEvent]) -> Vec<ChannelId> {
    events
        .iter()
        .filter_map(|event| match event {
            BusEvent::Detect(DetectEvent::ChannelDiscovered { channel, .. }) => Some(*channel),
            _ => None,
        })
        .collect()
}
