//! The consumer slots: where each layer's bus consumer plugs into a
//! [`Live`](super::Live) process.
//!
//! A layer consumer is a [`Stage`]: the subjects it reads, and a `handle`
//! for one envelope. It is built from a [`StageContext`] (the shared
//! stores, the publish path, the clock) and put in its [`Slot`] with
//! [`Stages::fill`]. `Live::start` subscribes every filled slot's consumer
//! group (named after the slot) before anything is published, then runs
//! each stage on its own task:
//!
//! ```text
//! bus ── group <slot> ──▶ Stage::handle(&envelope) ─ Ok ──────────▶ ack
//!                                                  ─ Err(Retry) ──▶ nack (redelivered, then dead-lettered)
//!                                                  ─ Err(Reject) ─▶ ack, logged at error
//! ```
//!
//! A stage writes the stores through the spec's write traits on
//! [`StageContext::stores`] (whose own events reach the bus through the
//! outbox) and publishes what it decides through [`Publisher`].

use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use crosstalk_api::MemoryStores;
use crosstalk_spec::events::{BusEvent, Envelope, Subject};
use crosstalk_spec::ids::EventId;
use crosstalk_spec::interfaces::l2_transport::{ConsumerGroup, RetryPolicy, Subscription};
use crosstalk_spec::support::{Clock, Timestamp};
use crosstalk_transport::{MpscBus, MpscSubscription};

use super::blobs::LiveBlobs;
use crate::pipeline::{Ingester, PublishError};

/// The stores a live process shares between its stages and its surface.
pub type LiveStores = MemoryStores<LiveBlobs>;

/// Where a stage publishes the events it decides: the pipeline's own
/// publish path, so every envelope gets an id from the one generator.
#[derive(Debug, Clone)]
pub struct Publisher {
    ingester: Ingester<LiveBlobs, MpscBus>,
}

impl Publisher {
    pub(crate) fn new(ingester: Ingester<LiveBlobs, MpscBus>) -> Self {
        Self { ingester }
    }

    /// Publish `event` stamped with the clock's reading.
    pub async fn publish(&self, event: BusEvent) -> Result<EventId, PublishError> {
        self.ingester.publish(event, self.ingester.now()).await
    }

    /// Publish `event` stamped `at`.
    pub async fn publish_at(
        &self,
        event: BusEvent,
        at: Timestamp,
    ) -> Result<EventId, PublishError> {
        self.ingester.publish(event, at).await
    }
}

/// What every stage is built from. Clones share everything.
#[derive(Clone)]
pub struct StageContext {
    /// The stores the surface reads: write them through the spec's traits.
    pub stores: LiveStores,
    pub publisher: Publisher,
    /// The clock the pipeline stamps with (a corpus or sim clock under
    /// replay).
    pub clock: Arc<dyn Clock>,
}

/// Why a stage did not handle an envelope.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum StageError {
    /// Worth another try (a store or the bus was unavailable): nacked,
    /// redelivered under the bus's retry policy, then dead-lettered.
    #[error("retry: {reason}")]
    Retry { reason: String },
    /// Another delivery would fail the same way: acked and logged.
    #[error("rejected: {reason}")]
    Reject { reason: String },
}

/// A bus consumer. Idempotent on the envelope id and the entity ids inside
/// it: delivery is at least once.
pub trait Stage: Send + 'static {
    /// The subjects its group subscribes to.
    fn subjects(&self) -> Vec<Subject>;

    /// Handle one delivered envelope.
    fn handle(
        &mut self,
        envelope: &Envelope,
    ) -> impl Future<Output = Result<(), StageError>> + Send;
}

/// Each place a consumer plugs in. Its consumer group is
/// [`Slot::group`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Slot {
    /// L3 identity and threading (`crosstalk-reconstruct`).
    L3Reconstruct,
    /// L4 provenance (`crosstalk-provenance`).
    L4Provenance,
    /// L5 extraction and correlation (`crosstalk-flow`).
    L5Flow,
    /// L6 classification (the gateway's minimal classifier, until
    /// `crosstalk-analysis` has a consumer).
    L6Classify,
    /// L7 edges (`crosstalk-topology`).
    L7Topology,
    /// Spans, accesses and resources into the surface's evidence records.
    Evidence,
    /// Every event but `ExchangeCaptured` to the surface's node facts and
    /// live feed.
    SurfaceRelay,
}

impl Slot {
    pub const ALL: [Slot; 7] = [
        Slot::L3Reconstruct,
        Slot::L4Provenance,
        Slot::L5Flow,
        Slot::L6Classify,
        Slot::L7Topology,
        Slot::Evidence,
        Slot::SurfaceRelay,
    ];

    pub fn name(self) -> &'static str {
        match self {
            Slot::L3Reconstruct => "l3-reconstruct",
            Slot::L4Provenance => "l4-provenance",
            Slot::L5Flow => "l5-flow",
            Slot::L6Classify => "l6-classify",
            Slot::L7Topology => "l7-topology",
            Slot::Evidence => "evidence",
            Slot::SurfaceRelay => "surface-relay",
        }
    }

    /// The slot's consumer group on the bus.
    pub fn group(self) -> ConsumerGroup {
        ConsumerGroup(format!("live-{}", self.name()))
    }
}

/// A slot was filled twice.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("the {} slot is already filled", .0.name())]
pub struct SlotTaken(pub Slot);

type Run = Box<dyn FnOnce(MpscSubscription, RetryPolicy) -> RunFuture + Send>;
type RunFuture = Pin<Box<dyn Future<Output = ()> + Send>>;

/// A stage, type-erased for its slot.
pub(crate) struct Plug {
    pub(crate) subjects: Vec<Subject>,
    pub(crate) run: Run,
}

/// The filled slots.
#[derive(Default)]
pub struct Stages {
    slots: BTreeMap<Slot, Plug>,
}

impl std::fmt::Debug for Stages {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Stages")
            .field("filled", &self.filled())
            .finish()
    }
}

impl Stages {
    /// Put `stage` in `slot`.
    pub fn fill<S: Stage>(&mut self, slot: Slot, stage: S) -> Result<(), SlotTaken> {
        if self.slots.contains_key(&slot) {
            return Err(SlotTaken(slot));
        }
        let plug = Plug {
            subjects: stage.subjects(),
            run: Box::new(move |subscription, retry| {
                Box::pin(run(slot, stage, subscription, retry))
            }),
        };
        self.slots.insert(slot, plug);
        Ok(())
    }

    /// The filled slots, in slot order.
    pub fn filled(&self) -> Vec<Slot> {
        self.slots.keys().copied().collect()
    }

    /// The slots nothing fills yet, in slot order.
    pub fn unfilled(&self) -> Vec<Slot> {
        Slot::ALL
            .into_iter()
            .filter(|slot| !self.slots.contains_key(slot))
            .collect()
    }

    pub(crate) fn into_plugs(self) -> BTreeMap<Slot, Plug> {
        self.slots
    }
}

/// Every subject but `ExchangeCaptured`: what the surface relay reads.
pub const DERIVED_SUBJECTS: [Subject; 27] = [
    Subject::ConversationDelta,
    Subject::AgentSeen,
    Subject::AgentMerged,
    Subject::AgentUnmerged,
    Subject::AgentRenamed,
    Subject::SpanOriginated,
    Subject::SpanRelayed,
    Subject::ContentMatched,
    Subject::AccessRecorded,
    Subject::ChannelDiscovered,
    Subject::ChannelCrossAccessed,
    Subject::DeclaredChannelUnused,
    Subject::ChannelPromoted,
    Subject::TransmissionConfirmed,
    Subject::TransmissionSuspected,
    Subject::VerdictSet,
    Subject::TransmissionClassified,
    Subject::TopicVersionReady,
    Subject::TopicVersionActivated,
    Subject::TopicVersionDropped,
    Subject::WatermarkAdvanced,
    Subject::EdgeUpdated,
    Subject::AlertOpened,
    Subject::AlertChanged,
    Subject::AlertRuleChanged,
    Subject::PolicyChanged,
    Subject::Changed,
];

/// Handle every delivery of `subscription` with `stage` until the bus
/// shuts down.
async fn run<S: Stage>(
    slot: Slot,
    mut stage: S,
    mut subscription: MpscSubscription,
    retry: RetryPolicy,
) {
    tracing::debug!(stage = slot.name(), "stage running");
    while let Some(next) = subscription.next().await {
        let delivery = match next {
            Ok(delivery) => delivery,
            Err(error) => {
                tracing::warn!(stage = slot.name(), error = ?error, "delivery failed");
                continue;
            }
        };
        let event = delivery.envelope.id.ulid_text();
        let settled = match stage.handle(&delivery.envelope).await {
            Ok(()) => subscription.ack(delivery.id).await,
            Err(StageError::Retry { reason }) => {
                tracing::warn!(
                    stage = slot.name(),
                    event = %event,
                    attempt = delivery.attempt.get(),
                    reason = %reason,
                    "stage will retry"
                );
                subscription
                    .nack(delivery.id, retry.initial_backoff(), reason)
                    .await
            }
            Err(StageError::Reject { reason }) => {
                tracing::error!(
                    stage = slot.name(),
                    event = %event,
                    reason = %reason,
                    "stage rejected an event"
                );
                subscription.ack(delivery.id).await
            }
        };
        if let Err(error) = settled {
            tracing::warn!(stage = slot.name(), event = %event, error = ?error, "delivery not settled");
        }
    }
    tracing::debug!(stage = slot.name(), "stage stopped: the bus shut down");
}
