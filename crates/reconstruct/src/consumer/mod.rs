//! The L3 bus consumer: group [`GROUP`], subject `exchange_captured`.
//!
//! For each `ExchangeCaptured` ([`ReconstructConsumer::handle`]):
//!
//! 1. Read the request's opening (its messages through the first user
//!    message) from the blob store and derive the evidence
//!    (`EvidenceDeriver`); an exchange carrying none is not attributed.
//! 2. Attribute it ([`attribute`]): resolve, then create, attach, advance
//!    or merge.
//! 3. Record its activity and, when it carries one, its harness claim
//!    against the attributed agent.
//! 4. Thread it (`Threader::thread`) under the attributed agent.
//! 5. Publish `AgentSeen` for each item of evidence newly attributed, then
//!    the `ConversationDelta`, each in an envelope whose id is a function
//!    of the exchange ([`crate::ids::derived_event_id`]), so a redelivery
//!    publishes the same envelopes again and consumers deduplicate them
//!    (`reconstruct.delta.single-envelope-per-exchange`). The stores
//!    publish their own events (`AgentMerged`, `Changed`) through their
//!    sinks.
//!
//! [`run`] drives a subscription: a handled delivery is acked; a failed
//! one is nacked and redelivered after a backoff, then dead-lettered when
//! the group's retry budget is spent. Every step is idempotent under
//! redelivery: resolution finds the agent created before, evidence already
//! attached is not attached (or announced) again, claims and activity keep
//! the latest time, and threading returns the recorded outcome.
//!
//! A gateway wires it as a pipeline stage: subscribe [`subjects`] in
//! [`group`] on the bus, then spawn [`run`] with a [`ReconstructConsumer`]
//! over the agent store, a threader, the blob store and the bus.

pub mod attribute;

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use crosstalk_spec::events::ingest::IngestEvent;
use crosstalk_spec::events::{BusEvent, Envelope, Subject};
use crosstalk_spec::ids::AgentId;
use crosstalk_spec::ids::mint::UlidExhausted;
use crosstalk_spec::interfaces::l2_transport::{
    BlobStore, BusError, ConsumerGroup, EventBus, Subscription,
};
use crosstalk_spec::interfaces::l3_reconstruction::agents::{
    ActivityStore, AgentReadError, AgentReads,
};
use crosstalk_spec::interfaces::l3_reconstruction::lifecycle::{
    AgentLifecycle, AgentLifecycleError,
};
use crosstalk_spec::interfaces::l3_reconstruction::{
    AgentDirectory, ClaimStore, EvidenceDeriver, IdentityResolver, ResolveError, ThreadError,
    ThreadOutcome, Threader,
};
use crosstalk_spec::observed::exchange::Exchange;
use crosstalk_spec::observed::message::{Message, Role};
use crosstalk_spec::support::{NonEmpty, Timestamp};
use serde::{Deserialize, Serialize};

pub use attribute::{Attribution, attribute, corroborated, derive_parent};

use crate::error::StorageFailure;
use crate::ids::{IdSource, derived_event_id};
use crate::thread::MessageReader;

/// The consumer group L3 reads with.
pub const GROUP: &str = "reconstruct";

/// The group as the bus names it.
pub fn group() -> ConsumerGroup {
    ConsumerGroup(GROUP.to_owned())
}

/// The subjects the group subscribes to.
pub fn subjects() -> [Subject; 1] {
    [Subject::ExchangeCaptured]
}

/// Every L3 agent store trait, as one bound.
pub trait AgentStore:
    AgentDirectory
    + IdentityResolver
    + AgentLifecycle
    + ClaimStore
    + ActivityStore
    + AgentReads
    + Send
    + Sync
{
}

impl<T> AgentStore for T where
    T: AgentDirectory
        + IdentityResolver
        + AgentLifecycle
        + ClaimStore
        + ActivityStore
        + AgentReads
        + Send
        + Sync
{
}

/// Why handling a delivery failed. The delivery is nacked and redelivered.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ConsumeError {
    #[error("agent store: {0:?}")]
    Resolve(ResolveError),
    #[error("agent lifecycle: {0:?}")]
    Lifecycle(AgentLifecycleError),
    #[error("agent reads: {0:?}")]
    Reads(AgentReadError),
    #[error("threading: {0:?}")]
    Thread(ThreadError),
    #[error("message bodies: {0}")]
    Messages(String),
    #[error("no agent id is left to mint: {0}")]
    Ids(UlidExhausted),
    #[error("publishing: {0:?}")]
    Publish(BusError),
}

impl From<ResolveError> for ConsumeError {
    fn from(error: ResolveError) -> Self {
        Self::Resolve(error)
    }
}

impl From<AgentLifecycleError> for ConsumeError {
    fn from(error: AgentLifecycleError) -> Self {
        Self::Lifecycle(error)
    }
}

impl From<AgentReadError> for ConsumeError {
    fn from(error: AgentReadError) -> Self {
        Self::Reads(error)
    }
}

impl From<ThreadError> for ConsumeError {
    fn from(error: ThreadError) -> Self {
        Self::Thread(error)
    }
}

impl From<StorageFailure> for ConsumeError {
    fn from(failure: StorageFailure) -> Self {
        Self::Messages(failure.to_string())
    }
}

/// What handling one delivery did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Handled {
    /// Attributed to `agent` and threaded.
    Threaded {
        agent: AgentId,
        outcome: ThreadOutcome,
    },
    /// The exchange carries no identity evidence.
    Unattributed,
    /// The evidence points at several agents and no merge settled it.
    Review { candidates: NonEmpty<AgentId> },
    /// Not an `ExchangeCaptured`.
    Ignored,
}

/// The consumer's counters.
#[derive(Debug, Default)]
pub struct ConsumerStats {
    threaded: AtomicU64,
    unattributed: AtomicU64,
    review: AtomicU64,
    failed: AtomicU64,
}

/// A reading of [`ConsumerStats`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct ConsumerCounts {
    pub threaded: u64,
    pub unattributed: u64,
    pub review: u64,
    pub failed: u64,
}

impl ConsumerStats {
    pub fn snapshot(&self) -> ConsumerCounts {
        ConsumerCounts {
            threaded: self.threaded.load(Ordering::Relaxed),
            unattributed: self.unattributed.load(Ordering::Relaxed),
            review: self.review.load(Ordering::Relaxed),
            failed: self.failed.load(Ordering::Relaxed),
        }
    }
}

/// L3's consumer of `ExchangeCaptured`.
pub struct ReconstructConsumer<A, T, B, D, X, E> {
    agents: A,
    threader: T,
    messages: Arc<MessageReader<B>>,
    deriver: D,
    agent_ids: X,
    bus: Arc<E>,
    stats: Arc<ConsumerStats>,
}

impl<A, T, B, D, X, E> std::fmt::Debug for ReconstructConsumer<A, T, B, D, X, E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReconstructConsumer")
            .field("stats", &self.stats.snapshot())
            .finish_non_exhaustive()
    }
}

/// The parts a [`ReconstructConsumer`] is built from.
pub struct ConsumerParts<A, T, B, D, X, E> {
    /// The L3 agent store (`PgAgents`, or a reference store in tests).
    pub agents: A,
    pub threader: T,
    /// The blob store reader shared with the threader.
    pub messages: Arc<MessageReader<B>>,
    pub deriver: D,
    /// Where new agents' ids come from, minted at each exchange's start.
    pub agent_ids: X,
    /// Where `ConversationDelta` and `AgentSeen` are published.
    pub bus: Arc<E>,
}

impl<A, T, B, D, X, E> ReconstructConsumer<A, T, B, D, X, E>
where
    A: AgentStore,
    T: Threader + Send,
    B: BlobStore + Send + Sync,
    D: EvidenceDeriver + Send + Sync,
    X: IdSource<AgentId>,
    E: EventBus + Send + Sync,
{
    pub fn new(parts: ConsumerParts<A, T, B, D, X, E>) -> Self {
        Self {
            agents: parts.agents,
            threader: parts.threader,
            messages: parts.messages,
            deriver: parts.deriver,
            agent_ids: parts.agent_ids,
            bus: parts.bus,
            stats: Arc::new(ConsumerStats::default()),
        }
    }

    /// The counters, shared with health endpoints.
    pub fn stats(&self) -> Arc<ConsumerStats> {
        Arc::clone(&self.stats)
    }

    /// The agent store.
    pub fn agents(&self) -> &A {
        &self.agents
    }

    /// The threader.
    pub fn threader(&self) -> &T {
        &self.threader
    }

    /// The request's messages through its first user message: what the
    /// evidence derivers read (the prompt fingerprint is the system prompt
    /// plus the first user turn; the other sources read no message).
    async fn opening(&self, exchange: &Exchange) -> Result<Vec<Message>, ConsumeError> {
        let mut opening = Vec::new();
        for hash in &exchange.request {
            let facts = self.messages.facts(*hash).await?;
            opening.push(self.messages.message(*hash).await?);
            if facts.role == Role::User {
                break;
            }
        }
        Ok(opening)
    }

    /// Handle one envelope.
    pub async fn handle(&mut self, envelope: &Envelope) -> Result<Handled, ConsumeError> {
        let BusEvent::Ingest(IngestEvent::ExchangeCaptured(exchange)) = &envelope.event else {
            return Ok(Handled::Ignored);
        };
        let result = self.process(exchange, envelope.at).await;
        let counter = match &result {
            Ok(Handled::Threaded { .. }) => &self.stats.threaded,
            Ok(Handled::Unattributed) => &self.stats.unattributed,
            Ok(Handled::Review { .. }) => &self.stats.review,
            Ok(Handled::Ignored) => return result,
            Err(_) => &self.stats.failed,
        };
        counter.fetch_add(1, Ordering::Relaxed);
        result
    }

    async fn process(&mut self, exchange: &Exchange, at: Timestamp) -> Result<Handled, ConsumeError> {
        let meta = &exchange.meta;
        let opening = self.opening(exchange).await?;
        let Some(evidence) = NonEmpty::from_vec(self.deriver.derive(meta, &opening)) else {
            tracing::debug!(exchange = %meta.id.ulid_text(), "exchange carries no identity evidence; not attributed");
            return Ok(Handled::Unattributed);
        };
        let attribution =
            attribute::attribute(&mut self.agents, &self.agent_ids, meta, evidence).await?;
        let (agent, seen) = match attribution {
            Attribution::Agent { agent, seen, .. } => (agent, seen),
            Attribution::Review {
                candidates,
                evidence,
            } => {
                tracing::warn!(
                    exchange = %meta.id.ulid_text(),
                    candidates = ?candidates.iter().map(|id| id.ulid_text()).collect::<Vec<_>>(),
                    deciding = evidence.iter().count(),
                    "identity conflict left for operator review"
                );
                return Ok(Handled::Review { candidates });
            }
        };
        ActivityStore::record(&mut self.agents, agent, meta.started_at).await?;
        if let Some(claim) = &meta.client.harness {
            ClaimStore::record(&mut self.agents, agent, claim, meta.started_at).await?;
        }
        let outcome = self.threader.thread(exchange, agent).await?;
        for item in seen {
            let salt = crate::agents::codec::json(&(agent, &item))
                .map_err(|error| ConsumeError::Messages(error.to_string()))?;
            self.publish(Envelope {
                id: derived_event_id(meta.id, "agent-seen", salt.as_bytes()),
                at,
                event: BusEvent::Ingest(IngestEvent::AgentSeen {
                    agent,
                    evidence: item,
                }),
            })
            .await?;
        }
        self.publish(Envelope {
            id: derived_event_id(meta.id, "conversation-delta", &[]),
            at,
            event: BusEvent::Ingest(IngestEvent::ConversationDelta(outcome.delta().clone())),
        })
        .await?;
        Ok(Handled::Threaded { agent, outcome })
    }

    async fn publish(&self, envelope: Envelope) -> Result<(), ConsumeError> {
        self.bus
            .publish(envelope)
            .await
            .map_err(ConsumeError::Publish)
    }
}

/// How long a failed delivery waits before the bus redelivers it (clamped
/// to the group's retry policy by the bus).
const RETRY_AFTER: Duration = Duration::from_millis(200);

/// Handle every delivery of `subscription` until the bus shuts down.
pub async fn run<S, A, T, B, D, X, E>(
    mut subscription: S,
    mut consumer: ReconstructConsumer<A, T, B, D, X, E>,
) where
    S: Subscription,
    A: AgentStore,
    T: Threader + Send,
    B: BlobStore + Send + Sync,
    D: EvidenceDeriver + Send + Sync,
    X: IdSource<AgentId>,
    E: EventBus + Send + Sync,
{
    tracing::info!(group = GROUP, "reconstruct consumer started");
    while let Some(next) = subscription.next().await {
        let delivery = match next {
            Ok(delivery) => delivery,
            Err(error) => {
                tracing::warn!(group = GROUP, error = ?error, "undecodable delivery skipped");
                continue;
            }
        };
        let event = delivery.envelope.id.ulid_text();
        match consumer.handle(&delivery.envelope).await {
            Ok(handled) => {
                tracing::debug!(group = GROUP, event = %event, handled = ?handled_kind(&handled), "exchange reconstructed");
                if let Err(error) = subscription.ack(delivery.id).await {
                    tracing::warn!(group = GROUP, event = %event, error = ?error, "ack failed; the bus will redeliver");
                }
            }
            Err(error) => {
                tracing::error!(group = GROUP, event = %event, attempt = delivery.attempt.get(), error = %error, "reconstruction failed");
                if let Err(error) = subscription
                    .nack(delivery.id, RETRY_AFTER, error.to_string())
                    .await
                {
                    tracing::warn!(group = GROUP, event = %event, error = ?error, "nack failed");
                }
            }
        }
    }
    tracing::info!(group = GROUP, "reconstruct consumer stopped");
}

fn handled_kind(handled: &Handled) -> &'static str {
    match handled {
        Handled::Threaded { .. } => "threaded",
        Handled::Unattributed => "unattributed",
        Handled::Review { .. } => "review",
        Handled::Ignored => "ignored",
    }
}
