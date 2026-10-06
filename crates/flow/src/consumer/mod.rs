//! The L5 flow consumer (group [`GROUP`]): records accesses, correlates
//! them with content matches, and applies what the correlator decides to
//! the stores, through the spec's traits only.
//!
//! ```text
//! extraction step ── Extracted (local input) ─┐   bus: ExchangeCaptured, ContentMatched,
//!   Write without result ─▶ HeldWrites         │        ChannelDiscovered, ChannelPromoted,
//!                                              │        AgentMerged/Unmerged
//!     result or settle (Unknown) ─┐            │              │
//!                                 ▼            ▼              ▼
//!                          steps, in order (one queue; a failed step waits at its head)
//!   Read / Write: ChannelTraffic::add_resource (resource for the locator, on the lookup's channel)
//!                 ChannelTraffic::record_access ─▶ publish AccessRecorded { access, channel }
//!                 Correlate (writes that pair only) ─▶ Shards::access
//!   Content:      AgentReads::cluster for kinship ─▶ Shards::content
//!   Exchange:     Shards::exchange
//!   Discovered:   Shards::rekey(resource ─▶ channel)   Promoted: Shards::rekey(superseded ─▶ promoted)
//!   Tick(clock):  Shards::tick
//!   Decide:       OpenChannel on a resource ─▶ ChannelTraffic::discover ─▶ Shards::rekey (handoff)
//!                 TransmissionStore::save ─▶ Record: ChannelTraffic::record_transmission (channel routes)
//!                 ─▶ Publish: ChannelCrossAccessed / TransmissionConfirmed / TransmissionSuspected
//! ```
//!
//! Each step runs once its predecessors did, so a store write commits
//! before the event announcing it is published, and the correlator sees
//! its inputs in arrival order. A step that fails transiently stays at the
//! head of the queue and is retried before anything else; a permanent
//! failure is logged and dropped.
//!
//! **Durability** ([`FlowDurability`]). A volatile consumer (memory mode)
//! acks each bus delivery once its steps ran. A durable one keeps held
//! writes and tool calls in its store as it takes them, and acks a
//! delivery only once a checkpoint of its shards covers it
//! ([`FlowConsumer::checkpoint`], every `Settings::checkpoint_every` or
//! once `Settings::max_unacked` deliveries wait). After a restart
//! [`FlowConsumer::restore`] loads the checkpoint and the held writes and
//! re-feeds the accesses and tool calls recorded after it; the bus
//! redelivers what was not acked (`flow.consumer.restore-equivalent`).
//! Everything it publishes carries an envelope id derived from the event
//! ([`publish`]), so a repeat lands on the id the bus already holds.
//!
//! **Time.** Every window closes on ticks, and a tick's time is read from
//! the injected `Clock`: under replay that is the replay clock, so corpus
//! timestamps far in the past settle on the replay's time, never on wall
//! time. [`FlowConsumer::tick`] drives a tick directly.

mod apply;
pub mod checkpoint;
pub mod durability;
pub mod error;
pub mod held;
pub mod input;
pub mod publish;
mod resources;
mod restore;
pub mod settings;
pub mod shards;

#[cfg(test)]
pub(crate) mod tests;

use std::collections::VecDeque;
use std::sync::Arc;

use crosstalk_spec::derived::flow::access::Access;
use crosstalk_spec::derived::flow::resource::Locator;
use crosstalk_spec::derived::flow::transmission::Transmission;
use crosstalk_spec::derived::provenance::matching::ContentMatch;
use crosstalk_spec::events::detect::DetectEvent;
use crosstalk_spec::events::ingest::IngestEvent;
use crosstalk_spec::events::{BusEvent, Subject};
use crosstalk_spec::ids::{AccessId, AgentId, ChannelId, ExchangeId, ResourceId};
use crosstalk_spec::interfaces::l2_transport::{
    ConsumerGroup, DeliveryId, EventBus, Subscription,
};
use crosstalk_spec::interfaces::l3_reconstruction::agents::AgentReads;
use crosstalk_spec::interfaces::l5_flow::ChannelRegistry;
use crosstalk_spec::interfaces::l5_flow::channels::ChannelTraffic;
use crosstalk_spec::interfaces::l5_flow::transmissions::TransmissionStore;
use crosstalk_spec::support::{Clock, Timestamp};
use tokio::time::MissedTickBehavior;

pub use self::checkpoint::{
    Checkpoint, CheckpointError, FlowRestoreError, Incompatible, SNAPSHOT_FORMAT, ShardSnapshot,
    StoredCheckpoint,
};
pub use self::durability::{
    DurabilityError, FlowDurability, MemoryDurability, Recorded, Recovered, Resolved, Volatile,
};
pub use self::error::StepError;
pub use self::held::HeldWrites;
pub use self::input::{
    DurableInputs, Extracted, ExtractedBatch, FlowInputs, InputSource, NotDurable, Observed,
    ReadResult, ToolCalled, WriteCall,
};
pub use self::publish::{PublishError, Publisher, envelope_id};
pub use self::restore::{Checkpointed, Restored};
pub use self::settings::{FlowConfig, InvalidFlowConfig, Settings};
pub use self::shards::Shards;
use crate::correlate::pairing::{self, WriteOutcome};
use crate::correlate::{Decided, Kin, MediumKey};

/// The consumer group the flow consumer reads with.
pub const GROUP: &str = "flow";

/// The group as the bus names it.
pub fn group() -> ConsumerGroup {
    ConsumerGroup(GROUP.to_owned())
}

/// The subjects the flow consumer subscribes to.
pub const SUBJECTS: [Subject; 6] = [
    Subject::ExchangeCaptured,
    Subject::ContentMatched,
    Subject::ChannelDiscovered,
    Subject::ChannelPromoted,
    Subject::AgentMerged,
    Subject::AgentUnmerged,
];

/// The stores, bus and clock the consumer runs over.
pub struct FlowDeps<R, T, A, B> {
    /// `ChannelRegistry` (lookups) and `ChannelTraffic` (the writes).
    pub registry: R,
    pub transmissions: T,
    /// For `Delegation`: each agent's canonical agent and parent.
    pub agents: A,
    pub bus: B,
    /// Ticks, checkpoint times and envelope times: the replay clock under
    /// replay.
    pub clock: Arc<dyn Clock>,
}

/// One unit of the consumer's work.
#[derive(Debug, Clone, PartialEq)]
enum Step {
    Read(Observed<ReadResult>),
    /// A write with its outcome; `held` when it was released from the held
    /// writes, whose stored row goes once its access is recorded.
    Write {
        write: Observed<WriteCall>,
        outcome: WriteOutcome,
        held: bool,
    },
    /// Hold a write until its result or its settle time.
    Hold(Observed<WriteCall>),
    /// A held write's result arrived.
    Release {
        access: AccessId,
        outcome: WriteOutcome,
    },
    /// A released write's access is recorded: drop its stored hold.
    Unhold(AccessId),
    /// A tool call: recorded, then named in every shard.
    ToolCalled(ToolCalled),
    /// A tool call recorded before a restart, named again.
    ToolNamed(ToolCalled),
    /// An access recorded before a restart, after the checkpoint: taken
    /// again exactly as when it was first recorded.
    Refeed {
        access: Access,
        locator: Locator,
        resolved: Resolved,
    },
    Correlate(Access, Option<ChannelId>),
    Content(ContentMatch),
    Exchange(ExchangeId, Timestamp),
    /// A channel discovered from `resource`, by this consumer or another.
    Discovered {
        resource: ResourceId,
        channel: ChannelId,
    },
    Promoted {
        channel: ChannelId,
        superseded: Vec<ChannelId>,
    },
    Tick(Timestamp),
    Decide(Decided),
    Record(Transmission),
    Publish(BusEvent),
}

/// The flow consumer, keeping what survives a restart in `D`.
pub struct FlowConsumer<R, T, A, B, D = Volatile> {
    settings: Settings,
    registry: R,
    transmissions: T,
    agents: A,
    publisher: Publisher<B>,
    clock: Arc<dyn Clock>,
    shards: Shards,
    held: HeldWrites,
    backlog: VecDeque<Step>,
    durability: D,
}

impl<R, T, A, B> FlowConsumer<R, T, A, B, Volatile>
where
    R: ChannelRegistry + ChannelTraffic + Send + Sync,
    T: TransmissionStore + Send + Sync,
    A: AgentReads + Send + Sync,
    B: EventBus + Send + Sync,
{
    /// A volatile consumer: nothing it holds survives the process.
    pub fn new(settings: Settings, deps: FlowDeps<R, T, A, B>) -> Self {
        Self::with_durability(settings, deps, Volatile)
    }
}

impl<R, T, A, B, D> FlowConsumer<R, T, A, B, D>
where
    R: ChannelRegistry + ChannelTraffic + Send + Sync,
    T: TransmissionStore + Send + Sync,
    A: AgentReads + Send + Sync,
    B: EventBus + Send + Sync,
    D: FlowDurability,
{
    /// A consumer keeping its held writes, tool calls and checkpoints in
    /// `durability`. A durable one is [`FlowConsumer::restore`]d before it
    /// takes any input.
    pub fn with_durability(settings: Settings, deps: FlowDeps<R, T, A, B>, durability: D) -> Self {
        let FlowDeps {
            registry,
            transmissions,
            agents,
            bus,
            clock,
        } = deps;
        Self {
            shards: Shards::new(settings.timing, settings.content_retention, settings.shards),
            publisher: Publisher::new(bus, Arc::clone(&clock)),
            settings,
            registry,
            transmissions,
            agents,
            clock,
            held: HeldWrites::default(),
            backlog: VecDeque::new(),
            durability,
        }
    }

    pub fn registry(&self) -> &R {
        &self.registry
    }

    pub fn transmissions(&self) -> &T {
        &self.transmissions
    }

    pub fn shards(&self) -> &Shards {
        &self.shards
    }

    pub fn held_writes(&self) -> &HeldWrites {
        &self.held
    }

    pub fn durability(&self) -> &D {
        &self.durability
    }

    pub fn settings(&self) -> &Settings {
        &self.settings
    }

    /// Steps waiting for a retry.
    pub fn backlog(&self) -> usize {
        self.backlog.len()
    }

    /// One bus event. Events the consumer does not use are ignored.
    pub async fn handle_event(&mut self, event: &BusEvent) {
        match event {
            BusEvent::Ingest(IngestEvent::ExchangeCaptured(exchange)) => {
                self.queue(Step::Exchange(exchange.meta.id, exchange.meta.started_at));
            }
            BusEvent::Ingest(
                IngestEvent::AgentMerged { .. } | IngestEvent::AgentUnmerged { .. },
            ) => {
                self.shards.forget_kin();
            }
            BusEvent::Detect(DetectEvent::ContentMatched(content)) => {
                self.queue(Step::Content(content.clone()));
            }
            BusEvent::Detect(DetectEvent::ChannelDiscovered { channel, seed }) => {
                self.queue(Step::Discovered {
                    resource: seed.resource,
                    channel: *channel,
                });
            }
            BusEvent::Detect(DetectEvent::ChannelPromoted {
                channel,
                superseded,
                ..
            }) => {
                self.queue(Step::Promoted {
                    channel: *channel,
                    superseded: superseded.clone(),
                });
            }
            BusEvent::Ingest(_)
            | BusEvent::Detect(_)
            | BusEvent::Insight(_)
            | BusEvent::Changed(_) => {}
        }
        self.drain().await;
    }

    /// One input from the extraction step.
    pub async fn handle_extracted(&mut self, input: Extracted) {
        self.queue(Self::step_of(input));
        self.drain().await;
    }

    /// A batch from the extraction step, in order: `Ok` once every step
    /// it caused ran, so its accesses, held writes and tool calls are
    /// stored; `Backlogged` when a step failed transiently and waits in
    /// the queue (the caller retries its delta; every input is
    /// idempotent here).
    pub async fn handle_batch(&mut self, inputs: Vec<Extracted>) -> Result<(), NotDurable> {
        for input in inputs {
            self.queue(Self::step_of(input));
        }
        self.drain().await;
        match self.backlog.is_empty() {
            true => Ok(()),
            false => Err(NotDurable::Backlogged),
        }
    }

    fn step_of(input: Extracted) -> Step {
        match input {
            Extracted::Read(read) => Step::Read(read),
            Extracted::Write {
                write,
                outcome: Some(outcome),
            } => Step::Write {
                write,
                outcome,
                held: false,
            },
            Extracted::Write {
                write,
                outcome: None,
            } => Step::Hold(write),
            Extracted::WriteResult { access, outcome } => Step::Release { access, outcome },
            Extracted::ToolCall {
                agent,
                call,
                name,
                at,
            } => Step::ToolCalled(ToolCalled {
                agent,
                call,
                name,
                at,
            }),
        }
    }

    /// A tick at `now`: release the writes whose settle window closed (as
    /// `Unknown`), then close windows and expire suspicions up to `now`.
    pub async fn tick(&mut self, now: Timestamp) {
        for (write, outcome) in self.held.settle(now) {
            self.queue(Step::Write {
                write,
                outcome,
                held: true,
            });
        }
        self.queue(Step::Tick(now));
        self.drain().await;
    }

    /// Consume `subscription` and `inputs`, ticking every
    /// `Settings::tick_every` on the injected clock, until the bus shuts
    /// down. A volatile consumer acks each delivery once its steps ran; a
    /// durable one keeps them unacked until a checkpoint covers them
    /// (every `Settings::checkpoint_every`, or once
    /// `Settings::max_unacked` wait). Batches waiting on their durability
    /// are answered once their steps ran. The consumer stays usable
    /// afterwards (its stores and shards can be read).
    pub async fn run<S: Subscription, I: InputSource>(&mut self, mut subscription: S, mut inputs: I) {
        let durable = self.durability.survives_restart();
        tracing::info!(
            group = GROUP,
            shards = self.shards.count(),
            durable,
            "flow consumer started"
        );
        let mut ticker = tokio::time::interval(self.settings.tick_every);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
        let mut checkpoints = tokio::time::interval(self.settings.checkpoint_every);
        checkpoints.set_missed_tick_behavior(MissedTickBehavior::Delay);
        let mut unacked: Vec<DeliveryId> = Vec::new();
        let mut inputs_open = true;
        loop {
            tokio::select! {
                biased;
                _ = ticker.tick() => {
                    let now = self.clock.now();
                    self.tick(now).await;
                }
                _ = checkpoints.tick(), if durable => {
                    self.checkpoint_and_ack(&mut subscription, &mut unacked).await;
                }
                batch = inputs.recv(), if inputs_open => match batch {
                    Some(batch) => self.answer(batch).await,
                    None => inputs_open = false,
                },
                next = subscription.next() => match next {
                    None => break,
                    Some(Err(error)) => {
                        tracing::warn!(group = GROUP, error = ?error, "undecodable delivery skipped");
                    }
                    Some(Ok(delivery)) => {
                        self.handle_event(&delivery.envelope.event).await;
                        if durable {
                            unacked.push(delivery.id);
                            if unacked.len() >= self.settings.max_unacked.get() {
                                self.checkpoint_and_ack(&mut subscription, &mut unacked).await;
                            }
                        } else if let Err(error) = subscription.ack(delivery.id).await {
                            let event = delivery.envelope.id.ulid_text();
                            tracing::warn!(group = GROUP, event = %event, error = ?error, "ack failed; the bus will redeliver");
                        }
                    }
                },
            }
        }
        if durable {
            self.checkpoint_and_ack(&mut subscription, &mut unacked).await;
        }
        tracing::info!(
            group = GROUP,
            backlog = self.backlog.len(),
            unacked = unacked.len(),
            "flow consumer stopped"
        );
    }

    /// Handle `batch` and answer whoever waits on it.
    pub async fn answer(&mut self, batch: ExtractedBatch) {
        let ExtractedBatch { inputs, reply } = batch;
        let outcome = self.handle_batch(inputs).await;
        if let Some(reply) = reply
            && reply.send(outcome).is_err()
        {
            tracing::debug!(group = GROUP, "nobody waits for the batch's answer");
        }
    }

    /// Checkpoint, then ack every delivery the checkpoint covers. A
    /// checkpoint that cannot be taken now leaves them for the next.
    async fn checkpoint_and_ack<S: Subscription>(
        &mut self,
        subscription: &mut S,
        unacked: &mut Vec<DeliveryId>,
    ) {
        match self.checkpoint().await {
            Ok(_) => {
                for id in unacked.drain(..) {
                    if let Err(error) = subscription.ack(id).await {
                        tracing::warn!(group = GROUP, error = ?error, "ack failed; the bus will redeliver what the checkpoint holds");
                    }
                }
            }
            Err(error) => {
                tracing::warn!(group = GROUP, error = %error, unacked = unacked.len(), "checkpoint not taken; its deliveries stay unacked");
            }
        }
    }

    fn queue(&mut self, step: Step) {
        self.backlog.push_back(step);
    }

    /// Run queued steps in order until the queue is empty or a step fails
    /// transiently.
    async fn drain(&mut self) {
        while let Some(step) = self.backlog.pop_front() {
            match self.execute(step.clone()).await {
                Ok(produced) => {
                    for next in produced.into_iter().rev() {
                        self.backlog.push_front(next);
                    }
                }
                Err(error) if error.is_permanent() => {
                    tracing::error!(group = GROUP, error = %error, "flow step failed for good; dropped");
                }
                Err(error) => {
                    tracing::warn!(group = GROUP, error = %error, backlog = self.backlog.len() + 1, "flow step failed; retried with the next input");
                    self.backlog.push_front(step);
                    return;
                }
            }
        }
    }

    async fn execute(&mut self, step: Step) -> Result<Vec<Step>, StepError> {
        match step {
            Step::Read(read) => self.record_read(read).await,
            Step::Write {
                write,
                outcome,
                held,
            } => {
                let id = write.id;
                let mut steps = self.record_write(write, outcome).await?;
                if held {
                    steps.insert(0, Step::Unhold(id));
                }
                Ok(steps)
            }
            Step::Hold(write) => {
                if !self.held.contains(write.id) {
                    let settles_at = pairing::write_settles_at(self.settings.timing, write.at);
                    self.durability.hold(&write, settles_at).await?;
                    self.held.hold(write, self.settings.timing);
                }
                Ok(Vec::new())
            }
            Step::Release { access, outcome } => match self.held.release(access) {
                Some(write) => Ok(vec![Step::Write {
                    write,
                    outcome,
                    held: true,
                }]),
                None => {
                    tracing::debug!(access = %access.ulid_text(), "result for a write not held; ignored");
                    Ok(Vec::new())
                }
            },
            Step::Unhold(access) => {
                self.durability.release(access).await?;
                Ok(Vec::new())
            }
            Step::ToolCalled(call) => {
                self.durability.tool_called(&call).await?;
                self.shards
                    .tool_named(call.agent, &call.call, &call.name, call.at);
                Ok(Vec::new())
            }
            Step::ToolNamed(call) => {
                self.shards
                    .tool_named(call.agent, &call.call, &call.name, call.at);
                Ok(Vec::new())
            }
            Step::Refeed {
                access,
                locator,
                resolved,
            } => self.refeed(access, locator, resolved).await,
            Step::Correlate(access, channel) => Ok(decisions(self.shards.access(&access, channel))),
            Step::Content(content) => {
                self.refresh_kin(content.origin_agent()).await;
                self.refresh_kin(content.reader()).await;
                Ok(decisions(self.shards.content(&content)))
            }
            Step::Exchange(exchange, started_at) => {
                Ok(decisions(self.shards.exchange(exchange, started_at)))
            }
            Step::Discovered { resource, channel } => Ok(decisions(
                self.shards
                    .rekey(MediumKey::Resource(resource), MediumKey::Channel(channel)),
            )),
            Step::Promoted {
                channel,
                superseded,
            } => {
                let mut out = Vec::new();
                for old in superseded {
                    out.extend(
                        self.shards
                            .rekey(MediumKey::Channel(old), MediumKey::Channel(channel)),
                    );
                }
                Ok(decisions(out))
            }
            Step::Tick(now) => Ok(decisions(self.shards.tick(now))),
            Step::Decide(decided) => self.decide(decided).await,
            Step::Record(transmission) => {
                self.registry.record_transmission(&transmission).await?;
                Ok(Vec::new())
            }
            Step::Publish(event) => {
                self.publisher.publish(event).await?;
                Ok(Vec::new())
            }
        }
    }

    /// Learn how `agent` resolves, unless every shard knows it already. A
    /// read failure leaves it unknown, so no `Delegation` is assumed.
    async fn refresh_kin(&mut self, agent: AgentId) {
        if self.shards.knows_agent(agent) {
            return;
        }
        match self.agents.cluster(agent).await {
            Ok(Some(cluster)) => {
                let profile = cluster.profile();
                self.shards.learn_kin(
                    agent,
                    Kin {
                        canonical: profile.id(),
                        parent: profile.parent(),
                    },
                );
            }
            Ok(None) => self.shards.learn_kin(agent, Kin::root(agent)),
            Err(error) => {
                tracing::warn!(agent = %agent.ulid_text(), error = ?error, "agent read failed; no delegation assumed");
            }
        }
    }
}

fn decisions(decided: Vec<Decided>) -> Vec<Step> {
    decided.into_iter().map(Step::Decide).collect()
}
