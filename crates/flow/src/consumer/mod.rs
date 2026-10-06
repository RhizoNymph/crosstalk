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
//! failure is logged and dropped. Bus deliveries are acked once their
//! steps are queued: what the correlator absorbed is never redelivered.
//!
//! **Time.** Every window closes on ticks, and a tick's time is read from
//! the injected `Clock`: under replay that is the replay clock, so corpus
//! timestamps far in the past settle on the replay's time, never on wall
//! time. [`FlowConsumer::tick`] drives a tick directly.

mod apply;
pub mod error;
pub mod held;
pub mod input;
pub mod publish;
mod resources;
pub mod settings;
pub mod shards;

#[cfg(test)]
mod tests;

use std::collections::VecDeque;
use std::sync::Arc;

use crosstalk_spec::derived::flow::access::Access;
use crosstalk_spec::derived::flow::transmission::Transmission;
use crosstalk_spec::derived::provenance::matching::ContentMatch;
use crosstalk_spec::events::detect::DetectEvent;
use crosstalk_spec::events::ingest::IngestEvent;
use crosstalk_spec::events::{BusEvent, Subject};
use crosstalk_spec::ids::{AgentId, ChannelId, ExchangeId, ResourceId, SeededRandom};
use crosstalk_spec::interfaces::l2_transport::{ConsumerGroup, EventBus, Subscription};
use crosstalk_spec::interfaces::l3_reconstruction::agents::AgentReads;
use crosstalk_spec::interfaces::l5_flow::ChannelRegistry;
use crosstalk_spec::interfaces::l5_flow::channels::ChannelTraffic;
use crosstalk_spec::interfaces::l5_flow::transmissions::TransmissionStore;
use crosstalk_spec::support::{Clock, Timestamp};
use tokio::sync::mpsc;
use tokio::time::MissedTickBehavior;

pub use self::error::StepError;
pub use self::held::HeldWrites;
pub use self::input::{Extracted, FlowInputs, NotDurable, Observed, ReadResult, WriteCall};
pub use self::publish::{PublishError, Publisher};
pub use self::settings::{FlowConfig, InvalidFlowConfig, Settings};
pub use self::shards::Shards;
use crate::correlate::pairing::WriteOutcome;
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
    /// Ticks and envelope times: the replay clock under replay.
    pub clock: Arc<dyn Clock>,
    /// Seeds envelope ids: `SeededRandom::from_entropy` in the gateway, a
    /// fixed seed under simulation.
    pub entropy: SeededRandom,
}

/// One unit of the consumer's work.
#[derive(Debug, Clone, PartialEq)]
enum Step {
    Read(Observed<ReadResult>),
    Write(Observed<WriteCall>, WriteOutcome),
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

/// The flow consumer.
pub struct FlowConsumer<R, T, A, B> {
    settings: Settings,
    registry: R,
    transmissions: T,
    agents: A,
    publisher: Publisher<B>,
    clock: Arc<dyn Clock>,
    shards: Shards,
    held: HeldWrites,
    backlog: VecDeque<Step>,
}

impl<R, T, A, B> FlowConsumer<R, T, A, B>
where
    R: ChannelRegistry + ChannelTraffic + Send + Sync,
    T: TransmissionStore + Send + Sync,
    A: AgentReads + Send + Sync,
    B: EventBus + Send + Sync,
{
    pub fn new(settings: Settings, deps: FlowDeps<R, T, A, B>) -> Self {
        let FlowDeps {
            registry,
            transmissions,
            agents,
            bus,
            clock,
            entropy,
        } = deps;
        Self {
            shards: Shards::new(settings.timing, settings.content_retention, settings.shards),
            publisher: Publisher::new(bus, Arc::clone(&clock), entropy),
            settings,
            registry,
            transmissions,
            agents,
            clock,
            held: HeldWrites::default(),
            backlog: VecDeque::new(),
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
        match input {
            Extracted::Read(read) => self.queue(Step::Read(read)),
            Extracted::Write {
                write,
                outcome: Some(outcome),
            } => self.queue(Step::Write(write, outcome)),
            Extracted::Write {
                write,
                outcome: None,
            } => self.held.hold(write, self.settings.timing),
            Extracted::WriteResult { access, outcome } => match self.held.release(access) {
                Some(write) => self.queue(Step::Write(write, outcome)),
                None => {
                    tracing::debug!(access = %access.ulid_text(), "result for a write not held; ignored");
                }
            },
            Extracted::ToolCall {
                agent,
                call,
                name,
                at,
            } => self.shards.tool_named(agent, &call, &name, at),
        }
        self.drain().await;
    }

    /// A tick at `now`: release the writes whose settle window closed (as
    /// `Unknown`), then close windows and expire suspicions up to `now`.
    pub async fn tick(&mut self, now: Timestamp) {
        for (write, outcome) in self.held.settle(now) {
            self.queue(Step::Write(write, outcome));
        }
        self.queue(Step::Tick(now));
        self.drain().await;
    }

    /// Consume `subscription` and `extracted`, ticking every
    /// `Settings::tick_every` on the injected clock, until the bus shuts
    /// down. The consumer stays usable afterwards (its stores and shards
    /// can be read).
    pub async fn run<S: Subscription>(
        &mut self,
        mut subscription: S,
        mut extracted: mpsc::Receiver<Extracted>,
    ) {
        tracing::info!(
            group = GROUP,
            shards = self.shards.count(),
            "flow consumer started"
        );
        let mut ticker = tokio::time::interval(self.settings.tick_every);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
        let mut inputs_open = true;
        loop {
            tokio::select! {
                biased;
                _ = ticker.tick() => {
                    let now = self.clock.now();
                    self.tick(now).await;
                }
                input = extracted.recv(), if inputs_open => match input {
                    Some(input) => self.handle_extracted(input).await,
                    None => inputs_open = false,
                },
                next = subscription.next() => match next {
                    None => break,
                    Some(Err(error)) => {
                        tracing::warn!(group = GROUP, error = ?error, "undecodable delivery skipped");
                    }
                    Some(Ok(delivery)) => {
                        let event = delivery.envelope.id.ulid_text();
                        self.handle_event(&delivery.envelope.event).await;
                        if let Err(error) = subscription.ack(delivery.id).await {
                            tracing::warn!(group = GROUP, event = %event, error = ?error, "ack failed; the bus will redeliver");
                        }
                    }
                },
            }
        }
        tracing::info!(
            group = GROUP,
            backlog = self.backlog.len(),
            "flow consumer stopped"
        );
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
            Step::Write(write, outcome) => self.record_write(write, outcome).await,
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
