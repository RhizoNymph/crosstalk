//! Checkpoint and restore (`flow.consumer.restore-equivalent`, INV-1215).
//!
//! ```text
//! checkpoint (idle: no step queued)
//!   Shards::checkpoint ─▶ FlowDurability::save (snapshot rows + shard_ticks, one txn,
//!                         covering every access and tool call recorded so far)
//!   ─▶ the caller acks every delivery taken before it
//! restore (start, before any input)
//!   FlowDurability::load ─▶ the checkpoint (or empty shards), the held writes,
//!                           the inputs recorded after the checkpoint
//!   Shards::restore (a format or shard count it cannot read: IncompatibleSnapshot)
//!   held writes back in HeldWrites (one whose access was re-fed: its row dropped)
//!   re-feed each recorded input, in recording order, through the steps that
//!   first took it: Refeed (resource, access, AccessRecorded, correlation), ToolNamed
//!   ─▶ the caller subscribes; the bus redelivers every delivery not acked
//! ```
//!
//! The snapshot covers exactly the inputs taken before it. Everything after
//! is redelivered or re-fed, and the correlator accepts evidence late,
//! duplicated and in any order, so the decisions are the ones an
//! uninterrupted consumer takes, at the same derived ids.

use std::collections::BTreeSet;

use crosstalk_spec::derived::flow::access::Access;
use crosstalk_spec::derived::flow::resource::Locator;
use crosstalk_spec::events::BusEvent;
use crosstalk_spec::events::detect::DetectEvent;
use crosstalk_spec::ids::AccessId;
use crosstalk_spec::interfaces::l2_transport::EventBus;
use crosstalk_spec::interfaces::l3_reconstruction::agents::AgentReads;
use crosstalk_spec::interfaces::l5_flow::ChannelRegistry;
use crosstalk_spec::interfaces::l5_flow::channels::ChannelTraffic;
use crosstalk_spec::interfaces::l5_flow::transmissions::TransmissionStore;
use crosstalk_spec::support::Timestamp;

use super::checkpoint::{CheckpointError, FlowRestoreError};
use super::durability::{FlowDurability, Recorded, Resolved};
use super::held::HeldWrites;
use super::input::Observed;
use super::{FlowConsumer, Shards, Step, StepError, decisions};
use crate::correlate::MediumKey;
use crate::correlate::pairing::{self, WriteOutcome};

/// A checkpoint taken.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Checkpointed {
    /// The tick every shard ran through; `None` before the first tick.
    pub ticked_through: Option<Timestamp>,
}

/// What a restore found.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Restored {
    /// Whether a checkpoint was restored (else the shards start empty).
    pub checkpoint: bool,
    /// The tick the restored shards ran through.
    pub ticked_through: Option<Timestamp>,
    pub held_writes: usize,
    /// Accesses and tool calls recorded after the checkpoint, re-fed.
    pub accesses_refed: usize,
    pub tool_calls_refed: usize,
    /// Steps a transient failure left queued after the re-feed.
    pub backlog: usize,
}

impl<R, T, A, B, D> FlowConsumer<R, T, A, B, D>
where
    R: ChannelRegistry + ChannelTraffic + Send + Sync,
    T: TransmissionStore + Send + Sync,
    A: AgentReads + Send + Sync,
    B: EventBus + Send + Sync,
    D: FlowDurability,
{
    /// Store a checkpoint of every shard. Refused while steps wait in the
    /// queue: their inputs' effects are not stored yet, and a delivery
    /// acked on this checkpoint would be lost.
    pub async fn checkpoint(&mut self) -> Result<Checkpointed, CheckpointError> {
        if !self.backlog.is_empty() {
            return Err(CheckpointError::Busy {
                backlog: self.backlog.len(),
            });
        }
        let checkpoint = self.shards.checkpoint()?;
        self.durability.save(&checkpoint, self.clock.now()).await?;
        let ticked_through = self.shards.last_tick();
        tracing::debug!(
            shards = checkpoint.shards.len(),
            ticked_through = ticked_through.map(|at| at.as_micros()),
            "flow checkpoint stored"
        );
        Ok(Checkpointed { ticked_through })
    }

    /// Restore from what the durability port kept: call once, before the
    /// first input. An incompatible checkpoint is an error, and the
    /// consumer must not run (decision Q2).
    pub async fn restore(&mut self) -> Result<Restored, FlowRestoreError> {
        let recovered = self.durability.load().await?;
        let settings = self.settings;
        self.shards = match &recovered.checkpoint {
            Some(stored) => Shards::restore(
                settings.timing,
                settings.content_retention,
                settings.shards,
                stored,
            )
            .map_err(FlowRestoreError::IncompatibleSnapshot)?,
            None => Shards::new(settings.timing, settings.content_retention, settings.shards),
        };
        self.backlog.clear();
        let refed: BTreeSet<AccessId> = recovered
            .inputs
            .iter()
            .filter_map(|input| match input {
                Recorded::Access { access, .. } => Some(access.id),
                Recorded::ToolCall(_) => None,
            })
            .collect();
        self.held = HeldWrites::default();
        let mut held_writes = 0;
        for (write, settles_at) in recovered.held {
            if refed.contains(&write.id) {
                // Released and recorded, the stored hold not yet dropped.
                self.queue(Step::Unhold(write.id));
            } else {
                self.held.restore(write, settles_at);
                held_writes += 1;
            }
        }
        let (mut accesses_refed, mut tool_calls_refed) = (0, 0);
        for input in recovered.inputs {
            match input {
                Recorded::Access {
                    access,
                    locator,
                    resolved,
                } => {
                    accesses_refed += 1;
                    self.queue(Step::Refeed {
                        access,
                        locator,
                        resolved,
                    });
                }
                Recorded::ToolCall(call) => {
                    tool_calls_refed += 1;
                    self.queue(Step::ToolNamed(call));
                }
            }
        }
        self.drain().await;
        let restored = Restored {
            checkpoint: recovered.checkpoint.is_some(),
            ticked_through: self.shards.last_tick(),
            held_writes,
            accesses_refed,
            tool_calls_refed,
            backlog: self.backlog.len(),
        };
        tracing::info!(
            checkpoint = restored.checkpoint,
            ticked_through = restored.ticked_through.map(|at| at.as_micros()),
            held_writes,
            accesses_refed,
            tool_calls_refed,
            backlog = restored.backlog,
            "flow consumer restored"
        );
        Ok(restored)
    }

    /// An access recorded before a restart, taken as when it was first
    /// recorded: in the medium its resource was resolved to then (the
    /// resource's evidence handed to that channel first), announced (under
    /// the same envelope id), then correlated unless it is a write that
    /// does not pair. The resource may be on another channel since (one
    /// this consumer discovered from it, or a promotion's): resolving it
    /// again would correlate the access where it never was, and open its
    /// transmissions under other ids. An access whose resolution was not
    /// stored is recorded again as new.
    pub(super) async fn refeed(
        &mut self,
        access: Access,
        locator: Locator,
        resolved: Resolved,
    ) -> Result<Vec<Step>, StepError> {
        let outcome = pairing::outcome(&access);
        let channel = match resolved {
            Resolved::Channel(channel) => Some(channel),
            Resolved::NoChannel => None,
            Resolved::Unknown => {
                let observed = Observed {
                    id: access.id,
                    agent: access.agent,
                    exchange: access.exchange,
                    at: access.at,
                    locator,
                    via: access.via,
                    op: (),
                };
                return self.record(observed, access.op, outcome).await;
            }
        };
        let mut steps = Vec::new();
        if let Some(channel) = channel {
            steps.extend(decisions(self.shards.rekey(
                MediumKey::Resource(access.resource),
                MediumKey::Channel(channel),
            )));
        }
        steps.push(Step::Publish(BusEvent::Detect(DetectEvent::AccessRecorded {
            access: access.clone(),
            channel,
        })));
        if outcome.is_none_or(WriteOutcome::pairs) {
            steps.push(Step::Correlate(access, channel));
        }
        Ok(steps)
    }
}
