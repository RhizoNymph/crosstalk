//! The mutable half of the fixture: everything an operator action can
//! change. Generation fills it with history; `act` changes it under a write
//! lock.

use std::collections::BTreeMap;

use crosstalk_spec::aggregates::alert::{Alert, AlertRuleSet, RuleStatus};
use crosstalk_spec::aggregates::projection::{Projection, ProjectionInfo};
use crosstalk_spec::aggregates::topic::TopicModelVersion;
use crosstalk_spec::aggregates::topic_history::{TopicVersionHistory, TopicVersionInfo};
use crosstalk_spec::derived::flow::channel::policy::{PolicyDecision, PolicyHistory, Recorded};
use crosstalk_spec::derived::flow::channel::{Channel, ChannelOrigin};
use crosstalk_spec::derived::flow::verdict::VerdictLog;
use crosstalk_spec::ids::{ChannelId, TransmissionId};
use crosstalk_spec::interfaces::l2_transport::DeadLetter;
use crosstalk_spec::support::Timestamp;

use crate::contract::research::AuditEntry;

use super::clock::Mint;
use super::identity::Identity;

/// A stored channel with its policy history. Built only through
/// [`ChannelRecord::new`] and changed only through its methods, so the
/// channel's policy is always [`PolicyHistory::current`] of its history
/// (`flow.policy.current-is-history-latest`). Supersession lives in the
/// channel's origin.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChannelRecord {
    channel: Channel,
    history: PolicyHistory,
    /// When the channel was declared or discovered.
    pub created: Timestamp,
}

impl ChannelRecord {
    /// `channel` with its policy set from `history`.
    pub fn new(mut channel: Channel, history: PolicyHistory, created: Timestamp) -> Self {
        channel.policy = history.current();
        Self {
            channel,
            history,
            created,
        }
    }

    pub fn channel(&self) -> &Channel {
        &self.channel
    }

    /// Every decision recorded for the channel, oldest first.
    pub fn history(&self) -> &PolicyHistory {
        &self.history
    }

    /// Records a decision in time order and sets the policy to the
    /// history's current one, in one step.
    pub fn record(&mut self, decision: PolicyDecision) -> Recorded {
        let recorded = self.history.record(decision);
        self.channel.policy = self.history.current();
        recorded
    }

    /// Replaces the origin (a promotion or a supersession); id, resources
    /// and policy stay.
    pub fn set_origin(&mut self, origin: ChannelOrigin) {
        self.channel.origin = origin;
    }
}

/// A projection job as the store keeps it: a ready job with its frame, or
/// the record of a job in any other status (queued, fitting, failed, or
/// expired with its frame dropped).
#[derive(Debug, Clone, PartialEq)]
pub enum Job {
    Ready(Box<Projection>),
    Record(Box<ProjectionInfo>),
}

impl Job {
    pub fn ready(projection: Projection) -> Self {
        Self::Ready(Box::new(projection))
    }

    pub fn record(info: ProjectionInfo) -> Self {
        Self::Record(Box::new(info))
    }

    pub fn info(&self) -> &ProjectionInfo {
        match self {
            Self::Ready(projection) => projection.info(),
            Self::Record(info) => info,
        }
    }
}

#[derive(Debug, Clone)]
pub struct State {
    /// The agents, the merge log and the vetoes.
    pub identity: Identity,
    pub channels: BTreeMap<ChannelId, ChannelRecord>,
    /// The topic catalog: every version with its status and retention.
    /// Pins change it (`TopicCatalog::pin`, `unpin`).
    pub catalog: TopicVersionHistory,
    /// Each judged transmission's append-only log. A transmission never
    /// judged has no entry: its log is empty.
    pub verdicts: BTreeMap<TransmissionId, VerdictLog>,
    /// Every alert, oldest raise first.
    pub alerts: Vec<Alert>,
    /// Each built-in rule once, and the user rules; none is ever removed.
    pub rules: AlertRuleSet,
    /// Append-only, oldest first.
    pub audit: Vec<AuditEntry>,
    pub dead_letters: Vec<DeadLetter>,
    /// Projection jobs, oldest first.
    pub projections: Vec<Job>,
    pub mint: Mint,
}

impl State {
    pub fn new(
        identity: Identity,
        channels: Vec<ChannelRecord>,
        catalog: TopicVersionHistory,
        mint: Mint,
    ) -> Self {
        Self {
            identity,
            channels: channels.into_iter().map(|r| (r.channel.id, r)).collect(),
            catalog,
            verdicts: BTreeMap::new(),
            alerts: Vec::new(),
            // Generation replaces it with the configured settings.
            rules: AlertRuleSet::new(|_| (RuleStatus::Enabled, Vec::new())),
            audit: Vec::new(),
            dead_letters: Vec::new(),
            projections: Vec::new(),
            mint,
        }
    }

    /// The version graphs and series read, and views default to.
    pub fn active_version(&self) -> TopicModelVersion {
        self.catalog.active().version()
    }

    pub fn version_info(&self, version: TopicModelVersion) -> Option<&TopicVersionInfo> {
        self.catalog.get(version)
    }

    /// Whether `version` is known and its data not dropped.
    pub fn retains(&self, version: TopicModelVersion) -> bool {
        self.version_info(version)
            .is_some_and(|info| info.retention().is_retained())
    }

    /// The channel in force for `id` (`Channel::canonical`: one step, a
    /// superseding channel is never superseded). Unknown ids resolve to
    /// themselves.
    pub fn canonical_channel(&self, id: ChannelId) -> ChannelId {
        self.channels
            .get(&id)
            .map_or(id, |record| record.channel.canonical())
    }
}
