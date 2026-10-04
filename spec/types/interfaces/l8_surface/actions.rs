//! Operator actions: what an operator can ask for, the permission each
//! needs, the entities it names and what an accepted one did.
//!
//! [`OperatorAction`], [`ActionKind`] and every per-action function match
//! exhaustively, with no wildcard arm, so adding an action fails to compile
//! until its kind, permission and subjects are decided. The audit log stores
//! the action value itself ([`super::audit::OperatorRecord`]), so every
//! action is audited the same way.

use crate::aggregates::alert::{RuleName, UserRule};
use crate::aggregates::topic::TopicModelVersion;
use crate::derived::flow::resource::ResourcePattern;
use crate::derived::flow::verdict::Verdict;
use crate::ids::{
    AgentId, AlertId, AlertRuleId, ChannelId, EventId, MergeId, SinkId, TransmissionId,
};
use crate::interfaces::l2_transport::ConsumerGroup;
use crate::observed::agent::{AgentLabel, MergeRequest};

use super::audit::AuditSubject;
use super::{Permission, PolicyKind};

/// `OperatorAction` is `PartialEq` but not `Eq`: user rules hold
/// similarity thresholds, which are floats.
#[derive(Debug, Clone, PartialEq)]
pub enum OperatorAction {
    SetPolicy {
        channel: ChannelId,
        policy: PolicyKind,
        note: Option<String>,
    },
    /// Built with `MergeAuthor::Operator` of the caller; self-merges cannot
    /// be expressed. Both agents must be canonical. Returns
    /// `ActionOutcome::Merged` with the new record's id.
    MergeAgents(MergeRequest),
    /// Revert one merge record exactly (`IdentityResolver::unmerge`).
    Unmerge {
        merge: MergeId,
    },
    /// Set (`Some`) or clear (`None`) an active agent's display label. A
    /// merged agent is refused, not redirected.
    RenameAgent {
        agent: AgentId,
        label: Option<AgentLabel>,
    },
    /// Promote a discovered channel: attach `pattern`, making it declared
    /// under the same id, record `policy` (with `note`) as the operator's
    /// decision, and supersede every other discovered channel whose seed the
    /// pattern matches. The surface stamps the operator and time into a
    /// `Promotion` and calls `ChannelRegistry::promote`; success is
    /// `ChannelPromoted { channel, superseded }`, the same id and the
    /// channels it superseded, so the audit entry names all of them.
    PromoteChannel {
        channel: ChannelId,
        pattern: ResourcePattern,
        policy: PolicyKind,
        note: Option<String>,
    },
    Acknowledge {
        alert: AlertId,
    },
    Resolve {
        alert: AlertId,
        note: Option<String>,
    },
    /// Create an enabled user rule. The server assigns its id and returns
    /// `ActionOutcome::RuleCreated`. "Watch this topic" is
    /// [`UserRule::watch_topic`].
    CreateRule {
        name: RuleName,
        rule: UserRule,
        sinks: Vec<SinkId>,
    },
    /// Set (`Some`) or withdraw (`None`) the operator's verdict on a
    /// transmission (`TransmissionVerdicts::set`). The transmission's state
    /// never changes. `Applied` when a record was appended, `Unchanged` when
    /// the verdict was already current; an unknown transmission is
    /// `NotFound`, and a `Detected` or `AwaitingContent` one is
    /// `Conflict(TransmissionNotJudgeable)`. A `FalseDetection` verdict
    /// suppresses the transmission's active alerts once L6 sees `VerdictSet`.
    SetVerdict {
        transmission: TransmissionId,
        verdict: Option<Verdict>,
        note: Option<String>,
    },
    /// Replace a user rule's name, definition and sinks. A stale rule is
    /// retargeted to the current version or model and enabled.
    UpdateRule {
        id: AlertRuleId,
        name: RuleName,
        rule: UserRule,
        sinks: Vec<SinkId>,
    },
    /// Enable or disable any rule. Staleness cannot be set. Enabling a stale
    /// rule is `Conflict(RuleStale)`; `UpdateRule` retargets and enables it.
    /// Disabling is always allowed.
    SetRuleEnabled {
        id: AlertRuleId,
        enabled: bool,
    },
    /// Redeliver a dead-lettered envelope to its consumer group.
    ReplayDeadLetter {
        group: ConsumerGroup,
        id: EventId,
    },
    /// Keep `version`'s data whatever the retention policy
    /// (`TopicCatalog::pin`), stamped with the caller and the acceptance
    /// time. `Applied` when it pins, `Unchanged` when already pinned;
    /// `NotFound` for an unknown version, `Conflict(TopicVersionFitting)` for
    /// a fitting one and `Conflict(TopicVersionDropped)` for a dropped one.
    PinTopicVersion {
        version: TopicModelVersion,
    },
    /// Remove `version`'s pin (`TopicCatalog::unpin`); retention may then
    /// drop it. `Applied` when it was pinned, `Unchanged` otherwise (a
    /// dropped version included); `NotFound` for an unknown version.
    UnpinTopicVersion {
        version: TopicModelVersion,
    },
}

/// Which action, without its arguments. The audit log filters on it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ActionKind {
    SetPolicy,
    MergeAgents,
    Unmerge,
    RenameAgent,
    PromoteChannel,
    Acknowledge,
    Resolve,
    SetVerdict,
    CreateRule,
    UpdateRule,
    SetRuleEnabled,
    ReplayDeadLetter,
    PinTopicVersion,
    UnpinTopicVersion,
}

impl OperatorAction {
    pub fn kind(&self) -> ActionKind {
        match self {
            Self::SetPolicy { .. } => ActionKind::SetPolicy,
            Self::MergeAgents(_) => ActionKind::MergeAgents,
            Self::Unmerge { .. } => ActionKind::Unmerge,
            Self::RenameAgent { .. } => ActionKind::RenameAgent,
            Self::PromoteChannel { .. } => ActionKind::PromoteChannel,
            Self::Acknowledge { .. } => ActionKind::Acknowledge,
            Self::Resolve { .. } => ActionKind::Resolve,
            Self::SetVerdict { .. } => ActionKind::SetVerdict,
            Self::CreateRule { .. } => ActionKind::CreateRule,
            Self::UpdateRule { .. } => ActionKind::UpdateRule,
            Self::SetRuleEnabled { .. } => ActionKind::SetRuleEnabled,
            Self::ReplayDeadLetter { .. } => ActionKind::ReplayDeadLetter,
            Self::PinTopicVersion { .. } => ActionKind::PinTopicVersion,
            Self::UnpinTopicVersion { .. } => ActionKind::UnpinTopicVersion,
        }
    }

    /// The permission the caller must hold, checked before any effect; a
    /// caller without it gets `Forbidden`. Govern for identity, policy,
    /// rules and topic-version pins, Triage for alerts and verdicts, Operate
    /// for the pipeline. No action needs View, Content or Audit, which are
    /// read permissions.
    ///
    /// `SetVerdict` needs Triage alone, not Content as well: it reveals no
    /// content (its outcome and the records it writes hold no message text),
    /// and reading the text to judge from is already gated by `transmission`
    /// and `search`. One permission per action keeps `OperatorRecord`'s
    /// `Forbidden` check exact.
    pub fn required_permission(&self) -> Permission {
        match self {
            Self::SetPolicy { .. }
            | Self::MergeAgents(_)
            | Self::Unmerge { .. }
            | Self::RenameAgent { .. }
            | Self::PromoteChannel { .. }
            | Self::CreateRule { .. }
            | Self::UpdateRule { .. }
            | Self::SetRuleEnabled { .. }
            | Self::PinTopicVersion { .. }
            | Self::UnpinTopicVersion { .. } => Permission::Govern,
            Self::Acknowledge { .. } | Self::Resolve { .. } | Self::SetVerdict { .. } => {
                Permission::Triage
            }
            Self::ReplayDeadLetter { .. } => Permission::Operate,
        }
    }

    /// The entities the action names, as requested (not resolved through
    /// merges). The audit log's subject filter matches these, together with
    /// the ids the outcome names ([`ActionOutcome::subjects`]: an id it
    /// created, or the channels a promotion superseded). A dead-letter
    /// replay names no entity, and a rule creation names none until its
    /// outcome (`RuleCreated`) carries the new rule's id.
    pub fn subjects(&self) -> Vec<AuditSubject> {
        match self {
            Self::SetPolicy { channel, .. } | Self::PromoteChannel { channel, .. } => {
                vec![AuditSubject::Channel(*channel)]
            }
            Self::MergeAgents(request) => vec![
                AuditSubject::Agent(request.source()),
                AuditSubject::Agent(request.target()),
            ],
            Self::Unmerge { merge } => vec![AuditSubject::Merge(*merge)],
            Self::RenameAgent { agent, .. } => vec![AuditSubject::Agent(*agent)],
            Self::Acknowledge { alert } | Self::Resolve { alert, .. } => {
                vec![AuditSubject::Alert(*alert)]
            }
            Self::SetVerdict { transmission, .. } => {
                vec![AuditSubject::Transmission(*transmission)]
            }
            Self::CreateRule { .. } => Vec::new(),
            Self::UpdateRule { id, .. } | Self::SetRuleEnabled { id, .. } => {
                vec![AuditSubject::Rule(*id)]
            }
            Self::PinTopicVersion { version } | Self::UnpinTopicVersion { version } => {
                vec![AuditSubject::TopicVersion(*version)]
            }
            Self::ReplayDeadLetter { .. } => Vec::new(),
        }
    }
}

/// What an accepted operator action did, including any ids it created or
/// retired, so the UI can navigate to them and the audit log can find the
/// action from any of them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ActionOutcome {
    /// The action changed state.
    Applied,
    /// Accepted, but the state already matched: acknowledging an
    /// acknowledged alert, or the losing request of a race.
    Unchanged,
    RuleCreated(AlertRuleId),
    /// `channel` was promoted under its own id, and every channel in
    /// `superseded` now resolves to it (`Promoted::superseded` from
    /// `ChannelRegistry::promote`).
    ChannelPromoted {
        channel: ChannelId,
        superseded: SupersededChannels,
    },
    Merged(MergeId),
}

impl ActionOutcome {
    /// The entities the outcome names that the action alone does not: the
    /// id it created (a rule, a merge record), or the channels a promotion
    /// superseded, after the promoted channel. These exist only once the
    /// action is applied, so this is the only place an audit entry can take
    /// them from. Empty for `Applied` and `Unchanged`.
    pub fn subjects(&self) -> Vec<AuditSubject> {
        match self {
            Self::Applied | Self::Unchanged => Vec::new(),
            Self::RuleCreated(rule) => vec![AuditSubject::Rule(*rule)],
            Self::ChannelPromoted {
                channel,
                superseded,
            } => std::iter::once(*channel)
                .chain(superseded.iter())
                .map(AuditSubject::Channel)
                .collect(),
            Self::Merged(merge) => vec![AuditSubject::Merge(*merge)],
        }
    }
}

/// The channels one promotion superseded: sorted by id, each once, so two
/// outcomes of the same promotion are equal however the registry listed
/// them. Built only by [`SupersededChannels::new`], which sorts and
/// deduplicates.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Default)]
pub struct SupersededChannels(Vec<ChannelId>);

impl SupersededChannels {
    /// Sorts `channels` by id and drops repeats.
    pub fn new(channels: impl IntoIterator<Item = ChannelId>) -> Self {
        let mut channels: Vec<ChannelId> = channels.into_iter().collect();
        channels.sort_unstable();
        channels.dedup();
        Self(channels)
    }

    pub fn as_slice(&self) -> &[ChannelId] {
        &self.0
    }

    pub fn iter(&self) -> impl Iterator<Item = ChannelId> + '_ {
        self.0.iter().copied()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}
