//! Operator actions and their outcomes (items 13 to 18).

use crosstalk_spec::derived::flow::resource::ResourcePattern;
use crosstalk_spec::ids::{AgentId, AlertId, AlertRuleId, ChannelId, EventId, TransmissionId};
use crosstalk_spec::interfaces::l2_transport::ConsumerGroup;
use crosstalk_spec::interfaces::l8_surface::{Permission, PolicyKind};
use crosstalk_spec::observed::agent::{AgentLabel, MergeRequest};

use crosstalk_spec::aggregates::alert::{RuleName, UserRule};
use crosstalk_spec::derived::flow::verdict::Verdict;
use crosstalk_spec::ids::MergeId;
use crosstalk_spec::ids::SinkId;

/// Replaces `crosstalk_spec::interfaces::l8_surface::OperatorAction`, adding
/// items 14 to 18. Authors and times are stamped by the surface from the
/// caller.
#[derive(Debug, Clone, PartialEq)]
pub enum OperatorAction {
    SetPolicy {
        channel: ChannelId,
        policy: PolicyKind,
        note: Option<String>,
    },
    MergeAgents(MergeRequest),
    Acknowledge {
        alert: AlertId,
    },
    Resolve {
        alert: AlertId,
        note: Option<String>,
    },
    ReplayDeadLetter {
        group: ConsumerGroup,
        id: EventId,
    },
    /// `None` clears the label.
    RenameAgent {
        agent: AgentId,
        label: Option<AgentLabel>,
    },
    Unmerge {
        merge: MergeId,
    },
    PromoteChannel {
        channel: ChannelId,
        pattern: ResourcePattern,
        policy: PolicyKind,
        note: Option<String>,
    },
    /// `None` withdraws the caller's earlier verdict.
    SetVerdict {
        transmission: TransmissionId,
        verdict: Option<Verdict>,
        note: Option<String>,
    },
    /// Create an enabled user rule; the store embeds a semantic query's
    /// text. Exactly the spec's `CreateRule`.
    CreateRule {
        name: RuleName,
        rule: UserRule,
        sinks: Vec<SinkId>,
    },
    /// Replace a user rule's name, definition and sinks, embedding a
    /// semantic query's text again. A stale rule is retargeted and enabled.
    /// Exactly the spec's `UpdateRule`.
    UpdateRule {
        id: AlertRuleId,
        name: RuleName,
        rule: UserRule,
        sinks: Vec<SinkId>,
    },
    /// Enable or disable any rule; enabling a stale one is
    /// `Conflict(RuleStale)`. Exactly the spec's `SetRuleEnabled`.
    SetRuleEnabled {
        id: AlertRuleId,
        enabled: bool,
    },
}

impl OperatorAction {
    /// The permission the action needs. Verdicts also need `Content`, since
    /// judging a transmission means reading it; see [`Self::also_requires`].
    pub fn requires(&self) -> Permission {
        match self {
            Self::SetPolicy { .. }
            | Self::MergeAgents(_)
            | Self::RenameAgent { .. }
            | Self::Unmerge { .. }
            | Self::PromoteChannel { .. }
            | Self::CreateRule { .. }
            | Self::UpdateRule { .. }
            | Self::SetRuleEnabled { .. } => Permission::Govern,
            Self::Acknowledge { .. } | Self::Resolve { .. } | Self::SetVerdict { .. } => {
                Permission::Triage
            }
            Self::ReplayDeadLetter { .. } => Permission::Operate,
        }
    }

    pub fn also_requires(&self) -> Option<Permission> {
        match self {
            Self::SetVerdict { .. } => Some(Permission::Content),
            _ => None,
        }
    }
}

/// What an action created or changed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ActionOutcome {
    Applied,
    RuleCreated(AlertRuleId),
    ChannelPromoted(ChannelId),
    Merged(MergeId),
}
