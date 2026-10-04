//! The operator action a client sends ([`ActionRequest`]) and how the
//! surface stamps it into the [`OperatorAction`] it acts on.

use serde::{Deserialize, Serialize};

use crate::aggregates::alert::{RuleName, UserRule};
use crate::aggregates::topic::TopicModelVersion;
use crate::derived::flow::resource::ResourcePattern;
use crate::derived::flow::verdict::Verdict;
use crate::ids::{
    AgentId, AlertId, AlertRuleId, ChannelId, EventId, MergeId, SinkId, TransmissionId,
};
use crate::interfaces::l2_transport::ConsumerGroup;
use crate::interfaces::l8_surface::{Caller, PolicyKind};
use crate::observed::agent::{AgentLabel, SelfMerge};
use crate::wire::WireRequest;

use super::{ActionKind, OperatorAction};

/// An operator action as a client asks for it: one variant per
/// [`OperatorAction`] variant, of the same name and with the same fields,
/// except that nothing the surface stamps is here. `MergeAgents` names the
/// two agents and no author; the surface makes the caller's operator the
/// author ([`ActionRequest::into_action`]). Every other variant carries
/// exactly the fields of its action, which already hold nothing stamped:
/// where an action records an operator or a time (a policy decision, a
/// promotion, a verdict, a pin, a rule's creator), the surface stamps it
/// from the caller and its clock when it applies the action.
///
/// A request (`WireRequest`): the body of the action route, decoded only
/// through `decode_request`. `PartialEq` but not `Eq`, as
/// [`OperatorAction`]: user rules hold similarity thresholds.
///
/// On the wire, adjacently tagged like the action:
/// `{"type": "merge_agents", "data": {"from": "01J..", "into": "01J.."}}`.
/// A request naming one agent twice decodes; stamping refuses it
/// (`SelfMerge`), so it never becomes an action and is never audited.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "type",
    content = "data",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum ActionRequest {
    /// [`OperatorAction::SetPolicy`].
    SetPolicy {
        channel: ChannelId,
        policy: PolicyKind,
        note: Option<String>,
    },
    /// [`OperatorAction::MergeAgents`]: merge `from` into `into`, authored
    /// by the caller.
    MergeAgents { from: AgentId, into: AgentId },
    /// [`OperatorAction::Unmerge`].
    Unmerge { merge: MergeId },
    /// [`OperatorAction::RenameAgent`].
    RenameAgent {
        agent: AgentId,
        label: Option<AgentLabel>,
    },
    /// [`OperatorAction::PromoteChannel`].
    PromoteChannel {
        channel: ChannelId,
        pattern: ResourcePattern,
        policy: PolicyKind,
        note: Option<String>,
    },
    /// [`OperatorAction::Acknowledge`].
    Acknowledge { alert: AlertId },
    /// [`OperatorAction::Resolve`].
    Resolve {
        alert: AlertId,
        note: Option<String>,
    },
    /// [`OperatorAction::CreateRule`].
    CreateRule {
        name: RuleName,
        rule: UserRule,
        sinks: Vec<SinkId>,
    },
    /// [`OperatorAction::SetVerdict`].
    SetVerdict {
        transmission: TransmissionId,
        verdict: Option<Verdict>,
        note: Option<String>,
    },
    /// [`OperatorAction::UpdateRule`].
    UpdateRule {
        id: AlertRuleId,
        name: RuleName,
        rule: UserRule,
        sinks: Vec<SinkId>,
    },
    /// [`OperatorAction::SetRuleEnabled`].
    SetRuleEnabled { id: AlertRuleId, enabled: bool },
    /// [`OperatorAction::ReplayDeadLetter`].
    ReplayDeadLetter { group: ConsumerGroup, id: EventId },
    /// [`OperatorAction::PinTopicVersion`].
    PinTopicVersion { version: TopicModelVersion },
    /// [`OperatorAction::UnpinTopicVersion`].
    UnpinTopicVersion { version: TopicModelVersion },
}

/// A client chooses every field of an action request; the author of a
/// merge is not one of them.
impl WireRequest for ActionRequest {}

impl ActionRequest {
    /// The action the caller asks for, stamped: a merge is authored by the
    /// caller's operator ([`OperatorAction::merge_agents`]), and every
    /// other request becomes the action of the same name with the same
    /// fields. A merge naming one agent twice is `SelfMerge`, which the
    /// surface returns as `InvalidInput(SelfMerge)`
    /// (`ActionError::from(SelfMerge)`) without calling `act`, so it is
    /// not audited.
    ///
    /// Times are not stamped here: no action holds one. Where applying an
    /// action records a time, the surface stamps the time it accepted the
    /// action then, with the same caller.
    pub fn into_action(self, caller: &Caller) -> Result<OperatorAction, SelfMerge> {
        Ok(match self {
            Self::SetPolicy {
                channel,
                policy,
                note,
            } => OperatorAction::SetPolicy {
                channel,
                policy,
                note,
            },
            Self::MergeAgents { from, into } => {
                return OperatorAction::merge_agents(caller, from, into);
            }
            Self::Unmerge { merge } => OperatorAction::Unmerge { merge },
            Self::RenameAgent { agent, label } => OperatorAction::RenameAgent { agent, label },
            Self::PromoteChannel {
                channel,
                pattern,
                policy,
                note,
            } => OperatorAction::PromoteChannel {
                channel,
                pattern,
                policy,
                note,
            },
            Self::Acknowledge { alert } => OperatorAction::Acknowledge { alert },
            Self::Resolve { alert, note } => OperatorAction::Resolve { alert, note },
            Self::CreateRule { name, rule, sinks } => {
                OperatorAction::CreateRule { name, rule, sinks }
            }
            Self::SetVerdict {
                transmission,
                verdict,
                note,
            } => OperatorAction::SetVerdict {
                transmission,
                verdict,
                note,
            },
            Self::UpdateRule {
                id,
                name,
                rule,
                sinks,
            } => OperatorAction::UpdateRule {
                id,
                name,
                rule,
                sinks,
            },
            Self::SetRuleEnabled { id, enabled } => OperatorAction::SetRuleEnabled { id, enabled },
            Self::ReplayDeadLetter { group, id } => OperatorAction::ReplayDeadLetter { group, id },
            Self::PinTopicVersion { version } => OperatorAction::PinTopicVersion { version },
            Self::UnpinTopicVersion { version } => OperatorAction::UnpinTopicVersion { version },
        })
    }

    /// The request `action` was stamped from: the action without its
    /// stamps (a merge without its author). The one request variant each
    /// action variant comes from; this match and
    /// [`ActionRequest::into_action`]'s are both exhaustive, so an action
    /// added without a request form does not compile. For an action
    /// whose merge the caller's operator authored,
    /// `ActionRequest::of(&action).into_action(&caller) == Ok(action)`.
    pub fn of(action: &OperatorAction) -> Self {
        match action.clone() {
            OperatorAction::SetPolicy {
                channel,
                policy,
                note,
            } => Self::SetPolicy {
                channel,
                policy,
                note,
            },
            OperatorAction::MergeAgents(request) => Self::MergeAgents {
                from: request.source(),
                into: request.target(),
            },
            OperatorAction::Unmerge { merge } => Self::Unmerge { merge },
            OperatorAction::RenameAgent { agent, label } => Self::RenameAgent { agent, label },
            OperatorAction::PromoteChannel {
                channel,
                pattern,
                policy,
                note,
            } => Self::PromoteChannel {
                channel,
                pattern,
                policy,
                note,
            },
            OperatorAction::Acknowledge { alert } => Self::Acknowledge { alert },
            OperatorAction::Resolve { alert, note } => Self::Resolve { alert, note },
            OperatorAction::CreateRule { name, rule, sinks } => {
                Self::CreateRule { name, rule, sinks }
            }
            OperatorAction::SetVerdict {
                transmission,
                verdict,
                note,
            } => Self::SetVerdict {
                transmission,
                verdict,
                note,
            },
            OperatorAction::UpdateRule {
                id,
                name,
                rule,
                sinks,
            } => Self::UpdateRule {
                id,
                name,
                rule,
                sinks,
            },
            OperatorAction::SetRuleEnabled { id, enabled } => Self::SetRuleEnabled { id, enabled },
            OperatorAction::ReplayDeadLetter { group, id } => Self::ReplayDeadLetter { group, id },
            OperatorAction::PinTopicVersion { version } => Self::PinTopicVersion { version },
            OperatorAction::UnpinTopicVersion { version } => Self::UnpinTopicVersion { version },
        }
    }

    /// The kind of the action this request becomes: the same as
    /// [`OperatorAction::kind`] of [`ActionRequest::into_action`]'s result.
    pub fn kind(&self) -> ActionKind {
        match self {
            Self::SetPolicy { .. } => ActionKind::SetPolicy,
            Self::MergeAgents { .. } => ActionKind::MergeAgents,
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
}
