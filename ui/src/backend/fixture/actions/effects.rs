//! State changes shared by operator actions and by history generation, so
//! the generated past follows the same rules as live actions.

use crosstalk_spec::aggregates::alert::AlertSubject;
use crosstalk_spec::ids::{AlertRuleId, ChannelId, TransmissionId};
use crosstalk_spec::support::Timestamp;

use crate::backend::fixture::store::State;
use crate::contract::AuditId;
use crate::contract::actions::{ActionOutcome, OperatorAction};
use crate::contract::alerts::{Alert, AlertState, SuppressReason};
use crate::contract::research::{Actor, AuditEntry, AuditOutcome, AuditSubject, AuditedAction};

pub fn is_active(alert: &Alert) -> bool {
    alert.state.is_active()
}

/// Suppresses the active alerts whose subject is `channel` (after
/// supersession). Alerts about transmissions on it stay.
pub fn suppress_channel_alerts(state: &mut State, channel: ChannelId, at: Timestamp) -> u32 {
    let target = state.canonical_channel(channel);
    let subjects: Vec<bool> = state
        .alerts
        .iter()
        .map(|a| match a.subject {
            AlertSubject::Channel(c) => state.canonical_channel(c) == target,
            _ => false,
        })
        .collect();
    let mut count = 0;
    for (alert, matches) in state.alerts.iter_mut().zip(subjects) {
        if matches && is_active(alert) {
            alert.state = AlertState::Suppressed {
                at,
                reason: SuppressReason::ChannelSanctioned,
            };
            count += 1;
        }
    }
    count
}

/// Suppresses the active alerts raised by `rule`.
pub fn suppress_rule_alerts(state: &mut State, rule: AlertRuleId, at: Timestamp) -> u32 {
    let mut count = 0;
    for alert in state.alerts.iter_mut() {
        if alert.rule == rule && is_active(alert) {
            alert.state = AlertState::Suppressed {
                at,
                reason: SuppressReason::RuleDisabled,
            };
            count += 1;
        }
    }
    count
}

/// Suppresses the active alerts about `transmission` after a
/// false-detection verdict.
pub fn reject_transmission_alerts(
    state: &mut State,
    transmission: TransmissionId,
    at: Timestamp,
) -> u32 {
    let mut count = 0;
    for alert in state.alerts.iter_mut() {
        if alert.subject == AlertSubject::Transmission(transmission) && is_active(alert) {
            alert.state = AlertState::Suppressed {
                at,
                reason: SuppressReason::OperatorRejected,
            };
            count += 1;
        }
    }
    count
}

/// Appends an audit entry and returns its id.
pub fn audit(
    state: &mut State,
    at: Timestamp,
    by: Actor,
    action: AuditedAction,
    subject: Option<AuditSubject>,
    outcome: AuditOutcome,
) -> AuditId {
    let id = AuditId::from_ulid(state.mint.ulid(at));
    state.audit.push(AuditEntry {
        id,
        at,
        by,
        action,
        subject,
        outcome,
    });
    id
}

/// What an operator action is about: its target, or what it created when
/// it names none.
pub fn subject(action: &OperatorAction, outcome: Option<&ActionOutcome>) -> Option<AuditSubject> {
    match action {
        OperatorAction::SetPolicy { channel, .. }
        | OperatorAction::PromoteChannel { channel, .. } => Some(AuditSubject::Channel(*channel)),
        OperatorAction::MergeAgents(request) => Some(AuditSubject::Agent(request.source())),
        OperatorAction::RenameAgent { agent, .. } => Some(AuditSubject::Agent(*agent)),
        OperatorAction::Unmerge { merge } => Some(AuditSubject::Merge(*merge)),
        OperatorAction::SetVerdict { transmission, .. } => {
            Some(AuditSubject::Transmission(*transmission))
        }
        OperatorAction::Acknowledge { alert } | OperatorAction::Resolve { alert, .. } => {
            Some(AuditSubject::Alert(*alert))
        }
        OperatorAction::UpdateRule { id, .. } | OperatorAction::SetRuleEnabled { id, .. } => {
            Some(AuditSubject::Rule(*id))
        }
        OperatorAction::CreateRule { .. } => match outcome {
            Some(ActionOutcome::RuleCreated(id)) => Some(AuditSubject::Rule(*id)),
            _ => None,
        },
        OperatorAction::ReplayDeadLetter { .. } => None,
    }
}
