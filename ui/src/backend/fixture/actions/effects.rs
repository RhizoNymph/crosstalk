//! State changes shared by operator actions and by history generation, so
//! the generated past follows the same rules as live actions.

use crosstalk_spec::aggregates::alert::AlertSubject;
use crosstalk_spec::ids::{AlertRuleId, ChannelId, TransmissionId};
use crosstalk_spec::support::Timestamp;

use crate::contract::alerts::{Alert, AlertState, SuppressReason};
use crate::backend::fixture::store::{AuditRecord, State};
use crate::contract::AuditId;
use crate::contract::actions::OperatorAction;
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
    outcome: AuditOutcome,
    subjects: Vec<AuditSubject>,
) -> AuditId {
    let id = AuditId::from_ulid(state.mint.ulid(at));
    state.audit.push(AuditRecord {
        entry: AuditEntry {
            id,
            at,
            by,
            action,
            outcome,
        },
        subjects,
    });
    id
}

/// What an operator action is about, for `AuditFilter::subject`.
pub fn subjects(state: &State, action: &OperatorAction) -> Vec<AuditSubject> {
    match action {
        OperatorAction::SetPolicy { channel, .. }
        | OperatorAction::PromoteChannel { channel, .. } => vec![AuditSubject::Channel(*channel)],
        OperatorAction::MergeAgents(request) => vec![
            AuditSubject::Agent(request.source()),
            AuditSubject::Agent(request.target()),
        ],
        OperatorAction::RenameAgent { agent, .. } => vec![AuditSubject::Agent(*agent)],
        OperatorAction::Unmerge { merge } => state
            .merges
            .iter()
            .find(|m| m.id == *merge)
            .map(|m| vec![AuditSubject::Agent(m.from), AuditSubject::Agent(m.into)])
            .unwrap_or_default(),
        OperatorAction::SetVerdict { transmission, .. } => {
            vec![AuditSubject::Transmission(*transmission)]
        }
        OperatorAction::Acknowledge { alert } | OperatorAction::Resolve { alert, .. } => {
            vec![AuditSubject::Alert(*alert)]
        }
        OperatorAction::UpdateRule { id, .. } | OperatorAction::SetRuleEnabled { id, .. } => {
            vec![AuditSubject::Rule(*id)]
        }
        OperatorAction::CreateRule { .. } | OperatorAction::ReplayDeadLetter { .. } => Vec::new(),
    }
}
