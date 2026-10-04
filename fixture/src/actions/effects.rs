//! State changes shared by operator actions and by history generation, so
//! the generated past follows the same rules as live actions.

use crosstalk_spec::aggregates::alert::{Alert, AlertState, AlertSubject, SuppressReason};
use crosstalk_spec::ids::{AlertRuleId, ChannelId, TransmissionId};
use crosstalk_spec::support::Timestamp;

use crate::alert_state;
use crate::store::State;

pub fn is_active(alert: &Alert) -> bool {
    alert_state::is_active(&alert.state)
}

/// `AlertTriage::channel_sanctioned`: suppresses the active alerts whose
/// subject is `channel` or a channel it superseded (subjects compared
/// resolved). Alerts about transmissions on it stay.
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

/// `AlertTriage::rule_disabled`: suppresses the active alerts raised by
/// `rule`.
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

/// `AlertTriage::transmission_judged` with a newer `FalseDetection`:
/// suppresses the active alerts about `transmission`, whatever their rule.
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
