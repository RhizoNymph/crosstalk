//! Alerts as rows: state, who moved it there, rule name and subject link.

use crosstalk_spec::aggregates::alert::{Alert, AlertState, SuppressReason};
use crosstalk_spec::interfaces::l8_surface::AlertStateKind;

use crate::backend::alert_state;
use crate::components::{format_time, short_id};
use crate::pages::common::links::{alert_subject, alert_url};
use crate::pages::common::lookup::OperatorNames;
use crate::pages::common::rules::RuleNames;
use crate::url::ulid::UlidId;
use crate::url::view_state::ViewState;

pub fn suppress_reason(reason: SuppressReason) -> &'static str {
    match reason {
        SuppressReason::ChannelSanctioned => "channel sanctioned",
        SuppressReason::RuleDisabled => "rule disabled",
        SuppressReason::OperatorRejected => "rejected as a false detection",
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlertRow {
    /// The full id, for action forms.
    pub id: String,
    pub short: String,
    /// The alert's own page.
    pub url: String,
    pub state: AlertStateKind,
    /// Who moved the alert to its state and when, or why it was suppressed.
    pub state_detail: String,
    pub note: Option<String>,
    pub rule: String,
    pub subject_url: String,
    pub subject_label: String,
    pub occurrences: u32,
    pub raised: String,
}

impl AlertRow {
    pub fn new(
        alert: &Alert,
        rules: &RuleNames,
        operators: &OperatorNames,
        state: &ViewState,
    ) -> Self {
        let (state_detail, note) = match &alert.state {
            AlertState::Open => (String::new(), None),
            AlertState::Acknowledged { by, at } => (
                format!("by {} at {}", operators.name(*by), format_time(*at)),
                None,
            ),
            AlertState::Resolved { by, at, note } => (
                format!("by {} at {}", operators.name(*by), format_time(*at)),
                note.clone(),
            ),
            AlertState::Suppressed { at, reason } => (
                format!("{} at {}", suppress_reason(*reason), format_time(*at)),
                None,
            ),
        };
        let (subject_url, subject_label) = alert_subject(&alert.subject, state);
        Self {
            id: alert.id.to_ulid(),
            short: short_id(alert.id.to_ulid()),
            url: alert_url(alert.id, state),
            state: alert_state::kind(&alert.state),
            state_detail,
            note,
            rule: rules.name(alert.rule),
            subject_url,
            subject_label,
            occurrences: alert.occurrences,
            raised: format_time(alert.raised_at),
        }
    }

    /// Open alerts can be acknowledged.
    pub fn can_acknowledge(&self) -> bool {
        self.state == AlertStateKind::Open
    }

    /// Open and acknowledged alerts can be resolved.
    pub fn can_resolve(&self) -> bool {
        matches!(
            self.state,
            AlertStateKind::Open | AlertStateKind::Acknowledged
        )
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use crosstalk_spec::aggregates::alert::AlertSubject;
    use crosstalk_spec::ids::{AlertId, AlertRuleId, ChannelId, OperatorId};
    use crosstalk_spec::support::Timestamp;

    use super::*;
    use crate::components::href::tests::state;

    pub fn alert(id: u128, state: AlertState) -> Alert {
        Alert {
            id: AlertId::from_ulid(id),
            rule: AlertRuleId::from_ulid(1),
            subject: AlertSubject::Channel(ChannelId::from_ulid(2)),
            raised_at: Timestamp::from_micros(1_790_985_600_000_000),
            occurrences: 4,
            state,
        }
    }

    #[test]
    fn rows_name_rule_operator_and_subject() {
        let rules = RuleNames::new([(AlertRuleId::from_ulid(1), "new channel".to_owned())]);
        let operators = OperatorNames::new([(OperatorId::from_ulid(3), "ada".to_owned())]);
        let resolved = alert(
            5,
            AlertState::Resolved {
                by: OperatorId::from_ulid(3),
                at: Timestamp::from_micros(1_790_985_600_000_000),
                note: Some("expected".into()),
            },
        );
        let row = AlertRow::new(&resolved, &rules, &operators, &state());
        assert_eq!(row.rule, "new channel");
        assert_eq!(row.state, AlertStateKind::Resolved);
        assert_eq!(row.state_detail, "by ada at 2026-10-03 00:00:00 UTC");
        assert_eq!(row.note.as_deref(), Some("expected"));
        assert!(row.subject_url.starts_with("/channels/"));
        assert!(!row.can_resolve() && !row.can_acknowledge());
    }

    #[test]
    fn actions_follow_state() {
        let rows = |s| {
            AlertRow::new(
                &alert(1, s),
                &RuleNames::default(),
                &OperatorNames::default(),
                &state(),
            )
        };
        let open = rows(AlertState::Open);
        assert!(open.can_acknowledge() && open.can_resolve());
        let acked = rows(AlertState::Acknowledged {
            by: OperatorId::from_ulid(1),
            at: Timestamp::from_micros(0),
        });
        assert!(!acked.can_acknowledge() && acked.can_resolve());
        let suppressed = rows(AlertState::Suppressed {
            at: Timestamp::from_micros(0),
            reason: SuppressReason::RuleDisabled,
        });
        assert!(suppressed.state_detail.starts_with("rule disabled at"));
        assert!(!suppressed.can_resolve());
        assert!(open.rule.starts_with("rule …"));
    }

    #[test]
    fn rejected_alerts_say_they_were_judged_false() {
        let row = AlertRow::new(
            &alert(
                1,
                AlertState::Suppressed {
                    at: Timestamp::from_micros(1_790_985_600_000_000),
                    reason: SuppressReason::OperatorRejected,
                },
            ),
            &RuleNames::default(),
            &OperatorNames::default(),
            &state(),
        );
        assert_eq!(row.state, AlertStateKind::Suppressed);
        assert_eq!(
            row.state_detail,
            "rejected as a false detection at 2026-10-03 00:00:00 UTC"
        );
    }
}
