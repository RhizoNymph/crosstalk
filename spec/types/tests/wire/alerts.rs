//! The alert inbox on the wire: `QueryApi::alerts` (an `AlertFilter` and a
//! `PageRequest<AlertList>` in, a `Page<Alert, AlertList>` out) and
//! `QueryApi::alert` (an `AlertId` in, an `Option<Alert>` out). The
//! reference area for the wire conventions.

use super::harness::{assert_golden, assert_rejected, assert_request_golden};
use super::{ULID_A, ULID_B, ULID_C, id, ts};
use crate::aggregates::alert::{Alert, AlertState, AlertSubject, SuppressReason};
use crate::ids::{AgentId, AlertId, AlertRuleId, ChannelId, OperatorId, TransmissionId};
use crate::interfaces::l8_surface::{AlertFilter, AlertStateKind};
use crate::paging::{AlertList, Cursor, Page, PageSize};
use crate::support::NonEmpty;

const AREA: &str = "alerts";

fn operator() -> OperatorId {
    id(OperatorId::from_ulid_text, ULID_C)
}

fn alert(state: AlertState) -> Alert {
    Alert {
        id: id(AlertId::from_ulid_text, ULID_A),
        rule: AlertRuleId::from_ulid(1),
        subject: AlertSubject::Channel(id(ChannelId::from_ulid_text, ULID_B)),
        raised_at: ts("2026-10-04T12:34:56.789012Z"),
        occurrences: 3,
        state,
    }
}

/// One alert in every state.
fn every_state() -> Vec<(&'static str, AlertState)> {
    fn declared(state: AlertState) -> AlertState {
        match state {
            AlertState::Open
            | AlertState::Acknowledged { .. }
            | AlertState::Resolved { .. }
            | AlertState::Suppressed { .. } => state,
        }
    }
    [
        ("alert_open", AlertState::Open),
        (
            "alert_acknowledged",
            AlertState::Acknowledged {
                by: operator(),
                at: ts("2026-10-04T12:35:10.000000Z"),
            },
        ),
        (
            "alert_resolved",
            AlertState::Resolved {
                by: operator(),
                at: ts("2026-10-04T13:00:00.250000Z"),
                note: Some("expected: the planner briefs the coder through the wiki".into()),
            },
        ),
        (
            "alert_suppressed",
            AlertState::Suppressed {
                at: ts("2026-10-04T13:05:00.000000Z"),
                reason: SuppressReason::ChannelSanctioned,
            },
        ),
    ]
    .into_iter()
    .map(|(name, state)| (name, declared(state)))
    .collect()
}

#[test]
fn alerts_golden_in_every_state() {
    for (name, state) in every_state() {
        assert_golden(AREA, name, &alert(state));
    }
}

#[test]
fn alert_subjects_and_reasons_golden() {
    fn declared(subject: AlertSubject) -> AlertSubject {
        match subject {
            AlertSubject::Channel(_) | AlertSubject::Transmission(_) | AlertSubject::Agent(_) => {
                subject
            }
        }
    }
    let subjects = [
        AlertSubject::Channel(id(ChannelId::from_ulid_text, ULID_A)),
        AlertSubject::Transmission(id(TransmissionId::from_ulid_text, ULID_B)),
        AlertSubject::Agent(id(AgentId::from_ulid_text, ULID_C)),
    ]
    .map(declared);
    assert_golden(AREA, "alert_subjects", &subjects.to_vec());

    fn reason(reason: SuppressReason) -> SuppressReason {
        match reason {
            SuppressReason::ChannelSanctioned
            | SuppressReason::RuleDisabled
            | SuppressReason::OperatorRejected => reason,
        }
    }
    let reasons = [
        SuppressReason::ChannelSanctioned,
        SuppressReason::RuleDisabled,
        SuppressReason::OperatorRejected,
    ]
    .map(reason);
    assert_golden(AREA, "suppress_reasons", &reasons.to_vec());
}

/// `QueryApi::alerts`: the request's two arguments and the page it returns.
#[test]
fn alert_list_request_and_response_golden() {
    fn kind(kind: AlertStateKind) -> AlertStateKind {
        match kind {
            AlertStateKind::Open
            | AlertStateKind::Acknowledged
            | AlertStateKind::Resolved
            | AlertStateKind::Suppressed => kind,
        }
    }
    let filter = AlertFilter {
        states: [AlertStateKind::Open, AlertStateKind::Acknowledged]
            .map(kind)
            .to_vec(),
        channel: Some(id(ChannelId::from_ulid_text, ULID_B)),
    };
    assert_request_golden(AREA, "alert_filter", &filter);
    assert_request_golden(AREA, "alert_filter_everything", &AlertFilter::default());
    let kinds = [
        AlertStateKind::Open,
        AlertStateKind::Acknowledged,
        AlertStateKind::Resolved,
        AlertStateKind::Suppressed,
    ]
    .map(kind);
    assert_golden(AREA, "alert_state_kinds", &kinds.to_vec());

    let size = PageSize::new(2).expect("a valid size");
    let next: Cursor<AlertList> =
        Cursor::from_token("YWxlcnRzLWFmdGVyLTAxSjla".into()).expect("URL-safe base64");
    let items = NonEmpty::from_vec(
        every_state()
            .into_iter()
            .take(2)
            .map(|(_, state)| alert(state))
            .collect(),
    )
    .expect("two alerts");
    let page = Page::more(size, items, next).expect("two alerts fit a page of two");
    assert_golden(AREA, "alerts_page", &page);
    let last: Page<Alert, AlertList> =
        Page::last(size, vec![alert(AlertState::Open)]).expect("one alert fits");
    assert_golden(AREA, "alerts_last_page", &last);
}

/// `QueryApi::alert`: an alert, or `null` for an unknown id.
#[test]
fn single_alert_response_golden() {
    assert_golden(AREA, "alert_found", &Some(alert(AlertState::Open)));
    assert_golden(AREA, "alert_unknown", &None::<Alert>);
}

#[test]
fn alerts_refuse_unknown_fields_and_variants() {
    let at = "2026-10-04T12:35:10.000000Z";
    assert_rejected::<AlertState>(r#"{"type": "snoozed"}"#, "unknown variant `snoozed`");
    assert_rejected::<AlertState>(r#"{"type": "Open"}"#, "unknown variant `Open`");
    assert_rejected::<AlertState>(
        &format!(
            r#"{{"type": "acknowledged", "data": {{"by": "{ULID_C}", "at": "{at}", "via": "slack"}}}}"#
        ),
        "unknown field `via`",
    );
    assert_rejected::<AlertState>(r#"{"type": "acknowledged"}"#, "missing field `data`");
    assert_rejected::<AlertState>(
        r#"{"type": "open", "since": "yesterday"}"#,
        r#"expected "type" or "data""#,
    );
    assert_rejected::<AlertSubject>(
        &format!(r#"{{"type": "resource", "data": "{ULID_A}"}}"#),
        "unknown variant `resource`",
    );
    assert_rejected::<AlertSubject>(
        r#"{"type": "channel", "data": "not-an-id"}"#,
        "invalid ULID text",
    );
    assert_rejected::<SuppressReason>(r#""snoozed""#, "unknown variant `snoozed`");
    let alert = format!(
        r#"{{"id": "{ULID_A}", "rule": "00000000000000000000000001",
            "subject": {{"type": "agent", "data": "{ULID_B}"}},
            "raised_at": "{at}", "occurrences": 1, "state": {{"type": "open"}},
            "severity": "high"}}"#
    );
    assert_rejected::<Alert>(&alert, "unknown field `severity`");
}

#[test]
fn alert_filters_refuse_unknown_fields_and_kinds() {
    assert_rejected::<AlertFilter>(
        r#"{"states": ["open"], "channel": null, "by": "01J9Z3N4P5Q6R7S8T9V0W1X2Y3"}"#,
        "unknown field `by`",
    );
    assert_rejected::<AlertFilter>(
        r#"{"states": ["dismissed"], "channel": null}"#,
        "unknown variant `dismissed`",
    );
}
