//! The live feed on the wire: each SSE event carries one `LiveItem` as its
//! data, named by its variant and identified by its cursor's text. One
//! golden of every `UiEvent`, one per `LiveItem` variant, and the cursor,
//! resync reasons, stream ends and resume points.

use serde_json::Value;

use super::super::harness::{assert_golden, assert_rejected, assert_round_trips};
use super::super::{ULID_A, ULID_B, ULID_C, id, ts};
use crate::aggregates::topic::TopicModelVersion;
use crate::ids::{AgentId, AlertId, AlertRuleId, ChannelId, ProjectionId, TransmissionId};
use crate::interfaces::l8_surface::live::{
    FeedEpoch, LiveCursor, LiveEnd, LiveItem, Resume, ResyncReason, UiEvent,
};
use crate::support::Watermark;

const AREA: &str = "surface_actions/live";

fn cursor(seq: u64) -> LiveCursor {
    LiveCursor {
        epoch: FeedEpoch(7),
        seq,
    }
}

/// One event of every variant.
fn every_event() -> Vec<UiEvent> {
    fn declared(event: UiEvent) -> UiEvent {
        match event {
            UiEvent::AlertChanged { .. }
            | UiEvent::ChannelChanged { .. }
            | UiEvent::AgentChanged { .. }
            | UiEvent::RuleChanged { .. }
            | UiEvent::VerdictChanged { .. }
            | UiEvent::Watermark { .. }
            | UiEvent::TopicVersionReady { .. }
            | UiEvent::ProjectionReady { .. } => event,
        }
    }
    [
        UiEvent::AlertChanged {
            id: id(AlertId::from_ulid_text, ULID_A),
        },
        UiEvent::ChannelChanged {
            id: id(ChannelId::from_ulid_text, ULID_B),
        },
        UiEvent::AgentChanged {
            id: id(AgentId::from_ulid_text, ULID_C),
        },
        UiEvent::RuleChanged {
            id: id(AlertRuleId::from_ulid_text, ULID_A),
        },
        UiEvent::VerdictChanged {
            id: id(TransmissionId::from_ulid_text, ULID_B),
        },
        UiEvent::Watermark {
            at: Watermark(ts("2026-10-04T12:30:00.000000Z")),
        },
        UiEvent::TopicVersionReady {
            version: TopicModelVersion(4),
        },
        UiEvent::ProjectionReady {
            id: id(ProjectionId::from_ulid_text, ULID_C),
        },
    ]
    .into_iter()
    .map(declared)
    .collect()
}

/// One item of every variant, with its golden name.
fn every_item() -> Vec<(&'static str, LiveItem)> {
    let items = [
        LiveItem::Event {
            cursor: cursor(1042),
            event: UiEvent::AlertChanged {
                id: id(AlertId::from_ulid_text, ULID_A),
            },
        },
        LiveItem::Resync {
            cursor: cursor(1042),
            reason: ResyncReason::Expired,
        },
        LiveItem::Heartbeat {
            cursor: cursor(1043),
        },
    ];
    items
        .into_iter()
        .map(|item| {
            let name = match item {
                LiveItem::Event { .. } => "live_item_event",
                LiveItem::Resync { .. } => "live_item_resync",
                LiveItem::Heartbeat { .. } => "live_item_heartbeat",
            };
            (name, item)
        })
        .collect()
}

#[test]
fn ui_events_golden_with_every_variant() {
    assert_golden(AREA, "ui_events", &every_event());
}

#[test]
fn live_items_golden_with_every_variant() {
    for (name, item) in every_item() {
        assert_golden(AREA, name, &item);
    }
    for event in every_event() {
        assert_round_trips(&LiveItem::Event {
            cursor: cursor(9),
            event,
        });
    }
}

#[test]
fn cursor_reasons_ends_and_resumes_golden() {
    assert_golden(AREA, "live_cursor", &cursor(1042));
    assert_round_trips(&FeedEpoch(7));
    assert_round_trips(&LiveCursor {
        epoch: FeedEpoch(u64::MAX),
        seq: u64::MAX,
    });
    assert_round_trips(&LiveCursor {
        epoch: FeedEpoch(0),
        seq: 0,
    });

    fn reason(reason: ResyncReason) -> ResyncReason {
        match reason {
            ResyncReason::Expired
            | ResyncReason::OtherEpoch
            | ResyncReason::AheadOfHead
            | ResyncReason::Unreadable => reason,
        }
    }
    let reasons = [
        ResyncReason::Expired,
        ResyncReason::OtherEpoch,
        ResyncReason::AheadOfHead,
        ResyncReason::Unreadable,
    ]
    .map(reason);
    assert_golden(AREA, "resync_reasons", &reasons.to_vec());

    fn end(end: LiveEnd) -> LiveEnd {
        match end {
            LiveEnd::Lagged
            | LiveEnd::SessionEnded
            | LiveEnd::ShuttingDown
            | LiveEnd::Unreachable => end,
        }
    }
    let ends = [
        LiveEnd::Lagged,
        LiveEnd::SessionEnded,
        LiveEnd::ShuttingDown,
        LiveEnd::Unreachable,
    ]
    .map(end);
    assert_golden(AREA, "live_ends", &ends.to_vec());

    fn resume(resume: Resume) -> Resume {
        match resume {
            Resume::Fresh | Resume::From(_) | Resume::Unreadable => resume,
        }
    }
    let resumes = [
        Resume::Fresh,
        Resume::From(cursor(1042)),
        Resume::Unreadable,
    ]
    .map(resume);
    assert_golden(AREA, "resumes", &resumes.to_vec());
}

/// The SSE framing: the `event` field is the item's JSON `type`, the `id`
/// field is the cursor's text (equal to the JSON's `cursor`, and what
/// `Last-Event-ID` sends back), and the `data` field is the item's JSON on
/// one line. The end of a stream has its own name, which no item has.
#[test]
fn sse_event_names_and_ids_match_the_item_json() {
    for (_, item) in every_item() {
        let data = serde_json::to_string(&item).expect("encodes");
        assert!(!data.contains('\n'), "SSE data is one line: {data}");
        let json: Value = serde_json::from_str(&data).expect("JSON");
        assert_eq!(json["type"], item.event_name(), "{data}");
        assert_ne!(item.event_name(), LiveEnd::EVENT_NAME);
        let id = item.cursor().encode();
        assert_eq!(json["data"]["cursor"], id.as_str(), "{data}");
        assert_eq!(
            Resume::from_last_event_id(Some(&id)),
            Resume::From(item.cursor())
        );
    }
    let end = serde_json::to_string(&LiveEnd::Lagged).expect("encodes");
    assert_eq!(end, r#""lagged""#);
}

#[test]
fn live_cursors_refuse_other_text() {
    for text in [
        "",
        "7",
        "7-",
        "-1",
        "a-1",
        "7-1-2",
        " 7-1",
        "+7-1",
        "07-1042",
        "7-01042",
        "7-18446744073709551616",
    ] {
        assert_rejected::<LiveCursor>(&format!("\"{text}\""), "invalid live cursor");
        assert_eq!(LiveCursor::decode(text), None, "{text}");
    }
    assert_rejected::<LiveCursor>("71042", "invalid type: integer");
    assert_rejected::<LiveCursor>(r#"{"epoch": 7, "seq": 1042}"#, "invalid type: map");
}

#[test]
fn live_items_refuse_unknown_fields_and_variants() {
    assert_rejected::<LiveItem>(
        r#"{"type": "ping", "data": {"cursor": "7-1"}}"#,
        "unknown variant `ping`",
    );
    assert_rejected::<LiveItem>(
        r#"{"type": "heartbeat", "data": {"cursor": "7-1", "at": "2026-10-04T12:34:56.789012Z"}}"#,
        "unknown field `at`",
    );
    assert_rejected::<UiEvent>(
        &format!(r#"{{"type": "export_changed", "data": {{"id": "{ULID_A}"}}}}"#),
        "unknown variant `export_changed`",
    );
    assert_rejected::<UiEvent>(
        &format!(r#"{{"type": "alert_changed", "data": {{"id": "{ULID_A}", "state": "open"}}}}"#),
        "unknown field `state`",
    );
    assert_rejected::<ResyncReason>(r#""restarted""#, "unknown variant `restarted`");
    assert_rejected::<LiveEnd>(r#""timeout""#, "unknown variant `timeout`");
    assert_rejected::<Resume>(r#"{"type": "latest"}"#, "unknown variant `latest`");
}
