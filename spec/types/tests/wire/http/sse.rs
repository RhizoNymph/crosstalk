//! `GET /live`: where a subscription resumes, and the exact bytes of each
//! SSE event.

use super::super::{ULID_A, id, ts};
use crate::ids::AlertId;
use crate::interfaces::l8_surface::http::sse::{
    CURSOR_PARAM, EVENT_STREAM, HEADERS, LAST_EVENT_ID, end_frame, event_frame, resume,
};
use crate::interfaces::l8_surface::live::{
    FeedEpoch, FeedWindow, LiveCursor, LiveEnd, LiveItem, Resume, ResumePlan, ResyncReason, UiEvent,
};
use crate::support::Watermark;

fn cursor(seq: u64) -> LiveCursor {
    LiveCursor {
        epoch: FeedEpoch(7),
        seq,
    }
}

/// `Last-Event-ID` wins over the `cursor` parameter, either is read as a
/// cursor's text, and neither is `Fresh`.
#[test]
fn resume_prefers_last_event_id_then_the_cursor_parameter() {
    assert_eq!(resume(None, None), Resume::Fresh);
    assert_eq!(resume(None, Some("7-10")), Resume::From(cursor(10)));
    assert_eq!(resume(Some("7-12"), None), Resume::From(cursor(12)));
    assert_eq!(resume(Some("7-12"), Some("7-10")), Resume::From(cursor(12)));
    // Text that is not a cursor resyncs; it is never an error.
    assert_eq!(resume(None, Some("\"7-10\"")), Resume::Unreadable);
    assert_eq!(resume(Some("07-12"), Some("7-10")), Resume::Unreadable);
    let window = FeedWindow::new(FeedEpoch(7), 5, 20).unwrap_or_else(|e| panic!("{e:?}"));
    assert_eq!(
        window.resume(resume(None, Some("7-10"))),
        ResumePlan::Replay { after: 10 }
    );
    assert_eq!(
        window.resume(resume(Some("x"), Some("7-10"))),
        ResumePlan::Resync(ResyncReason::Unreadable)
    );
    assert_eq!(window.resume(resume(None, None)), ResumePlan::Live);
}

#[test]
fn an_item_is_one_event_of_three_fields() {
    let item = LiveItem::Event {
        cursor: cursor(1042),
        event: UiEvent::AlertChanged {
            id: id(AlertId::from_ulid_text, ULID_A),
        },
    };
    assert_eq!(
        event_frame(&item),
        Ok(concat!(
            "event: event\n",
            "id: 7-1042\n",
            r#"data: {"type":"event","data":{"cursor":"7-1042","event":{"type":"alert_changed","data":{"id":"01J9Z3K8M4Q7R2T5V6W8X9Y0ZA"}}}}"#,
            "\n\n"
        )
        .to_owned())
    );
    let heartbeat = LiveItem::Heartbeat { cursor: cursor(9) };
    assert_eq!(
        event_frame(&heartbeat),
        Ok("event: heartbeat\nid: 7-9\ndata: {\"type\":\"heartbeat\",\"data\":{\"cursor\":\"7-9\"}}\n\n".to_owned())
    );
    let resync = LiveItem::Resync {
        cursor: cursor(20),
        reason: ResyncReason::Expired,
    };
    let frame = event_frame(&resync).unwrap_or_else(|e| panic!("{e}"));
    assert!(
        frame.starts_with("event: resync\nid: 7-20\ndata: "),
        "{frame}"
    );
    let watermark = LiveItem::Event {
        cursor: cursor(3),
        event: UiEvent::Watermark {
            at: Watermark(ts("2026-10-04T12:30:00.000000Z")),
        },
    };
    let frame = event_frame(&watermark).unwrap_or_else(|e| panic!("{e}"));
    assert_eq!(
        frame.matches('\n').count(),
        4,
        "three field lines and a blank line"
    );
}

/// The end event's data is the `LiveEnd`'s JSON, and it has no `id`.
#[test]
fn the_end_event_carries_the_reason_and_no_id() {
    for end in [
        LiveEnd::Lagged,
        LiveEnd::SessionEnded,
        LiveEnd::ShuttingDown,
    ] {
        let json = serde_json::to_string(&end).unwrap_or_else(|e| panic!("{e}"));
        let frame = end_frame(end);
        assert_eq!(frame, format!("event: end\ndata: {json}\n\n"));
        assert!(
            !frame.contains("\nid:") && !frame.starts_with("id:"),
            "{frame}"
        );
    }
}

#[test]
fn the_stream_is_uncached_event_stream() {
    assert_eq!(EVENT_STREAM, "text/event-stream");
    assert!(HEADERS.contains(&("content-type", "text/event-stream")));
    assert!(HEADERS.contains(&("cache-control", "no-store")));
    assert_eq!((LAST_EVENT_ID, CURSOR_PARAM), ("last-event-id", "cursor"));
}
