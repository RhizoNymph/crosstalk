//! `GET /live`: resumed from `Last-Event-ID`, else `cursor`, else fresh,
//! and framed item by item with the `end` event last.

use std::sync::Arc;

use axum::body::Body;
use axum::http::header::{AUTHORIZATION, CACHE_CONTROL, CONTENT_LENGTH, CONTENT_TYPE};
use axum::http::{Request, StatusCode};
use crosstalk_spec::interfaces::l8_surface::http::sse::{end_frame, event_frame};
use crosstalk_spec::interfaces::l8_surface::live::{LiveEnd, LiveItem, Resume};
use crosstalk_spec::interfaces::l8_surface::{Permission, QueryError};
use serde_json::json;

use super::fake::{Call, Fake};
use super::{FULL, error_json, golden, operator, send, server, token, without};

fn items() -> Vec<LiveItem> {
    vec![
        golden("surface_actions/live/live_item_event"),
        golden("surface_actions/live/live_item_heartbeat"),
        golden("surface_actions/live/live_item_resync"),
    ]
}

fn feed(end: LiveEnd) -> Arc<Fake> {
    let fake = Arc::new(Fake::default());
    let mut stream: Vec<Result<LiveItem, LiveEnd>> = items().into_iter().map(Ok).collect();
    stream.push(Err(end));
    fake.live_with(stream);
    fake
}

fn live(uri: &str, last_event_id: Option<&str>, as_operator: u128) -> Request<Body> {
    let mut builder = Request::builder()
        .uri(uri)
        .header(AUTHORIZATION, format!("Bearer {}", token(as_operator)));
    if let Some(id) = last_event_id {
        builder = builder.header("last-event-id", id);
    }
    builder.body(Body::empty()).expect("a request")
}

/// The subscription starts from `Last-Event-ID` when the request has it,
/// else from `cursor`, else fresh; text that is not a cursor is
/// `Unreadable` (a resync), never an error. The response is the event
/// stream with its headers.
#[tokio::test]
async fn live_resumes_from_last_event_id_or_cursor() {
    let cases = [
        ("/live", None, json!({"type": "fresh"})),
        (
            "/live?cursor=7-1042",
            None,
            json!({"type": "from", "data": "7-1042"}),
        ),
        (
            "/live?cursor=7-1042",
            Some("7-1050"),
            json!({"type": "from", "data": "7-1050"}),
        ),
        (
            "/live",
            Some("7-1050"),
            json!({"type": "from", "data": "7-1050"}),
        ),
        (
            "/live?cursor=not-a-cursor",
            None,
            json!({"type": "unreadable"}),
        ),
        ("/live?cursor=07-1", None, json!({"type": "unreadable"})),
        ("/live", Some("garbage"), json!({"type": "unreadable"})),
        (
            "/live?cursor=7-1042",
            Some(""),
            json!({"type": "unreadable"}),
        ),
    ];
    for (uri, header, resume) in cases {
        let line = format!("{uri} {header:?}");
        let fake = feed(LiveEnd::ShuttingDown);
        let reply = send(&server(&fake), live(uri, header, FULL)).await;
        assert_eq!(reply.status, StatusCode::OK, "{line}");
        assert_eq!(
            reply.header(CONTENT_TYPE.as_str()),
            Some("text/event-stream")
        );
        assert_eq!(reply.header(CACHE_CONTROL.as_str()), Some("no-store"));
        assert_eq!(reply.header("x-accel-buffering"), Some("no"));
        assert_eq!(
            reply.header(CONTENT_LENGTH.as_str()),
            None,
            "{line}: streamed"
        );
        let _: Resume = serde_json::from_value(resume.clone()).expect("a resume");
        assert_eq!(
            fake.calls(),
            vec![Call {
                method: "subscribe",
                operator: operator(FULL),
                args: json!({ "resume": resume }),
            }],
            "{line}"
        );
    }
    // Neither JSON nor any other query parameter is read.
    let fake = feed(LiveEnd::ShuttingDown);
    let reply = send(&server(&fake), live("/live?cursor=7-1&since=1", None, FULL)).await;
    assert_eq!(reply.status, StatusCode::BAD_REQUEST);
    assert!(fake.untouched());
    // Without View, no stream: a 403 with the error's JSON.
    let fake = feed(LiveEnd::ShuttingDown);
    let reply = send(
        &server(&fake),
        live("/live", None, without(Permission::View)),
    )
    .await;
    assert_eq!(reply.status, StatusCode::FORBIDDEN);
    assert_eq!(
        reply.json(),
        error_json(&QueryError::Forbidden {
            missing: Permission::View
        })
    );
    assert!(fake.untouched());
}

/// Each item is one event (`event`, `id`, `data`), exactly `event_frame`'s
/// bytes; the stream's end is one `end` event with no `id`, and then the
/// body ends.
#[tokio::test]
async fn sse_frames_carry_name_and_cursor() {
    for end in [
        LiveEnd::Lagged,
        LiveEnd::SessionEnded,
        LiveEnd::ShuttingDown,
    ] {
        let fake = feed(end);
        let reply = send(&server(&fake), live("/live", None, FULL)).await;
        assert_eq!(reply.status, StatusCode::OK);
        let mut expected = String::new();
        for item in items() {
            expected.push_str(&event_frame(&item).expect("an item with JSON"));
        }
        expected.push_str(&end_frame(end));
        let body = String::from_utf8(reply.body.to_vec()).expect("UTF-8");
        assert_eq!(body, expected, "{end:?}");
        let events: Vec<&str> = body.trim_end().split("\n\n").collect();
        assert_eq!(events.len(), items().len() + 1);
        for (event, item) in events.iter().zip(items()) {
            let fields: Vec<&str> = event.lines().collect();
            let data = serde_json::to_string(&item).expect("JSON");
            assert_eq!(
                fields,
                vec![
                    format!("event: {}", item.event_name()),
                    format!("id: {}", item.cursor().encode()),
                    format!("data: {data}"),
                ]
            );
        }
        let last = events.last().expect("the end event");
        assert!(last.starts_with("event: end\n"), "{last}");
        assert!(!last.contains("\nid:"), "the end event has no id");
        let reason: LiveEnd =
            serde_json::from_str(last.trim_start_matches("event: end\ndata: ")).expect("JSON");
        assert_eq!(reason, end);
    }
    // The spec's example, byte for byte.
    let item: LiveItem = golden("surface_actions/live/live_item_event");
    let frame = event_frame(&item).expect("JSON");
    assert!(frame.starts_with("event: event\nid: "), "{frame}");
    assert!(frame.ends_with("}\n\n"), "{frame}");
}
