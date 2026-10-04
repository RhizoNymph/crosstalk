//! `GET /live`: the live feed over Server-Sent Events.
//!
//! ```text
//! GET /live[?cursor=7-1042]  (Last-Event-ID: 7-1042 on a browser's reconnect)
//!   ─▶ resume(last_event_id, cursor) ─▶ LiveFeed::subscribe(caller, resume)
//!        ├─ Err(e) ─▶ e.status(), e's JSON (403 without View): no stream
//!        └─ Ok(stream) ─▶ 200, text/event-stream
//!             event_frame(item) per LiveItem (heartbeats at least every LiveConfig::heartbeat)
//!             … end_frame(end) once the stream ends, then the response closes
//! ```
//!
//! **Resume.** The browser's `EventSource` sends the last event id it
//! received as `Last-Event-ID` when it reconnects by itself. A client that
//! opens a new `EventSource` (after a page load, or after the stream ended
//! with `end`) cannot set that header, so it may pass the same text as
//! the `cursor` query parameter. [`resume`] reads the header when present
//! (it is the newer of the two on an automatic reconnect), else the
//! parameter, with [`Resume::from_last_event_id`] either way: neither is
//! JSON, and text that is not a cursor is [`Resume::Unreadable`], which the
//! feed answers with a resync rather than an error.
//!
//! **Framing.** Each item is one event of three fields, `event` (its
//! [`LiveItem::event_name`]), `id` (its cursor's text) and `data` (the
//! item's JSON on one line), ended by a blank line ([`event_frame`]). The
//! last event of a stream is named `end`, carries the [`LiveEnd`] as its
//! data and has no `id` ([`end_frame`]), so the client's resume point stays
//! on its last item; then the response ends. No `retry` field and no
//! comment lines are sent: heartbeats are items, so they move the client's
//! cursor.
//!
//! **Headers.** `Content-Type: text/event-stream`, `Cache-Control:
//! no-store` and `X-Accel-Buffering: no` (so a buffering proxy passes each
//! event at once); no `Content-Length`.

use std::fmt;

use super::super::live::{LiveEnd, LiveItem, Resume};

/// The content type of the live feed.
pub const EVENT_STREAM: &str = "text/event-stream";

/// The request header an `EventSource` resumes with.
pub const LAST_EVENT_ID: &str = "last-event-id";

/// The query parameter a new `EventSource` resumes with.
pub const CURSOR_PARAM: &str = "cursor";

/// The response headers of a live stream.
pub const HEADERS: [(&str, &str); 3] = [
    ("content-type", EVENT_STREAM),
    ("cache-control", "no-store"),
    ("x-accel-buffering", "no"),
];

/// Where a subscription starts: `Last-Event-ID` if the request has it,
/// else the `cursor` parameter, else `Fresh`.
pub fn resume(last_event_id: Option<&str>, cursor: Option<&str>) -> Resume {
    Resume::from_last_event_id(last_event_id.or(cursor))
}

/// A value with no JSON: an item whose watermark lies after year 9999.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Unencodable(pub String);

impl fmt::Display for Unencodable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "live item has no JSON: {}", self.0)
    }
}

/// One item as its SSE event: `event: <name>\nid: <cursor>\ndata: <json>\n\n`.
pub fn event_frame(item: &LiveItem) -> Result<String, Unencodable> {
    let data = serde_json::to_string(item).map_err(|error| Unencodable(error.to_string()))?;
    Ok(format!(
        "event: {}\nid: {}\ndata: {data}\n\n",
        item.event_name(),
        item.cursor().encode()
    ))
}

/// The stream's last event: `event: end\ndata: "<reason>"\n\n`, no `id`.
pub fn end_frame(end: LiveEnd) -> String {
    let reason = match end {
        LiveEnd::Lagged => "lagged",
        LiveEnd::SessionEnded => "session_ended",
        LiveEnd::ShuttingDown => "shutting_down",
    };
    format!("event: {}\ndata: \"{reason}\"\n\n", LiveEnd::EVENT_NAME)
}
