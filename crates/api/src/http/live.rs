//! `GET /live`: the live feed over Server-Sent Events
//! (`surface.http.sse-resume`, `surface.live.sse-frame-matches-item`).
//!
//! ```text
//! sse::resume(Last-Event-ID, cursor) ─▶ LiveFeed::subscribe(caller, resume)
//!   ├─ Err(e) ─▶ e.status(), e's JSON (403 without View): no stream
//!   └─ Ok(stream) ─▶ 200, text/event-stream, no-store, X-Accel-Buffering: no
//!        LiveStream::next ─ Ok(item) ─▶ event_frame(item), one chunk each
//!                         ─ Err(end) ─▶ end_frame(end), then the body ends
//! ```
//!
//! Heartbeats are the stream's items, written like any other. An item
//! with no JSON (`Unencodable`) cannot be framed: the body is aborted, so
//! the client reconnects from its last id rather than skipping an item.

use axum::body::{Body, Bytes};
use axum::http::{HeaderName, HeaderValue, StatusCode};
use axum::response::Response;
use crosstalk_spec::interfaces::l8_surface::http::sse::{
    CURSOR_PARAM, HEADERS, LAST_EVENT_ID, end_frame, event_frame, resume,
};
use crosstalk_spec::interfaces::l8_surface::live::LiveStream;
use crosstalk_spec::interfaces::l8_surface::{Caller, QueryError};
use futures_util::stream;

use super::Surface;
use super::input::Input;

/// Why a live body stopped before its `end` event.
#[derive(Debug, thiserror::Error)]
#[error("live item has no SSE frame: {0}")]
struct Unframable(String);

pub(super) async fn serve<S: Surface>(
    surface: &S,
    caller: &Caller,
    input: &Input,
) -> Result<Response, QueryError> {
    let last_event_id = input.header(LAST_EVENT_ID);
    let from = resume(last_event_id.as_deref(), input.query_text(CURSOR_PARAM));
    let subscription = surface.subscribe(caller, from).await?;
    let events = stream::unfold(Some(subscription), |state| async move {
        let mut subscription = state?;
        match subscription.next().await {
            Ok(item) => match event_frame(&item) {
                Ok(frame) => Some((Ok(Bytes::from(frame)), Some(subscription))),
                Err(unencodable) => {
                    tracing::warn!(error = %unencodable, "live stream aborted");
                    Some((Err(Unframable(unencodable.0)), None))
                }
            },
            Err(end) => Some((Ok(Bytes::from(end_frame(end))), None)),
        }
    });
    let mut response = Response::new(Body::from_stream(events));
    *response.status_mut() = StatusCode::OK;
    let headers = response.headers_mut();
    for (name, value) in HEADERS {
        // The spec's constants are lower-case names and visible ASCII.
        if let (Ok(name), Ok(value)) = (HeaderName::try_from(name), HeaderValue::try_from(value)) {
            headers.insert(name, value);
        }
    }
    Ok(response)
}
