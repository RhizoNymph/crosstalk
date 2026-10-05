//! `LiveFeed` over HTTP: `GET /live` read as Server-Sent Events.
//!
//! ```text
//! subscribe(resume) ─▶ GET /live, Last-Event-ID: <cursor> (From) | none (Fresh)
//!   ├─ error ─▶ Err(QueryError) (403 without View)
//!   └─ 200 text/event-stream ─▶ HttpLiveStream
//! next():
//!   event: <item name> / id: <cursor> / data: <LiveItem>  ─▶ Ok(item), the resume point moves to its cursor
//!   event: end / data: <LiveEnd> (no id)                  ─▶ Err(end), the stream is closed
//!   cut: the body ends or fails without `end`, no bytes within the idle timeout,
//!        or an event that is not the binding's framing
//!     ─▶ reconnect with Last-Event-ID: <last cursor>, as a browser's EventSource does
//!          ├─ 200 ─▶ carry on; the surface replays what followed the cursor, or resyncs
//!          ├─ 401 or 403 ─▶ Err(SessionEnded): the caller must sign in again
//!          └─ fails ─▶ retry per ReconnectPolicy; out of attempts ─▶ Err(ShuttingDown)
//! ```
//!
//! **Resume.** An item's cursor is its SSE id, and the surface replays
//! every retained entry after the cursor a request names (or sends a
//! resync first), so reconnecting with the last cursor received loses
//! nothing and repeats nothing (`surface.http.sse-resume`,
//! `surface.http.client-live-resumes`). The client sends the cursor as
//! `Last-Event-ID`, which the surface prefers over the `cursor` parameter;
//! a `Resume::Unreadable` is sent as text that is not a cursor, so the
//! surface answers it as it answers any unreadable id, with a resync.
//!
//! **Framing.** Every event must be the binding's framing of what it
//! carries (`surface.live.sse-frame-matches-item`): an item's event name
//! is its `LiveItem::event_name` and its id its cursor's text; the end
//! event has no id. An event that is not is treated as a cut, so the
//! client never moves its resume point to a cursor it did not read.

pub(crate) mod sse;

use crosstalk_spec::interfaces::l8_surface::http::sse::{EVENT_STREAM, LAST_EVENT_ID};
use crosstalk_spec::interfaces::l8_surface::http::{RequestBuilder, Route};
use crosstalk_spec::interfaces::l8_surface::live::{
    LiveCursor, LiveEnd, LiveFeed, LiveItem, LiveStream, Resume,
};
use crosstalk_spec::interfaces::l8_surface::{Caller, QueryError};
use hyper::StatusCode;
use hyper::header::HeaderMap;

use crate::body::{self, ChunkReader};
use crate::client::{HttpClient, expect_content_type};
use crate::error::{ClientError, TransportError, decode_error};

use sse::{SseEvent, SseParser};

/// The `Last-Event-ID` sent for `Resume::Unreadable`: any text that is not
/// a cursor reads as unreadable.
const UNREADABLE: &str = "unreadable";

/// A live feed subscription over HTTP: the items of `GET /live`, across
/// reconnects, until the surface ends the stream or the client gives up.
#[derive(Debug)]
pub struct HttpLiveStream {
    client: HttpClient,
    resume: Resume,
    connection: Option<Connection>,
    /// Connection attempts since the last item delivered.
    attempts: u32,
    ended: Option<LiveEnd>,
}

#[derive(Debug)]
struct Connection {
    chunks: ChunkReader,
    parser: SseParser,
}

/// What one event is under the binding's framing.
#[derive(Debug, PartialEq)]
enum Frame {
    Item(LiveItem),
    End(LiveEnd),
    Invalid(String),
}

fn frame(event: SseEvent) -> Frame {
    if event.name == LiveEnd::EVENT_NAME {
        if event.id.is_some() {
            return Frame::Invalid("the end event carries an id".to_owned());
        }
        return match serde_json::from_str::<LiveEnd>(&event.data) {
            Ok(end) => Frame::End(end),
            Err(error) => Frame::Invalid(format!("end data: {error}")),
        };
    }
    let item = match serde_json::from_str::<LiveItem>(&event.data) {
        Ok(item) => item,
        Err(error) => return Frame::Invalid(format!("item data: {error}")),
    };
    if event.name != item.event_name() {
        return Frame::Invalid(format!(
            "event `{}` carries a `{}` item",
            event.name,
            item.event_name()
        ));
    }
    let cursor = item.cursor().encode();
    if event.id.as_deref() != Some(cursor.as_str()) {
        return Frame::Invalid(format!("id {:?} for cursor {cursor}", event.id));
    }
    Frame::Item(item)
}

impl<H> HttpClient<H> {
    /// `GET /live` from `resume`, up to a `200` event stream.
    async fn open_live(&self, resume: Resume) -> Result<Connection, ClientError<QueryError>> {
        let route = Route::Live;
        let last_event_id = match resume {
            Resume::Fresh => None,
            Resume::From(cursor) => Some(cursor.encode()),
            Resume::Unreadable => Some(UNREADABLE.to_owned()),
        };
        let request = RequestBuilder::new(route)
            .header(LAST_EVENT_ID, last_event_id.as_deref())
            .build()
            .map_err(ClientError::Encode)?;
        let config = *self.config();
        let timeout = || TransportError::Timeout {
            millis: config.request_timeout().as_millis(),
        };
        let response = tokio::time::timeout(
            config.request_timeout(),
            self.send(route, request, EVENT_STREAM, HeaderMap::new()),
        )
        .await
        .map_err(|_| timeout())??;
        let (parts, body) = response.into_parts();
        let status = parts.status.as_u16();
        if parts.status != StatusCode::OK {
            let error_body = tokio::time::timeout(
                config.request_timeout(),
                body::collect(body, config.max_response_bytes()),
            )
            .await
            .map_err(|_| timeout())??;
            return Err(decode_error(route, status, &error_body));
        }
        expect_content_type(route, status, &parts.headers, EVENT_STREAM)?;
        tracing::info!(resume = ?resume, "live feed connected");
        Ok(Connection {
            chunks: ChunkReader::new(body, config.idle_timeout()),
            parser: SseParser::new(config.max_response_bytes()),
        })
    }
}

impl<H> LiveFeed for HttpClient<H> {
    type Stream = HttpLiveStream;

    async fn subscribe(&self, _: &Caller, resume: Resume) -> Result<Self::Stream, QueryError> {
        let connection = self.open_live(resume).await.map_err(QueryError::from)?;
        Ok(HttpLiveStream {
            client: self.with_row_hasher(),
            resume,
            connection: Some(connection),
            attempts: 0,
            ended: None,
        })
    }
}

impl HttpLiveStream {
    /// The cursor of the last item delivered: where a new subscription
    /// resumes after this stream ends. `None` before the first.
    pub fn last_cursor(&self) -> Option<LiveCursor> {
        match self.resume {
            Resume::From(cursor) => Some(cursor),
            Resume::Fresh | Resume::Unreadable => None,
        }
    }

    /// Drops the connection; the next call reconnects.
    fn cut(&mut self, reason: &str) {
        tracing::warn!(reason = %reason, last = ?self.last_cursor(), "live feed cut");
        self.connection = None;
    }

    /// The next event of the current connection, reading as needed; `None`
    /// after cutting it.
    async fn read_event(&mut self) -> Option<SseEvent> {
        let connection = self.connection.as_mut()?;
        loop {
            if let Some(event) = connection.parser.next_event() {
                return Some(event);
            }
            let failure = match connection.chunks.chunk().await {
                Ok(Some(bytes)) => match connection.parser.feed(&bytes) {
                    Ok(()) => continue,
                    Err(error) => error.to_string(),
                },
                Ok(None) => "the response ended without an end event".to_owned(),
                Err(error) => error.to_string(),
            };
            self.cut(&failure);
            return None;
        }
    }

    /// Opens a new connection from the last cursor, after the policy's
    /// delay; ends the stream when the surface refuses the caller or the
    /// attempts run out.
    async fn reconnect(&mut self) {
        let policy = self.client.config().reconnect();
        self.attempts += 1;
        if self.attempts > policy.attempts().get() {
            tracing::error!(attempts = self.attempts - 1, "live feed unreachable");
            self.ended = Some(LiveEnd::ShuttingDown);
            return;
        }
        tokio::time::sleep(policy.delay(self.attempts)).await;
        match self.client.open_live(self.resume).await {
            Ok(connection) => self.connection = Some(connection),
            Err(ClientError::Unauthenticated(error)) => {
                tracing::info!(reason = ?error.reason, "live feed: no caller");
                self.ended = Some(LiveEnd::SessionEnded);
            }
            Err(ClientError::Api(QueryError::Forbidden { missing })) => {
                tracing::info!(missing = ?missing, "live feed: permission withdrawn");
                self.ended = Some(LiveEnd::SessionEnded);
            }
            Err(error) => {
                tracing::warn!(attempt = self.attempts, error = %error, "live feed reconnect failed");
            }
        }
    }
}

impl LiveStream for HttpLiveStream {
    async fn next(&mut self) -> Result<LiveItem, LiveEnd> {
        loop {
            if let Some(end) = self.ended {
                return Err(end);
            }
            if self.connection.is_none() {
                self.reconnect().await;
                continue;
            }
            let Some(event) = self.read_event().await else {
                continue;
            };
            match frame(event) {
                Frame::Item(item) => {
                    self.resume = Resume::From(item.cursor());
                    self.attempts = 0;
                    return Ok(item);
                }
                Frame::End(end) => {
                    tracing::info!(end = ?end, "live feed ended");
                    self.connection = None;
                    self.ended = Some(end);
                }
                Frame::Invalid(reason) => self.cut(&reason),
            }
        }
    }
}
