//! `GET /data/live`: the live feed (`LiveFeed::subscribe`) as server-sent
//! events, resumed from the `Last-Event-ID` header
//! (`Resume::from_last_event_id`). Needs `View`.
//!
//! Each `LiveItem` is one SSE event; its `id:` is the item's
//! `LiveCursor::encode` (`<epoch>-<seq>`), so a reconnecting `EventSource`
//! resumes after it:
//!
//! | Item | `event:` | `data:` |
//! | --- | --- | --- |
//! | `AlertChanged`, `ChannelChanged`, `AgentChanged`, `RuleChanged`, `VerdictChanged` (the transmission), `ProjectionReady` | `alert`, `channel`, `agent`, `rule`, `verdict`, `projection` | `{"id": "<ulid>"}` |
//! | `TopicVersionReady` | `topic-version` | `{"version": <n>}` |
//! | `Watermark` | `watermark` | `{"at": "<RFC 3339>"}` |
//! | `Resync` | `resync` | `{"reason": "expired" \| "other-epoch" \| "ahead-of-head" \| "unreadable"}` |
//! | `Heartbeat` | `heartbeat` | `{}` |
//!
//! When the stream ends (`LiveEnd`) a last event without an id is sent,
//! `event: end` with `{"reason": "lagged" \| "session-ended" \|
//! "shutting-down"}`, and the response closes; the browser reconnects
//! with its last id.

use std::convert::Infallible;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use crosstalk_spec::interfaces::l8_surface::Permission;
use crosstalk_spec::interfaces::l8_surface::live::{
    LiveEnd, LiveFeed, LiveItem, LiveStream, Resume, ResyncReason, UiEvent,
};
use futures_core::Stream;
use serde_json::{Value, json};
use topcoat::context::Cx;
use topcoat::router::content::sse::{Event, Sse, last_event_id};
use topcoat::router::route;

use super::errors::query_error;
use super::require;
use crate::app::{AppBackend, backend, caller};
use crate::url::ulid::UlidId;
use crate::url::view_state::format_time;

/// The backend's subscription.
type Subscription = <AppBackend as LiveFeed>::Stream;

type Waiting = Pin<Box<dyn Future<Output = (Subscription, Result<LiveItem, LiveEnd>)> + Send>>;

/// A subscription as a `Stream` of SSE events.
pub struct LiveEvents {
    state: State,
}

enum State {
    /// Between items.
    Idle(Box<Subscription>),
    /// Waiting for the next item; the future owns the subscription and
    /// gives it back with the item.
    Waiting(Waiting),
    /// The end event was sent.
    Done,
}

impl LiveEvents {
    pub fn new(subscription: Subscription) -> Self {
        Self {
            state: State::Idle(Box::new(subscription)),
        }
    }
}

impl Stream for LiveEvents {
    type Item = Result<Event, Infallible>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        loop {
            match std::mem::replace(&mut this.state, State::Done) {
                State::Idle(mut subscription) => {
                    this.state = State::Waiting(Box::pin(async move {
                        let item = subscription.next().await;
                        (*subscription, item)
                    }));
                }
                State::Waiting(mut waiting) => {
                    return match waiting.as_mut().poll(cx) {
                        Poll::Pending => {
                            this.state = State::Waiting(waiting);
                            Poll::Pending
                        }
                        Poll::Ready((subscription, Ok(item))) => {
                            this.state = State::Idle(Box::new(subscription));
                            Poll::Ready(Some(Ok(frame(item))))
                        }
                        Poll::Ready((_, Err(end))) => Poll::Ready(Some(Ok(ended(end)))),
                    };
                }
                State::Done => return Poll::Ready(None),
            }
        }
    }
}

/// The `event:` name and the `data:` of a feed event.
pub fn event_fields(event: UiEvent) -> (&'static str, Value) {
    match event {
        UiEvent::AlertChanged { id } => ("alert", json!({ "id": id.to_ulid() })),
        UiEvent::ChannelChanged { id } => ("channel", json!({ "id": id.to_ulid() })),
        UiEvent::AgentChanged { id } => ("agent", json!({ "id": id.to_ulid() })),
        UiEvent::RuleChanged { id } => ("rule", json!({ "id": id.to_ulid() })),
        UiEvent::VerdictChanged { id } => ("verdict", json!({ "id": id.to_ulid() })),
        UiEvent::ProjectionReady { id } => ("projection", json!({ "id": id.to_ulid() })),
        UiEvent::TopicVersionReady { version } => {
            ("topic-version", json!({ "version": version.0 }))
        }
        UiEvent::Watermark { at } => ("watermark", json!({ "at": format_time(at.at()) })),
    }
}

fn reason(reason: ResyncReason) -> &'static str {
    match reason {
        ResyncReason::Expired => "expired",
        ResyncReason::OtherEpoch => "other-epoch",
        ResyncReason::AheadOfHead => "ahead-of-head",
        ResyncReason::Unreadable => "unreadable",
    }
}

/// One SSE event per item, its id the item's cursor.
pub fn frame(item: LiveItem) -> Event {
    let (kind, data) = match item {
        LiveItem::Event { event, .. } => event_fields(event),
        LiveItem::Resync { reason: why, .. } => ("resync", json!({ "reason": reason(why) })),
        LiveItem::Heartbeat { .. } => ("heartbeat", json!({})),
    };
    Event::new()
        .id(item.cursor().encode())
        .event(kind)
        .data(data.to_string())
}

/// The last event of a stream that ended.
pub fn ended(end: LiveEnd) -> Event {
    let why = match end {
        LiveEnd::Lagged => "lagged",
        LiveEnd::SessionEnded => "session-ended",
        LiveEnd::ShuttingDown => "shutting-down",
    };
    Event::new()
        .event("end")
        .data(json!({ "reason": why }).to_string())
}

#[route(GET "/data/live")]
async fn live(cx: &Cx) -> topcoat::Result<Sse<LiveEvents>> {
    let caller = caller(cx);
    require(&caller, Permission::View)?;
    let resume = Resume::from_last_event_id(last_event_id(cx));
    let subscription = backend(cx)
        .subscribe(&caller, resume)
        .await
        .map_err(query_error)?;
    tracing::debug!(operator = ?caller.operator(), resume = ?resume, "live feed subscribed");
    Ok(Sse::new(LiveEvents::new(subscription)))
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU32;
    use std::time::Duration;

    use crosstalk_spec::ids::AlertId;
    use crosstalk_spec::interfaces::l8_surface::live::LiveConfig;
    use crosstalk_spec::interfaces::l8_surface::{
        AlertFilter, AlertStateKind, OperatorAction, OperatorActions, QueryApi,
    };
    use topcoat::router::header::CONTENT_TYPE;
    use topcoat::router::request::Request;
    use topcoat::router::response::IntoResponse;
    use topcoat::router::{Body, BodyDataStream, Router, RouterBuilderDiscoverExt, StatusCode};

    use super::*;
    use crate::backend::fixture::FixtureBackend;
    use crate::testing::{SEED, operator};

    const WAIT: Duration = Duration::from_secs(5);

    fn backend(buffer: u32, heartbeat_ms: u64) -> FixtureBackend {
        let config = LiveConfig::new(
            NonZeroU32::new(buffer).expect("buffer"),
            Duration::from_millis(heartbeat_ms),
            Duration::from_secs(3600),
        )
        .expect("config");
        FixtureBackend::try_new(SEED)
            .expect("fixture generates")
            .with_live_config(config)
    }

    /// Acknowledges `n` open alerts, oldest first, returning their ids.
    async fn acknowledge(backend: &FixtureBackend, n: usize) -> Vec<AlertId> {
        let caller = operator().caller();
        let mut acknowledged = Vec::new();
        for _ in 0..n {
            let filter = AlertFilter {
                states: vec![AlertStateKind::Open],
                channel: None,
            };
            let page = backend
                .alerts(
                    &caller,
                    &filter,
                    &crate::pages::common::paging::first(NonZeroU32::MIN),
                )
                .await
                .expect("alerts");
            let open = page.items().first().expect("an open alert").id;
            backend
                .act(&caller, OperatorAction::Acknowledge { alert: open })
                .await
                .expect("acknowledge");
            acknowledged.push(open);
        }
        acknowledged
    }

    async fn next_frame(body: &mut BodyDataStream) -> String {
        let next = tokio::time::timeout(
            WAIT,
            std::future::poll_fn(|cx| Pin::new(&mut *body).poll_next(cx)),
        )
        .await
        .expect("a frame in time");
        let bytes = next.expect("the stream is open").expect("a frame");
        String::from_utf8(bytes.to_vec()).expect("utf8")
    }

    /// `GET /data/live` through the router, with `Last-Event-ID` when
    /// given. The router is returned too: dropping it drops the backend,
    /// whose feed then shuts every stream down.
    async fn open(
        backend: FixtureBackend,
        last: Option<&str>,
    ) -> (Router, StatusCode, String, BodyDataStream) {
        let router = Router::builder()
            .discover()
            .app_context(crate::identity::Identity::fixed(operator()))
            .app_context(crate::backend::AppBackend::from(backend))
            .build();
        let mut request = Request::builder().uri("/data/live");
        if let Some(last) = last {
            request = request.header("last-event-id", last);
        }
        let response = router
            .handle(request.body(Body::empty()).expect("request"))
            .await;
        let status = response.status();
        let content_type = response
            .headers()
            .get(CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .unwrap_or_default()
            .to_owned();
        (
            router,
            status,
            content_type,
            response.into_body().into_data_stream(),
        )
    }

    #[tokio::test]
    async fn the_feed_streams_heartbeats_with_the_newest_cursor() {
        let backend = backend(16, 30);
        acknowledge(&backend, 2).await;
        let epoch = backend.feed_epoch().0;
        let (_router, status, content_type, mut body) = open(backend, None).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(content_type, "text/event-stream");
        assert_eq!(
            next_frame(&mut body).await,
            format!("event: heartbeat\ndata: {{}}\nid: {epoch}-2\n\n")
        );
    }

    #[tokio::test]
    async fn last_event_id_replays_what_was_missed() {
        let backend = backend(16, 600_000);
        let acknowledged = acknowledge(&backend, 2).await;
        let epoch = backend.feed_epoch().0;
        let (_router, status, _, mut body) = open(backend, Some(&format!("{epoch}-1"))).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            next_frame(&mut body).await,
            format!(
                "event: alert\ndata: {{\"id\":\"{}\"}}\nid: {epoch}-2\n\n",
                acknowledged[1].to_ulid()
            )
        );
    }

    #[tokio::test]
    async fn an_unreadable_or_foreign_last_event_id_resyncs() {
        for (last, why) in [("yesterday", "unreadable"), ("1-1", "other-epoch")] {
            let backend = backend(16, 600_000);
            acknowledge(&backend, 1).await;
            let epoch = backend.feed_epoch().0;
            let (_router, _, _, mut body) = open(backend, Some(last)).await;
            assert_eq!(
                next_frame(&mut body).await,
                format!("event: resync\ndata: {{\"reason\":\"{why}\"}}\nid: {epoch}-1\n\n")
            );
        }
    }

    #[tokio::test]
    async fn live_events_follow_and_a_slow_stream_ends_lagged() {
        let backend = backend(2, 600_000);
        let caller = operator().caller();
        let subscription = backend
            .subscribe(&caller, Resume::Fresh)
            .await
            .expect("subscribe");
        let response = Sse::new(LiveEvents::new(
            crate::backend::dispatch::AppStream::Fixture(subscription),
        ))
        .into_response(&Cx::default())
        .expect("response");
        let mut body = response.into_body().into_data_stream();
        let first = acknowledge(&backend, 1).await;
        let epoch = backend.feed_epoch().0;
        assert_eq!(
            next_frame(&mut body).await,
            format!(
                "event: alert\ndata: {{\"id\":\"{}\"}}\nid: {epoch}-1\n\n",
                first[0].to_ulid()
            )
        );
        acknowledge(&backend, 4).await;
        assert_eq!(
            next_frame(&mut body).await,
            "event: end\ndata: {\"reason\":\"lagged\"}\n\n"
        );
        let after = tokio::time::timeout(
            WAIT,
            std::future::poll_fn(|cx| Pin::new(&mut body).poll_next(cx)),
        )
        .await
        .expect("closed in time");
        assert!(after.is_none(), "the response ends after the end event");
    }

    #[test]
    fn every_event_kind_names_its_id() {
        let alert = AlertId::from_ulid(7);
        assert_eq!(
            event_fields(UiEvent::AlertChanged { id: alert }),
            ("alert", json!({ "id": alert.to_ulid() }))
        );
        assert_eq!(
            event_fields(UiEvent::TopicVersionReady {
                version: crosstalk_spec::aggregates::topic::TopicModelVersion(2)
            }),
            ("topic-version", json!({ "version": 2 }))
        );
    }
}
