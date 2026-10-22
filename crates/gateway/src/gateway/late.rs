//! [`LateRouter`]: the HTTP API's listener, bound at start, serving the
//! surface once it exists; [`LateValue`]: anything else the ops listener
//! reads from the live process once it runs.
//!
//! In Postgres mode the surface is built by the recovery sequence, after
//! the database answers, while the listeners bind at start. Until then
//! every request is answered `503` with the API's error JSON (a `Store`
//! error whose reason is the [`Unserved`] cause), so a client sees an
//! answer it understands rather than a connection that hangs. A lost
//! pipeline lock clears both: the abandoned incarnation's surface is never
//! served again, and the API answers `503` (lock lost) until the next
//! incarnation sets its own. A planned handover clears the router too: the
//! API answers `503` (handed over) while the process stops.

use std::sync::Arc;

use axum::Router;
use axum::body::Body;
use axum::extract::{Request, State};
use axum::http::header::{CACHE_CONTROL, CONTENT_TYPE};
use axum::http::{HeaderValue, StatusCode};
use axum::response::Response;
use crosstalk_spec::interfaces::l8_surface::QueryError;
use tokio::sync::watch;
use tower::ServiceExt;

/// A value the live process sets once it runs and a lost lock clears.
/// Clones share it; readers see the latest value.
#[derive(Debug)]
pub struct LateValue<T>(watch::Sender<Option<T>>);

impl<T> Default for LateValue<T> {
    fn default() -> Self {
        Self(watch::Sender::new(None))
    }
}

impl<T: Clone> LateValue<T> {
    /// Hold `value` from now on (replacing any earlier one).
    pub fn set(&self, value: T) {
        self.0.send_replace(Some(value));
    }

    /// Hold nothing again.
    pub fn clear(&self) {
        self.0.send_replace(None);
    }

    /// The value held now.
    pub fn get(&self) -> Option<T> {
        self.0.borrow().clone()
    }

    pub fn is_set(&self) -> bool {
        self.0.borrow().is_some()
    }
}

/// Why the API serves no surface: the reason its `503` gives.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Unserved {
    /// No surface yet: the recovery sequence has not built one.
    #[default]
    Starting,
    /// The incarnation's lock was lost; the next one sets its own surface.
    LockLost,
    /// A planned shutdown handed the pipeline over; this process is
    /// stopping and serves no surface again.
    HandedOver,
}

impl Unserved {
    /// The `Store` error's reason.
    pub fn reason(self) -> &'static str {
        match self {
            Self::Starting => "the surface is starting: the pipeline is recovering from its stores",
            Self::LockLost => {
                "the surface is unavailable: the pipeline lock was lost and the pipeline is resuming"
            }
            Self::HandedOver => {
                "the surface is unavailable: this gateway is shutting down and has handed the \
                 pipeline over"
            }
        }
    }
}

/// What the API serves now.
#[derive(Clone)]
enum Serving {
    Router(Router),
    Unserved(Unserved),
}

/// The router the API serves once it is set. Clones share it.
#[derive(Clone)]
pub struct LateRouter(Arc<watch::Sender<Serving>>);

impl Default for LateRouter {
    fn default() -> Self {
        Self(Arc::new(watch::Sender::new(Serving::Unserved(
            Unserved::Starting,
        ))))
    }
}

impl std::fmt::Debug for LateRouter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LateRouter")
            .field("unserved", &self.unserved())
            .finish()
    }
}

impl LateRouter {
    /// Serve `router` from now on, in place of any earlier one.
    pub fn set(&self, router: Router) {
        self.0.send_replace(Serving::Router(router));
    }

    /// Answer `503` for `why`, until the next [`LateRouter::set`].
    pub fn clear(&self, why: Unserved) {
        self.0.send_replace(Serving::Unserved(why));
    }

    pub fn is_set(&self) -> bool {
        self.unserved().is_none()
    }

    /// Why no surface is served now, `None` while one is.
    pub fn unserved(&self) -> Option<Unserved> {
        match &*self.0.borrow() {
            Serving::Router(_) => None,
            Serving::Unserved(why) => Some(*why),
        }
    }

    /// The router the listener runs: the late one once set, `503` before.
    pub fn router(&self) -> Router {
        Router::new().fallback(forward).with_state(self.clone())
    }
}

async fn forward(State(late): State<LateRouter>, request: Request) -> Response {
    let serving = late.0.borrow().clone();
    match serving {
        Serving::Router(router) => match router.oneshot(request).await {
            Ok(response) => response,
            Err(never) => match never {},
        },
        Serving::Unserved(why) => unavailable(why),
    }
}

/// The answer while no surface is served.
fn unavailable(why: Unserved) -> Response {
    let error = QueryError::Store {
        reason: why.reason().to_owned(),
    };
    let body = serde_json::to_vec(&error).unwrap_or_default();
    let mut response = Response::new(Body::from(body));
    *response.status_mut() = StatusCode::SERVICE_UNAVAILABLE;
    let headers = response.headers_mut();
    headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    headers.insert(CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response
}

#[cfg(test)]
mod tests {
    use axum::body::to_bytes;
    use axum::routing::get;

    use super::*;

    async fn answer(late: &LateRouter) -> (StatusCode, Vec<u8>) {
        let request = Request::builder()
            .uri("/v1/me")
            .body(Body::empty())
            .expect("request");
        let response = late
            .router()
            .oneshot(request)
            .await
            .unwrap_or_else(|never| match never {});
        let status = response.status();
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body");
        (status, body.to_vec())
    }

    fn reason_of(body: &[u8]) -> String {
        match serde_json::from_slice::<QueryError>(body).expect("query error") {
            QueryError::Store { reason } => reason,
            other => panic!("not a store error: {other:?}"),
        }
    }

    #[tokio::test]
    async fn each_cause_gives_its_own_reason() {
        let late = LateRouter::default();
        assert_eq!(late.unserved(), Some(Unserved::Starting));
        for why in [Unserved::Starting, Unserved::LockLost, Unserved::HandedOver] {
            late.clear(why);
            let (status, body) = answer(&late).await;
            assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
            assert_eq!(reason_of(&body), why.reason());
        }
    }

    #[tokio::test]
    async fn a_set_router_serves_until_cleared() {
        let late = LateRouter::default();
        late.set(Router::new().route("/v1/me", get(|| async { "me" })));
        assert!(late.is_set());
        let (status, body) = answer(&late).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, b"me");
        late.clear(Unserved::HandedOver);
        assert_eq!(late.unserved(), Some(Unserved::HandedOver));
        let (status, body) = answer(&late).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(reason_of(&body), Unserved::HandedOver.reason());
    }
}
