//! [`LateRouter`]: the HTTP API's listener, bound at start, serving the
//! surface once it exists; [`LateValue`]: anything else the ops listener
//! reads from the live process once it runs.
//!
//! In Postgres mode the surface is built by the recovery sequence, after
//! the database answers, while the listeners bind at start. Until then
//! every request is answered `503` with the API's error JSON (a `Store`
//! error saying the surface is starting), so a client sees an answer it
//! understands rather than a connection that hangs. A lost pipeline lock
//! clears both: the abandoned incarnation's surface is never served again,
//! and the API answers `503` until the next incarnation sets its own.

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

/// The router the API serves once it is set. Clones share it.
#[derive(Clone, Default)]
pub struct LateRouter(Arc<LateValue<Router>>);

impl std::fmt::Debug for LateRouter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LateRouter")
            .field("set", &self.0.is_set())
            .finish()
    }
}

impl LateRouter {
    /// Serve `router` from now on, in place of any earlier one.
    pub fn set(&self, router: Router) {
        self.0.set(router);
    }

    /// Answer `503` again, until the next [`LateRouter::set`].
    pub fn clear(&self) {
        self.0.clear();
    }

    pub fn is_set(&self) -> bool {
        self.0.is_set()
    }

    /// The router the listener runs: the late one once set, `503` before.
    pub fn router(&self) -> Router {
        Router::new().fallback(forward).with_state(self.clone())
    }
}

async fn forward(State(late): State<LateRouter>, request: Request) -> Response {
    match late.0.get() {
        Some(router) => match router.oneshot(request).await {
            Ok(response) => response,
            Err(never) => match never {},
        },
        None => starting(),
    }
}

/// The answer before the surface exists.
fn starting() -> Response {
    let error = QueryError::Store {
        reason: "the surface is starting: the pipeline is recovering from its stores".to_owned(),
    };
    let body = serde_json::to_vec(&error).unwrap_or_default();
    let mut response = Response::new(Body::from(body));
    *response.status_mut() = StatusCode::SERVICE_UNAVAILABLE;
    let headers = response.headers_mut();
    headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    headers.insert(CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response
}
