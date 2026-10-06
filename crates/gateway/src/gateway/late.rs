//! [`LateRouter`]: the HTTP API's listener, bound at start, serving the
//! surface once it exists.
//!
//! In Postgres mode the surface is built by the recovery sequence, after
//! the database answers, while the listeners bind at start. Until then
//! every request is answered `503` with the API's error JSON (a `Store`
//! error saying the surface is starting), so a client sees an answer it
//! understands rather than a connection that hangs.

use std::sync::{Arc, OnceLock};

use axum::Router;
use axum::body::Body;
use axum::extract::{Request, State};
use axum::http::header::{CACHE_CONTROL, CONTENT_TYPE};
use axum::http::{HeaderValue, StatusCode};
use axum::response::Response;
use crosstalk_spec::interfaces::l8_surface::QueryError;
use tower::ServiceExt;

/// The router the API serves once it is set. Clones share it.
#[derive(Clone, Default)]
pub struct LateRouter(Arc<OnceLock<Router>>);

impl std::fmt::Debug for LateRouter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LateRouter")
            .field("set", &self.0.get().is_some())
            .finish()
    }
}

impl LateRouter {
    /// Serve `router` from now on. A second router is ignored (the first
    /// keeps serving).
    pub fn set(&self, router: Router) {
        if self.0.set(router).is_err() {
            tracing::warn!("the API router was already set; the first keeps serving");
        }
    }

    pub fn is_set(&self) -> bool {
        self.0.get().is_some()
    }

    /// The router the listener runs: the late one once set, `503` before.
    pub fn router(&self) -> Router {
        Router::new().fallback(forward).with_state(self.clone())
    }
}

async fn forward(State(late): State<LateRouter>, request: Request) -> Response {
    match late.0.get() {
        Some(router) => match router.clone().oneshot(request).await {
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
