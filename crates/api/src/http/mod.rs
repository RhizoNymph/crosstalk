//! The HTTP API server (roadmap P7.1): the P0.4 binding of the L8 surface,
//! served with axum over any implementation of the spec's L8 traits.
//!
//! ```text
//! request ─▶ Auth::caller (Authorization / __Host-crosstalk-session only) ── none ─▶ 401 AuthError
//!         ─▶ the router, built from Route::all() ([`routes`])            ── no route ─▶ 404 NotFound
//!         ─▶ read path, query and body against the route ([`input`])     ── unreadable ─▶ 400 MalformedRequest
//!         ─▶ checked constructors (IdBatch, TransmissionSelection, ExcerptWindow, ActionRequest::into_action)
//!                                                                         ── refused ─▶ 422 InvalidInput
//!         ─▶ QueryApi method / OperatorActions::act / LiveFeed::subscribe
//!              ├─ Ok ─▶ the route's success: JSON, a frame ([`frame`]), SSE ([`live`]) or an export ([`export`])
//!              └─ Err(e) ─▶ e.status(), e's wire JSON, Cache-Control: no-store
//! ```
//!
//! Every response but a ready projection frame carries
//! `Cache-Control: no-store` (a router layer adds it to any response that
//! has none). The binding itself (the route table, decoding, statuses,
//! authentication, SSE framing, frame caching and export headers) is spec:
//! [`crosstalk_spec::interfaces::l8_surface::http`]. This module only
//! wires it to axum and to a surface.
//!
//! The server is built with [`HttpApi::new`] and mounted with
//! [`HttpApi::router`]; [`serve`] runs a router on a listener (the
//! gateway's `--role api`, on `api.listen`).

mod actions;
mod auth;
mod checked;
mod dispatch;
mod export;
mod frame;
mod input;
mod live;
mod respond;
mod routes;
mod serve;

#[cfg(test)]
mod integration;

use std::sync::Arc;

use axum::Router;
use crosstalk_spec::aggregates::projection::FrameRetention;
use crosstalk_spec::interfaces::l8_surface::live::LiveFeed;
use crosstalk_spec::interfaces::l8_surface::{OperatorActions, QueryApi};
use crosstalk_spec::support::Clock;

pub use auth::{Auth, BearerToken, CredentialVerifier, InvalidBearerToken, StaticTokens};
pub use export::written_formats;
pub use serve::{ServeError, bind, serve};

/// Everything the server serves: the query API, the operator actions and
/// the live feed of one L8 surface, shareable across connections.
pub trait Surface: QueryApi + OperatorActions + LiveFeed + Send + Sync + 'static {}

impl<T> Surface for T where T: QueryApi + OperatorActions + LiveFeed + Send + Sync + 'static {}

/// What the server needs besides the surface and authentication.
#[derive(Clone)]
pub struct HttpConfig {
    /// The projection store's frame retention, which a ready frame's
    /// `Cache-Control: max-age` runs to (the surface's
    /// `Present::frame_retention_micros`).
    pub frame_retention: FrameRetention,
    /// The wall clock the frame's remaining retention is measured on.
    pub clock: Arc<dyn Clock>,
}

impl std::fmt::Debug for HttpConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HttpConfig")
            .field("frame_retention", &self.frame_retention)
            .finish_non_exhaustive()
    }
}

/// The HTTP binding over one surface.
pub struct HttpApi<S> {
    shared: Arc<Shared<S>>,
}

/// The router's state: one per server, shared by every request.
struct Shared<S> {
    surface: Arc<S>,
    auth: Auth,
    config: HttpConfig,
}

impl<S: Surface> HttpApi<S> {
    pub fn new(surface: Arc<S>, auth: Auth, config: HttpConfig) -> Self {
        Self {
            shared: Arc::new(Shared {
                surface,
                auth,
                config,
            }),
        }
    }

    /// Every route of the table, mounted at the router's root. A
    /// deployment that shares an origin nests it under a prefix
    /// (`Router::nest`).
    pub fn router(self) -> Router {
        routes::router(self.shared)
    }
}
