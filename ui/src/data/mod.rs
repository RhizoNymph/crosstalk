//! `#[route]` endpoints feeding the custom elements, and their payloads.
//!
//! Each element fetches its payload from a route here, never from L8, so
//! authorization stays server-side: every route builds the request's
//! [`Caller`](crosstalk_spec::interfaces::l8_surface::Caller), checks the
//! permission it needs and maps [`UiError`](crate::error::UiError)
//! to an HTTP status ([`errors`]).
//!
//! | Route | Payload | Needs |
//! | --- | --- | --- |
//! | `GET /data/topology?<view state>` | [`topology::TopologyPayload`] (JSON) | `View` |
//! | `GET /data/timeline?<view state>&buckets=<n>` | [`timeline::TimelinePayload`] (JSON) | `View` |
//! | `GET /data/projection/{id}` | [`projection::format`] (binary) | `Content` |
//!
//! View-state routes require the canonical query ([`query`]): an incomplete
//! one is a 400, never a redirect, because the element asked for exactly
//! that URL.
//!
//! The payload shapes are mirrored by the zod schemas in
//! `ui/elements/src/payloads/`; `fixtures` (tests) writes the payloads of
//! hand-built contract values to `ui/elements/test/fixtures/`, which the
//! TypeScript tests parse.

pub mod elements;
pub mod errors;
pub mod names;
pub mod projection;
pub mod query;
pub mod timeline;
pub mod topology;

#[cfg(test)]
mod fixtures;
#[cfg(test)]
mod route_tests;

use crosstalk_spec::interfaces::l8_surface::{Caller, Permission};
use topcoat::router::error::forbidden;

use crate::app::can;

/// A 403 unless the caller holds `permission`.
pub fn require(caller: &Caller, permission: Permission) -> topcoat::Result<()> {
    if can(caller, permission) {
        Ok(())
    } else {
        tracing::info!(operator = ?caller.operator(), missing = ?permission, "data route forbidden");
        Err(forbidden().into())
    }
}
