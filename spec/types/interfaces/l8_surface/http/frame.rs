//! A projection over HTTP: `GET /projections/{id}` is the job record
//! (`QueryApi::projection_status`, [`ProjectionInfo`] JSON) and
//! `GET /projections/{id}/frame` the frame bytes (`QueryApi::projection`,
//! `application/octet-stream`, [`ProjectionFrame::encode`]).
//!
//! ```text
//! GET /projections/{id}/frame [If-None-Match: "<digest>"]
//!   ─▶ QueryApi::projection(caller, id)           permission and readiness first, always
//!        ├─ Err(e) ─▶ e.status() with e's JSON, Cache-Control: no-store
//!        │     NotFound 404 · Conflict(ProjectionNotReady) 409 · Conflict(ProjectionFailed) 409
//!        │     ProjectionNotRetained 410 · Forbidden 403
//!        └─ Ok(projection) ─▶ FrameCache::of(projection, digest, retention, now)
//!              ├─ If-None-Match matches its ETag ─▶ 304, no body
//!              └─ otherwise ─▶ 200, the frame's bytes
//! ```
//!
//! **Caching.** A ready frame is identical on every read until retention
//! drops it, so its response is cacheable:
//! - `ETag` is a strong validator, the frame's digest: BLAKE3 of the bytes
//!   `ProjectionFrame::encode` writes, in lower-case hex, quoted
//!   ([`FrameCache::etag`]); the frame's layout is canonical, so equal
//!   frames have equal digests, and the format version is in the bytes;
//! - `Cache-Control: private, max-age=<seconds>, immutable`, where the
//!   seconds run to when the frame's retention ends (`fitted_at` plus the
//!   projection store's frame retention), at most a year; `private`
//!   because the frame is Content and the response depends on the caller.
//!
//! The conditional check runs after `projection` returns `Ok`, so a `304`
//! is only ever sent to a caller with Content, for a frame that is still
//! ready; a revoked permission or an expired frame answers as usual.
//!
//! **Not ready.** A queued or fitting job is `409` with
//! `Conflict(ProjectionNotReady { status })` and `no-store`; the client
//! waits for the live feed's `ProjectionReady` event (or polls
//! `GET /projections/{id}`) and asks again. A failed job is `409`
//! `ProjectionFailed`, an expired one `410` `ProjectionNotRetained`.
//!
//! [`ProjectionInfo`]: crate::aggregates::projection::ProjectionInfo
//! [`ProjectionFrame::encode`]: crate::aggregates::projection::frame::ProjectionFrame::encode

use std::time::Duration;

use crate::aggregates::projection::{Projection, ProjectionStatus};
use crate::support::{Blake3, Timestamp};

/// The content type of a frame.
pub const OCTET_STREAM: &str = "application/octet-stream";

/// The longest `max-age` sent: a year, the most RFC 9111 caches honour.
pub const MAX_AGE_LIMIT: Duration = Duration::from_secs(365 * 24 * 60 * 60);

/// The caching headers of one ready frame.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FrameCache {
    digest: Blake3,
    max_age: Duration,
}

impl FrameCache {
    /// For `projection`, whose frame's digest is `digest`, at `now`, with
    /// the projection store's frame `retention`. `max-age` is the time left
    /// until `fitted_at + retention`, in whole seconds, zero once past.
    pub fn of(
        projection: &Projection,
        digest: Blake3,
        retention: Duration,
        now: Timestamp,
    ) -> Self {
        let fitted_at = match projection.info().status() {
            ProjectionStatus::Ready(fitted) => fitted.fitted_at,
            // `Projection::new` holds only ready jobs; any other status
            // would be one whose frame is not retained, so nothing is left.
            ProjectionStatus::Queued
            | ProjectionStatus::Fitting { .. }
            | ProjectionStatus::Failed { .. }
            | ProjectionStatus::Expired { .. } => now,
        };
        let retention_micros = u64::try_from(retention.as_micros()).unwrap_or(u64::MAX);
        let expires = fitted_at.as_micros().saturating_add(retention_micros);
        let left = Duration::from_micros(expires.saturating_sub(now.as_micros()));
        Self {
            digest,
            max_age: Duration::from_secs(left.as_secs()).min(MAX_AGE_LIMIT),
        }
    }

    /// The `ETag` header: the digest in lower-case hex, quoted (strong).
    pub fn etag(&self) -> String {
        format!("\"{}\"", self.digest.to_hex())
    }

    /// The `Cache-Control` header.
    pub fn cache_control(&self) -> String {
        format!("private, max-age={}, immutable", self.max_age.as_secs())
    }

    /// Whether a request's `If-None-Match` matches this frame, so the
    /// answer is `304 Not Modified`: `*`, or a list naming this ETag, weak
    /// or strong (RFC 9110 compares `If-None-Match` weakly).
    pub fn not_modified(&self, if_none_match: Option<&str>) -> bool {
        let Some(header) = if_none_match else {
            return false;
        };
        let etag = self.etag();
        header.trim() == "*"
            || header
                .split(',')
                .map(|tag| tag.trim())
                .map(|tag| tag.strip_prefix("W/").unwrap_or(tag))
                .any(|tag| tag == etag)
    }
}
