//! `GET /projections/{id}/frame`: the stored frame's bytes, cached by its
//! digest until its retention ends (`surface.http.frame-cache`).
//!
//! ```text
//! QueryApi::projection(caller, id)            permission and readiness first, always
//!   ├─ Err(e) ─▶ e.status(), e's JSON, no-store   (409 not ready or failed, 410 expired, 404, 403)
//!   └─ Ok(projection) ─▶ bytes = ProjectionFrame::encode, digest = BLAKE3(bytes)
//!        FrameCache::of(projection, digest, frame retention, clock.now())
//!          ├─ If-None-Match matches its ETag ─▶ 304, ETag, Cache-Control, no body
//!          └─ otherwise ─▶ 200 application/octet-stream, ETag, Cache-Control: private, max-age, immutable
//! ```
//!
//! The conditional check runs only after `projection` returned the
//! projection, so a 304 answers only a caller with Content, for a frame
//! that is still ready.

use axum::body::Body;
use axum::http::header::{CACHE_CONTROL, CONTENT_TYPE, ETAG, IF_NONE_MATCH};
use axum::http::{HeaderValue, StatusCode};
use axum::response::Response;
use crosstalk_spec::ids::ProjectionId;
use crosstalk_spec::interfaces::l8_surface::http::frame::{FrameCache, OCTET_STREAM};
use crosstalk_spec::interfaces::l8_surface::{Caller, QueryError};
use crosstalk_spec::support::Blake3;

use super::input::Input;
use super::{Shared, Surface, respond};

pub(super) async fn serve<S: Surface>(
    shared: &Shared<S>,
    caller: &Caller,
    input: &Input,
) -> Result<Response, QueryError> {
    let id: ProjectionId = input.path("id")?;
    let projection = shared.surface.projection(caller, id).await?;
    let bytes = projection.frame().encode();
    let cache = FrameCache::of(
        &projection,
        Blake3::of(&bytes),
        shared.config.frame_retention.as_duration(),
        shared.config.clock.now(),
    );
    let etag = respond::header_value(&cache.etag())?;
    let cache_control = respond::header_value(&cache.cache_control())?;
    let not_modified = cache.not_modified(input.header(IF_NONE_MATCH.as_str()).as_deref());
    let mut response = if not_modified {
        let mut response = Response::new(Body::empty());
        *response.status_mut() = StatusCode::NOT_MODIFIED;
        response
    } else {
        let mut response = Response::new(Body::from(bytes));
        response
            .headers_mut()
            .insert(CONTENT_TYPE, HeaderValue::from_static(OCTET_STREAM));
        response
    };
    let headers = response.headers_mut();
    headers.insert(ETAG, etag);
    headers.insert(CACHE_CONTROL, cache_control);
    Ok(response)
}
