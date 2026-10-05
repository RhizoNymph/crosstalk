//! `QueryApi::projection` over HTTP: the frame from
//! `GET /projections/{id}/frame`, then the job record from
//! `GET /projections/{id}`, joined with `Projection::new`.
//!
//! ```text
//! GET /projections/{id}/frame [If-None-Match: <cached ETag>]
//!   ├─ error ─▶ the error (403, 404, 409 not ready or failed, 410 expired): nothing else is read
//!   ├─ 304 ─▶ the cached frame
//!   └─ 200 ─▶ the bytes, checked against their ETag (BLAKE3, quoted hex), decoded, cached
//! GET /projections/{id} ─▶ ProjectionInfo ─▶ Projection::new(info, frame)
//! ```
//!
//! The frame comes first so the surface's permission and readiness checks
//! answer before anything else is read, exactly as for the in-process
//! method. A ready frame never changes until it expires, so the only way
//! the record can disagree with it is expiry in between, which is
//! `ProjectionNotRetained` as the method would have answered.
//!
//! **Cache.** A frame can be 5 MB and is identical on every read, so the
//! client keeps the last few by projection with their ETag
//! (`ClientConfig::with_frame_cache`) and revalidates with
//! `If-None-Match`; the surface answers `304` only after
//! `QueryApi::projection` succeeded for this caller
//! (`surface.http.frame-cache`), so a cached frame is never shown to a
//! caller the surface would refuse.

use std::collections::VecDeque;

use crosstalk_spec::aggregates::projection::frame::ProjectionFrame;
use crosstalk_spec::aggregates::projection::{
    Projection, ProjectionInfo, ProjectionMismatch, ProjectionStatusKind,
};
use crosstalk_spec::ids::ProjectionId;
use crosstalk_spec::interfaces::l8_surface::QueryError;
use crosstalk_spec::interfaces::l8_surface::http::Route;
use crosstalk_spec::interfaces::l8_surface::http::frame::OCTET_STREAM;
use crosstalk_spec::support::Blake3;
use hyper::header::{ETAG, HeaderMap, HeaderValue, IF_NONE_MATCH};

use crate::client::HttpClient;
use crate::error::ClientError;

/// The last few ready frames, newest last, each with its `ETag`.
#[derive(Debug)]
pub(crate) struct FrameCache {
    capacity: usize,
    entries: VecDeque<CachedFrame>,
}

#[derive(Debug)]
struct CachedFrame {
    id: ProjectionId,
    etag: String,
    frame: ProjectionFrame,
}

impl FrameCache {
    pub(crate) fn new(capacity: usize) -> Self {
        Self {
            capacity,
            entries: VecDeque::new(),
        }
    }

    fn etag(&self, id: ProjectionId) -> Option<String> {
        self.entries
            .iter()
            .find(|entry| entry.id == id)
            .map(|entry| entry.etag.clone())
    }

    fn frame(&self, id: ProjectionId, etag: &str) -> Option<ProjectionFrame> {
        self.entries
            .iter()
            .find(|entry| entry.id == id && entry.etag == etag)
            .map(|entry| entry.frame.clone())
    }

    fn put(&mut self, id: ProjectionId, etag: String, frame: ProjectionFrame) {
        self.remove(id);
        if self.capacity == 0 {
            return;
        }
        while self.entries.len() >= self.capacity {
            self.entries.pop_front();
        }
        self.entries.push_back(CachedFrame { id, etag, frame });
    }

    fn remove(&mut self, id: ProjectionId) {
        self.entries.retain(|entry| entry.id != id);
    }
}

/// The strong `ETag` of a frame's bytes, as the surface writes it
/// (`FrameCache::etag`): their BLAKE3 digest in lower-case hex, quoted.
fn etag_of(bytes: &[u8]) -> String {
    format!("\"{}\"", Blake3::of(bytes).to_hex())
}

type Result<T> = std::result::Result<T, ClientError<QueryError>>;

impl<H> HttpClient<H> {
    fn with_frames<T>(&self, use_cache: impl FnOnce(&mut FrameCache) -> T) -> T {
        // A poisoned lock only means another task panicked mid-update; the
        // cache holds whole entries, so its contents are still usable.
        let mut frames = self
            .shared()
            .frames
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        use_cache(&mut frames)
    }

    pub(crate) async fn fetch_projection(&self, id: ProjectionId) -> Result<Projection> {
        let frame = self.fetch_frame(id).await?;
        let info = self
            .call::<ProjectionInfo, QueryError>(Route::ProjectionStatus, |b| b.path("id", &id))
            .await?;
        Projection::new(info, frame).map_err(|mismatch| match mismatch {
            ProjectionMismatch::NotReady(ProjectionStatusKind::Expired) => {
                self.with_frames(|frames| frames.remove(id));
                ClientError::Api(QueryError::ProjectionNotRetained { projection: id })
            }
            other => ClientError::unexpected(
                Route::ProjectionStatus,
                200,
                format!("the job record does not match its frame: {other:?}"),
            ),
        })
    }

    /// The frame, revalidated against the cache when it holds one. A `304`
    /// for a frame the cache no longer holds (another task evicted it) is
    /// asked again without `If-None-Match`.
    async fn fetch_frame(&self, id: ProjectionId) -> Result<ProjectionFrame> {
        let route = Route::ProjectionFrame;
        let mut revalidate = self.with_frames(|frames| frames.etag(id));
        loop {
            let mut extra = HeaderMap::new();
            if let Some(etag) = &revalidate {
                let value = HeaderValue::from_str(etag).map_err(|error| {
                    ClientError::unexpected(route, 0, format!("cached ETag: {error}"))
                })?;
                extra.insert(IF_NONE_MATCH, value);
            }
            let exchanged = self
                .exchange::<QueryError>(route, |b| b.path("id", &id), OCTET_STREAM, extra)
                .await?;
            if exchanged.status == 304 {
                let Some(etag) = revalidate.take() else {
                    return Err(ClientError::unexpected(
                        route,
                        304,
                        "not modified, but no If-None-Match was sent",
                    ));
                };
                match self.with_frames(|frames| frames.frame(id, &etag)) {
                    Some(frame) => return Ok(frame),
                    None => continue,
                }
            }
            exchanged.success(route, OCTET_STREAM)?;
            let etag = exchanged
                .headers
                .get(ETAG)
                .and_then(|value| value.to_str().ok())
                .map(str::to_owned);
            let expected = etag_of(&exchanged.body);
            if etag.as_deref() != Some(expected.as_str()) {
                return Err(ClientError::unexpected(
                    route,
                    exchanged.status,
                    format!("ETag {etag:?} is not the frame's digest {expected}"),
                ));
            }
            let frame = ProjectionFrame::decode(&exchanged.body).map_err(|error| {
                ClientError::unexpected(route, exchanged.status, format!("frame: {error:?}"))
            })?;
            if frame.header().projection != id {
                return Err(ClientError::unexpected(
                    route,
                    exchanged.status,
                    "the frame of another projection",
                ));
            }
            self.with_frames(|frames| frames.put(id, expected, frame.clone()));
            return Ok(frame);
        }
    }
}
