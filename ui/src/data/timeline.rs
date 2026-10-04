//! The `<ct-timebrush>` payload and its route.
//!
//! `GET /data/timeline?<view state>&buckets=<n>` answers with a
//! [`TimelinePayload`] as JSON (`Backend::timeline`). `buckets` is 1 to
//! [`MAX_BUCKETS`](super::query::MAX_BUCKETS), default 96. Needs `View`.
//!
//! ```json
//! {
//!   "window": { "from": "2026-10-02T00:00:00Z", "to": "2026-10-03T00:00:00Z" },
//!   "bucketMs": 900000,
//!   "watermark": "2026-10-02T23:22:30Z",
//!   "buckets": [
//!     { "from": "2026-10-02T00:00:00Z", "to": "2026-10-02T00:15:00Z",
//!       "transmissions": 14, "matchedBytes": 9310, "final": true }
//!   ]
//! }
//! ```
//!
//! A bucket is final when it ends at or before the watermark: its counts
//! can no longer change.

use crosstalk_spec::interfaces::l8_surface::Permission;
use serde::Serialize;
use topcoat::context::Cx;
use topcoat::router::content::Json;
use topcoat::router::route;

use super::errors::query_error;
use super::query::{buckets, view_state};
use super::require;
use super::topology::WindowPayload;
use crate::app::{backend, caller};
use crate::backend::Backend;
use crate::contract::graph::Timeline;
use crate::url::view_state::format_time;

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TimelinePayload {
    /// The view state's window the buckets cover.
    pub window: WindowPayload,
    /// Bucket width in milliseconds.
    pub bucket_ms: u64,
    pub watermark: String,
    pub buckets: Vec<BucketPayload>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BucketPayload {
    pub from: String,
    pub to: String,
    pub transmissions: u64,
    pub matched_bytes: u64,
    /// `to <= watermark`.
    #[serde(rename = "final")]
    pub is_final: bool,
}

impl TimelinePayload {
    pub fn new(window: WindowPayload, timeline: &Timeline) -> Self {
        Self {
            window,
            bucket_ms: u64::try_from(timeline.bucket_width.as_millis()).unwrap_or(u64::MAX),
            watermark: format_time(timeline.watermark),
            buckets: timeline
                .buckets
                .iter()
                .map(|b| BucketPayload {
                    from: format_time(b.bucket.start()),
                    to: format_time(b.bucket.end()),
                    transmissions: b.transmissions,
                    matched_bytes: b.matched_bytes,
                    is_final: b.bucket.end() <= timeline.watermark,
                })
                .collect(),
        }
    }
}

#[route(GET "/data/timeline")]
async fn timeline_data(cx: &Cx) -> topcoat::Result<Json<TimelinePayload>> {
    let caller = caller(cx);
    require(&caller, Permission::View)?;
    let state = view_state(cx)?;
    let buckets = buckets(cx)?;
    let timeline = backend(cx)
        .timeline(&caller, &state.scope, buckets)
        .await
        .map_err(query_error)?;
    let payload = TimelinePayload::new(state.scope.window.into(), &timeline);
    tracing::debug!(buckets = payload.buckets.len(), "timeline payload");
    Ok(Json(payload))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::fixtures;

    #[test]
    fn marks_buckets_final_up_to_the_watermark() {
        let (window, timeline) = fixtures::timeline();
        let payload = TimelinePayload::new(window.into(), &timeline);
        assert_eq!(payload.buckets.len(), timeline.buckets.len());
        assert_eq!(payload.bucket_ms, 15 * 60 * 1000);
        for (bucket, source) in payload.buckets.iter().zip(&timeline.buckets) {
            assert_eq!(bucket.is_final, source.bucket.end() <= timeline.watermark);
            assert_eq!(bucket.transmissions, source.transmissions);
        }
        let finals = payload.buckets.iter().filter(|b| b.is_final).count();
        assert!(finals > 0 && finals < payload.buckets.len());
        assert!(
            payload.buckets[..finals].iter().all(|b| b.is_final),
            "final buckets form a prefix"
        );
    }

    #[test]
    fn serializes_final_under_its_json_name() {
        let (window, timeline) = fixtures::timeline();
        let json = serde_json::to_value(TimelinePayload::new(window.into(), &timeline))
            .expect("serialize");
        assert_eq!(json["buckets"][0]["final"], true);
        assert!(json["bucketMs"].is_u64());
    }
}
