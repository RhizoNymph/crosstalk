//! The `<ct-timebrush>` payload and its route.
//!
//! `GET /data/timeline?<view state>&buckets=<n>` answers with a
//! [`TimelinePayload`] as JSON, from two `Backend::series` calls on one grid
//! (transmissions, then matched bytes, both `SeriesGrouping::Total`, under
//! the view's filter). `buckets` is 1 to
//! [`MAX_BUCKETS`](super::query::MAX_BUCKETS), default 96. Needs `View`.
//!
//! **The grid** ([`timeline_grid`]): the view's window is on bucket
//! boundaries (the strict view state refuses any other). The step is the
//! smallest multiple of the bucket width that is at least `(to - from) / n`,
//! and the grid starts at `from` and runs a whole number of steps: it ends at
//! the first step boundary at or after `to`, so the last bucket may run past
//! `to` (by less than one step). There are at most `n` buckets, every edge is
//! a bucket boundary, and `bucketMs` is the step.
//!
//! ```json
//! {
//!   "window": { "from": "2026-10-02T00:00:00Z", "to": "2026-10-03T00:00:00Z" },
//!   "bucketMs": 900000,
//!   "watermark": "2026-10-02T23:20:00Z",
//!   "buckets": [
//!     { "from": "2026-10-02T00:00:00Z", "to": "2026-10-02T00:15:00Z",
//!       "transmissions": 14, "matchedBytes": 9310, "final": true }
//!   ]
//! }
//! ```
//!
//! A bucket is final when it ends at or before the watermark (the earlier
//! of the two responses'): its counts can no longer change.

use std::num::{NonZeroU32, NonZeroU64};

use crosstalk_spec::aggregates::edge::Weighting;
use crosstalk_spec::aggregates::series::{
    BucketWidth, InvalidGrid, InvalidStep, SeriesGrid, SeriesGrouping, SeriesGroups, SeriesStep,
    TopologySeries,
};
use crosstalk_spec::aggregates::watermark::Watermarked;
use crosstalk_spec::interfaces::l8_surface::Permission;
use crosstalk_spec::support::{TimeWindow, Timestamp};
use serde::Serialize;
use topcoat::context::Cx;
use topcoat::router::content::Json;
use topcoat::router::error::{bad_request, internal_server_error};
use topcoat::router::route;

use super::errors::query_error;
use super::query::{buckets, view_state};
use super::require;
use super::topology::WindowPayload;
use crate::app::{backend, caller};
use crate::backend::Backend;
use crate::contract::present::Present;
use crate::url::view_state::format_time;

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TimelinePayload {
    /// The view state's window the buckets cover.
    pub window: WindowPayload,
    /// Bucket width in milliseconds: the grid's step.
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

/// Why no grid could be laid over a window.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum GridError {
    #[error("from, to: the window is not on bucket boundaries")]
    Unaligned,
    #[error("buckets: no step fits ({0:?})")]
    Step(InvalidStep),
    #[error("buckets: no grid fits ({0:?})")]
    Grid(InvalidGrid),
}

/// The grid for `n` buckets over the aligned `window`, as the module docs
/// define it.
pub fn timeline_grid(
    window: TimeWindow,
    bucket: BucketWidth,
    n: NonZeroU32,
) -> Result<SeriesGrid, GridError> {
    if !bucket.is_boundary(window.start()) || !bucket.is_boundary(window.end()) {
        return Err(GridError::Unaligned);
    }
    let width = bucket.as_micros().get();
    let start = window.start().as_micros();
    let length = window.end().as_micros() - start;
    let per_bucket = length.div_ceil(u64::from(n.get()));
    let step = per_bucket.div_ceil(width).max(1).saturating_mul(width);
    let points = length.div_ceil(step);
    let end = Timestamp::from_micros(start.saturating_add(points.saturating_mul(step)));
    let step = NonZeroU64::new(step).ok_or(GridError::Unaligned)?;
    let step = SeriesStep::new(bucket, step).map_err(GridError::Step)?;
    let window = TimeWindow::new(window.start(), end).map_err(|_| GridError::Unaligned)?;
    SeriesGrid::new(window, step).map_err(GridError::Grid)
}

/// Why two series do not make one timeline. Always a backend bug.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum TimelineError {
    #[error("the transmissions and matched-bytes series are on different grids")]
    Grids,
    #[error("a timeline series is not the {0:?} total")]
    Shape(Weighting),
}

fn totals(series: &TopologySeries, weighting: Weighting) -> Result<&[u64], TimelineError> {
    match series.groups() {
        SeriesGroups::Total(values) if series.weighting() == weighting => Ok(values),
        _ => Err(TimelineError::Shape(weighting)),
    }
}

impl TimelinePayload {
    /// The payload of two total series on one grid: transmissions and
    /// matched bytes.
    pub fn new(
        window: WindowPayload,
        transmissions: &Watermarked<TopologySeries>,
        matched_bytes: &Watermarked<TopologySeries>,
    ) -> Result<Self, TimelineError> {
        let grid = transmissions.value.grid();
        if matched_bytes.value.grid() != grid {
            return Err(TimelineError::Grids);
        }
        let counts = totals(&transmissions.value, Weighting::Transmissions)?;
        let bytes = totals(&matched_bytes.value, Weighting::MatchedBytes)?;
        let watermark = transmissions.watermark.min(matched_bytes.watermark);
        let buckets = grid
            .point_windows()
            .zip(counts.iter().zip(bytes))
            .map(|(bucket, (transmissions, matched_bytes))| BucketPayload {
                from: format_time(bucket.start()),
                to: format_time(bucket.end()),
                transmissions: *transmissions,
                matched_bytes: *matched_bytes,
                is_final: watermark.finalizes(bucket),
            })
            .collect();
        Ok(Self {
            window,
            bucket_ms: grid.step().as_micros().get() / 1000,
            watermark: format_time(watermark.at()),
            buckets,
        })
    }
}

#[route(GET "/data/timeline")]
async fn timeline_data(cx: &Cx) -> topcoat::Result<Json<TimelinePayload>> {
    let caller = caller(cx);
    require(&caller, Permission::View)?;
    let state = view_state(cx).await?;
    let n = buckets(cx)?;
    let backend = backend(cx);
    let grid = timeline_grid(state.scope.window, backend.bucket_width(), n)
        .map_err(|e| bad_request(e.to_string()))?;
    let filter = state.scope.topology_filter();
    let total = SeriesGrouping::Total;
    let transmissions = backend
        .series(&caller, grid, Weighting::Transmissions, total, &filter)
        .await
        .map_err(query_error)?;
    let matched_bytes = backend
        .series(&caller, grid, Weighting::MatchedBytes, total, &filter)
        .await
        .map_err(query_error)?;
    let payload = TimelinePayload::new(state.scope.window.into(), &transmissions, &matched_bytes)
        .map_err(|error| {
        tracing::error!(error = %error, "timeline series disagree");
        internal_server_error(error)
    })?;
    tracing::debug!(buckets = payload.buckets.len(), "timeline payload");
    Ok(Json(payload))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::fixtures;

    const MINUTE: u64 = 60_000_000;

    fn width() -> BucketWidth {
        BucketWidth::from_micros(NonZeroU64::new(5 * MINUTE).expect("width"))
    }

    fn window(from_minutes: u64, to_minutes: u64) -> TimeWindow {
        TimeWindow::new(
            Timestamp::from_micros(from_minutes * MINUTE),
            Timestamp::from_micros(to_minutes * MINUTE),
        )
        .expect("window")
    }

    fn n(value: u32) -> NonZeroU32 {
        NonZeroU32::new(value).expect("non-zero")
    }

    #[test]
    fn grids_divide_evenly_when_they_can() {
        let day = window(0, 24 * 60);
        let grid = timeline_grid(day, width(), n(96)).expect("grid");
        assert_eq!(grid.points().get(), 96);
        assert_eq!(grid.step().as_micros().get(), 15 * MINUTE);
        assert_eq!(grid.window(), day);
    }

    #[test]
    fn grids_round_the_step_up_and_extend_the_end() {
        // 7 buckets over 60 minutes: steps of 10 minutes (the smallest
        // multiple of 5 at least 60 / 7), so 6 points ending on the hour.
        let grid = timeline_grid(window(0, 60), width(), n(7)).expect("grid");
        assert_eq!(grid.step().as_micros().get(), 10 * MINUTE);
        assert_eq!(grid.points().get(), 6);
        assert_eq!(grid.window(), window(0, 60));
        // 4 buckets over 55 minutes: 15-minute steps, ending 5 minutes late.
        let grid = timeline_grid(window(0, 55), width(), n(4)).expect("grid");
        assert_eq!(grid.step().as_micros().get(), 15 * MINUTE);
        assert_eq!(grid.window(), window(0, 60));
        // More buckets than fit: one per bucket width.
        let grid = timeline_grid(window(10, 30), width(), n(1000)).expect("grid");
        assert_eq!(grid.points().get(), 4);
        for point in grid.point_windows() {
            assert!(width().is_boundary(point.start()) && width().is_boundary(point.end()));
        }
    }

    #[test]
    fn unaligned_windows_have_no_grid() {
        let odd = TimeWindow::new(
            Timestamp::from_micros(0),
            Timestamp::from_micros(7 * MINUTE),
        )
        .expect("window");
        assert_eq!(timeline_grid(odd, width(), n(4)), Err(GridError::Unaligned));
    }

    #[test]
    fn marks_buckets_final_up_to_the_watermark() {
        let (window, transmissions, bytes) = fixtures::timeline();
        let payload = TimelinePayload::new(window.into(), &transmissions, &bytes).expect("payload");
        let grid = transmissions.value.grid();
        assert_eq!(payload.buckets.len(), grid.points().get() as usize);
        assert_eq!(payload.bucket_ms, 15 * 60 * 1000);
        let SeriesGroups::Total(counts) = transmissions.value.groups() else {
            panic!("total series")
        };
        for ((bucket, point), count) in payload.buckets.iter().zip(grid.point_windows()).zip(counts)
        {
            assert_eq!(bucket.is_final, point.end() <= transmissions.watermark.at());
            assert_eq!(bucket.transmissions, *count);
        }
        let finals = payload.buckets.iter().filter(|b| b.is_final).count();
        assert!(finals > 0 && finals < payload.buckets.len());
        assert!(
            payload.buckets[..finals].iter().all(|b| b.is_final),
            "final buckets form a prefix"
        );
    }

    #[test]
    fn refuses_series_that_are_not_one_timeline() {
        let (window, transmissions, bytes) = fixtures::timeline();
        assert_eq!(
            TimelinePayload::new(window.into(), &bytes, &transmissions),
            Err(TimelineError::Shape(Weighting::Transmissions))
        );
    }

    #[test]
    fn serializes_final_under_its_json_name() {
        let (window, transmissions, bytes) = fixtures::timeline();
        let payload = TimelinePayload::new(window.into(), &transmissions, &bytes).expect("payload");
        let json = serde_json::to_value(payload).expect("serialize");
        assert_eq!(json["buckets"][0]["final"], true);
        assert!(json["bucketMs"].is_u64());
    }
}
