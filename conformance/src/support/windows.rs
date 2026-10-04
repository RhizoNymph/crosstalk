//! Windows on bucket boundaries, derived from the harness's extent and
//! bucket width, and the time-brush grid the UI builds.

use std::num::{NonZeroU32, NonZeroU64};

use crosstalk_spec::aggregates::series::{BucketWidth, SeriesGrid, SeriesStep};
use crosstalk_spec::support::{TimeWindow, Timestamp};

/// `window`, or a panic naming it.
pub fn window(start: Timestamp, end: Timestamp) -> TimeWindow {
    TimeWindow::new(start, end).unwrap_or_else(|_| panic!("window {start:?}..{end:?}"))
}

/// The boundary at or before `at`.
pub fn align_down(bucket: BucketWidth, at: Timestamp) -> Timestamp {
    let width = bucket.as_micros().get();
    Timestamp::from_micros(at.as_micros() - at.as_micros() % width)
}

/// The boundary at or after `at`.
pub fn align_up(bucket: BucketWidth, at: Timestamp) -> Timestamp {
    let down = align_down(bucket, at);
    if down == at {
        at
    } else {
        Timestamp::from_micros(down.as_micros() + bucket.as_micros().get())
    }
}

/// The aligned bucket holding `at`.
pub fn bucket_of(bucket: BucketWidth, at: Timestamp) -> TimeWindow {
    let start = align_down(bucket, at);
    window(
        start,
        Timestamp::from_micros(start.as_micros() + bucket.as_micros().get()),
    )
}

/// The aligned window holding `at` and the buckets either side of it
/// up to `extent`, cut into the part before `at`'s bucket, the bucket and
/// the part after (either side may be empty).
pub fn split_at(bucket: BucketWidth, extent: TimeWindow, at: Timestamp) -> Split {
    let own = bucket_of(bucket, at);
    Split {
        before: TimeWindow::new(extent.start(), own.start()).ok(),
        own,
        after: TimeWindow::new(own.end(), extent.end()).ok(),
    }
}

/// An extent cut around one bucket.
#[derive(Debug, Clone, Copy)]
pub struct Split {
    pub before: Option<TimeWindow>,
    pub own: TimeWindow,
    pub after: Option<TimeWindow>,
}

/// The extent cut in two on a bucket boundary near its middle: windows
/// whose counts must add up to the whole (INV-354).
pub fn halves(bucket: BucketWidth, extent: TimeWindow) -> Option<(TimeWindow, TimeWindow)> {
    let middle = Timestamp::from_micros(
        extent.start().as_micros() + (extent.end().as_micros() - extent.start().as_micros()) / 2,
    );
    let cut = align_down(bucket, middle);
    Some((
        TimeWindow::new(extent.start(), cut).ok()?,
        TimeWindow::new(cut, extent.end()).ok()?,
    ))
}

/// The last `buckets` buckets of `extent` (all of it when shorter).
pub fn tail(bucket: BucketWidth, extent: TimeWindow, buckets: u64) -> TimeWindow {
    let span = bucket.as_micros().get().saturating_mul(buckets);
    let start = extent.end().as_micros().saturating_sub(span);
    window(
        Timestamp::from_micros(start.max(extent.start().as_micros())),
        extent.end(),
    )
}

/// One aligned bucket long before any traffic: at the epoch.
pub fn quiet(bucket: BucketWidth) -> TimeWindow {
    window(
        Timestamp::from_micros(0),
        Timestamp::from_micros(bucket.as_micros().get()),
    )
}

/// A window starting one microsecond after a boundary: never aligned.
pub fn unaligned(extent: TimeWindow) -> TimeWindow {
    window(
        Timestamp::from_micros(extent.start().as_micros() + 1),
        extent.end(),
    )
}

/// The time-brush grid the UI builds (`ui/src/data/timeline.rs`): `points`
/// steps of whole buckets over an aligned window, fewer when they do not
/// divide it, the window's end moved up to the last step's.
pub fn grid(bucket: BucketWidth, window_: TimeWindow, points: NonZeroU32) -> SeriesGrid {
    assert!(bucket.is_boundary(window_.start()) && bucket.is_boundary(window_.end()));
    let width = bucket.as_micros().get();
    let start = window_.start().as_micros();
    let length = window_.end().as_micros() - start;
    let per_point = length.div_ceil(u64::from(points.get()));
    let step = per_point.div_ceil(width).max(1).saturating_mul(width);
    let count = length.div_ceil(step);
    let end = Timestamp::from_micros(start.saturating_add(count.saturating_mul(step)));
    let step = NonZeroU64::new(step)
        .and_then(|step| SeriesStep::new(bucket, step).ok())
        .unwrap_or_else(|| panic!("step over {window_:?}"));
    SeriesGrid::new(window(window_.start(), end), step)
        .unwrap_or_else(|e| panic!("grid over {window_:?}: {e:?}"))
}

/// `n`, which must not be zero.
pub fn points(n: u32) -> NonZeroU32 {
    NonZeroU32::new(n).unwrap_or(NonZeroU32::MIN)
}
