//! Time series over the edge table: one value per step of an aligned grid,
//! per group. They back the UI's time brush and its per-topic, per-route and
//! per-edge trend lines.
//!
//! A series query reads the same buckets as a graph query: those of the
//! topic-model version its filter's selector resolves to (reported as
//! [`TopologySeries::topic_version`]), with agents resolved through merge aliases,
//! self-edges dropped and the same [`TopologyFilter`] semantics. Each point
//! is the stat under the [`Weighting`] summed over one step of the grid, so
//! for the same window, weighting, filter and topic version:
//!
//! - summing every value of every series gives [`TopologyGraph::total`];
//! - grouped by edge, the series keys are the graph's edges and each series
//!   sums to that edge's stat.
//!
//! ```text
//! window  [───────────────────────────────────────────)
//! buckets |b |b |b |b |b |b |b |b |b |b |b |b |
//! step    [─────────)[─────────)[─────────)[─────────)   3 buckets per step
//! points  v0         v1         v2         v3
//! ```
//!
//! [`TopologyFilter`]: crate::aggregates::edge::TopologyFilter

use std::collections::HashSet;
use std::hash::Hash;
use std::num::{NonZeroU32, NonZeroU64};

use serde::{Deserialize, Serialize};

use crate::aggregates::edge::{EdgeStats, RouteKind, TopologyGraph, Weighting};
use crate::aggregates::topic::TopicModelVersion;
use crate::derived::flow::transmission::Route;
use crate::ids::{AgentId, TopicId};
use crate::support::{TimeWindow, Timestamp};
use crate::wire::{Rejected, WireRequest};

/// The edge table's bucket width, in microseconds. Buckets are aligned to
/// multiples of the width from the Unix epoch, so every instant belongs to
/// exactly one bucket. One width per edge store; graph windows and series
/// grids are aligned to it. On the wire, the number of microseconds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct BucketWidth(NonZeroU64);

impl BucketWidth {
    pub const fn from_micros(micros: NonZeroU64) -> Self {
        Self(micros)
    }

    pub const fn as_micros(self) -> NonZeroU64 {
        self.0
    }

    /// Whether `at` is the start of a bucket.
    pub fn is_boundary(self, at: Timestamp) -> bool {
        at.as_micros().is_multiple_of(self.0.get())
    }
}

/// The width of one series point: a whole number of buckets, so every point
/// is a union of buckets and no bucket is split between two points.
///
/// Built only through [`SeriesStep::new`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", try_from = "RawSeriesStep")]
pub struct SeriesStep {
    bucket: BucketWidth,
    micros: NonZeroU64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InvalidStep {
    NotBucketMultiple {
        bucket: NonZeroU64,
        step: NonZeroU64,
    },
}

/// [`SeriesStep`]'s fields, decoded without the check.
#[derive(Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
struct RawSeriesStep {
    bucket: BucketWidth,
    micros: NonZeroU64,
}

impl TryFrom<RawSeriesStep> for SeriesStep {
    type Error = Rejected<InvalidStep>;

    fn try_from(raw: RawSeriesStep) -> Result<Self, Self::Error> {
        Self::new(raw.bucket, raw.micros).map_err(|error| Rejected::new("series step", error))
    }
}

impl SeriesStep {
    /// Rejects a step that is not a multiple of `bucket`.
    pub fn new(bucket: BucketWidth, step: NonZeroU64) -> Result<Self, InvalidStep> {
        if !step.get().is_multiple_of(bucket.as_micros().get()) {
            return Err(InvalidStep::NotBucketMultiple {
                bucket: bucket.as_micros(),
                step,
            });
        }
        Ok(Self {
            bucket,
            micros: step,
        })
    }

    pub fn bucket(self) -> BucketWidth {
        self.bucket
    }

    pub fn as_micros(self) -> NonZeroU64 {
        self.micros
    }

    /// At least 1.
    pub fn buckets_per_step(self) -> NonZeroU64 {
        NonZeroU64::new(self.micros.get() / self.bucket.as_micros()).unwrap_or(NonZeroU64::MIN)
    }
}

/// The points of a series: `window` cut into consecutive steps, starting at
/// the window's start. The window starts on a bucket boundary and is a whole
/// number of steps long, so it also ends on one, and every point covers
/// exactly one step.
///
/// Built only through [`SeriesGrid::new`].
///
/// A request (`series`). On the wire, `{"window": .., "step": ..}`: the
/// point count follows from them, and decoding goes through
/// [`SeriesGrid::new`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(try_from = "RawSeriesGrid", into = "RawSeriesGrid")]
pub struct SeriesGrid {
    window: TimeWindow,
    step: SeriesStep,
    points: NonZeroU32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InvalidGrid {
    /// The window does not start on a bucket boundary.
    UnalignedStart,
    /// The window's length is not a whole number of steps.
    PartialStep { remainder_micros: u64 },
    /// More than [`SeriesGrid::MAX_POINTS`] points.
    TooManyPoints { points: u64 },
}

/// [`SeriesGrid`]'s wire form: its window and step, without the point
/// count, which [`SeriesGrid::new`] computes.
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
struct RawSeriesGrid {
    window: TimeWindow,
    step: SeriesStep,
}

impl From<SeriesGrid> for RawSeriesGrid {
    fn from(grid: SeriesGrid) -> Self {
        Self {
            window: grid.window,
            step: grid.step,
        }
    }
}

impl TryFrom<RawSeriesGrid> for SeriesGrid {
    type Error = Rejected<InvalidGrid>;

    fn try_from(raw: RawSeriesGrid) -> Result<Self, Self::Error> {
        Self::new(raw.window, raw.step).map_err(|error| Rejected::new("series grid", error))
    }
}

/// A client picks the grid of a series query.
impl WireRequest for SeriesGrid {}

impl SeriesGrid {
    /// The most points one series may have. Bounds the work and the response
    /// size of a single query; a longer window needs a coarser step.
    pub const MAX_POINTS: u32 = 10_000;

    pub fn new(window: TimeWindow, step: SeriesStep) -> Result<Self, InvalidGrid> {
        if !step.bucket().is_boundary(window.start()) {
            return Err(InvalidGrid::UnalignedStart);
        }
        let length = window.end().as_micros() - window.start().as_micros();
        let step_micros = step.as_micros().get();
        let remainder_micros = length % step_micros;
        if remainder_micros != 0 {
            return Err(InvalidGrid::PartialStep { remainder_micros });
        }
        let count = length / step_micros;
        let points = u32::try_from(count)
            .ok()
            .filter(|points| *points <= Self::MAX_POINTS)
            .and_then(NonZeroU32::new)
            .ok_or(InvalidGrid::TooManyPoints { points: count })?;
        Ok(Self {
            window,
            step,
            points,
        })
    }

    pub fn window(self) -> TimeWindow {
        self.window
    }

    pub fn step(self) -> SeriesStep {
        self.step
    }

    /// The number of points of every series on this grid.
    pub fn points(self) -> NonZeroU32 {
        self.points
    }

    /// The step covered by point `index`, or `None` past the last point.
    pub fn point_window(self, index: u32) -> Option<TimeWindow> {
        if index >= self.points.get() {
            return None;
        }
        let step = self.step.as_micros().get();
        let start = self.window.start().as_micros() + u64::from(index) * step;
        TimeWindow::new(
            Timestamp::from_micros(start),
            Timestamp::from_micros(start + step),
        )
        .ok()
    }

    /// Every point's step, in order. Adjacent, and together exactly
    /// [`SeriesGrid::window`].
    pub fn point_windows(self) -> impl Iterator<Item = TimeWindow> {
        (0..self.points.get()).filter_map(move |index| self.point_window(index))
    }
}

/// How a series query splits the counted transmissions into series.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SeriesGrouping {
    /// One series of everything the filter admits.
    Total,
    /// One series per topic of the resolved version, plus one for outliers.
    Topic,
    RouteKind,
    /// One series per (from, to, route) over canonical agents: the graph's
    /// edges.
    Edge,
}

impl WireRequest for SeriesGrouping {}

/// The key of a per-edge series. Never a self-edge.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct SeriesEdge {
    pub from: AgentId,
    pub to: AgentId,
    pub route: Route,
}

/// One series: `values[i]` is the stat under the query's weighting summed
/// over the grid's point `i`. Zero when nothing was counted in that step.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct Series<K> {
    pub key: K,
    pub values: Vec<u64>,
}

impl<K> Series<K> {
    /// Saturates at `u64::MAX`.
    pub fn sum(&self) -> u64 {
        saturating_sum(&self.values)
    }
}

/// The series of one query, shaped by its grouping. A grouped variant holds
/// one series per key that counted anything in the window; keys with no
/// transmissions are left out, as edges are from a graph.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "type",
    content = "data",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum SeriesGroups {
    Total(Vec<u64>),
    /// `None` is the outlier series.
    ByTopic(Vec<Series<Option<TopicId>>>),
    ByRouteKind(Vec<Series<RouteKind>>),
    ByEdge(Vec<Series<SeriesEdge>>),
}

impl SeriesGroups {
    pub fn grouping(&self) -> SeriesGrouping {
        match self {
            Self::Total(_) => SeriesGrouping::Total,
            Self::ByTopic(_) => SeriesGrouping::Topic,
            Self::ByRouteKind(_) => SeriesGrouping::RouteKind,
            Self::ByEdge(_) => SeriesGrouping::Edge,
        }
    }
}

/// The answer to a series query over one grid.
///
/// Built only through [`TopologySeries::new`]: every series has exactly one
/// value per grid point, keys are distinct within a grouping, no grouped
/// series is all zeros, and no edge series is a self-edge.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", try_from = "RawTopologySeries")]
pub struct TopologySeries {
    grid: SeriesGrid,
    weighting: Weighting,
    topic_version: TopicModelVersion,
    groups: SeriesGroups,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InvalidSeries {
    WrongPointCount {
        expected: u32,
        got: usize,
    },
    DuplicateKey,
    /// A grouped series that counted nothing in the window.
    ZeroSeries,
    SelfEdge,
}

/// [`TopologySeries`]'s fields, decoded without the checks.
#[derive(Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
struct RawTopologySeries {
    grid: SeriesGrid,
    weighting: Weighting,
    topic_version: TopicModelVersion,
    groups: SeriesGroups,
}

impl TryFrom<RawTopologySeries> for TopologySeries {
    type Error = Rejected<InvalidSeries>;

    fn try_from(raw: RawTopologySeries) -> Result<Self, Self::Error> {
        Self::new(raw.grid, raw.weighting, raw.topic_version, raw.groups)
            .map_err(|error| Rejected::new("topology series", error))
    }
}

impl TopologySeries {
    pub fn new(
        grid: SeriesGrid,
        weighting: Weighting,
        topic_version: TopicModelVersion,
        groups: SeriesGroups,
    ) -> Result<Self, InvalidSeries> {
        let points = grid.points().get();
        match &groups {
            SeriesGroups::Total(values) => check_point_count(values, points)?,
            SeriesGroups::ByTopic(series) => check_grouped(series, points, |_| false)?,
            SeriesGroups::ByRouteKind(series) => check_grouped(series, points, |_| false)?,
            SeriesGroups::ByEdge(series) => {
                check_grouped(series, points, |edge| edge.from == edge.to)?;
            }
        }
        Ok(Self {
            grid,
            weighting,
            topic_version,
            groups,
        })
    }

    pub fn grid(&self) -> SeriesGrid {
        self.grid
    }

    pub fn weighting(&self) -> Weighting {
        self.weighting
    }

    pub fn topic_version(&self) -> TopicModelVersion {
        self.topic_version
    }

    pub fn groups(&self) -> &SeriesGroups {
        &self.groups
    }

    /// The sum of every value of every series. Equals
    /// [`TopologyGraph::total`] of the graph over the grid's window with the
    /// same weighting, filter and topic version. Saturates at `u64::MAX`.
    pub fn total(&self) -> u64 {
        match &self.groups {
            SeriesGroups::Total(values) => saturating_sum(values),
            SeriesGroups::ByTopic(series) => sum_series(series),
            SeriesGroups::ByRouteKind(series) => sum_series(series),
            SeriesGroups::ByEdge(series) => sum_series(series),
        }
    }
}

fn check_point_count(values: &[u64], points: u32) -> Result<(), InvalidSeries> {
    if values.len() == points as usize {
        Ok(())
    } else {
        Err(InvalidSeries::WrongPointCount {
            expected: points,
            got: values.len(),
        })
    }
}

fn check_grouped<K: Eq + Hash>(
    series: &[Series<K>],
    points: u32,
    is_self_edge: impl Fn(&K) -> bool,
) -> Result<(), InvalidSeries> {
    let mut keys = HashSet::new();
    for one in series {
        check_point_count(&one.values, points)?;
        if is_self_edge(&one.key) {
            return Err(InvalidSeries::SelfEdge);
        }
        if one.values.iter().all(|value| *value == 0) {
            return Err(InvalidSeries::ZeroSeries);
        }
        if !keys.insert(&one.key) {
            return Err(InvalidSeries::DuplicateKey);
        }
    }
    Ok(())
}

fn saturating_sum(values: &[u64]) -> u64 {
    values
        .iter()
        .fold(0, |sum, value| sum.saturating_add(*value))
}

fn sum_series<K>(series: &[Series<K>]) -> u64 {
    series
        .iter()
        .fold(0, |sum, one| sum.saturating_add(one.sum()))
}

impl Weighting {
    /// The stat this weighting measures.
    pub fn stat(self, stats: EdgeStats) -> NonZeroU64 {
        match self {
            Self::Transmissions => stats.transmissions,
            Self::MatchedBytes => stats.matched_bytes,
        }
    }
}

impl RouteKind {
    pub fn of(route: &Route) -> Self {
        match route {
            Route::Channel(_) => Self::Channel,
            Route::Delegation(_) => Self::Delegation,
            Route::Direct(_) => Self::Direct,
            Route::Unobserved => Self::Unobserved,
        }
    }
}

impl TopologyGraph {
    /// The stat under the graph's weighting summed over its edges: the
    /// denominator of every share. Saturates at `u64::MAX`.
    pub fn total(&self) -> u64 {
        let weighting = self.weighting();
        self.edges().iter().fold(0, |sum, edge| {
            sum.saturating_add(weighting.stat(edge.stats).get())
        })
    }
}
