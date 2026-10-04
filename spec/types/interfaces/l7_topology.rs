//! L7 topology: edge aggregation. Consumer group `topology`, triggered by
//! `TransmissionClassified` (after analysis, so edges can be filtered by
//! topic) and `TopicVersionReady` (switch queries to the new version's
//! buckets; once `EdgeStore::activate` has switched, publish
//! `TopicVersionActivated`). Graph and series queries resolve agents,
//! including the ids named in a filter, through the `AgentDirectory`. A
//! contribution rejected as a self-edge is a permanent outcome: its delivery
//! is acked, not retried.
//!
//! When the store's watermark advances, L7 publishes `Changed::Watermark`
//! with the new value, once it is what graph and series queries report.
//!
//! A series query is a graph query cut into steps: for the same window,
//! weighting, filter and topic version, the sum of every series value is the
//! graph's [`TopologyGraph::total`], and grouped by edge each series sums to
//! that edge's stat in the graph.
//!
//! **Verdicts are subtracted at query time.** Buckets hold detector output
//! only: a verdict never rewrites a bucket, so it never makes a settled
//! bucket unsettled and an `Include` query never depends on verdicts. The
//! store also consumes `VerdictSet` ([`EdgeStore::judge`]) into its own copy
//! of each transmission's current verdict. A query with
//! `FalseDetections::Exclude` reads the buckets as an `Include` query would
//! and subtracts the stored contributions of the transmissions it holds as
//! `FalseDetection` that the rest of the filter admits, per edge and step,
//! in one snapshot; the drill-down skips their rows. A verdict that changes
//! after aggregation is therefore reflected by the next query that starts
//! after `judge` returns, in every window, with nothing to rebuild.
//! Verdicts are operator judgement, not detector data: an `Exclude` result
//! is as of the verdicts the store held when it ran and has no settling
//! point.
//!
//! **Retention.** Activation drops nothing. A version's buckets and stored
//! contributions are deleted only on `TopicVersionDropped`
//! ([`EdgeStore::drop_version`]), which the topic catalog publishes when
//! retention marks the version dropped
//! ([`crate::aggregates::retention`]). A query reading a version either
//! sees all of its buckets or fails with `Version(NotRetained)`.
//!
//! **Watermark.** The topology consumer recomputes the watermark from a
//! [`FrontierSource`] at least once per bucket width
//! ([`EdgeStore::advance_watermark`]) and publishes `WatermarkAdvanced` each
//! time it strictly advances, after persisting it. Once a watermark is
//! exposed, no bucket of a version that has been activated whose window ends
//! at or before it changes: `apply` refuses such a contribution with
//! `LateContribution`, which signals a frontier that broke its contract and
//! is logged at error and dead-lettered. Graph, series and transmission
//! queries read the watermark before their data and return it with the
//! result ([`Watermarked`]); see [`crate::aggregates::watermark`].
//!
//! **Topic version.** Graph, series and edge-transmission queries read the
//! buckets and contributions of one version: the filter's selector resolved
//! with [`TopicVersionSelector::resolve`] against the `TopicCatalog`'s
//! history, with `retained` true for the versions whose buckets this store
//! still holds (every version retention has not dropped). `Current` is the
//! catalog's active version; the store activates a version before the
//! catalog marks it active, so that version is always retained here. A
//! filter listing topics outside the resolved version fails with
//! `TopicsNotInVersion`. Every response reports the resolved version.
//!
//! Implementations: `TimescaleEdgeStore` (continuous aggregates),
//! `InMemoryEdgeStore` (tests).

use std::num::NonZeroU64;

use crate::aggregates::edge::{
    EdgeKey, EdgeSelector, EdgeTransmissionPage, TopologyFilter, TopologyGraph, Weighting,
};
#[cfg(doc)]
use crate::aggregates::filter::TopicVersionSelector;
use crate::aggregates::filter::VersionUnavailable;
use crate::aggregates::series::{BucketWidth, SeriesGrid, SeriesGrouping, TopologySeries};
use crate::aggregates::topic::TopicModelVersion;
use crate::aggregates::watermark::{PipelineFrontier, Watermark, Watermarked};
use crate::derived::flow::transmission::{Classification, Route};
use crate::derived::flow::verdict::{Observed, Verdict, VerdictRevision};
use crate::ids::{AgentId, TopicId, TransmissionId};
use crate::paging::{EdgeTransmissionList, PageRequest};
use crate::support::{TimeWindow, Timestamp};

/// One classified transmission, as the edge store counts it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EdgeContribution {
    pub transmission: TransmissionId,
    pub from: AgentId,
    pub to: AgentId,
    pub route: Route,
    pub at: Timestamp,
    pub matched_bytes: NonZeroU64,
    pub classification: Classification,
}

pub trait EdgeStore {
    /// Idempotent on (`transmission`, classification version). Returns the
    /// bucket it landed in, or `SelfEdge` if sender and reader are the same
    /// agent. Returns `LateContribution` and changes nothing when the
    /// classification version has been activated and the bucket ends at or
    /// before the exposed watermark, and `VersionNotRetained` for a dropped
    /// version.
    async fn apply(&mut self, contribution: &EdgeContribution) -> Result<EdgeKey, EdgeError>;

    /// Record `transmission`'s verdict at `revision` in the store's verdict
    /// copy (`CurrentVerdict::observe`), whether or not the transmission has
    /// been applied yet. Changes no bucket. Idempotent, and a revision not
    /// newer than the one held is `Stale` and changes nothing.
    async fn judge(
        &mut self,
        transmission: TransmissionId,
        verdict: Option<Verdict>,
        revision: VerdictRevision,
    ) -> Result<Observed, EdgeError>;

    /// Switch queries to `version` once its buckets are complete. Ignores a
    /// version older than the active one. Drops nothing: see
    /// [`EdgeStore::drop_version`].
    async fn activate(&mut self, version: TopicModelVersion) -> Result<(), EdgeError>;

    /// Delete every bucket and stored contribution of `version`, on
    /// `TopicVersionDropped`. The version is marked dropped before any row
    /// is deleted, so from then on every query naming it returns
    /// `VersionNotRetained` rather than part of its buckets. Idempotent.
    /// Refuses the active version or a newer one with `VersionInUse` and
    /// deletes nothing.
    async fn drop_version(&mut self, version: TopicModelVersion) -> Result<(), EdgeError>;

    /// The exposed watermark. Persisted with the buckets, so it never moves
    /// back, restarts included. Starts at the epoch.
    async fn watermark(&self) -> Result<Watermark, EdgeQueryError>;

    /// Recompute the watermark as `Watermark::settled(frontier, timing,
    /// bucket_width)` and expose it if it is later than the exposed one.
    /// Returns the new watermark when it advanced, after persisting it; the
    /// consumer then publishes `WatermarkAdvanced`. Never lowers the exposed
    /// watermark.
    async fn advance_watermark(
        &mut self,
        frontier: PipelineFrontier,
    ) -> Result<Option<Watermark>, EdgeError>;

    /// The graph, with the watermark read before its buckets. Fails with
    /// `UnalignedWindow` for a window not on bucket boundaries.
    async fn graph(
        &self,
        window: TimeWindow,
        weighting: Weighting,
        filter: &TopologyFilter,
    ) -> Result<Watermarked<TopologyGraph>, EdgeQueryError>;

    /// The applied contributions behind one edge: those `graph` counts into
    /// the edge (`from`, `to`, `route`) for the same window and filter, one
    /// row per transmission, newest `Confirmed::at` first. Served from the
    /// stored contributions, so the window need not be bucket-aligned. The
    /// first page resolves the filter's topic version and its cursor pins
    /// it; if that version's contributions are dropped mid-traversal, the
    /// next page fails with `Version(NotRetained)`. Each page carries the
    /// watermark read before it.
    async fn transmissions(
        &self,
        edge: &EdgeSelector,
        window: TimeWindow,
        filter: &TopologyFilter,
        page: &PageRequest<EdgeTransmissionList>,
    ) -> Result<Watermarked<EdgeTransmissionPage>, EdgeQueryError>;

    /// The width of every bucket in this store. Graph windows and series
    /// grids must be aligned to it.
    fn bucket_width(&self) -> BucketWidth;

    /// One series per group of `grouping`, one value per grid point: the
    /// stat under `weighting` summed over that step, counted exactly as
    /// [`EdgeStore::graph`] counts it over the step's window, under the same
    /// resolved topic version (grouped by topic, one series per topic of
    /// that version). Fails with `BucketWidthMismatch` when the grid was
    /// built for another width. The watermark is read before the buckets.
    async fn series(
        &self,
        grid: SeriesGrid,
        weighting: Weighting,
        grouping: SeriesGrouping,
        filter: &TopologyFilter,
    ) -> Result<Watermarked<TopologySeries>, EdgeQueryError>;
}

/// Where the topology consumer learns how far the pipeline has progressed.
///
/// `oldest_pending` is the earliest event time among:
///
/// - exchanges in flight at the proxy (started, not yet published as
///   `ExchangeCaptured`): their `started_at`;
/// - deliveries not yet acked (pending, retrying or dead-lettered) in the
///   consumer groups `reconstruct`, `provenance`, `flow`, `analyze` and
///   `topology`: the `started_at` of the exchange an exchange, delta, span or
///   content-match event derives from (for a content match, the reader
///   exchange), `Access::at` for an access, and `Confirmed::at` for a
///   transmission event. `TransmissionClassified` with cause `Refit` is left
///   out: it builds a version that is not active yet, and activation waits
///   for all of it.
///
/// `ticked_through` is the earliest last-processed tick among the flow
/// correlator shards.
///
/// This bounds what can still reach a bucket because every consumer on the
/// path acks a delivery only after publishing the events it derives from it,
/// and the correlator's own held state is bounded by `settle_after`.
///
/// Implementations: `PgFrontierSource` (the transport's delivery and
/// dead-letter tables, the shards' tick checkpoints and the proxy's
/// in-flight registry), `ManualFrontier` (tests).
pub trait FrontierSource {
    async fn frontier(&self) -> Result<PipelineFrontier, EdgeError>;
}

/// Why a write (`apply`, `judge`, `activate`, `drop_version`,
/// `advance_watermark`) or the frontier read failed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EdgeError {
    Store {
        reason: String,
    },
    SelfEdge,
    /// A contribution of a topic-model version retention has dropped.
    VersionNotRetained {
        version: TopicModelVersion,
    },
    /// `drop_version` of the active version or a newer one.
    VersionInUse {
        version: TopicModelVersion,
    },
    /// A contribution into a final bucket: its version has been activated
    /// and the bucket ends at or before the exposed watermark.
    LateContribution {
        bucket: TimeWindow,
        watermark: Watermark,
    },
}

/// Why a read (`graph`, `series`, `transmissions`, `watermark`) failed. A
/// dropped version is `Version(NotRetained)`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EdgeQueryError {
    Store {
        reason: String,
    },
    /// The window is not aligned to bucket boundaries.
    UnalignedWindow,
    /// A series grid built for a bucket width other than the store's.
    BucketWidthMismatch {
        store: BucketWidth,
        grid: BucketWidth,
    },
    Version(VersionUnavailable),
    /// The filter lists topics that are not in the resolved version.
    TopicsNotInVersion {
        version: TopicModelVersion,
        topics: Vec<TopicId>,
    },
    /// A cursor the store did not issue, or issued for another edge, window
    /// or filter.
    InvalidCursor,
}
