//! L7 topology: edge aggregation. Consumer group `topology`, triggered by
//! `TransmissionClassified` (after analysis, so edges can be filtered by
//! topic) and `TopicVersionReady` (switch queries to the new version's
//! buckets; once `EdgeStore::activate` has switched, publish
//! `TopicVersionActivated`). Graph and series queries resolve agents,
//! including the ids named in a filter, through the `AgentDirectory`. A
//! contribution rejected as a self-edge is a permanent outcome: its delivery
//! is acked, not retried.
//!
//! A series query is a graph query cut into steps: for the same window,
//! weighting, filter and topic version, the sum of every series value is the
//! graph's [`TopologyGraph::total`], and grouped by edge each series sums to
//! that edge's stat in the graph.
//!
//! **Retention.** Activation drops nothing. A version's buckets and stored
//! contributions are deleted only on `TopicVersionDropped`
//! ([`EdgeStore::drop_version`]), which the topic catalog publishes when
//! retention marks the version dropped
//! ([`crate::aggregates::retention`]). A query reading a version either
//! sees all of its buckets or fails with `VersionNotRetained`.
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
//! Implementations: `TimescaleEdgeStore` (continuous aggregates),
//! `InMemoryEdgeStore` (tests).

use std::num::NonZeroU64;

use crate::aggregates::edge::{
    EdgeKey, EdgeSelector, EdgeTransmissionPage, TopologyFilter, TopologyGraph, Weighting,
};
use crate::aggregates::series::{BucketWidth, SeriesGrid, SeriesGrouping, TopologySeries};
use crate::aggregates::topic::TopicModelVersion;
use crate::aggregates::watermark::{PipelineFrontier, Watermark, Watermarked};
use crate::derived::flow::transmission::{Classification, Route};
use crate::ids::{AgentId, TransmissionId};
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
    async fn watermark(&self) -> Result<Watermark, EdgeError>;

    /// Recompute the watermark as `Watermark::settled(frontier, timing,
    /// bucket_width)` and expose it if it is later than the exposed one.
    /// Returns the new watermark when it advanced, after persisting it; the
    /// consumer then publishes `WatermarkAdvanced`. Never lowers the exposed
    /// watermark.
    async fn advance_watermark(
        &mut self,
        frontier: PipelineFrontier,
    ) -> Result<Option<Watermark>, EdgeError>;

    /// The graph, with the watermark read before its buckets.
    async fn graph(
        &self,
        window: TimeWindow,
        weighting: Weighting,
        filter: &TopologyFilter,
    ) -> Result<Watermarked<TopologyGraph>, EdgeError>;

    /// The applied contributions behind one edge: those `graph` counts into
    /// the edge (`from`, `to`, `route`) for the same window and filter, one
    /// row per transmission, newest `Confirmed::at` first. Served from the
    /// stored contributions, so the window need not be bucket-aligned. The
    /// first page pins the active topic-model version into its cursor; if
    /// that version's contributions are dropped mid-traversal, the next page
    /// fails with `InvalidCursor`. Each page carries the watermark read
    /// before it.
    async fn transmissions(
        &self,
        edge: &EdgeSelector,
        window: TimeWindow,
        filter: &TopologyFilter,
        page: &PageRequest<EdgeTransmissionList>,
    ) -> Result<Watermarked<EdgeTransmissionPage>, EdgeError>;

    /// The width of every bucket in this store. Graph windows and series
    /// grids must be aligned to it.
    fn bucket_width(&self) -> BucketWidth;

    /// One series per group of `grouping`, one value per grid point: the
    /// stat under `weighting` summed over that step, counted exactly as
    /// [`EdgeStore::graph`] counts it over the step's window. Fails with
    /// `BucketWidthMismatch` when the grid was built for another width. The
    /// watermark is read before the buckets.
    async fn series(
        &self,
        grid: SeriesGrid,
        weighting: Weighting,
        grouping: SeriesGrouping,
        filter: &TopologyFilter,
    ) -> Result<Watermarked<TopologySeries>, EdgeError>;
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EdgeError {
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
    SelfEdge,
    /// A cursor the store did not issue, issued for another edge, window or
    /// filter, or pinning a topic-model version whose contributions are gone.
    InvalidCursor,
    /// A topic-model version retention has dropped.
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
