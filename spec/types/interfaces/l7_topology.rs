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
//! is logged at error and dead-lettered. Graph, totals, series and
//! transmission queries read the watermark before their data and return it
//! with the result ([`Watermarked`]); see [`crate::aggregates::watermark`].
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
//! Accesses: the same consumer counts every `AccessRecorded` into an
//! [`AccessEdge`] bucket (`EdgeStore::apply_access`), so the channel-centred
//! view ([`EdgeStore::channel_topology`]) shows writes nobody has read yet.
//!
//! Read-time resolution: every query resolves stored agent ids through the
//! `AgentDirectory` and stored channel ids (in routes and access buckets)
//! through the `ChannelDirectory`, including the ids a filter names, then
//! sums what became equal. Graph responses describe their nodes
//! ([`crate::aggregates::node`]) from the agent store, L3's `ClaimStore` and
//! the channel registry, read at query time.
//!
//! Implementations: `TimescaleEdgeStore` (continuous aggregates),
//! `InMemoryEdgeStore` (tests).

use std::collections::BTreeMap;
use std::num::NonZeroU64;

use crate::aggregates::access::{AccessEdge, BipartiteGraph};
use crate::aggregates::agents::AgentTraffic;
use crate::aggregates::edge::{
    EdgeKey, EdgeSelector, EdgeTotals, EdgeTransmissionPage, TopologyFilter, TopologyGraph,
    Weighting,
};
#[cfg(doc)]
use crate::aggregates::filter::TopicVersionSelector;
use crate::aggregates::filter::VersionUnavailable;
use crate::aggregates::series::{BucketWidth, SeriesGrid, SeriesGrouping, TopologySeries};
use crate::aggregates::topic::TopicModelVersion;
use crate::aggregates::watermark::{PipelineFrontier, Watermark, Watermarked};
use crate::derived::flow::access::AccessKind;
use crate::derived::flow::transmission::{Classification, Route};
use crate::derived::flow::verdict::{Observed, Verdict, VerdictRevision};
use crate::ids::{AccessId, AgentId, ChannelId, TopicId, TransmissionId};
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

/// One recorded access, as the edge store counts it: an `AccessRecorded`
/// event's access and channel.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AccessContribution {
    pub access: AccessId,
    pub agent: AgentId,
    pub channel: ChannelId,
    pub op: AccessKind,
    pub at: Timestamp,
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

    /// Switch queries to `version` once its buckets are complete: its
    /// `TopicVersionReady` has arrived and the store has processed (applied,
    /// or rejected as a self-edge) as many distinct transmissions classified
    /// under it with cause `Refit` as the event counts. Classifications
    /// under it with cause `Confirmation`, which follow the event, do not
    /// count. Ignores a version older than the active one. Drops nothing:
    /// see [`EdgeStore::drop_version`].
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

    /// Count one access into its bucket (agent and channel as recorded,
    /// `op`, the bucket holding `at`) and return the bucket after the apply.
    /// Idempotent on `access`: a redelivered access changes nothing and
    /// returns the bucket as it is. A write: fails with `EdgeError`.
    async fn apply_access(&mut self, access: &AccessContribution) -> Result<AccessEdge, EdgeError>;

    /// The graph over canonical agents: edges resolved, summed, filtered and
    /// shared, and one node per endpoint and ancestor
    /// (`TopologyGraph::new` accepts it), with the watermark read before
    /// its buckets. Fails with `UnalignedWindow` for a window not on bucket
    /// boundaries.
    async fn graph(
        &self,
        window: TimeWindow,
        weighting: Weighting,
        filter: &TopologyFilter,
    ) -> Result<Watermarked<TopologyGraph>, EdgeQueryError>;

    /// What `graph` counts for the same window and filter, without nodes,
    /// edges or shares: exactly [`EdgeTotals::of`] of `graph`'s value, with
    /// the watermark read before the buckets. Cheaper than `graph`: no node
    /// metadata is read and nothing is returned per edge. Fails like
    /// `graph`.
    async fn totals(
        &self,
        window: TimeWindow,
        filter: &TopologyFilter,
    ) -> Result<Watermarked<EdgeTotals>, EdgeQueryError>;

    /// The channel-centred graph: access buckets in `window` with agents and
    /// channels resolved, filtered by [`TopologyFilter::admits_access`] and
    /// summed per (agent, channel, op), with shares over all of them; the
    /// transmission edges exactly as `graph` returns them for the same
    /// window, weighting and filter, under the same topic version; nodes for
    /// every agent and channel they name (`BipartiteGraph::new` holds). The
    /// watermark ([`EdgeStore::watermark`]) is read before the buckets, as
    /// for `graph`: access and transmission buckets are both keyed by event
    /// time. Fails like `graph` (`UnalignedWindow`, the topic version's
    /// errors).
    ///
    /// [`TopologyFilter::admits_access`]: crate::aggregates::filter::TopologyFilter::admits_access
    async fn channel_topology(
        &self,
        window: TimeWindow,
        weighting: Weighting,
        filter: &TopologyFilter,
    ) -> Result<Watermarked<BipartiteGraph>, EdgeQueryError>;

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

    /// The traffic of each listed agent's canonical agent in `window`,
    /// keyed by the id listed: for canonical agent `a`, the sums of the
    /// transmissions of the edges into and out of `a` that `graph` returns
    /// for the same window and `TopologyFilter::default()` (active version,
    /// every route and topic, false detections included, self-edges after
    /// resolving merges dropped), so they equal `a`'s node counts in that
    /// graph, and zero when it has no node there. Unknown agents count
    /// zero. The watermark is read before the buckets, as for `graph`.
    /// Fails like `graph` (`UnalignedWindow`, the active version's errors).
    ///
    /// A `BTreeMap`, so the map has one order: ascending id, which is also
    /// ascending ULID text, the order its keys take when it is encoded.
    async fn agent_traffic(
        &self,
        window: TimeWindow,
        agents: &[AgentId],
    ) -> Result<Watermarked<BTreeMap<AgentId, AgentTraffic>>, EdgeQueryError>;

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

/// Why a read (`graph`, `totals`, `series`, `transmissions`, `watermark`)
/// failed. A dropped version is `Version(NotRetained)`.
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
