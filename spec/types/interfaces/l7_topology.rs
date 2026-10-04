//! L7 topology: edge aggregation. Consumer group `topology`, triggered by
//! `TransmissionClassified` (after analysis, so edges can be filtered by
//! topic) and `TopicVersionReady` (switch queries to the new version's
//! buckets, then publish `TopicVersionActivated`). Graph and series queries
//! resolve agents, including the ids named in a filter, through the
//! `AgentDirectory`. A contribution rejected as a self-edge is a permanent
//! outcome: its delivery is acked, not retried.
//!
//! A series query is a graph query cut into steps: for the same window,
//! weighting, filter and topic version, the sum of every series value is the
//! graph's [`TopologyGraph::total`], and grouped by edge each series sums to
//! that edge's stat in the graph.
//!
//! Implementations: `TimescaleEdgeStore` (continuous aggregates),
//! `InMemoryEdgeStore` (tests).

use std::num::NonZeroU64;

use crate::aggregates::edge::{
    EdgeKey, EdgeSelector, EdgeTransmissionPage, TopologyFilter, TopologyGraph, Weighting,
};
use crate::aggregates::series::{BucketWidth, SeriesGrid, SeriesGrouping, TopologySeries};
use crate::aggregates::topic::TopicModelVersion;
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
    /// agent.
    async fn apply(&mut self, contribution: &EdgeContribution) -> Result<EdgeKey, EdgeError>;

    /// Switch queries to `version` once its buckets are complete. Ignores a
    /// version older than the active one. Buckets of versions older than the
    /// previous one are dropped after a switch.
    async fn activate(&mut self, version: TopicModelVersion) -> Result<(), EdgeError>;

    async fn graph(
        &self,
        window: TimeWindow,
        weighting: Weighting,
        filter: &TopologyFilter,
    ) -> Result<TopologyGraph, EdgeError>;

    /// The applied contributions behind one edge: those `graph` counts into
    /// the edge (`from`, `to`, `route`) for the same window and filter, one
    /// row per transmission, newest `Confirmed::at` first. Served from the
    /// stored contributions, so the window need not be bucket-aligned. The
    /// first page pins the active topic-model version into its cursor; if
    /// that version's contributions are dropped mid-traversal, the next page
    /// fails with `InvalidCursor`.
    async fn transmissions(
        &self,
        edge: &EdgeSelector,
        window: TimeWindow,
        filter: &TopologyFilter,
        page: &PageRequest<EdgeTransmissionList>,
    ) -> Result<EdgeTransmissionPage, EdgeError>;

    /// The width of every bucket in this store. Graph windows and series
    /// grids must be aligned to it.
    fn bucket_width(&self) -> BucketWidth;

    /// One series per group of `grouping`, one value per grid point: the
    /// stat under `weighting` summed over that step, counted exactly as
    /// [`EdgeStore::graph`] counts it over the step's window. Fails with
    /// `BucketWidthMismatch` when the grid was built for another width.
    async fn series(
        &self,
        grid: SeriesGrid,
        weighting: Weighting,
        grouping: SeriesGrouping,
        filter: &TopologyFilter,
    ) -> Result<TopologySeries, EdgeError>;
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
}
