//! Topology edges: confirmed transmissions counted per sender, reader, route,
//! topic and time bucket.
//!
//! Edges are stored under the agent ids the transmissions were attributed
//! to. A graph query resolves every agent through the merge aliases first,
//! sums edges that become equal, and drops edges that become self-edges.
//!
//! The transmissions counted into one edge of a graph can be listed with an
//! [`EdgeSelector`] (see `EdgeStore::transmissions`), so a click on an edge
//! drills down to exactly what it counts.

use std::num::NonZeroU64;

use crate::aggregates::topic::TopicModelVersion;
use crate::derived::flow::transmission::Route;
use crate::ids::{AgentId, TopicId, TransmissionId};
use crate::paging::{EdgeTransmissionList, Page};
use crate::support::{Share, TimeWindow, Timestamp};

pub use crate::aggregates::filter::TopologyFilter;

/// The topic dimension of an edge bucket. Buckets are kept per topic-model
/// version; after a re-fit the new version's buckets are rebuilt, and queries
/// switch to them once the rebuild completes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TopicSlot {
    pub version: TopicModelVersion,
    /// `None` for outlier transmissions.
    pub topic: Option<TopicId>,
}

/// One bucket of the edge table. Buckets are fixed-width (continuous
/// aggregates); query windows are unions of buckets.
///
/// Built only through [`EdgeKey::new`], which rejects a self-edge.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct EdgeKey {
    from: AgentId,
    to: AgentId,
    route: Route,
    topic: TopicSlot,
    bucket: TimeWindow,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SelfEdge;

impl EdgeKey {
    pub fn new(
        from: AgentId,
        to: AgentId,
        route: Route,
        topic: TopicSlot,
        bucket: TimeWindow,
    ) -> Result<Self, SelfEdge> {
        if from == to {
            return Err(SelfEdge);
        }
        Ok(Self {
            from,
            to,
            route,
            topic,
            bucket,
        })
    }

    pub fn from(&self) -> AgentId {
        self.from
    }

    pub fn to(&self) -> AgentId {
        self.to
    }

    pub fn route(&self) -> &Route {
        &self.route
    }

    pub fn topic(&self) -> TopicSlot {
        self.topic
    }

    pub fn bucket(&self) -> TimeWindow {
        self.bucket
    }
}

/// Counts for one bucket. A bucket exists only once a transmission has been
/// counted into it, so both counts are non-zero.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EdgeStats {
    pub transmissions: NonZeroU64,
    /// Bytes of the sender's originated text that reached the reader.
    pub matched_bytes: NonZeroU64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Edge {
    pub key: EdgeKey,
    pub stats: EdgeStats,
}

/// What an edge's share is a share of.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Weighting {
    Transmissions,
    MatchedBytes,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RouteKind {
    Channel,
    Delegation,
    Direct,
    Unobserved,
}

impl From<&Route> for RouteKind {
    fn from(route: &Route) -> Self {
        match route {
            Route::Channel(_) => Self::Channel,
            Route::Delegation(_) => Self::Delegation,
            Route::Direct(_) => Self::Direct,
            Route::Unobserved => Self::Unobserved,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct WeightedEdge {
    pub from: AgentId,
    pub to: AgentId,
    pub route: Route,
    pub stats: EdgeStats,
    pub share: Share,
}

/// The communication graph for one query window, over canonical agents.
///
/// Invariant: the shares of `edges` sum to 1 (within float error) unless
/// `edges` is empty. Each edge's share is its stat under `weighting` divided
/// by the total of that stat across the window, after filtering.
#[derive(Debug, Clone, PartialEq)]
pub struct TopologyGraph {
    pub window: TimeWindow,
    pub weighting: Weighting,
    /// The version the filter's selector resolved to.
    pub topic_version: TopicModelVersion,
    pub edges: Vec<WeightedEdge>,
}

/// One edge of a [`TopologyGraph`]: canonical sender, canonical reader and
/// route, as in a [`WeightedEdge`]. Ids that have since been merged away are
/// resolved through `AgentDirectory` before matching; an edge whose two ends
/// resolve to one agent counts nothing, as in the graph.
///
/// Built only through [`EdgeSelector::new`], which rejects a self-edge.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct EdgeSelector {
    from: AgentId,
    to: AgentId,
    route: Route,
}

impl EdgeSelector {
    pub fn new(from: AgentId, to: AgentId, route: Route) -> Result<Self, SelfEdge> {
        if from == to {
            return Err(SelfEdge);
        }
        Ok(Self { from, to, route })
    }

    pub fn from(&self) -> AgentId {
        self.from
    }

    pub fn to(&self) -> AgentId {
        self.to
    }

    pub fn route(&self) -> &Route {
        &self.route
    }
}

/// One transmission counted into an edge. Sender, reader and route are the
/// selector's. Holds no message content.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EdgeTransmission {
    pub transmission: TransmissionId,
    /// `Confirmed::at`: what the window is tested against and what edges are
    /// bucketed by.
    pub confirmed_at: Timestamp,
    pub matched_bytes: NonZeroU64,
    /// The topic under the page's `topic_version`; `None` for an outlier.
    pub topic: Option<TopicId>,
}

/// One page of the transmissions behind an edge, newest confirmation first.
///
/// Every page of one traversal evaluates topics (the filter's and each row's)
/// under the same `topic_version`, the version the first page resolved the
/// filter's selector to; the cursor pins it. Read with no apply in between, a full
/// traversal for an aligned window lists exactly the transmissions
/// `EdgeStore::graph` counts into that edge for the same window and filter.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EdgeTransmissionPage {
    pub topic_version: TopicModelVersion,
    pub page: Page<EdgeTransmission, EdgeTransmissionList>,
}
