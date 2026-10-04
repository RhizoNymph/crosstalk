//! Topology edges: confirmed transmissions counted per sender, reader, route,
//! topic and time bucket.
//!
//! Edges are stored under the agent ids the transmissions were attributed
//! to. A graph query resolves every agent through the merge aliases first,
//! sums edges that become equal, and drops edges that become self-edges.

use std::num::NonZeroU64;

use crate::aggregates::topic::TopicModelVersion;
use crate::derived::flow::transmission::Route;
use crate::ids::{AgentId, ChannelId, TopicId};
use crate::support::{Share, TimeWindow};

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

/// Restricts which transmissions a graph counts. Empty lists do not
/// restrict. Non-empty lists combine with AND across fields.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct TopologyFilter {
    /// Keep edges whose sender OR reader is one of these (after alias
    /// resolution).
    pub agents: Vec<AgentId>,
    /// Keep only channel-routed edges on these channels.
    pub channels: Vec<ChannelId>,
    pub route_kinds: Vec<RouteKind>,
    /// Keep only transmissions whose topic, under the current topic-model
    /// version, is one of these. Outliers never match a topic filter.
    pub topics: Vec<TopicId>,
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
    pub topic_version: TopicModelVersion,
    pub edges: Vec<WeightedEdge>,
}
