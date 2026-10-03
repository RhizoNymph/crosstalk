//! Topology edges: confirmed transmissions counted per sender, reader, route
//! and time window.

use crate::derived::flow::transmission::Route;
use crate::ids::{AgentId, ChannelId, TopicId};
use crate::support::{Share, TimeWindow};

/// One bucket of the edge table. Windows are fixed-width buckets
/// (continuous aggregates); query windows are unions of buckets.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct EdgeKey {
    pub from: AgentId,
    pub to: AgentId,
    pub route: Route,
    pub bucket: TimeWindow,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct EdgeStats {
    pub transmissions: u64,
    /// Bytes of the sender's originated text that reached the reader.
    pub matched_bytes: u64,
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

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct TopologyFilter {
    pub agents: Vec<AgentId>,
    pub channels: Vec<ChannelId>,
    pub topics: Vec<TopicId>,
    pub include_direct: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct WeightedEdge {
    pub from: AgentId,
    pub to: AgentId,
    pub route: Route,
    pub stats: EdgeStats,
    pub share: Share,
}

/// The communication graph for one query window.
///
/// Invariant: the shares of `edges` sum to 1 (within float error) unless
/// `edges` is empty. Each edge's share is its stat under `weighting` divided
/// by the total of that stat across the window, after filtering.
#[derive(Debug, Clone, PartialEq)]
pub struct TopologyGraph {
    pub window: TimeWindow,
    pub weighting: Weighting,
    pub edges: Vec<WeightedEdge>,
}
