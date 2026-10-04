//! Topology edges: confirmed transmissions counted per sender, reader, route,
//! topic and time bucket.
//!
//! Edges are stored under the agent ids the transmissions were attributed
//! to and the channel ids they were routed through. A graph query resolves
//! every agent through the merge aliases and every channel through
//! supersession first ([`crate::aliases`]), sums edges that become equal,
//! and drops edges that become self-edges.
//!
//! The transmissions counted into one edge of a graph can be listed with an
//! [`EdgeSelector`] (see `EdgeStore::transmissions`), so a click on an edge
//! drills down to exactly what it counts.

use std::collections::HashSet;
use std::num::NonZeroU64;

use serde::{Deserialize, Serialize};

use crate::aggregates::node::{GraphNode, InvalidNodes, check_graph_nodes};
use crate::aggregates::topic::TopicModelVersion;
use crate::derived::flow::transmission::Route;
use crate::ids::{AgentId, TopicId, TransmissionId};
use crate::paging::{EdgeTransmissionList, Page};
use crate::support::{Share, TimeWindow, Timestamp};
use crate::wire::{Rejected, WireRequest};

pub use crate::aggregates::filter::TopologyFilter;

/// The topic dimension of an edge bucket. Buckets are kept per topic-model
/// version; after a re-fit the new version's buckets are rebuilt, and queries
/// switch to them once the rebuild completes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct TopicSlot {
    pub version: TopicModelVersion,
    /// `None` for outlier transmissions.
    pub topic: Option<TopicId>,
}

/// One bucket of the edge table. Buckets are fixed-width (continuous
/// aggregates); query windows are unions of buckets.
///
/// Built only through [`EdgeKey::new`], which rejects a self-edge.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", try_from = "RawEdgeKey")]
pub struct EdgeKey {
    from: AgentId,
    to: AgentId,
    route: Route,
    topic: TopicSlot,
    bucket: TimeWindow,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SelfEdge;

/// [`EdgeKey`]'s fields, decoded without the check.
#[derive(Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
struct RawEdgeKey {
    from: AgentId,
    to: AgentId,
    route: Route,
    topic: TopicSlot,
    bucket: TimeWindow,
}

impl TryFrom<RawEdgeKey> for EdgeKey {
    type Error = Rejected<SelfEdge>;

    fn try_from(raw: RawEdgeKey) -> Result<Self, Self::Error> {
        Self::new(raw.from, raw.to, raw.route, raw.topic, raw.bucket)
            .map_err(|error| Rejected::new("edge key", error))
    }
}

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
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
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

/// What an edge's share is a share of. A request (`topology`,
/// `channel_topology`, `series`): `"transmissions"`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Weighting {
    Transmissions,
    MatchedBytes,
}

impl WireRequest for Weighting {}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct WeightedEdge {
    pub from: AgentId,
    pub to: AgentId,
    pub route: Route,
    pub stats: EdgeStats,
    pub share: Share,
}

/// The communication graph for one query window, over canonical agents and
/// canonical channels (a `Route::Channel` names the channel after
/// supersession).
///
/// Built only through [`TopologyGraph::new`], which checks every rule:
///
/// - no edge is a self-edge and no (from, to, route) appears twice;
/// - each edge's share is its stat under `weighting` divided by the total of
///   that stat across the window, after filtering, so the shares sum to 1
///   (within [`TopologyGraph::SHARE_TOLERANCE`]) unless there are no edges;
/// - `nodes` describes every agent the edges name, and their ancestors, once
///   each, with counts that agree with the edges
///   ([`crate::aggregates::node`]). It holds no channel nodes; the
///   channel-centred view does.
///
/// On the wire it is its parts ([`TopologyGraphParts`]), decoded through
/// the same constructor, so JSON cannot carry a graph that breaks a rule
/// either.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(try_from = "TopologyGraphParts", into = "TopologyGraphParts")]
pub struct TopologyGraph {
    parts: TopologyGraphParts,
}

/// The fields of a [`TopologyGraph`], before they are checked: its wire
/// shape.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct TopologyGraphParts {
    pub window: TimeWindow,
    pub weighting: Weighting,
    /// The version the filter's selector resolved to.
    pub topic_version: TopicModelVersion,
    pub nodes: Vec<GraphNode>,
    pub edges: Vec<WeightedEdge>,
}

impl TryFrom<TopologyGraphParts> for TopologyGraph {
    type Error = Rejected<InvalidGraph>;

    fn try_from(parts: TopologyGraphParts) -> Result<Self, Self::Error> {
        Self::new(parts).map_err(|error| Rejected::new("topology graph", error))
    }
}

impl From<TopologyGraph> for TopologyGraphParts {
    fn from(graph: TopologyGraph) -> Self {
        graph.into_parts()
    }
}

/// Why parts are not a [`TopologyGraph`]. Checks run in the order of the
/// variants and the first failure is returned; `index` is the edge's
/// position in `edges`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InvalidGraph {
    /// An edge from an agent to itself.
    SelfEdge {
        index: usize,
    },
    /// A second edge with the same (from, to, route).
    DuplicateEdge {
        index: usize,
    },
    /// A share other than the edge's stat under the weighting over the
    /// total of that stat.
    Share {
        index: usize,
    },
    Nodes(InvalidNodes),
}

impl TopologyGraph {
    /// How far a share may sit from its exact ratio (float error).
    pub const SHARE_TOLERANCE: f64 = 1e-9;

    /// Check `parts` against every rule of the type: no self-edge, no
    /// (from, to, route) twice, each share its stat under `weighting` over
    /// the total of that stat (so the shares sum to 1 unless there are no
    /// edges), and nodes as [`crate::aggregates::node`] requires.
    pub fn new(parts: TopologyGraphParts) -> Result<Self, InvalidGraph> {
        let mut seen = HashSet::new();
        for (index, edge) in parts.edges.iter().enumerate() {
            if edge.from == edge.to {
                return Err(InvalidGraph::SelfEdge { index });
            }
            if !seen.insert((edge.from, edge.to, &edge.route)) {
                return Err(InvalidGraph::DuplicateEdge { index });
            }
        }
        let weighting = parts.weighting;
        let total = stat_total(parts.edges.iter().map(|edge| weighting.stat(edge.stats)));
        for (index, edge) in parts.edges.iter().enumerate() {
            if !share_is(edge.share, weighting.stat(edge.stats), total) {
                return Err(InvalidGraph::Share { index });
            }
        }
        check_graph_nodes(&parts.nodes, &parts.edges).map_err(InvalidGraph::Nodes)?;
        Ok(Self { parts })
    }

    pub fn window(&self) -> TimeWindow {
        self.parts.window
    }

    pub fn weighting(&self) -> Weighting {
        self.parts.weighting
    }

    /// The version the filter's selector resolved to.
    pub fn topic_version(&self) -> TopicModelVersion {
        self.parts.topic_version
    }

    pub fn nodes(&self) -> &[GraphNode] {
        &self.parts.nodes
    }

    pub fn edges(&self) -> &[WeightedEdge] {
        &self.parts.edges
    }

    pub fn into_parts(self) -> TopologyGraphParts {
        self.parts
    }
}

/// The sum of some non-zero stats, saturating.
pub(crate) fn stat_total(values: impl Iterator<Item = NonZeroU64>) -> u64 {
    values.fold(0, |sum, value| sum.saturating_add(value.get()))
}

/// Whether `share` is `value / total`, within
/// [`TopologyGraph::SHARE_TOLERANCE`]. Precision loss in the casts is within
/// the tolerance for any realistic count.
#[allow(clippy::cast_precision_loss)]
pub(crate) fn share_is(share: Share, value: NonZeroU64, total: u64) -> bool {
    let exact = value.get() as f64 / total as f64;
    (share.get() - exact).abs() <= TopologyGraph::SHARE_TOLERANCE
}

/// What a [`TopologyGraph`] counts in total, without its nodes, edges or
/// shares: what the overview shows for a window and filter
/// (`EdgeStore::totals`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct EdgeTotals {
    /// The version the filter's selector resolved to.
    pub topic_version: TopicModelVersion,
    /// Transmissions counted into the graph's edges.
    pub transmissions: u64,
    /// Their matched bytes.
    pub matched_bytes: u64,
    /// Active channels: the distinct canonical channels that some edge's
    /// route names (`Route::Channel`), that is, every channel that carried
    /// at least one transmission the graph counts.
    pub active_channels: u64,
}

impl EdgeTotals {
    /// The definition: the totals of `graph`'s edges. Weighting and shares
    /// do not enter, so every weighting gives the same totals.
    pub fn of(graph: &TopologyGraph) -> Self {
        let mut channels = Vec::new();
        let (mut transmissions, mut matched_bytes) = (0u64, 0u64);
        for edge in graph.edges() {
            transmissions = transmissions.saturating_add(edge.stats.transmissions.get());
            matched_bytes = matched_bytes.saturating_add(edge.stats.matched_bytes.get());
            if let Route::Channel(channel) = edge.route
                && !channels.contains(&channel)
            {
                channels.push(channel);
            }
        }
        Self {
            topic_version: graph.topic_version(),
            transmissions,
            matched_bytes,
            active_channels: u64::try_from(channels.len()).unwrap_or(u64::MAX),
        }
    }
}

/// One edge of a [`TopologyGraph`]: canonical sender, canonical reader and
/// route, as in a [`WeightedEdge`]. Ids that have since been merged away are
/// resolved through `AgentDirectory` before matching; an edge whose two ends
/// resolve to one agent counts nothing, as in the graph.
///
/// Built only through [`EdgeSelector::new`], which rejects a self-edge.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", try_from = "RawEdgeSelector")]
pub struct EdgeSelector {
    from: AgentId,
    to: AgentId,
    route: Route,
}

/// [`EdgeSelector`]'s fields, decoded without the check.
#[derive(Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
struct RawEdgeSelector {
    from: AgentId,
    to: AgentId,
    route: Route,
}

impl TryFrom<RawEdgeSelector> for EdgeSelector {
    type Error = Rejected<SelfEdge>;

    fn try_from(raw: RawEdgeSelector) -> Result<Self, Self::Error> {
        Self::new(raw.from, raw.to, raw.route)
            .map_err(|error| Rejected::new("edge selector", error))
    }
}

/// A client picks the edge to drill into.
impl WireRequest for EdgeSelector {}

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
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
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
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct EdgeTransmissionPage {
    pub topic_version: TopicModelVersion,
    pub page: Page<EdgeTransmission, EdgeTransmissionList>,
}
