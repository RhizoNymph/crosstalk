//! Graph nodes, the bipartite view, timelines and edge drill-down (items 1,
//! 3, 4 and 6).

use std::collections::HashSet;
use std::num::NonZeroU64;
use std::time::Duration;

use crosstalk_spec::aggregates::edge::{RouteKind, TopologyGraph, WeightedEdge, Weighting};
use crosstalk_spec::aggregates::topic::TopicModelVersion;
use crosstalk_spec::derived::flow::access::AccessKind;
use crosstalk_spec::derived::flow::resource::{Locator, ResourcePattern};
use crosstalk_spec::derived::flow::transmission::Route;
use crosstalk_spec::ids::{AgentId, ChannelId, TopicId, TransmissionId};
use crosstalk_spec::interfaces::l8_surface::PolicyKind;
use crosstalk_spec::support::{Share, TimeWindow, Timestamp};

use super::agents::AgentSummary;
use super::channels::{DetectionKind, OriginKind};
use super::verdict::Verdict;

/// What a channel node shows as its location.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ChannelShape {
    /// A declared channel's pattern.
    Pattern(ResourcePattern),
    /// A discovered channel's seed resource.
    Seed(Locator),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChannelNode {
    pub id: ChannelId,
    pub origin: OriginKind,
    pub detection: DetectionKind,
    pub policy: PolicyKind,
    pub shape: ChannelShape,
}

/// A topology graph with its nodes. Built only through
/// [`TopologyView::new`], which requires exactly one node per edge endpoint.
#[derive(Debug, Clone, PartialEq)]
pub struct TopologyView {
    graph: TopologyGraph,
    nodes: Vec<AgentSummary>,
    watermark: Timestamp,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum InvalidNodes {
    #[error("edge endpoint {0:?} has no node")]
    MissingNode(AgentId),
    #[error("agent {0:?} has more than one node")]
    DuplicateNode(AgentId),
}

fn check_nodes<'a>(
    nodes: impl Iterator<Item = AgentId>,
    endpoints: impl Iterator<Item = &'a AgentId>,
) -> Result<(), InvalidNodes> {
    let mut seen = HashSet::new();
    for id in nodes {
        if !seen.insert(id) {
            return Err(InvalidNodes::DuplicateNode(id));
        }
    }
    for id in endpoints {
        if !seen.contains(id) {
            return Err(InvalidNodes::MissingNode(*id));
        }
    }
    Ok(())
}

impl TopologyView {
    pub fn new(
        graph: TopologyGraph,
        nodes: Vec<AgentSummary>,
        watermark: Timestamp,
    ) -> Result<Self, InvalidNodes> {
        check_nodes(
            nodes.iter().map(|n| n.id),
            graph.edges.iter().flat_map(|e| [&e.from, &e.to]),
        )?;
        Ok(Self {
            graph,
            nodes,
            watermark,
        })
    }

    pub fn graph(&self) -> &TopologyGraph {
        &self.graph
    }

    pub fn nodes(&self) -> &[AgentSummary] {
        &self.nodes
    }

    /// Every bucket before this is final.
    pub fn watermark(&self) -> Timestamp {
        self.watermark
    }
}

/// Accesses by one agent to one channel in a window.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct AccessEdge {
    pub agent: AgentId,
    pub channel: ChannelId,
    pub op: AccessKind,
    pub accesses: NonZeroU64,
    /// Share of all accesses in the window after filtering; normalised
    /// separately from transmission shares.
    pub share: Share,
}

/// The graph with channels as nodes. Built only through
/// [`BipartiteView::new`], which requires a node for every endpoint.
#[derive(Debug, Clone, PartialEq)]
pub struct BipartiteView {
    window: TimeWindow,
    weighting: Weighting,
    topic_version: TopicModelVersion,
    agents: Vec<AgentSummary>,
    channels: Vec<ChannelNode>,
    accesses: Vec<AccessEdge>,
    /// Transmissions not routed through a channel, drawn agent to agent.
    transmissions: Vec<WeightedEdge>,
    watermark: Timestamp,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum InvalidBipartite {
    #[error(transparent)]
    Agents(#[from] InvalidNodes),
    #[error("access edge references channel {0:?} with no node")]
    MissingChannel(ChannelId),
    #[error("channel-routed transmission edge {0:?}; those are drawn through access edges")]
    ChannelRouted(ChannelId),
}

impl BipartiteView {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        window: TimeWindow,
        weighting: Weighting,
        topic_version: TopicModelVersion,
        agents: Vec<AgentSummary>,
        channels: Vec<ChannelNode>,
        accesses: Vec<AccessEdge>,
        transmissions: Vec<WeightedEdge>,
        watermark: Timestamp,
    ) -> Result<Self, InvalidBipartite> {
        if let Some(edge) = transmissions.iter().find_map(|e| match e.route {
            Route::Channel(id) => Some(id),
            _ => None,
        }) {
            return Err(InvalidBipartite::ChannelRouted(edge));
        }
        check_nodes(
            agents.iter().map(|n| n.id),
            accesses
                .iter()
                .map(|a| &a.agent)
                .chain(transmissions.iter().flat_map(|e| [&e.from, &e.to])),
        )?;
        let known: HashSet<ChannelId> = channels.iter().map(|c| c.id).collect();
        if let Some(missing) = accesses.iter().find(|a| !known.contains(&a.channel)) {
            return Err(InvalidBipartite::MissingChannel(missing.channel));
        }
        Ok(Self {
            window,
            weighting,
            topic_version,
            agents,
            channels,
            accesses,
            transmissions,
            watermark,
        })
    }

    pub fn window(&self) -> TimeWindow {
        self.window
    }

    pub fn weighting(&self) -> Weighting {
        self.weighting
    }

    pub fn topic_version(&self) -> TopicModelVersion {
        self.topic_version
    }

    pub fn agents(&self) -> &[AgentSummary] {
        &self.agents
    }

    pub fn channels(&self) -> &[ChannelNode] {
        &self.channels
    }

    pub fn accesses(&self) -> &[AccessEdge] {
        &self.accesses
    }

    pub fn transmissions(&self) -> &[WeightedEdge] {
        &self.transmissions
    }

    pub fn watermark(&self) -> Timestamp {
        self.watermark
    }
}

/// Fixed-width buckets over a scope's window.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Timeline {
    pub bucket_width: Duration,
    pub buckets: Vec<TimelineBucket>,
    pub watermark: Timestamp,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TimelineBucket {
    pub bucket: TimeWindow,
    pub transmissions: u64,
    pub matched_bytes: u64,
}

/// Which transmissions to list.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TransmissionSelector {
    /// The transmissions counted into one edge of the graph (canonical
    /// agents).
    Edge {
        from: AgentId,
        to: AgentId,
        route: Route,
    },
    /// A lasso or search selection.
    Ids(Vec<TransmissionId>),
    /// Everything in the scope.
    All,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TransmissionStateKind {
    Detected,
    AwaitingContent,
    Suspected,
    Confirmed,
    Classified,
    Aggregated,
    Discarded,
}

/// A row in a transmission list. `from` is known once confirmed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransmissionSummary {
    pub id: TransmissionId,
    pub from: Option<AgentId>,
    pub to: AgentId,
    pub route: Route,
    pub route_kind: RouteKind,
    pub state: TransmissionStateKind,
    pub opened_at: Timestamp,
    pub topic: Option<TopicId>,
    pub matched_bytes: u64,
    pub verdict: Option<Verdict>,
}

pub fn route_kind(route: &Route) -> RouteKind {
    match route {
        Route::Channel(_) => RouteKind::Channel,
        Route::Delegation(_) => RouteKind::Delegation,
        Route::Direct(_) => RouteKind::Direct,
        Route::Unobserved => RouteKind::Unobserved,
    }
}
