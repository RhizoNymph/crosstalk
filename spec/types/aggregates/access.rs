//! Accesses as a graph: agents reading and writing channels.
//!
//! Splitting each `Route::Channel` edge A→B into A→C→B would show only
//! writes somebody read. Once a channel exists, writes to it that nobody
//! has read yet matter (a hijacked wiki keeps being written to), so the
//! channel-centred view draws accesses directly, from their own aggregate:
//!
//! ```text
//! AccessRecorded { access, channel } ─L7─▶ AccessEdge (agent, resource, op, bucket) += 1
//!
//! channel_topology(window, weighting, filter) -> Watermarked<BipartiteGraph>
//!   watermark:     EdgeStore::watermark, read before the buckets
//!   accesses:      AccessEdge buckets in the window, agents resolved, each
//!                  resource resolved to the channel holding it now, kept
//!                  when that channel is listed as a channel, filtered
//!                  (TopologyFilter::admits_access), summed
//!   transmissions: exactly topology(window, weighting, filter)'s edges
//!   nodes:         agents and channels at every endpoint, agent ancestors
//! ```
//!
//! Access buckets are kept like edge buckets: stored under the agent the
//! access was attributed to and the resource it touched, fixed-width and
//! aligned to the edge store's `BucketWidth`. They have no topic dimension.
//! Everything else is resolved at read time: agents through
//! [`crate::aliases`], and each resource to the channel the registry holds
//! it on now (`ChannelRegistry::channels_of`, already canonical). A resource
//! is on no channel until a channel is discovered from it or a declared
//! pattern claims it, so its accesses before then are in no channel's
//! graph; they join the channel's buckets at read time the moment it
//! exists, without rewriting a bucket. Of the channels that resolves to,
//! only those listed as channels (`Listing::Channel`: with cross-agent
//! traffic, once merges resolve) are drawn.
//!
//! A channel's resources and who used them are listed per resource
//! ([`ResourceUse`]), a page at a time.

use std::collections::HashSet;
use std::num::NonZeroU64;

use serde::{Deserialize, Serialize};

use crate::aggregates::edge::{TopologyGraph, WeightedEdge, Weighting, share_is, stat_total};
use crate::aggregates::node::{GraphNode, InvalidNodes, check_nodes};
use crate::aggregates::topic::TopicModelVersion;
use crate::derived::flow::access::AccessKind;
use crate::derived::flow::resource::Resource;
use crate::derived::flow::transmission::Route;
use crate::ids::{AgentId, ChannelId, ResourceId};
use crate::paging::{Page, ResourceUseList};
use crate::support::{Share, TimeWindow};
use crate::wire::Rejected;

/// One bucket of the access table: how often `agent` read or wrote
/// `resource` within `bucket`. A bucket exists only once an access has been
/// counted into it, so `accesses` is non-zero.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct AccessEdge {
    /// As attributed; resolved at read time.
    pub agent: AgentId,
    /// As accessed; resolved to the channel holding it at read time.
    pub resource: ResourceId,
    pub op: AccessKind,
    pub bucket: TimeWindow,
    pub accesses: NonZeroU64,
}

/// One access edge of a channel-centred graph, over a canonical agent and a
/// canonical channel.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct WeightedAccess {
    pub agent: AgentId,
    pub channel: ChannelId,
    pub op: AccessKind,
    pub accesses: NonZeroU64,
    /// `accesses` over the total of every access edge in the response.
    /// Access shares are normalized on their own, apart from transmission
    /// shares, and are counts whatever the weighting.
    pub share: Share,
}

/// The fields of a [`BipartiteGraph`], before checking.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct BipartiteParts {
    pub window: TimeWindow,
    /// The weighting of `transmissions`' shares.
    pub weighting: Weighting,
    pub topic_version: TopicModelVersion,
    pub nodes: Vec<GraphNode>,
    pub accesses: Vec<WeightedAccess>,
    /// The transmission edges `topology` returns for the same window,
    /// weighting and filter.
    pub transmissions: Vec<WeightedEdge>,
}

/// The channel-centred topology for one query window: agents and channels
/// as nodes, access edges between them, and agent-to-agent transmission
/// edges.
///
/// On the wire, its [`BipartiteParts`], decoded through
/// [`BipartiteGraph::new`].
///
/// Built only through [`BipartiteGraph::new`], which checks that:
/// - no access edge (agent, channel, op) and no transmission edge (from, to,
///   route) appears twice, and no transmission edge is a self-edge;
/// - each access share is its accesses over the total accesses, and each
///   transmission share is its stat under `weighting` over the total of that
///   stat, so each family's shares sum to 1 unless it is empty;
/// - the nodes follow the rules of [`crate::aggregates::node`]: one per
///   access agent, access channel, transmission endpoint and transmission
///   route channel, plus agent ancestors, with counts that agree with the
///   transmission edges.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(try_from = "BipartiteParts", into = "BipartiteParts")]
pub struct BipartiteGraph {
    parts: BipartiteParts,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InvalidBipartite {
    SelfEdge { index: usize },
    DuplicateTransmission { index: usize },
    DuplicateAccess { index: usize },
    AccessShare { index: usize },
    TransmissionShare { index: usize },
    Nodes(InvalidNodes),
}

impl TryFrom<BipartiteParts> for BipartiteGraph {
    type Error = Rejected<InvalidBipartite>;

    fn try_from(parts: BipartiteParts) -> Result<Self, Self::Error> {
        Self::new(parts).map_err(|error| Rejected::new("bipartite graph", error))
    }
}

impl From<BipartiteGraph> for BipartiteParts {
    fn from(graph: BipartiteGraph) -> Self {
        graph.into_parts()
    }
}

impl BipartiteGraph {
    /// How far a share may sit from its exact ratio (float error): the
    /// same as the agent-centred graph's.
    pub const SHARE_TOLERANCE: f64 = TopologyGraph::SHARE_TOLERANCE;

    pub fn new(parts: BipartiteParts) -> Result<Self, InvalidBipartite> {
        let mut edges = HashSet::new();
        for (index, edge) in parts.transmissions.iter().enumerate() {
            if edge.from == edge.to {
                return Err(InvalidBipartite::SelfEdge { index });
            }
            if !edges.insert((edge.from, edge.to, &edge.route)) {
                return Err(InvalidBipartite::DuplicateTransmission { index });
            }
        }
        let mut accesses = HashSet::new();
        for (index, access) in parts.accesses.iter().enumerate() {
            if !accesses.insert((access.agent, access.channel, access.op)) {
                return Err(InvalidBipartite::DuplicateAccess { index });
            }
        }
        let access_total = stat_total(parts.accesses.iter().map(|access| access.accesses));
        for (index, access) in parts.accesses.iter().enumerate() {
            if !share_is(access.share, access.accesses, access_total) {
                return Err(InvalidBipartite::AccessShare { index });
            }
        }
        let weighting = parts.weighting;
        let transmission_total = stat_total(
            parts
                .transmissions
                .iter()
                .map(|edge| weighting.stat(edge.stats)),
        );
        for (index, edge) in parts.transmissions.iter().enumerate() {
            if !share_is(edge.share, weighting.stat(edge.stats), transmission_total) {
                return Err(InvalidBipartite::TransmissionShare { index });
            }
        }
        let agents = parts.accesses.iter().map(|access| access.agent).chain(
            parts
                .transmissions
                .iter()
                .flat_map(|edge| [edge.from, edge.to]),
        );
        let channels = parts.accesses.iter().map(|access| access.channel).chain(
            parts
                .transmissions
                .iter()
                .filter_map(|edge| match edge.route {
                    Route::Channel(channel) => Some(channel),
                    Route::Delegation(_) | Route::Direct(_) | Route::Unobserved => None,
                }),
        );
        check_nodes(&parts.nodes, agents, channels, &parts.transmissions)
            .map_err(InvalidBipartite::Nodes)?;
        Ok(Self { parts })
    }

    pub fn window(&self) -> TimeWindow {
        self.parts.window
    }

    pub fn weighting(&self) -> Weighting {
        self.parts.weighting
    }

    pub fn topic_version(&self) -> TopicModelVersion {
        self.parts.topic_version
    }

    pub fn nodes(&self) -> &[GraphNode] {
        &self.parts.nodes
    }

    pub fn accesses(&self) -> &[WeightedAccess] {
        &self.parts.accesses
    }

    pub fn transmissions(&self) -> &[WeightedEdge] {
        &self.parts.transmissions
    }

    pub fn into_parts(self) -> BipartiteParts {
        self.parts
    }
}

/// How often one canonical agent read or wrote a resource in a window.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct AgentAccesses {
    pub agent: AgentId,
    pub accesses: NonZeroU64,
}

/// One resource of a channel and who used it within a window.
///
/// Built only through [`ResourceUse::new`]: at least one writer or reader,
/// no agent twice among the writers or among the readers, each list ordered
/// by accesses descending, ties by agent id. Agents are canonical, with the
/// accesses of merged aliases summed into them.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", try_from = "RawResourceUse")]
pub struct ResourceUse {
    resource: Resource,
    writers: Vec<AgentAccesses>,
    readers: Vec<AgentAccesses>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InvalidResourceUse {
    /// Neither written nor read in the window: such a resource is not
    /// listed.
    Unused,
    DuplicateWriter(AgentId),
    DuplicateReader(AgentId),
}

/// [`ResourceUse`]'s fields, decoded without the checks. Decoding goes
/// through [`ResourceUse::new`], which orders the writers and readers.
#[derive(Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
struct RawResourceUse {
    resource: Resource,
    writers: Vec<AgentAccesses>,
    readers: Vec<AgentAccesses>,
}

impl TryFrom<RawResourceUse> for ResourceUse {
    type Error = Rejected<InvalidResourceUse>;

    fn try_from(raw: RawResourceUse) -> Result<Self, Self::Error> {
        Self::new(raw.resource, raw.writers, raw.readers)
            .map_err(|error| Rejected::new("resource use", error))
    }
}

impl ResourceUse {
    pub fn new(
        resource: Resource,
        mut writers: Vec<AgentAccesses>,
        mut readers: Vec<AgentAccesses>,
    ) -> Result<Self, InvalidResourceUse> {
        if writers.is_empty() && readers.is_empty() {
            return Err(InvalidResourceUse::Unused);
        }
        if let Some(agent) = repeated(&writers) {
            return Err(InvalidResourceUse::DuplicateWriter(agent));
        }
        if let Some(agent) = repeated(&readers) {
            return Err(InvalidResourceUse::DuplicateReader(agent));
        }
        let order = |a: &AgentAccesses, b: &AgentAccesses| {
            b.accesses
                .cmp(&a.accesses)
                .then_with(|| a.agent.cmp(&b.agent))
        };
        writers.sort_by(order);
        readers.sort_by(order);
        Ok(Self {
            resource,
            writers,
            readers,
        })
    }

    pub fn resource(&self) -> &Resource {
        &self.resource
    }

    /// Most accesses first.
    pub fn writers(&self) -> &[AgentAccesses] {
        &self.writers
    }

    /// Most accesses first.
    pub fn readers(&self) -> &[AgentAccesses] {
        &self.readers
    }
}

fn repeated(entries: &[AgentAccesses]) -> Option<AgentId> {
    let mut seen = HashSet::new();
    entries
        .iter()
        .find(|entry| !seen.insert(entry.agent))
        .map(|entry| entry.agent)
}

/// One page of a channel's resources, newest resource first.
///
/// `channel` is the canonical channel the request resolved to: asking for a
/// superseded channel answers for the channel that superseded it, whose
/// resources include the superseded channels' resources. A resource is
/// listed when it was accessed within `window` (by `Access::at`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct ResourceUsePage {
    pub channel: ChannelId,
    pub window: TimeWindow,
    pub page: Page<ResourceUse, ResourceUseList>,
}
