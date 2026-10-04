//! Accesses as a graph: agents reading and writing channels.
//!
//! Splitting each `Route::Channel` edge A→B into A→C→B would show only
//! writes somebody read. The early stage of a hijacked wiki is writes that
//! nobody has read yet, so the channel-centred view draws accesses directly,
//! from their own aggregate:
//!
//! ```text
//! AccessRecorded { access, channel } ─L7─▶ AccessEdge (agent, channel, op, bucket) += 1
//!
//! channel_topology(window, weighting, filter)
//!   accesses:      AccessEdge buckets in the window, agents and channels
//!                  resolved, filtered (TopologyFilter::admits_access), summed
//!   transmissions: exactly topology(window, weighting, filter)'s edges
//!   nodes:         agents and channels at every endpoint, agent ancestors
//! ```
//!
//! Access buckets are kept like edge buckets: stored under the agent the
//! access was attributed to and the channel it was recorded on, fixed-width
//! and aligned to the edge store's `BucketWidth`, resolved through
//! [`crate::aliases`] at read time. They have no topic dimension.
//!
//! A channel's resources and who used them are listed per resource
//! ([`ResourceUse`]), a page at a time.

use std::collections::HashSet;
use std::num::NonZeroU64;

use crate::aggregates::edge::{WeightedEdge, Weighting};
use crate::aggregates::node::{GraphNode, InvalidNodes, check_nodes};
use crate::aggregates::topic::TopicModelVersion;
use crate::derived::flow::access::AccessKind;
use crate::derived::flow::resource::Resource;
use crate::derived::flow::transmission::Route;
use crate::ids::{AgentId, ChannelId};
use crate::paging::{Page, ResourceUseList};
use crate::support::{Share, TimeWindow, Watermark};

/// One bucket of the access table: how often `agent` read or wrote
/// `channel` within `bucket`. A bucket exists only once an access has been
/// counted into it, so `accesses` is non-zero.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct AccessEdge {
    /// As attributed; resolved at read time.
    pub agent: AgentId,
    /// As recorded; resolved at read time.
    pub channel: ChannelId,
    pub op: AccessKind,
    pub bucket: TimeWindow,
    pub accesses: NonZeroU64,
}

/// One access edge of a channel-centred graph, over a canonical agent and a
/// canonical channel.
#[derive(Debug, Clone, Copy, PartialEq)]
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
#[derive(Debug, Clone, PartialEq)]
pub struct BipartiteParts {
    pub window: TimeWindow,
    /// The weighting of `transmissions`' shares.
    pub weighting: Weighting,
    pub topic_version: TopicModelVersion,
    /// The edge store's watermark when the response was computed: buckets
    /// before it are final.
    pub watermark: Watermark,
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
#[derive(Debug, Clone, PartialEq)]
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

impl BipartiteGraph {
    /// How far a share may sit from its exact ratio (float error).
    pub const SHARE_TOLERANCE: f64 = 1e-9;

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
        let access_total = total(parts.accesses.iter().map(|access| access.accesses));
        for (index, access) in parts.accesses.iter().enumerate() {
            if !share_is(access.share, access.accesses, access_total) {
                return Err(InvalidBipartite::AccessShare { index });
            }
        }
        let weighting = parts.weighting;
        let stat_total = total(
            parts
                .transmissions
                .iter()
                .map(|edge| weighting.stat(edge.stats)),
        );
        for (index, edge) in parts.transmissions.iter().enumerate() {
            if !share_is(edge.share, weighting.stat(edge.stats), stat_total) {
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

    pub fn watermark(&self) -> Watermark {
        self.parts.watermark
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

fn total(values: impl Iterator<Item = NonZeroU64>) -> u64 {
    values.fold(0, |sum, value| sum.saturating_add(value.get()))
}

/// Whether `share` is `value / total`. Precision loss in the casts is
/// within the tolerance for any realistic count.
#[allow(clippy::cast_precision_loss)]
fn share_is(share: Share, value: NonZeroU64, total: u64) -> bool {
    let exact = value.get() as f64 / total as f64;
    (share.get() - exact).abs() <= BipartiteGraph::SHARE_TOLERANCE
}

/// How often one canonical agent read or wrote a resource in a window.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
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
#[derive(Debug, Clone, PartialEq, Eq)]
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
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResourceUsePage {
    pub channel: ChannelId,
    pub window: TimeWindow,
    pub page: Page<ResourceUse, ResourceUseList>,
}
