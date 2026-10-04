//! Graph nodes: what a graph response says about each agent and channel it
//! draws, so the UI renders it without one lookup per node.
//!
//! **Which nodes appear.** Every edge endpoint, plus the canonical ancestors
//! of every agent endpoint (its parent, the parent's parent, …), each once.
//! Ancestors let the UI collapse sub-agents into their parent; including the
//! whole chain means every node's `parent` names a node in the same
//! response. A [`TopologyGraph`] has agent nodes only. A
//! [`BipartiteGraph`](crate::aggregates::access::BipartiteGraph) also has a
//! channel node for every channel an access touches or a transmission is
//! routed through.
//!
//! **Nodes are canonical.** Node ids are resolved through
//! [`crate::aliases`] at query time: no node is a merged agent or a
//! superseded channel. The node kinds make that partly structural: an agent
//! node's state cannot be `Merged` ([`CanonicalStateKind`]) and a channel
//! node's origin cannot be superseded ([`CanonicalOriginKind`]).
//!
//! **Counts agree with edges.** An agent node's `transmissions_in` and
//! `transmissions_out` are the transmissions of the response's transmission
//! edges into and out of it, so a node that is only an ancestor has zero of
//! both. [`TopologyGraph::check_nodes`] and `BipartiteGraph::new` check all
//! of the above except canonicity, which needs the directories.

use std::collections::{HashMap, HashSet};

use crate::aggregates::edge::{TopologyGraph, WeightedEdge};
use crate::derived::flow::channel::detection::DetectionKind;
use crate::derived::flow::channel::policy::PolicyKind;
use crate::derived::flow::channel::{ChannelOrigin, DeclaredHistory};
use crate::ids::{AgentId, ChannelId};
use crate::observed::agent::{AgentLabel, AgentState, ClaimSet};
use crate::support::NonBlank;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GraphNode {
    Agent(AgentNode),
    /// Only in the channel-centred view.
    Channel(ChannelNode),
}

/// A node's id, tagged by kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum NodeId {
    Agent(AgentId),
    Channel(ChannelId),
}

impl GraphNode {
    pub fn id(&self) -> NodeId {
        match self {
            Self::Agent(node) => NodeId::Agent(node.id),
            Self::Channel(node) => NodeId::Channel(node.id),
        }
    }
}

/// A canonical agent as a graph draws it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentNode {
    pub id: AgentId,
    /// The canonical agent's current operator-set display label
    /// ([`Agent::label`](crate::observed::agent::Agent::label), set by
    /// `Agent::rename`). `None` when unlabelled. Display only, never
    /// identity.
    pub label: Option<AgentLabel>,
    pub state_kind: CanonicalStateKind,
    /// The canonical form of the agent's parent; never the agent itself.
    /// `None` for a top-level agent, and when the parent resolves to the
    /// agent (a sub-agent merged into its parent).
    pub parent: Option<AgentId>,
    /// The harness claims seen on the exchanges of this agent and every
    /// agent merged into it ([`ClaimSet::union`]). Shown as claimed, never as
    /// identity.
    pub claims: ClaimSet,
    /// Transmissions on this response's edges into the agent.
    pub transmissions_in: u64,
    /// Transmissions on this response's edges out of the agent.
    pub transmissions_out: u64,
}

/// A canonical channel as the channel-centred view draws it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChannelNode {
    pub id: ChannelId,
    /// An operator display label. Channels carry none yet, so this is `None`
    /// until they do; the UI shows `locator_summary`.
    pub label: Option<String>,
    pub origin_kind: CanonicalOriginKind,
    pub detection_kind: DetectionKind,
    pub policy_kind: PolicyKind,
    /// What the channel covers, as text: the pattern of a channel declared
    /// before traffic, otherwise its seed's locator, with the count of
    /// further resources when there are any (`https://wiki.example/a (+12)`).
    pub locator_summary: NonBlank,
}

/// An agent state a canonical agent can be in: every state but `Merged`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CanonicalStateKind {
    Registered,
    Provisional,
    Established,
}

impl CanonicalStateKind {
    /// `None` for a merged agent, which is never a node.
    pub fn of(state: &AgentState) -> Option<Self> {
        match state {
            AgentState::Registered { .. } => Some(Self::Registered),
            AgentState::Provisional { .. } => Some(Self::Provisional),
            AgentState::Established { .. } => Some(Self::Established),
            AgentState::Merged(_) => None,
        }
    }
}

/// A channel origin a canonical channel can have: every origin but
/// superseded.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CanonicalOriginKind {
    /// Declared in config or by an operator before any traffic.
    DeclaredBeforeTraffic,
    /// Discovered, then promoted by an operator.
    Promoted,
    Discovered,
}

impl CanonicalOriginKind {
    /// `None` for a superseded channel, which is never a node.
    pub fn of(origin: &ChannelOrigin) -> Option<Self> {
        match origin {
            ChannelOrigin::Declared {
                history: DeclaredHistory::BeforeTraffic(_),
                ..
            } => Some(Self::DeclaredBeforeTraffic),
            ChannelOrigin::Declared {
                history: DeclaredHistory::Promoted { .. },
                ..
            } => Some(Self::Promoted),
            ChannelOrigin::Discovered { .. } => Some(Self::Discovered),
            ChannelOrigin::Superseded { .. } => None,
        }
    }
}

/// Why a graph's nodes do not describe its edges. Checks run in the order
/// of the variants and the first failure is returned.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InvalidNodes {
    /// Two nodes with one id.
    Duplicate(NodeId),
    /// An edge endpoint (or, in the channel-centred view, a route's channel)
    /// without a node.
    Missing(NodeId),
    SelfParent(AgentId),
    /// An agent node whose parent has no node.
    MissingParent {
        agent: AgentId,
        parent: AgentId,
    },
    /// A node that is neither an endpoint nor an ancestor of one.
    Unexpected(NodeId),
    /// An agent node whose transmission counts differ from its edges'.
    Counts(AgentId),
}

impl TopologyGraph {
    /// Whether `nodes` holds exactly one agent node per edge endpoint and
    /// ancestor, no channel node, and counts that agree with `edges`.
    pub fn check_nodes(&self) -> Result<(), InvalidNodes> {
        let endpoints = self.edges.iter().flat_map(|edge| [edge.from, edge.to]);
        check_nodes(&self.nodes, endpoints, [], &self.edges)
    }
}

/// The node rules shared by both graph kinds: `agents` and `channels` are
/// the endpoints that need nodes; `edges` are the transmission edges the
/// agent counts follow.
pub(crate) fn check_nodes(
    nodes: &[GraphNode],
    agents: impl IntoIterator<Item = AgentId>,
    channels: impl IntoIterator<Item = ChannelId>,
    edges: &[WeightedEdge],
) -> Result<(), InvalidNodes> {
    let mut ids = HashSet::new();
    if let Some(node) = nodes.iter().find(|node| !ids.insert(node.id())) {
        return Err(InvalidNodes::Duplicate(node.id()));
    }
    let endpoints: Vec<NodeId> = agents
        .into_iter()
        .map(NodeId::Agent)
        .chain(channels.into_iter().map(NodeId::Channel))
        .collect();
    if let Some(missing) = endpoints.iter().find(|id| !ids.contains(id)) {
        return Err(InvalidNodes::Missing(*missing));
    }
    let agent_nodes: HashMap<AgentId, &AgentNode> = nodes
        .iter()
        .filter_map(|node| match node {
            GraphNode::Agent(agent) => Some((agent.id, agent)),
            GraphNode::Channel(_) => None,
        })
        .collect();
    let in_order = nodes.iter().filter_map(|node| match node {
        GraphNode::Agent(agent) => Some(agent),
        GraphNode::Channel(_) => None,
    });
    for node in in_order {
        match node.parent {
            Some(parent) if parent == node.id => return Err(InvalidNodes::SelfParent(node.id)),
            Some(parent) if !agent_nodes.contains_key(&parent) => {
                return Err(InvalidNodes::MissingParent {
                    agent: node.id,
                    parent,
                });
            }
            Some(_) | None => {}
        }
    }
    let mut expected: HashSet<NodeId> = endpoints.iter().copied().collect();
    let mut frontier: Vec<AgentId> = endpoints
        .iter()
        .filter_map(|id| match id {
            NodeId::Agent(agent) => Some(*agent),
            NodeId::Channel(_) => None,
        })
        .collect();
    while let Some(agent) = frontier.pop() {
        let parent = agent_nodes.get(&agent).and_then(|node| node.parent);
        if let Some(parent) = parent
            && expected.insert(NodeId::Agent(parent))
        {
            frontier.push(parent);
        }
    }
    if let Some(node) = nodes.iter().find(|node| !expected.contains(&node.id())) {
        return Err(InvalidNodes::Unexpected(node.id()));
    }
    let mut inbound: HashMap<AgentId, u64> = HashMap::new();
    let mut outbound: HashMap<AgentId, u64> = HashMap::new();
    for edge in edges {
        let count = edge.stats.transmissions.get();
        let into = inbound.entry(edge.to).or_default();
        *into = into.saturating_add(count);
        let out = outbound.entry(edge.from).or_default();
        *out = out.saturating_add(count);
    }
    let mismatch = nodes.iter().find_map(|node| match node {
        GraphNode::Agent(agent)
            if agent.transmissions_in != inbound.get(&agent.id).copied().unwrap_or(0)
                || agent.transmissions_out != outbound.get(&agent.id).copied().unwrap_or(0) =>
        {
            Some(agent.id)
        }
        GraphNode::Agent(_) | GraphNode::Channel(_) => None,
    });
    match mismatch {
        Some(agent) => Err(InvalidNodes::Counts(agent)),
        None => Ok(()),
    }
}
