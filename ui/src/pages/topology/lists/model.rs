//! What the topology page's agent and channel lists show, built from the
//! same graphs the `<ct-topology>` payload is built from.
//!
//! - Agents: every agent node of the mode's graph (`topology` in agents
//!   mode, `channel_topology` in channels mode), heaviest first.
//! - Channels: in agents mode the channels behind channel-routed edges,
//!   with the transmissions those edges carry (the mode's graph has no
//!   channel nodes; their policy comes from the bipartite graph of the same
//!   window and filter, which has a node for every channel a transmission
//!   edge names); in channels mode the channel nodes, with their accesses.
//!
//! Each channel carries its node's confirmation, so an unconfirmed one is
//! marked; under confirmed only (`u=confirmed`) both graphs already leave
//! it out, so the lists do too.
//!
//! Names are the payload's: [`agent_node_name`] and one `channel_names`
//! call. Each item's selection value is the one the graph emits for it, so
//! selecting from a list and selecting in the graph are the same thing.

use std::collections::HashMap;

use crosstalk_spec::aggregates::access::BipartiteGraph;
use crosstalk_spec::aggregates::edge::{TopologyGraph, WeightedEdge};
use crosstalk_spec::aggregates::node::{AgentNode, CanonicalStateKind, ChannelNode, GraphNode};
use crosstalk_spec::derived::flow::access::AccessKind;
use crosstalk_spec::derived::flow::channel::confirmation::Confirmation;
use crosstalk_spec::ids::{AgentId, ChannelId};
use crosstalk_spec::interfaces::l8_surface::{Caller, Permission, PolicyKind, QueryApi};
use crosstalk_spec::observed::client::HarnessClaim;
use topcoat::context::Cx;

use crate::app::backend;
use crate::components::{agent_node_name, family_name, short_id};
use crate::error::UiError;
use crate::pages::common::action::require;
use crate::pages::common::transmissions::{ChannelNames, channel_names, route_channel};
use crate::pages::topology::selection::Selection;
use crate::url::ulid::UlidId;
use crate::url::view_state::{GraphMode, ViewState};

/// An agent node of the graph.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentItem {
    pub id: AgentId,
    pub name: String,
    pub state: CanonicalStateKind,
    /// The parent's display name, for a sub-agent.
    pub parent: Option<String>,
    /// Shown as claims, never as identity.
    pub claims: Vec<HarnessClaim>,
    pub transmissions_in: u64,
    pub transmissions_out: u64,
}

impl AgentItem {
    /// What the graph sizes the node by.
    pub fn volume(&self) -> u64 {
        self.transmissions_in.saturating_add(self.transmissions_out)
    }

    /// The selection value clicking the node emits.
    pub fn code(&self) -> String {
        Selection::Agent(self.id).encode()
    }

    /// The lowercase text the list's filter matches: name, id, parent and
    /// claimed harnesses.
    pub fn search_key(&self) -> String {
        let mut key = format!("{} {}", self.name, self.id.to_ulid());
        if let Some(parent) = &self.parent {
            key.push_str(&format!(" sub-agent {parent}"));
        }
        for claim in &self.claims {
            key.push(' ');
            key.push_str(family_name(&claim.family));
        }
        key.to_lowercase()
    }
}

/// What a listed channel carries in the view.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Carried {
    /// Agents mode: the transmissions on the edges routed through it.
    Transmissions { transmissions: u64, edges: usize },
    /// Channels mode: its access edges' reads and writes (the node's size).
    Accesses { writes: u64, reads: u64 },
}

impl Carried {
    pub fn volume(self) -> u64 {
        match self {
            Self::Transmissions { transmissions, .. } => transmissions,
            Self::Accesses { writes, reads } => writes.saturating_add(reads),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChannelItem {
    pub id: ChannelId,
    pub name: String,
    /// `None` when the bipartite graph has no node for the channel, which
    /// its contract rules out; the item is listed without a policy badge.
    pub policy: Option<PolicyKind>,
    /// The node's confirmation; `None` exactly when `policy` is.
    pub confirmation: Option<Confirmation>,
    pub carried: Carried,
}

impl ChannelItem {
    /// The selection value: what clicking the channel node (channels mode)
    /// or an access edge emits.
    pub fn code(&self) -> String {
        Selection::Channel(self.id).encode()
    }

    /// Whether the row is marked unconfirmed: only suspected cross-agent
    /// traffic goes through the channel.
    pub fn is_unconfirmed(&self) -> bool {
        self.confirmation == Some(Confirmation::Unconfirmed)
    }

    pub fn search_key(&self) -> String {
        let mut key = format!("{} {}", self.name, self.id.to_ulid());
        if let Some(policy) = self.policy {
            key.push(' ');
            key.push_str(crate::components::Badge::label(&policy));
        }
        if self.is_unconfirmed() {
            key.push(' ');
            key.push_str(crate::components::Badge::label(&Confirmation::Unconfirmed));
        }
        key.to_lowercase()
    }
}

/// Both lists, heaviest first.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphLists {
    pub mode: GraphMode,
    pub agents: Vec<AgentItem>,
    pub channels: Vec<ChannelItem>,
}

/// The agent nodes among `nodes`, heaviest first (then by name). A parent
/// that is not among them is named by its id's tail.
pub fn agent_items(nodes: &[GraphNode]) -> Vec<AgentItem> {
    let agents: Vec<&AgentNode> = nodes
        .iter()
        .filter_map(|node| match node {
            GraphNode::Agent(agent) => Some(agent),
            GraphNode::Channel(_) => None,
        })
        .collect();
    let names: HashMap<AgentId, String> = agents
        .iter()
        .map(|agent| (agent.id, agent_node_name(agent)))
        .collect();
    let mut items: Vec<AgentItem> = agents
        .into_iter()
        .map(|agent| AgentItem {
            id: agent.id,
            name: agent_node_name(agent),
            state: agent.state_kind,
            parent: agent.parent.map(|parent| {
                names
                    .get(&parent)
                    .cloned()
                    .unwrap_or_else(|| short_id(parent.to_ulid()))
            }),
            claims: agent
                .claims
                .entries()
                .iter()
                .map(|seen| seen.claim.clone())
                .collect(),
            transmissions_in: agent.transmissions_in,
            transmissions_out: agent.transmissions_out,
        })
        .collect();
    items.sort_by(|a, b| {
        b.volume()
            .cmp(&a.volume())
            .then_with(|| a.name.cmp(&b.name))
    });
    items
}

fn sort_channels(items: &mut [ChannelItem]) {
    items.sort_by(|a, b| {
        b.carried
            .volume()
            .cmp(&a.carried.volume())
            .then_with(|| a.name.cmp(&b.name))
    });
}

/// The policies and confirmations of the channel nodes among `nodes`.
fn policies(nodes: &[GraphNode]) -> HashMap<ChannelId, (PolicyKind, Confirmation)> {
    nodes
        .iter()
        .filter_map(|node| match node {
            GraphNode::Channel(ChannelNode {
                id,
                policy_kind,
                confirmation,
                ..
            }) => Some((*id, (*policy_kind, *confirmation))),
            GraphNode::Agent(_) => None,
        })
        .collect()
}

/// The channels `edges` are routed through (agents mode), with what they
/// carry, heaviest first.
pub fn routed_channels(
    edges: &[WeightedEdge],
    policies: &HashMap<ChannelId, (PolicyKind, Confirmation)>,
    names: &ChannelNames,
) -> Vec<ChannelItem> {
    let mut carried: HashMap<ChannelId, (u64, usize)> = HashMap::new();
    for edge in edges {
        if let Some(channel) = route_channel(&edge.route) {
            let entry = carried.entry(channel).or_default();
            entry.0 = entry.0.saturating_add(edge.stats.transmissions.get());
            entry.1 += 1;
        }
    }
    let mut items: Vec<ChannelItem> = carried
        .into_iter()
        .map(|(id, (transmissions, edges))| ChannelItem {
            id,
            name: names.name(id),
            policy: policies.get(&id).map(|(policy, _)| *policy),
            confirmation: policies.get(&id).map(|(_, confirmation)| *confirmation),
            carried: Carried::Transmissions {
                transmissions,
                edges,
            },
        })
        .collect();
    sort_channels(&mut items);
    items
}

/// The channel nodes of a bipartite graph (channels mode), with their
/// reads and writes, heaviest first.
pub fn channel_node_items(graph: &BipartiteGraph, names: &ChannelNames) -> Vec<ChannelItem> {
    let mut items: Vec<ChannelItem> = graph
        .nodes()
        .iter()
        .filter_map(|node| match node {
            GraphNode::Channel(channel) => Some(channel),
            GraphNode::Agent(_) => None,
        })
        .map(|channel| {
            let (writes, reads) = graph
                .accesses()
                .iter()
                .filter(|a| a.channel == channel.id)
                .fold((0u64, 0u64), |(w, r), a| match a.op {
                    AccessKind::Write => (w.saturating_add(a.accesses.get()), r),
                    AccessKind::Read => (w, r.saturating_add(a.accesses.get())),
                });
            ChannelItem {
                id: channel.id,
                name: names.name(channel.id),
                policy: Some(channel.policy_kind),
                confirmation: Some(channel.confirmation),
                carried: Carried::Accesses { writes, reads },
            }
        })
        .collect();
    sort_channels(&mut items);
    items
}

/// Agents mode: `graph`'s agents and the channels behind its edges, their
/// policies read from `bipartite` (the same window and filter).
pub fn agents_mode(
    graph: &TopologyGraph,
    bipartite: Option<&BipartiteGraph>,
    names: &ChannelNames,
) -> GraphLists {
    let policies = bipartite.map(|b| policies(b.nodes())).unwrap_or_default();
    GraphLists {
        mode: GraphMode::Agents,
        agents: agent_items(&graph.nodes),
        channels: routed_channels(&graph.edges, &policies, names),
    }
}

/// Channels mode: the bipartite graph's agents and channels.
pub fn channels_mode(graph: &BipartiteGraph, names: &ChannelNames) -> GraphLists {
    GraphLists {
        mode: GraphMode::Channels,
        agents: agent_items(graph.nodes()),
        channels: channel_node_items(graph, names),
    }
}

/// Loads the lists for `state`. `graph` is the agents-mode graph of the
/// same state, which the page has already read for its header.
pub async fn load(
    cx: &Cx,
    caller: &Caller,
    state: &ViewState,
    graph: &TopologyGraph,
) -> Result<GraphLists, UiError> {
    require(caller, Permission::View)?;
    let backend = backend(cx);
    let filter = state.scope.topology_filter();
    match state.graph {
        GraphMode::Agents => {
            let routed: Vec<ChannelId> = graph
                .edges
                .iter()
                .filter_map(|e| route_channel(&e.route))
                .collect();
            if routed.is_empty() {
                return Ok(agents_mode(graph, None, &ChannelNames::default()));
            }
            let bipartite = backend
                .channel_topology(caller, state.scope.window, state.weighting, &filter)
                .await?;
            let names = channel_names(cx, caller, routed).await;
            Ok(agents_mode(graph, Some(&bipartite.value), &names))
        }
        GraphMode::Channels => {
            let bipartite = backend
                .channel_topology(caller, state.scope.window, state.weighting, &filter)
                .await?;
            let channels = bipartite
                .value
                .nodes()
                .iter()
                .filter_map(|node| match node {
                    GraphNode::Channel(channel) => Some(channel.id),
                    GraphNode::Agent(_) => None,
                });
            let names = channel_names(cx, caller, channels).await;
            Ok(channels_mode(&bipartite.value, &names))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::fixtures;

    fn names() -> ChannelNames {
        fixtures::channel_names()
    }

    #[test]
    fn agents_are_every_agent_node_heaviest_first() {
        let graph = fixtures::topology_graph().value;
        let items = agent_items(&graph.nodes);
        assert_eq!(items.len(), graph.nodes.len());
        assert_eq!(items[0].name, "planner");
        let volumes: Vec<u64> = items.iter().map(AgentItem::volume).collect();
        let mut sorted = volumes.clone();
        sorted.sort_by(|a, b| b.cmp(a));
        assert_eq!(volumes, sorted);
        let researcher = items
            .iter()
            .find(|a| a.name == "researcher")
            .expect("researcher");
        assert_eq!(researcher.parent.as_deref(), Some("planner"));
        assert_eq!(
            Selection::parse(&researcher.code()),
            Ok(Selection::Agent(researcher.id))
        );
        assert!(items[0].search_key().contains("claude code"));
        assert!(researcher.search_key().contains("sub-agent planner"));
    }

    #[test]
    fn agents_mode_lists_the_channels_behind_routed_edges() {
        let graph = fixtures::topology_graph().value;
        let bipartite = fixtures::bipartite_graph().value;
        let lists = agents_mode(&graph, Some(&bipartite), &names());
        assert_eq!(lists.mode, GraphMode::Agents);
        // Four channels carry the fixture's six channel-routed edges.
        assert_eq!(lists.channels.len(), 4);
        let first = &lists.channels[0];
        assert_eq!(first.name, "/srv/shared/handoff*");
        assert_eq!(first.policy, Some(PolicyKind::Sanctioned));
        assert_eq!(
            first.carried,
            Carried::Transmissions {
                transmissions: 35,
                edges: 2
            }
        );
        assert_eq!(
            Selection::parse(&first.code()),
            Ok(Selection::Channel(first.id))
        );
        let total: u64 = lists.channels.iter().map(|c| c.carried.volume()).sum();
        let routed: u64 = graph
            .edges
            .iter()
            .filter(|e| route_channel(&e.route).is_some())
            .map(|e| e.stats.transmissions.get())
            .sum();
        assert_eq!(total, routed);
    }

    #[test]
    fn a_channel_without_a_bipartite_node_has_no_policy() {
        let graph = fixtures::topology_graph().value;
        let lists = agents_mode(&graph, None, &names());
        assert_eq!(lists.channels.len(), 4);
        assert!(lists.channels.iter().all(|c| c.policy.is_none()));
        assert!(!lists.channels[0].search_key().contains("sanctioned"));
    }

    #[test]
    fn channels_mode_lists_the_channel_nodes_with_their_accesses() {
        let bipartite = fixtures::bipartite_graph().value;
        let lists = channels_mode(&bipartite, &names());
        assert_eq!(lists.mode, GraphMode::Channels);
        let channel_nodes = bipartite
            .nodes()
            .iter()
            .filter(|n| matches!(n, GraphNode::Channel(_)))
            .count();
        assert_eq!(lists.channels.len(), channel_nodes);
        for item in &lists.channels {
            let accesses: u64 = bipartite
                .accesses()
                .iter()
                .filter(|a| a.channel == item.id)
                .map(|a| a.accesses.get())
                .sum();
            assert_eq!(item.carried.volume(), accesses);
            assert!(item.policy.is_some());
        }
        let agents = bipartite
            .nodes()
            .iter()
            .filter(|n| matches!(n, GraphNode::Agent(_)))
            .count();
        assert_eq!(lists.agents.len(), agents);
    }

    #[test]
    fn search_keys_are_lowercase_for_the_browser_filter() {
        // The list's filter lowercases the text and matches it against
        // `data-key`, so every key must already be lowercase.
        let graph = fixtures::topology_graph().value;
        let bipartite = fixtures::bipartite_graph().value;
        let lists = agents_mode(&graph, Some(&bipartite), &names());
        let keys = lists
            .agents
            .iter()
            .map(AgentItem::search_key)
            .chain(lists.channels.iter().map(ChannelItem::search_key));
        for key in keys {
            assert_eq!(key, key.to_lowercase());
        }
        assert!(lists.channels[0].search_key().contains("sanctioned"));
    }

    #[test]
    fn channels_carry_their_nodes_confirmation_and_unconfirmed_ones_match_it() {
        let graph = fixtures::topology_graph().value;
        let bipartite = fixtures::bipartite_graph().value;
        let confirmations: HashMap<ChannelId, Confirmation> = policies(bipartite.nodes())
            .into_iter()
            .map(|(id, (_, confirmation))| (id, confirmation))
            .collect();
        for lists in [
            agents_mode(&graph, Some(&bipartite), &names()),
            channels_mode(&bipartite, &names()),
        ] {
            for item in &lists.channels {
                assert_eq!(item.confirmation, confirmations.get(&item.id).copied());
                assert_eq!(
                    item.search_key().ends_with(" unconfirmed"),
                    item.is_unconfirmed(),
                    "{}",
                    item.search_key()
                );
            }
        }
        let mut item = channels_mode(&bipartite, &names()).channels.remove(0);
        item.confirmation = Some(Confirmation::Unconfirmed);
        assert!(item.is_unconfirmed());
        assert!(item.search_key().contains("unconfirmed"));
        item.confirmation = Some(Confirmation::Confirmed);
        assert!(!item.is_unconfirmed());
        assert!(!item.search_key().contains("confirmed"));
        let unrouted = agents_mode(&graph, None, &names());
        assert!(unrouted.channels.iter().all(|c| c.confirmation.is_none()));
    }

    #[test]
    fn a_routed_channel_takes_its_confirmation_from_the_bipartite_node() {
        // The fixture's edges are confirmed transmissions, so every routed
        // channel is confirmed; mark one unconfirmed by hand.
        let graph = fixtures::topology_graph().value;
        let first = agents_mode(&graph, None, &names()).channels[0].id;
        let standings =
            HashMap::from([(first, (PolicyKind::Unreviewed, Confirmation::Unconfirmed))]);
        let items = routed_channels(&graph.edges, &standings, &names());
        let item = items.iter().find(|c| c.id == first).expect("routed");
        assert_eq!(item.confirmation, Some(Confirmation::Unconfirmed));
        assert!(item.is_unconfirmed());
        assert!(item.search_key().ends_with(" unreviewed unconfirmed"));
    }
}
