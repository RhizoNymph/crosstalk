//! The `<ct-topology>` payload and its route.
//!
//! `GET /data/topology?<view state>` answers with a [`TopologyPayload`] as
//! JSON: the agents-mode graph (`g=agents`, `Backend::topology`) or the
//! bipartite graph (`g=channels`, `Backend::channel_topology`). Needs
//! `View`. Example (agents mode, one node and one edge shown):
//!
//! ```json
//! {
//!   "mode": "agents",
//!   "window": { "from": "2026-10-02T00:00:00Z", "to": "2026-10-03T00:00:00Z" },
//!   "weighting": "tx",
//!   "topicVersion": 3,
//!   "watermark": "2026-10-02T23:30:00Z",
//!   "nodes": [
//!     { "kind": "agent", "id": "<ulid>", "name": "planner", "state": "established",
//!       "parent": null, "volume": 42, "transmissionsIn": 12, "transmissionsOut": 30,
//!       "claims": [ { "harness": "Claude Code", "version": "2.1.3",
//!                     "userAgent": "claude-cli/2.1.3", "lastSeen": "2026-10-02T22:00:00Z" } ] }
//!   ],
//!   "edges": [
//!     { "kind": "transmission", "from": "<ulid>", "to": "<ulid>", "route": "dl.p2c",
//!       "routeKind": "delegation", "share": 0.25, "transmissions": 10, "matchedBytes": 5120 }
//!   ]
//! }
//! ```
//!
//! Channels mode adds `{ "kind": "channel", "id", "name", "origin",
//! "detection", "policy", "volume" }` nodes and `{ "kind": "access",
//! "agent", "channel", "op": "read" | "write", "accesses", "share" }` edges.
//! Its transmission edges are the spec's channel-centred graph's edges not
//! routed through a channel (those are drawn as accesses), with their shares
//! of the whole filtered total, as in agents mode. Access shares are
//! normalised separately from transmission shares. Route codes are those of
//! [`crate::url::route`].
//!
//! The payload is read from the spec's graphs: agent nodes' `name` is
//! [`agent_node_name`], channel nodes' `name` the channel's pattern or seed
//! locator from one `channel_names` call (not the spec's `locator_summary`,
//! which adds a resource count); `origin` is `declared` for a channel
//! declared before traffic or promoted.

use crosstalk_spec::aggregates::access::{BipartiteGraph, WeightedAccess};
use crosstalk_spec::aggregates::edge::{RouteKind, TopologyGraph, WeightedEdge, Weighting};
use crosstalk_spec::aggregates::node::{
    AgentNode, CanonicalOriginKind, CanonicalStateKind, ChannelNode, GraphNode,
};
use crosstalk_spec::aggregates::watermark::Watermarked;
use crosstalk_spec::derived::flow::access::AccessKind;
use crosstalk_spec::derived::flow::channel::detection::DetectionKind;
use crosstalk_spec::derived::flow::transmission::Route;
use crosstalk_spec::interfaces::l8_surface::{Permission, PolicyKind};
use crosstalk_spec::observed::agent::SeenClaim;
use crosstalk_spec::support::TimeWindow;
use serde::{Deserialize, Serialize};
use topcoat::context::Cx;
use topcoat::router::content::Json;
use topcoat::router::route;

use super::errors::query_error;
use super::query::view_state;
use super::require;
use crate::app::{backend, caller};
use crate::backend::Backend;
use crate::components::{agent_node_name, family_name};
use crate::pages::common::transmissions::{ChannelNames, channel_names};
use crate::url::route::encode;
use crate::url::ulid::UlidId;
use crate::url::view_state::{GraphMode, format_time};

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TopologyPayload {
    pub mode: ModeCode,
    pub window: WindowPayload,
    pub weighting: WeightingCode,
    pub topic_version: u32,
    /// Every bucket before this is final.
    pub watermark: String,
    pub nodes: Vec<NodePayload>,
    pub edges: Vec<EdgePayload>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum ModeCode {
    Agents,
    Channels,
}

impl From<GraphMode> for ModeCode {
    fn from(mode: GraphMode) -> Self {
        match mode {
            GraphMode::Agents => Self::Agents,
            GraphMode::Channels => Self::Channels,
        }
    }
}

/// The view-state codes for weighting (`w=`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum WeightingCode {
    #[serde(rename = "tx")]
    Transmissions,
    #[serde(rename = "bytes")]
    MatchedBytes,
}

impl From<Weighting> for WeightingCode {
    fn from(weighting: Weighting) -> Self {
        match weighting {
            Weighting::Transmissions => Self::Transmissions,
            Weighting::MatchedBytes => Self::MatchedBytes,
        }
    }
}

/// A half-open window `[from, to)` in RFC 3339 UTC.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WindowPayload {
    pub from: String,
    pub to: String,
}

impl From<TimeWindow> for WindowPayload {
    fn from(window: TimeWindow) -> Self {
        Self {
            from: format_time(window.start()),
            to: format_time(window.end()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "camelCase")]
pub enum NodePayload {
    Agent(AgentNodePayload),
    Channel(ChannelNodePayload),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AgentNodePayload {
    pub id: String,
    /// The operator's label, else the id's tail
    /// (`components::agent_node_name`, as `components::agent_name`).
    pub name: String,
    pub state: AgentStateCode,
    /// The parent of a sub-agent. The contract includes canonical parents as
    /// nodes; elements still check before collapsing into it.
    pub parent: Option<String>,
    /// `transmissionsIn + transmissionsOut`.
    pub volume: u64,
    pub transmissions_in: u64,
    pub transmissions_out: u64,
    /// Harness claims: shown as claims, never as identity.
    pub claims: Vec<ClaimPayload>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum AgentStateCode {
    Registered,
    Provisional,
    Established,
}

impl From<CanonicalStateKind> for AgentStateCode {
    fn from(kind: CanonicalStateKind) -> Self {
        match kind {
            CanonicalStateKind::Registered => Self::Registered,
            CanonicalStateKind::Provisional => Self::Provisional,
            CanonicalStateKind::Established => Self::Established,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ClaimPayload {
    /// The claimed harness family's display name.
    pub harness: String,
    pub version: Option<String>,
    pub user_agent: String,
    pub last_seen: String,
}

impl From<&SeenClaim> for ClaimPayload {
    fn from(seen: &SeenClaim) -> Self {
        Self {
            harness: family_name(&seen.claim.family).to_owned(),
            version: seen.claim.version.clone(),
            user_agent: seen.claim.user_agent.clone(),
            last_seen: format_time(seen.last_seen),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ChannelNodePayload {
    pub id: String,
    /// The declared pattern or discovered seed ([`super::names`]).
    pub name: String,
    pub origin: OriginCode,
    pub detection: DetectionCode,
    pub policy: PolicyCode,
    /// Accesses in the window, reads and writes.
    pub volume: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum OriginCode {
    Declared,
    Discovered,
}

/// A promoted channel is declared: an operator gave it a pattern.
impl From<CanonicalOriginKind> for OriginCode {
    fn from(kind: CanonicalOriginKind) -> Self {
        match kind {
            CanonicalOriginKind::DeclaredBeforeTraffic | CanonicalOriginKind::Promoted => {
                Self::Declared
            }
            CanonicalOriginKind::Discovered => Self::Discovered,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum DetectionCode {
    AwaitingTraffic,
    Unused,
    Observed,
    Candidate,
    Active,
    Dormant,
}

impl From<DetectionKind> for DetectionCode {
    fn from(kind: DetectionKind) -> Self {
        match kind {
            DetectionKind::AwaitingTraffic => Self::AwaitingTraffic,
            DetectionKind::Unused => Self::Unused,
            DetectionKind::Observed => Self::Observed,
            DetectionKind::Candidate => Self::Candidate,
            DetectionKind::Active => Self::Active,
            DetectionKind::Dormant => Self::Dormant,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum PolicyCode {
    Unreviewed,
    Sanctioned,
    Unsanctioned,
}

impl From<PolicyKind> for PolicyCode {
    fn from(kind: PolicyKind) -> Self {
        match kind {
            PolicyKind::Unreviewed => Self::Unreviewed,
            PolicyKind::Sanctioned => Self::Sanctioned,
            PolicyKind::Unsanctioned => Self::Unsanctioned,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "camelCase")]
pub enum EdgePayload {
    Transmission(TransmissionEdgePayload),
    Access(AccessEdgePayload),
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TransmissionEdgePayload {
    pub from: String,
    pub to: String,
    /// The route's URL code (`ch.<ulid>`, `dl.p2c`, `dr.tool.<name>`, ...).
    pub route: String,
    pub route_kind: RouteKindCode,
    /// Share of the filtered total under the payload's weighting, in `[0, 1]`.
    pub share: f64,
    pub transmissions: u64,
    pub matched_bytes: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum RouteKindCode {
    Channel,
    Delegation,
    Direct,
    Unobserved,
}

impl From<RouteKind> for RouteKindCode {
    fn from(kind: RouteKind) -> Self {
        match kind {
            RouteKind::Channel => Self::Channel,
            RouteKind::Delegation => Self::Delegation,
            RouteKind::Direct => Self::Direct,
            RouteKind::Unobserved => Self::Unobserved,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AccessEdgePayload {
    pub agent: String,
    pub channel: String,
    pub op: AccessOpCode,
    pub accesses: u64,
    /// Share of all accesses after filtering, in `[0, 1]`.
    pub share: f64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum AccessOpCode {
    Read,
    Write,
}

impl From<AccessKind> for AccessOpCode {
    fn from(kind: AccessKind) -> Self {
        match kind {
            AccessKind::Read => Self::Read,
            AccessKind::Write => Self::Write,
        }
    }
}

fn agent_node(agent: &AgentNode) -> NodePayload {
    NodePayload::Agent(AgentNodePayload {
        id: agent.id.to_ulid(),
        name: agent_node_name(agent),
        state: agent.state_kind.into(),
        parent: agent.parent.map(UlidId::to_ulid),
        volume: agent
            .transmissions_in
            .saturating_add(agent.transmissions_out),
        transmissions_in: agent.transmissions_in,
        transmissions_out: agent.transmissions_out,
        claims: agent
            .claims
            .entries()
            .iter()
            .map(ClaimPayload::from)
            .collect(),
    })
}

fn channel_node(channel: &ChannelNode, accesses: &[WeightedAccess], name: String) -> NodePayload {
    let volume = accesses
        .iter()
        .filter(|a| a.channel == channel.id)
        .fold(0u64, |sum, a| sum.saturating_add(a.accesses.get()));
    NodePayload::Channel(ChannelNodePayload {
        id: channel.id.to_ulid(),
        name,
        origin: channel.origin_kind.into(),
        detection: channel.detection_kind.into(),
        policy: channel.policy_kind.into(),
        volume,
    })
}

fn transmission_edge(edge: &WeightedEdge) -> EdgePayload {
    EdgePayload::Transmission(TransmissionEdgePayload {
        from: edge.from.to_ulid(),
        to: edge.to.to_ulid(),
        route: encode(&edge.route),
        route_kind: RouteKind::from(&edge.route).into(),
        share: edge.share.get(),
        transmissions: edge.stats.transmissions.get(),
        matched_bytes: edge.stats.matched_bytes.get(),
    })
}

fn access_edge(access: &WeightedAccess) -> EdgePayload {
    EdgePayload::Access(AccessEdgePayload {
        agent: access.agent.to_ulid(),
        channel: access.channel.to_ulid(),
        op: access.op.into(),
        accesses: access.accesses.get(),
        share: access.share.get(),
    })
}

/// A graph's nodes, agents and channels in the graph's order. `name` names
/// a channel node.
fn nodes(
    nodes: &[GraphNode],
    accesses: &[WeightedAccess],
    name: impl Fn(&ChannelNode) -> String,
) -> Vec<NodePayload> {
    nodes
        .iter()
        .map(|node| match node {
            GraphNode::Agent(agent) => agent_node(agent),
            GraphNode::Channel(channel) => channel_node(channel, accesses, name(channel)),
        })
        .collect()
}

impl TopologyPayload {
    /// Agents mode: agent nodes and transmission edges.
    pub fn agents(graph: &Watermarked<TopologyGraph>) -> Self {
        let value = &graph.value;
        Self {
            mode: ModeCode::Agents,
            window: value.window.into(),
            weighting: value.weighting.into(),
            topic_version: value.topic_version.0,
            watermark: format_time(graph.watermark.at()),
            // A topology graph has agent nodes only.
            nodes: nodes(&value.nodes, &[], |c| c.locator_summary.as_str().to_owned()),
            edges: value.edges.iter().map(transmission_edge).collect(),
        }
    }

    /// Channels mode: agent and channel nodes (`names` names the channels),
    /// access edges, and the transmissions not routed through a channel.
    pub fn channels(graph: &Watermarked<BipartiteGraph>, names: &ChannelNames) -> Self {
        let value = &graph.value;
        let edges = value
            .accesses()
            .iter()
            .map(access_edge)
            .chain(
                value
                    .transmissions()
                    .iter()
                    .filter(|e| !matches!(e.route, Route::Channel(_)))
                    .map(transmission_edge),
            )
            .collect();
        Self {
            mode: ModeCode::Channels,
            window: value.window().into(),
            weighting: value.weighting().into(),
            topic_version: value.topic_version().0,
            watermark: format_time(graph.watermark.at()),
            nodes: nodes(value.nodes(), value.accesses(), |c| names.name(c.id)),
            edges,
        }
    }
}

#[route(GET "/data/topology")]
async fn topology_data(cx: &Cx) -> topcoat::Result<Json<TopologyPayload>> {
    let caller = caller(cx);
    require(&caller, Permission::View)?;
    let state = view_state(cx).await?;
    let backend = backend(cx);
    let (window, filter) = (state.scope.window, state.scope.topology_filter());
    let payload = match state.graph {
        GraphMode::Agents => {
            let graph = backend
                .topology(&caller, window, state.weighting, &filter)
                .await
                .map_err(query_error)?;
            TopologyPayload::agents(&graph)
        }
        GraphMode::Channels => {
            let graph = backend
                .channel_topology(&caller, window, state.weighting, &filter)
                .await
                .map_err(query_error)?;
            let channels = graph.value.nodes().iter().filter_map(|node| match node {
                GraphNode::Channel(channel) => Some(channel.id),
                GraphNode::Agent(_) => None,
            });
            let names = channel_names(cx, &caller, channels).await;
            TopologyPayload::channels(&graph, &names)
        }
    };
    tracing::debug!(
        mode = ?payload.mode,
        nodes = payload.nodes.len(),
        edges = payload.edges.len(),
        "topology payload"
    );
    Ok(Json(payload))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::fixtures;

    #[test]
    fn agents_mode_maps_nodes_and_edges() {
        let graph = fixtures::topology_graph();
        let payload = TopologyPayload::agents(&graph);
        assert_eq!(payload.mode, ModeCode::Agents);
        assert_eq!(payload.nodes.len(), graph.value.nodes.len());
        assert_eq!(payload.edges.len(), graph.value.edges.len());

        let NodePayload::Agent(planner) = &payload.nodes[0] else {
            panic!("agent node expected");
        };
        assert_eq!(planner.name, "planner");
        assert_eq!(
            planner.volume,
            planner.transmissions_in + planner.transmissions_out
        );
        assert_eq!(planner.claims[0].harness, "Claude Code");

        let unlabelled = payload
            .nodes
            .iter()
            .find_map(|n| match n {
                NodePayload::Agent(a) if a.name.starts_with('…') => Some(a),
                _ => None,
            })
            .expect("an unlabelled agent shows its id tail");
        assert_eq!(unlabelled.name.chars().count(), 7);

        let share_sum: f64 = payload
            .edges
            .iter()
            .map(|e| match e {
                EdgePayload::Transmission(t) => t.share,
                EdgePayload::Access(a) => a.share,
            })
            .sum();
        assert!((share_sum - 1.0).abs() < 1e-9, "shares sum to {share_sum}");
        assert_eq!(payload.watermark, format_time(graph.watermark.at()));
    }

    #[test]
    fn transmission_edges_carry_route_codes() {
        let graph = fixtures::topology_graph();
        let payload = TopologyPayload::agents(&graph);
        for (edge, source) in payload.edges.iter().zip(&graph.value.edges) {
            let EdgePayload::Transmission(edge) = edge else {
                panic!("agents mode has transmission edges only");
            };
            assert_eq!(edge.route, encode(&source.route));
            assert_eq!(
                crate::url::route::decode(&edge.route).as_ref(),
                Ok(&source.route)
            );
            assert_eq!(edge.from, source.from.to_ulid());
            assert_eq!(edge.transmissions, source.stats.transmissions.get());
        }
    }

    #[test]
    fn channels_mode_adds_channel_nodes_and_access_edges() {
        let graph = fixtures::bipartite_graph();
        let names = fixtures::channel_names();
        let payload = TopologyPayload::channels(&graph, &names);
        assert_eq!(payload.mode, ModeCode::Channels);
        let channels: Vec<&ChannelNodePayload> = payload
            .nodes
            .iter()
            .filter_map(|n| match n {
                NodePayload::Channel(c) => Some(c),
                NodePayload::Agent(_) => None,
            })
            .collect();
        let channel_nodes = graph
            .value
            .nodes()
            .iter()
            .filter(|n| matches!(n, GraphNode::Channel(_)))
            .count();
        assert_eq!(channels.len(), channel_nodes);
        for channel in &channels {
            let expected: u64 = graph
                .value
                .accesses()
                .iter()
                .filter(|a| a.channel.to_ulid() == channel.id)
                .map(|a| a.accesses.get())
                .sum();
            assert_eq!(channel.volume, expected);
            assert!(!channel.name.starts_with("channel "), "named by lookup");
        }
        let accesses = payload
            .edges
            .iter()
            .filter(|e| matches!(e, EdgePayload::Access(_)))
            .count();
        assert_eq!(accesses, graph.value.accesses().len());
        // Channel-routed transmissions are drawn as accesses; the others
        // keep their share of the whole filtered total.
        let direct: Vec<&WeightedEdge> = graph
            .value
            .transmissions()
            .iter()
            .filter(|e| !matches!(e.route, Route::Channel(_)))
            .collect();
        let drawn: Vec<&TransmissionEdgePayload> = payload
            .edges
            .iter()
            .filter_map(|e| match e {
                EdgePayload::Transmission(t) => Some(t),
                EdgePayload::Access(_) => None,
            })
            .collect();
        assert_eq!(drawn.len(), direct.len());
        assert!(drawn.iter().all(|t| t.route_kind != RouteKindCode::Channel));
        for (payload, edge) in drawn.iter().zip(direct) {
            assert!((payload.share - edge.share.get()).abs() < f64::EPSILON);
        }
    }

    #[test]
    fn promoted_and_declared_channels_are_declared() {
        assert_eq!(
            OriginCode::from(CanonicalOriginKind::Promoted),
            OriginCode::Declared
        );
        assert_eq!(
            OriginCode::from(CanonicalOriginKind::DeclaredBeforeTraffic),
            OriginCode::Declared
        );
        assert_eq!(
            OriginCode::from(CanonicalOriginKind::Discovered),
            OriginCode::Discovered
        );
    }

    #[test]
    fn serializes_with_kind_tags_and_camel_case() {
        let payload =
            TopologyPayload::channels(&fixtures::bipartite_graph(), &fixtures::channel_names());
        let json = serde_json::to_value(&payload).expect("serialize");
        assert_eq!(json["mode"], "channels");
        assert_eq!(json["weighting"], "tx");
        let kinds: Vec<&str> = json["nodes"]
            .as_array()
            .expect("nodes")
            .iter()
            .filter_map(|n| n["kind"].as_str())
            .collect();
        assert!(kinds.contains(&"agent") && kinds.contains(&"channel"));
        let access = json["edges"]
            .as_array()
            .expect("edges")
            .iter()
            .find(|e| e["kind"] == "access")
            .expect("access edge");
        assert!(access["op"] == "read" || access["op"] == "write");
        assert!(json["nodes"][0].get("transmissionsIn").is_some());
    }
}
