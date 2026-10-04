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
//! "agent", "channel", "op": "read" | "write", "accesses", "share" }` edges;
//! its transmission edges are the ones not routed through a channel. Access
//! shares are normalised separately from transmission shares. Route codes
//! are those of [`crate::url::route`].

use crosstalk_spec::aggregates::edge::{RouteKind, WeightedEdge, Weighting};
use crosstalk_spec::derived::flow::access::AccessKind;
use crosstalk_spec::interfaces::l8_surface::{Permission, PolicyKind};
use crosstalk_spec::support::TimeWindow;
use serde::{Deserialize, Serialize};
use topcoat::context::Cx;
use topcoat::router::content::Json;
use topcoat::router::route;

use super::errors::query_error;
use super::names::channel_node_name;
use super::query::view_state;
use super::require;
use crate::app::{backend, caller};
use crate::backend::Backend;
use crate::components::{agent_name, family_name};
use crate::contract::agents::{AgentStateKind, AgentSummary, ClaimSeen};
use crate::contract::channels::{DetectionKind, OriginKind};
use crate::contract::graph::{AccessEdge, BipartiteView, ChannelNode, TopologyView};
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
    /// The operator's label, else the id's tail (`components::agent_name`).
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

impl From<AgentStateKind> for AgentStateCode {
    fn from(kind: AgentStateKind) -> Self {
        match kind {
            AgentStateKind::Registered => Self::Registered,
            AgentStateKind::Provisional => Self::Provisional,
            AgentStateKind::Established => Self::Established,
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

impl From<&ClaimSeen> for ClaimPayload {
    fn from(seen: &ClaimSeen) -> Self {
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

impl From<OriginKind> for OriginCode {
    fn from(kind: OriginKind) -> Self {
        match kind {
            OriginKind::Declared => Self::Declared,
            OriginKind::Discovered => Self::Discovered,
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

fn agent_node(agent: &AgentSummary) -> NodePayload {
    NodePayload::Agent(AgentNodePayload {
        id: agent.id.to_ulid(),
        name: agent_name(agent),
        state: agent.state.into(),
        parent: agent.parent.map(UlidId::to_ulid),
        volume: agent
            .transmissions_in
            .saturating_add(agent.transmissions_out),
        transmissions_in: agent.transmissions_in,
        transmissions_out: agent.transmissions_out,
        claims: agent.claims.iter().map(ClaimPayload::from).collect(),
    })
}

fn channel_node(channel: &ChannelNode, accesses: &[AccessEdge]) -> NodePayload {
    let volume = accesses
        .iter()
        .filter(|a| a.channel == channel.id)
        .fold(0u64, |sum, a| sum.saturating_add(a.accesses.get()));
    NodePayload::Channel(ChannelNodePayload {
        id: channel.id.to_ulid(),
        name: channel_node_name(channel),
        origin: channel.origin.into(),
        detection: channel.detection.into(),
        policy: channel.policy.into(),
        volume,
    })
}

fn transmission_edge(edge: &WeightedEdge) -> EdgePayload {
    EdgePayload::Transmission(TransmissionEdgePayload {
        from: edge.from.to_ulid(),
        to: edge.to.to_ulid(),
        route: encode(&edge.route),
        route_kind: crate::contract::graph::route_kind(&edge.route).into(),
        share: edge.share.get(),
        transmissions: edge.stats.transmissions.get(),
        matched_bytes: edge.stats.matched_bytes.get(),
    })
}

fn access_edge(access: &AccessEdge) -> EdgePayload {
    EdgePayload::Access(AccessEdgePayload {
        agent: access.agent.to_ulid(),
        channel: access.channel.to_ulid(),
        op: access.op.into(),
        accesses: access.accesses.get(),
        share: access.share.get(),
    })
}

impl TopologyPayload {
    /// Agents mode: agent nodes and transmission edges.
    pub fn agents(view: &TopologyView) -> Self {
        let graph = view.graph();
        Self {
            mode: ModeCode::Agents,
            window: graph.window.into(),
            weighting: graph.weighting.into(),
            topic_version: graph.topic_version.0,
            watermark: format_time(view.watermark()),
            nodes: view.nodes().iter().map(agent_node).collect(),
            edges: graph.edges.iter().map(transmission_edge).collect(),
        }
    }

    /// Channels mode: agent and channel nodes, access edges, and the
    /// transmissions not routed through a channel.
    pub fn channels(view: &BipartiteView) -> Self {
        let nodes = view
            .agents()
            .iter()
            .map(agent_node)
            .chain(
                view.channels()
                    .iter()
                    .map(|c| channel_node(c, view.accesses())),
            )
            .collect();
        let edges = view
            .accesses()
            .iter()
            .map(access_edge)
            .chain(view.transmissions().iter().map(transmission_edge))
            .collect();
        Self {
            mode: ModeCode::Channels,
            window: view.window().into(),
            weighting: view.weighting().into(),
            topic_version: view.topic_version().0,
            watermark: format_time(view.watermark()),
            nodes,
            edges,
        }
    }
}

#[route(GET "/data/topology")]
async fn topology_data(cx: &Cx) -> topcoat::Result<Json<TopologyPayload>> {
    let caller = caller(cx);
    require(&caller, Permission::View)?;
    let state = view_state(cx)?;
    let backend = backend(cx);
    let payload = match state.graph {
        GraphMode::Agents => {
            let view = backend
                .topology(&caller, &state.scope, state.weighting)
                .await
                .map_err(query_error)?;
            TopologyPayload::agents(&view)
        }
        GraphMode::Channels => {
            let view = backend
                .channel_topology(&caller, &state.scope, state.weighting)
                .await
                .map_err(query_error)?;
            TopologyPayload::channels(&view)
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
        let view = fixtures::topology_view();
        let payload = TopologyPayload::agents(&view);
        assert_eq!(payload.mode, ModeCode::Agents);
        assert_eq!(payload.nodes.len(), view.nodes().len());
        assert_eq!(payload.edges.len(), view.graph().edges.len());

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
    }

    #[test]
    fn transmission_edges_carry_route_codes() {
        let view = fixtures::topology_view();
        let payload = TopologyPayload::agents(&view);
        for (edge, source) in payload.edges.iter().zip(&view.graph().edges) {
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
        let view = fixtures::bipartite_view();
        let payload = TopologyPayload::channels(&view);
        assert_eq!(payload.mode, ModeCode::Channels);
        let channels: Vec<&ChannelNodePayload> = payload
            .nodes
            .iter()
            .filter_map(|n| match n {
                NodePayload::Channel(c) => Some(c),
                NodePayload::Agent(_) => None,
            })
            .collect();
        assert_eq!(channels.len(), view.channels().len());
        for channel in &channels {
            let expected: u64 = view
                .accesses()
                .iter()
                .filter(|a| a.channel.to_ulid() == channel.id)
                .map(|a| a.accesses.get())
                .sum();
            assert_eq!(channel.volume, expected);
        }
        let accesses = payload
            .edges
            .iter()
            .filter(|e| matches!(e, EdgePayload::Access(_)))
            .count();
        assert_eq!(accesses, view.accesses().len());
        assert!(payload.edges.iter().all(|e| match e {
            EdgePayload::Transmission(t) => t.route_kind != RouteKindCode::Channel,
            EdgePayload::Access(_) => true,
        }));
    }

    #[test]
    fn serializes_with_kind_tags_and_camel_case() {
        let payload = TopologyPayload::channels(&fixtures::bipartite_view());
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
