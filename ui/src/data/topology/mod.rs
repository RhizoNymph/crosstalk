//! The `<ct-topology>` payload and its route.
//!
//! `GET /data/topology?<view state>` answers with a [`TopologyPayload`] as
//! JSON: the agents-mode graph (`g=agents`, `QueryApi::topology`) or the
//! bipartite graph (`g=channels`, `QueryApi::channel_topology`). Needs
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
//! "detection", "confirmation", "policy", "volume" }` nodes (only channels
//! with cross-agent traffic; `confirmation` is `unconfirmed` when all of it
//! is suspected, and those nodes are absent under `u=confirmed`) and
//! `{ "kind": "access",
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

use crate::pending::channel_semantics::{ChannelGraph, Confirmation};
use crosstalk_spec::aggregates::access::WeightedAccess;
use crosstalk_spec::aggregates::edge::{RouteKind, TopologyGraph, WeightedEdge, Weighting};
use crosstalk_spec::aggregates::node::{
    AgentNode, CanonicalOriginKind, CanonicalStateKind, ChannelNode, GraphNode,
};
use crosstalk_spec::aggregates::watermark::Watermarked;
use crosstalk_spec::derived::flow::access::AccessKind;
use crosstalk_spec::derived::flow::channel::detection::DetectionKind;
use crosstalk_spec::derived::flow::transmission::Route;
use crosstalk_spec::ids::ChannelId;
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
use crate::components::{agent_node_name, family_name};
use crate::pages::common::transmissions::{ChannelNames, channel_names};
use crate::url::route::encode;
use crate::url::ulid::UlidId;
use crate::url::view_state::{GraphMode, format_time};
use crosstalk_spec::interfaces::l8_surface::QueryApi;

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
    pub confirmation: ConfirmationCode,
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
    Active,
    Dormant,
}

/// Whether a channel node's cross-agent traffic is confirmed; an
/// unconfirmed node is drawn marked.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum ConfirmationCode {
    Confirmed,
    Unconfirmed,
}

impl From<Confirmation> for ConfirmationCode {
    fn from(confirmation: Confirmation) -> Self {
        match confirmation {
            Confirmation::Confirmed => Self::Confirmed,
            Confirmation::Unconfirmed => Self::Unconfirmed,
        }
    }
}

impl From<DetectionKind> for DetectionCode {
    fn from(kind: DetectionKind) -> Self {
        match kind {
            DetectionKind::AwaitingTraffic => Self::AwaitingTraffic,
            // No cross-agent transmission yet. The spec still has these
            // two states; the channel-semantics port removes them, no
            // channel node is built in either (only channels with
            // cross-agent traffic are drawn), and the element payload
            // keeps its four codes.
            DetectionKind::Observed | DetectionKind::Candidate => Self::AwaitingTraffic,
            DetectionKind::Unused => Self::Unused,
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

fn channel_node(
    channel: &ChannelNode,
    confirmation: Confirmation,
    accesses: &[WeightedAccess],
    name: String,
) -> NodePayload {
    let volume = accesses
        .iter()
        .filter(|a| a.channel == channel.id)
        .fold(0u64, |sum, a| sum.saturating_add(a.accesses.get()));
    NodePayload::Channel(ChannelNodePayload {
        id: channel.id.to_ulid(),
        name,
        origin: channel.origin_kind.into(),
        detection: channel.detection_kind.into(),
        confirmation: confirmation.into(),
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
/// a channel node and `confirmation` gives its confirmation (every channel
/// node of a [`ChannelGraph`] has one; the stand-in for
/// `ChannelNode::confirmation`).
fn nodes(
    nodes: &[GraphNode],
    accesses: &[WeightedAccess],
    name: impl Fn(&ChannelNode) -> String,
    confirmation: impl Fn(ChannelId) -> Option<Confirmation>,
) -> Vec<NodePayload> {
    nodes
        .iter()
        .map(|node| match node {
            GraphNode::Agent(agent) => agent_node(agent),
            GraphNode::Channel(channel) => channel_node(
                channel,
                confirmation(channel.id).unwrap_or(Confirmation::Confirmed),
                accesses,
                name(channel),
            ),
        })
        .collect()
}

impl TopologyPayload {
    /// Agents mode: agent nodes and transmission edges.
    pub fn agents(graph: &Watermarked<TopologyGraph>) -> Self {
        let value = &graph.value;
        Self {
            mode: ModeCode::Agents,
            window: value.window().into(),
            weighting: value.weighting().into(),
            topic_version: value.topic_version().0,
            watermark: format_time(graph.watermark.at()),
            // A topology graph has agent nodes only.
            nodes: nodes(
                value.nodes(),
                &[],
                |c| c.locator_summary.as_str().to_owned(),
                |_| None,
            ),
            edges: value.edges().iter().map(transmission_edge).collect(),
        }
    }

    /// Channels mode: agent and channel nodes (`names` names the channels),
    /// access edges, and the transmissions not routed through a channel.
    pub fn channels(graph: &Watermarked<ChannelGraph>, names: &ChannelNames) -> Self {
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
            nodes: nodes(
                value.nodes(),
                value.accesses(),
                |c| names.name(c.id),
                |id| value.confirmation(id),
            ),
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
mod tests;
