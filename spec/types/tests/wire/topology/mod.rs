//! Topology on the wire: the shared `TopologyFilter` and the other requests
//! of the linked views, the graphs (`topology`, `channel_topology`) with
//! their nodes, the drill-down behind an edge, the series, a channel's
//! resource use and detection quality. Every golden lives in
//! `golden/topology/`.
//!
//! Fixtures tell one story: the planner (`a`) writes the team wiki
//! (`wiki`), the coder (`b`, spawned by the orchestrator `d`) reads it,
//! and the coder briefs the reviewer (`c`) in a user turn.

mod access;
mod filter;
mod graph;
mod quality;
mod series;

use std::num::NonZeroU64;

use serde::Serialize;
use serde_json::Value;

use super::{ULID_A, ULID_B, ULID_C, id, ts};
use crate::aggregates::edge::{EdgeStats, WeightedEdge};
use crate::aggregates::node::{
    AgentNode, CanonicalOriginKind, CanonicalStateKind, ChannelNode, GraphNode,
};
use crate::aggregates::topic::TopicModelVersion;
use crate::derived::flow::channel::detection::DetectionKind;
use crate::derived::flow::channel::policy::PolicyKind;
use crate::derived::flow::transmission::{DirectCarrier, Route};
use crate::ids::{AgentId, ChannelId, TopicId};
use crate::observed::agent::{AgentLabel, ClaimSet};
use crate::observed::client::{HarnessClaim, HarnessFamily};
use crate::support::{NonBlank, Share, TimeWindow};

const AREA: &str = "topology";

/// Three more ULIDs, after `ULID_C` in order.
const ULID_D: &str = "01J9Z3P5Q6R7S8T9V0W1X2Y3Z4";
const ULID_E: &str = "01J9Z3Q6R7S8T9V0W1X2Y3Z4A5";
const ULID_F: &str = "01J9Z3R7S8T9V0W1X2Y3Z4A5B6";

fn a() -> AgentId {
    id(AgentId::from_ulid_text, ULID_A)
}

fn b() -> AgentId {
    id(AgentId::from_ulid_text, ULID_B)
}

fn c() -> AgentId {
    id(AgentId::from_ulid_text, ULID_C)
}

fn d() -> AgentId {
    id(AgentId::from_ulid_text, ULID_D)
}

fn wiki() -> ChannelId {
    id(ChannelId::from_ulid_text, ULID_E)
}

fn topic() -> TopicId {
    id(TopicId::from_ulid_text, ULID_F)
}

fn version() -> TopicModelVersion {
    TopicModelVersion(3)
}

/// 12:00 to 13:00, aligned to minute buckets.
fn hour() -> TimeWindow {
    TimeWindow::new(
        ts("2026-10-04T12:00:00.000000Z"),
        ts("2026-10-04T13:00:00.000000Z"),
    )
    .expect("the hour is not empty")
}

fn n(value: u64) -> NonZeroU64 {
    NonZeroU64::new(value).unwrap_or_else(|| panic!("{value} is a fixture count, never zero"))
}

fn share(value: f64) -> Share {
    Share::new(value).unwrap_or_else(|| panic!("{value} is a share"))
}

fn stats(transmissions: u64, matched_bytes: u64) -> EdgeStats {
    EdgeStats {
        transmissions: n(transmissions),
        matched_bytes: n(matched_bytes),
    }
}

fn edge(from: AgentId, to: AgentId, route: Route, stats: EdgeStats, value: f64) -> WeightedEdge {
    WeightedEdge {
        from,
        to,
        route,
        stats,
        share: share(value),
    }
}

/// The planner wrote 3 transmissions' worth of text to the wiki that the
/// coder read (1200 bytes); the coder briefed the reviewer once (300
/// bytes). Weighted by transmissions, 3/4 and 1/4.
fn edges() -> Vec<WeightedEdge> {
    vec![
        edge(a(), b(), Route::Channel(wiki()), stats(3, 1200), 0.75),
        edge(
            b(),
            c(),
            Route::Direct(DirectCarrier::UserTurn),
            stats(1, 300),
            0.25,
        ),
    ]
}

fn claims() -> ClaimSet {
    let mut claims = ClaimSet::default();
    claims.observe(
        HarnessClaim {
            family: HarnessFamily::ClaudeCode,
            version: Some("2.1.4".into()),
            user_agent: "claude-cli/2.1.4 (external, cli)".into(),
        },
        ts("2026-10-04T12:41:07.120000Z"),
    );
    claims
}

fn agent_node(
    id: AgentId,
    label: Option<&str>,
    state_kind: CanonicalStateKind,
    parent: Option<AgentId>,
    counts: (u64, u64),
) -> AgentNode {
    AgentNode {
        id,
        label: label.map(|text| AgentLabel::new(text).expect("a valid label")),
        state_kind,
        parent,
        claims: ClaimSet::default(),
        transmissions_in: counts.0,
        transmissions_out: counts.1,
    }
}

/// One node per endpoint of [`edges`] and the coder's parent, counts
/// agreeing with the edges.
fn agent_nodes() -> Vec<GraphNode> {
    vec![
        GraphNode::Agent(AgentNode {
            claims: claims(),
            ..agent_node(
                a(),
                Some("planner"),
                CanonicalStateKind::Established,
                None,
                (0, 3),
            )
        }),
        GraphNode::Agent(agent_node(
            b(),
            Some("coder"),
            CanonicalStateKind::Provisional,
            Some(d()),
            (3, 1),
        )),
        GraphNode::Agent(agent_node(
            c(),
            None,
            CanonicalStateKind::Provisional,
            None,
            (1, 0),
        )),
        GraphNode::Agent(agent_node(
            d(),
            Some("orchestrator"),
            CanonicalStateKind::Registered,
            None,
            (0, 0),
        )),
    ]
}

fn wiki_node() -> GraphNode {
    GraphNode::Channel(ChannelNode {
        id: wiki(),
        label: None,
        origin_kind: CanonicalOriginKind::Discovered,
        detection_kind: DetectionKind::Active,
        policy_kind: PolicyKind::Unreviewed,
        locator_summary: NonBlank::new("https://wiki.example/team/plan (+2)").expect("not blank"),
    })
}

/// `value`'s JSON after `edit`, as text: how a rejection test turns a valid
/// golden value into the one invalid input it is about.
fn edited<T: Serialize>(value: &T, edit: impl FnOnce(&mut Value)) -> String {
    let mut json =
        serde_json::to_value(value).unwrap_or_else(|error| panic!("a fixture encodes: {error}"));
    edit(&mut json);
    json.to_string()
}

/// The JSON value at `pointer` (RFC 6901), to edit in place.
fn at<'v>(json: &'v mut Value, pointer: &str) -> &'v mut Value {
    json.pointer_mut(pointer)
        .unwrap_or_else(|| panic!("no {pointer} in the fixture"))
}

/// The array at `pointer`.
fn array<'v>(json: &'v mut Value, pointer: &str) -> &'v mut Vec<Value> {
    at(json, pointer)
        .as_array_mut()
        .unwrap_or_else(|| panic!("{pointer} is not an array"))
}

/// The object at `pointer`, to add a field to.
fn object<'v>(json: &'v mut Value, pointer: &str) -> &'v mut serde_json::Map<String, Value> {
    at(json, pointer)
        .as_object_mut()
        .unwrap_or_else(|| panic!("{pointer} is not an object"))
}
