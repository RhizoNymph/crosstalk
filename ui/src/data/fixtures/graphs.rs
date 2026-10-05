//! The hand-built graphs and series: twelve agents with sub-agents and
//! claims, eighteen edges over every route kind, four channels with the
//! accesses behind their routed edges, and a day of 15-minute points.

use std::collections::BTreeMap;

use crosstalk_spec::aggregates::access::BipartiteGraph as ChannelGraph;

use crosstalk_spec::aggregates::access::{BipartiteGraph, BipartiteParts, WeightedAccess};
use crosstalk_spec::aggregates::edge::{
    EdgeStats, TopologyGraph, TopologyGraphParts, WeightedEdge, Weighting,
};
use crosstalk_spec::aggregates::node::{
    AgentNode, CanonicalOriginKind, CanonicalStateKind, ChannelNode, GraphNode,
};
use crosstalk_spec::aggregates::series::{
    BucketWidth, SeriesGrid, SeriesGroups, SeriesStep, TopologySeries,
};
use crosstalk_spec::aggregates::topic::TopicModelVersion;
use crosstalk_spec::aggregates::watermark::Watermarked;
use crosstalk_spec::derived::flow::access::AccessKind;
use crosstalk_spec::derived::flow::channel::confirmation::Confirmation;
use crosstalk_spec::derived::flow::channel::detection::DetectionKind;
use crosstalk_spec::derived::flow::resource::{Host, Locator, ResourcePattern};
use crosstalk_spec::derived::flow::transmission::{DelegationDirection, DirectCarrier, Route};
use crosstalk_spec::ids::{AgentId, ChannelId};
use crosstalk_spec::interfaces::l8_surface::PolicyKind;
use crosstalk_spec::observed::agent::{AgentLabel, ClaimSet};
use crosstalk_spec::observed::client::{HarnessClaim, HarnessFamily};
use crosstalk_spec::observed::message::ToolName;
use crosstalk_spec::support::{NonBlank, Share, TimeWindow};

use super::{HOUR_MICROS, Rng, agent_id, channel_id, non_zero, ts, watermarked, window};
use crate::data::names::shape_name;
use crate::pages::common::transmissions::ChannelNames;
use crosstalk_spec::interfaces::l8_surface::channels::ChannelShape;

struct AgentSpec {
    n: u128,
    label: Option<&'static str>,
    state: CanonicalStateKind,
    parent: Option<u128>,
    claim: Option<(HarnessFamily, Option<&'static str>, &'static str)>,
}

fn agent_specs() -> Vec<AgentSpec> {
    use CanonicalStateKind::{Established, Provisional, Registered};
    use HarnessFamily::{ClaudeCode, Codex, OhMyPi, Pi, Unknown};
    let spec = |n, label, state, parent, claim| AgentSpec {
        n,
        label,
        state,
        parent,
        claim,
    };
    vec![
        spec(
            1,
            Some("planner"),
            Established,
            None,
            Some((
                ClaudeCode,
                Some("2.1.3"),
                "claude-cli/2.1.3 (external, cli)",
            )),
        ),
        spec(
            2,
            Some("researcher"),
            Established,
            Some(1),
            Some((
                ClaudeCode,
                Some("2.1.3"),
                "claude-cli/2.1.3 (external, cli)",
            )),
        ),
        spec(
            3,
            None,
            Provisional,
            Some(1),
            Some((
                ClaudeCode,
                Some("2.1.3"),
                "claude-cli/2.1.3 (external, cli)",
            )),
        ),
        spec(
            4,
            Some("reviewer"),
            Established,
            None,
            Some((Codex, Some("0.48.0"), "codex_cli_rs/0.48.0")),
        ),
        spec(
            5,
            Some("ci-bot"),
            Established,
            None,
            Some((Pi, None, "claude-cli/2.0.14 (external, sdk-ts)")),
        ),
        spec(
            6,
            Some("docs-writer"),
            Established,
            None,
            Some((
                ClaudeCode,
                Some("2.0.77"),
                "claude-cli/2.0.77 (external, cli)",
            )),
        ),
        spec(
            7,
            None,
            Provisional,
            None,
            Some((Unknown, None, "python-httpx/0.28.1")),
        ),
        spec(
            8,
            Some("triage"),
            Established,
            None,
            Some((OhMyPi, Some("1.9.2"), "oh-my-pi/1.9.2")),
        ),
        spec(
            9,
            None,
            Provisional,
            Some(4),
            Some((Codex, Some("0.48.0"), "codex_cli_rs/0.48.0")),
        ),
        spec(10, Some("deploy"), Registered, None, None),
        spec(
            11,
            Some("scraper"),
            Established,
            None,
            Some((Codex, Some("0.47.1"), "codex_cli_rs/0.47.1")),
        ),
        spec(
            12,
            Some("summarizer"),
            Established,
            Some(6),
            Some((
                ClaudeCode,
                Some("2.0.77"),
                "claude-cli/2.0.77 (external, cli)",
            )),
        ),
    ]
}

fn route(code: &str) -> Route {
    match code {
        "p2c" => Route::Delegation(DelegationDirection::ParentToChild),
        "c2p" => Route::Delegation(DelegationDirection::ChildToParent),
        "user" => Route::Direct(DirectCarrier::UserTurn),
        "sys" => Route::Direct(DirectCarrier::SystemPrompt),
        "gh" => Route::Direct(DirectCarrier::ToolResult(ToolName(
            "mcp__github__get_pull_request".to_owned(),
        ))),
        "un" => Route::Unobserved,
        other => {
            let n = other
                .strip_prefix('c')
                .and_then(|n| n.parse::<u128>().ok())
                .expect("channel route code like c1");
            Route::Channel(channel_id(n))
        }
    }
}

/// `(from, to, route, transmissions)`.
const EDGES: [(u128, u128, &str, u64); 18] = [
    (1, 2, "p2c", 20),
    (2, 1, "c2p", 18),
    (1, 3, "p2c", 9),
    (3, 1, "c2p", 7),
    (1, 4, "c1", 25),
    (4, 1, "gh", 6),
    (4, 9, "p2c", 5),
    (5, 1, "c2", 12),
    (6, 12, "p2c", 8),
    (12, 6, "c2p", 8),
    (6, 4, "c1", 10),
    (7, 5, "un", 4),
    (8, 6, "user", 3),
    (11, 6, "c3", 15),
    (11, 8, "c4", 6),
    (10, 5, "sys", 2),
    (2, 11, "un", 3),
    (4, 10, "c2", 5),
];

fn edges() -> Vec<WeightedEdge> {
    let total: u64 = EDGES.iter().map(|e| e.3).sum();
    EDGES
        .iter()
        .map(|&(from, to, code, tx)| WeightedEdge {
            from: agent_id(from),
            to: agent_id(to),
            route: route(code),
            stats: EdgeStats {
                transmissions: non_zero(tx),
                matched_bytes: non_zero(tx * 731 + u64::try_from(from * 37).expect("small")),
            },
            share: Share::new(tx as f64 / total as f64).expect("share in [0, 1]"),
        })
        .collect()
}

pub(super) fn agents() -> Vec<AgentNode> {
    let edges = edges();
    agent_specs()
        .into_iter()
        .map(|spec| {
            let id = agent_id(spec.n);
            let sum = |pick: fn(&WeightedEdge) -> AgentId| -> u64 {
                edges
                    .iter()
                    .filter(|e| pick(e) == id)
                    .map(|e| e.stats.transmissions.get())
                    .sum()
            };
            let mut claims = ClaimSet::default();
            if let Some((family, version, user_agent)) = spec.claim {
                claims.observe(
                    HarnessClaim {
                        family,
                        version: version.map(str::to_owned),
                        user_agent: user_agent.to_owned(),
                    },
                    ts(22 * HOUR_MICROS + u64::try_from(spec.n).expect("small") * 60_000_000),
                );
            }
            AgentNode {
                id,
                label: spec
                    .label
                    .map(|l| AgentLabel::new(l).expect("fixture labels are valid")),
                state_kind: spec.state,
                parent: spec.parent.map(agent_id),
                claims,
                transmissions_in: sum(|e| e.to),
                transmissions_out: sum(|e| e.from),
            }
        })
        .collect()
}

fn agent_nodes() -> Vec<GraphNode> {
    agents().into_iter().map(GraphNode::Agent).collect()
}

pub fn topology_graph() -> Watermarked<TopologyGraph> {
    let graph = TopologyGraph::new(TopologyGraphParts {
        window: window(),
        weighting: Weighting::Transmissions,
        topic_version: TopicModelVersion(3),
        nodes: agent_nodes(),
        edges: edges(),
    })
    .expect("one node per endpoint");
    watermarked(graph)
}

pub fn empty_topology_graph() -> Watermarked<TopologyGraph> {
    let graph = TopologyGraph::new(TopologyGraphParts {
        window: window(),
        weighting: Weighting::Transmissions,
        topic_version: TopicModelVersion(3),
        nodes: Vec::new(),
        edges: Vec::new(),
    })
    .expect("an empty graph");
    watermarked(graph)
}

/// The fixture's channels: id number, origin, detection, confirmation,
/// policy and what names them. Each carries cross-agent traffic, so each is
/// a channels-mode node; the pastebin's is all suspected.
pub(super) fn channel_specs() -> Vec<(
    u128,
    CanonicalOriginKind,
    DetectionKind,
    Confirmation,
    PolicyKind,
    ChannelShape,
)> {
    let host = |h: &str| Host(h.to_owned());
    vec![
        (
            1,
            CanonicalOriginKind::DeclaredBeforeTraffic,
            DetectionKind::Active,
            Confirmation::Confirmed,
            PolicyKind::Sanctioned,
            ChannelShape::Pattern(ResourcePattern::PathPrefix {
                host: None,
                prefix: "/srv/shared/handoff".to_owned(),
            }),
        ),
        (
            2,
            CanonicalOriginKind::Discovered,
            DetectionKind::Active,
            Confirmation::Unconfirmed,
            PolicyKind::Unreviewed,
            ChannelShape::Seed(Locator::Url {
                scheme: "https".to_owned(),
                host: host("pastebin.com"),
                path: "/raw/x9Qe2LmP".to_owned(),
                query: None,
            }),
        ),
        (
            3,
            CanonicalOriginKind::Discovered,
            DetectionKind::Active,
            Confirmation::Confirmed,
            PolicyKind::Unsanctioned,
            ChannelShape::Seed(Locator::Mcp {
                server: "linear".to_owned(),
                tool: ToolName("get_issue".to_owned()),
                target: Some("ENG-4411".to_owned()),
            }),
        ),
        (
            4,
            CanonicalOriginKind::Promoted,
            DetectionKind::Dormant,
            Confirmation::Confirmed,
            PolicyKind::Unreviewed,
            ChannelShape::Pattern(ResourcePattern::UrlPrefix {
                host: host("github.com"),
                path_prefix: "/acme/ops-notes".to_owned(),
            }),
        ),
    ]
}

/// The channels' display names, as one `channel_names` call gives them.
pub fn channel_names() -> ChannelNames {
    ChannelNames::from_pairs(
        channel_specs()
            .into_iter()
            .map(|(n, _, _, _, _, shape)| (channel_id(n), shape_name(&shape))),
    )
}

fn channel_nodes() -> Vec<GraphNode> {
    channel_specs()
        .into_iter()
        .map(
            |(n, origin_kind, detection_kind, confirmation, policy_kind, shape)| {
                GraphNode::Channel(ChannelNode {
                    id: channel_id(n),
                    label: None,
                    origin_kind,
                    detection_kind,
                    policy_kind,
                    confirmation,
                    locator_summary: NonBlank::new(&shape_name(&shape))
                        .expect("names are not blank"),
                })
            },
        )
        .collect()
}

/// Each channel node's confirmation (the stand-in for
/// `ChannelNode::confirmation`).
/// Writes by the sender and reads by the reader of every channel-routed
/// edge, two accesses per transmission on each side.
fn accesses() -> Vec<WeightedAccess> {
    let mut counts: BTreeMap<(AgentId, ChannelId, u8), u64> = BTreeMap::new();
    for edge in edges() {
        if let Route::Channel(channel) = edge.route {
            let tx = edge.stats.transmissions.get();
            *counts.entry((edge.from, channel, 0)).or_default() += tx * 2;
            *counts.entry((edge.to, channel, 1)).or_default() += tx * 2 + 1;
        }
    }
    let total: u64 = counts.values().sum();
    counts
        .into_iter()
        .map(|((agent, channel, op), n)| WeightedAccess {
            agent,
            channel,
            op: if op == 0 {
                AccessKind::Write
            } else {
                AccessKind::Read
            },
            accesses: non_zero(n),
            share: Share::new(n as f64 / total as f64).expect("share in [0, 1]"),
        })
        .collect()
}

/// The channel-centred graph: every edge of [`topology_graph`], the
/// accesses behind its channel-routed ones, and the channels as nodes.
pub fn bipartite_graph() -> Watermarked<ChannelGraph> {
    let mut nodes = agent_nodes();
    nodes.extend(channel_nodes());
    let graph = BipartiteGraph::new(BipartiteParts {
        window: window(),
        weighting: Weighting::Transmissions,
        topic_version: TopicModelVersion(3),
        nodes,
        accesses: accesses(),
        transmissions: edges(),
    })
    .expect("every endpoint has a node");
    watermarked(graph)
}

/// 96 points of 15 minutes over [`window`], busier in working hours: the
/// transmissions and matched-bytes series of one grid.
pub fn timeline() -> (
    TimeWindow,
    Watermarked<TopologySeries>,
    Watermarked<TopologySeries>,
) {
    let bucket = BucketWidth::from_micros(non_zero(5 * 60_000_000));
    let step = SeriesStep::new(bucket, non_zero(15 * 60_000_000)).expect("a multiple of 5 min");
    let grid = SeriesGrid::new(window(), step).expect("whole steps over the day");
    let mut rng = Rng(0x7131_E11E);
    let (mut transmissions, mut matched_bytes) = (Vec::new(), Vec::new());
    for i in 0..96u64 {
        let hour = i / 4;
        let base: u64 = match hour {
            0..=5 => 3,
            6..=8 => 9 + (hour - 6) * 6,
            9..=17 => 26 + (hour % 3) * 4,
            18..=20 => 16 - (hour - 18) * 4,
            _ => 5,
        };
        let count = base + rng.below(base / 2 + 3);
        let burst = if (54..=57).contains(&i) { 22 } else { 0 };
        let count = count + burst;
        transmissions.push(count);
        matched_bytes.push(count * (480 + rng.below(420)));
    }
    let series = |weighting, values| {
        TopologySeries::new(
            grid,
            weighting,
            TopicModelVersion(3),
            SeriesGroups::Total(values),
        )
        .expect("one value per point")
    };
    (
        window(),
        watermarked(series(Weighting::Transmissions, transmissions)),
        watermarked(series(Weighting::MatchedBytes, matched_bytes)),
    )
}
