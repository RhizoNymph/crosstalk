//! Hand-built contract values for the data tests, and the element fixture
//! files generated from them.
//!
//! `ui/elements/test/fixtures/` holds the payloads these values encode to;
//! the TypeScript tests and the demo page read those files, so they parse
//! exactly what Rust emits. `element_fixtures_match_committed_files` fails
//! when they drift; regenerate with
//! `CT_UPDATE_FIXTURES=1 cargo test element_fixtures`.
//!
//! Everything is deterministic: a fixed seed, integer arithmetic and no
//! transcendental functions, so the bytes are the same on every platform.

use std::collections::BTreeMap;
use std::num::{NonZeroU16, NonZeroU32, NonZeroU64};
use std::path::PathBuf;
use std::time::Duration;

use crosstalk_spec::aggregates::edge::{
    EdgeStats, RouteKind, TopologyGraph, WeightedEdge, Weighting,
};
use crosstalk_spec::aggregates::topic::{EmbeddingModel, TopicModelVersion};
use crosstalk_spec::derived::flow::access::AccessKind;
use crosstalk_spec::derived::flow::resource::{Host, Locator, ResourcePattern};
use crosstalk_spec::derived::flow::transmission::{DelegationDirection, DirectCarrier, Route};
use crosstalk_spec::ids::{AgentId, ChannelId, TopicId, TransmissionId};
use crosstalk_spec::interfaces::l8_surface::PolicyKind;
use crosstalk_spec::observed::client::{HarnessClaim, HarnessFamily};
use crosstalk_spec::observed::message::ToolName;
use crosstalk_spec::support::{Share, TimeWindow, Timestamp};

use super::projection::format::{ProjectionTables, encode};
use super::timeline::TimelinePayload;
use super::topology::TopologyPayload;
use crate::components::agent_name;
use crate::contract::agents::{AgentLabel, AgentStateKind, AgentSummary, ClaimSeen};
use crate::contract::channels::{DetectionKind, OriginKind};
use crate::contract::graph::{
    AccessEdge, BipartiteView, ChannelNode, ChannelShape, Timeline, TimelineBucket, TopologyView,
    route_kind,
};
use crate::contract::research::{
    PointCategories, ProjectionMeta, ProjectionParams, ProjectionPoints,
};
use crate::url::scope::{Scope, ViewFilter};
use crosstalk_spec::ids::ProjectionId;

/// 2026-10-02T00:00:00Z.
const START_MICROS: u64 = 1_790_899_200_000_000;
const HOUR_MICROS: u64 = 3_600_000_000;
/// The 48-bit millisecond time of the ids, so they read like real ULIDs.
const ULID_TIME: u128 = 0x0199_A2B3_C400;

fn ulid(kind: u128, n: u128) -> u128 {
    (ULID_TIME << 80) | (kind << 64) | (0x5EED_0000_0000 + n)
}

fn agent_id(n: u128) -> AgentId {
    AgentId::from_ulid(ulid(0xA1, n))
}

fn channel_id(n: u128) -> ChannelId {
    ChannelId::from_ulid(ulid(0xC4, n))
}

fn ts(offset_micros: u64) -> Timestamp {
    Timestamp::from_micros(START_MICROS + offset_micros)
}

pub fn window() -> TimeWindow {
    TimeWindow::new(ts(0), ts(24 * HOUR_MICROS)).expect("a day is a window")
}

/// Every bucket before 23:22:30 is final.
fn watermark() -> Timestamp {
    ts(23 * HOUR_MICROS + 22 * 60_000_000 + 30_000_000)
}

/// SplitMix64: a small deterministic generator.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// Uniform in `[0, 1)`.
    fn unit(&mut self) -> f64 {
        (self.next() >> 11) as f64 / (1u64 << 53) as f64
    }

    /// Roughly standard normal: the Irwin-Hall sum of four uniforms.
    fn normal(&mut self) -> f64 {
        let sum: f64 = (0..4).map(|_| self.unit()).sum();
        (sum - 2.0) * 1.732
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

struct AgentSpec {
    n: u128,
    label: Option<&'static str>,
    state: AgentStateKind,
    parent: Option<u128>,
    claim: Option<(HarnessFamily, Option<&'static str>, &'static str)>,
}

fn agent_specs() -> Vec<AgentSpec> {
    use AgentStateKind::{Established, Provisional, Registered};
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

fn non_zero(n: u64) -> NonZeroU64 {
    NonZeroU64::new(n).expect("fixture counts are positive")
}

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

fn agents() -> Vec<AgentSummary> {
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
            AgentSummary {
                id,
                label: spec
                    .label
                    .map(|l| AgentLabel::new(l).expect("fixture labels are valid")),
                state: spec.state,
                parent: spec.parent.map(agent_id),
                claims: spec
                    .claim
                    .into_iter()
                    .map(|(family, version, user_agent)| ClaimSeen {
                        claim: HarnessClaim {
                            family,
                            version: version.map(str::to_owned),
                            user_agent: user_agent.to_owned(),
                        },
                        last_seen: ts(
                            22 * HOUR_MICROS + u64::try_from(spec.n).expect("small") * 60_000_000
                        ),
                    })
                    .collect(),
                transmissions_in: sum(|e| e.to),
                transmissions_out: sum(|e| e.from),
                last_seen: ts(23 * HOUR_MICROS),
            }
        })
        .collect()
}

pub fn topology_view() -> TopologyView {
    let graph = TopologyGraph {
        window: window(),
        weighting: Weighting::Transmissions,
        topic_version: TopicModelVersion(3),
        nodes: Vec::new(),
        edges: edges(),
    };
    TopologyView::new(graph, agents(), watermark()).expect("every endpoint has a node")
}

pub fn empty_topology_view() -> TopologyView {
    let graph = TopologyGraph {
        window: window(),
        weighting: Weighting::Transmissions,
        topic_version: TopicModelVersion(3),
        nodes: Vec::new(),
        edges: Vec::new(),
    };
    TopologyView::new(graph, Vec::new(), watermark()).expect("no endpoints")
}

fn channel_nodes() -> Vec<ChannelNode> {
    let host = |h: &str| Host(h.to_owned());
    vec![
        ChannelNode {
            id: channel_id(1),
            origin: OriginKind::Declared,
            detection: DetectionKind::Active,
            policy: PolicyKind::Sanctioned,
            shape: ChannelShape::Pattern(ResourcePattern::PathPrefix {
                host: None,
                prefix: "/srv/shared/handoff".to_owned(),
            }),
        },
        ChannelNode {
            id: channel_id(2),
            origin: OriginKind::Discovered,
            detection: DetectionKind::Candidate,
            policy: PolicyKind::Unreviewed,
            shape: ChannelShape::Seed(Locator::Url {
                scheme: "https".to_owned(),
                host: host("pastebin.com"),
                path: "/raw/x9Qe2LmP".to_owned(),
                query: None,
            }),
        },
        ChannelNode {
            id: channel_id(3),
            origin: OriginKind::Discovered,
            detection: DetectionKind::Active,
            policy: PolicyKind::Unsanctioned,
            shape: ChannelShape::Seed(Locator::Mcp {
                server: "linear".to_owned(),
                tool: ToolName("get_issue".to_owned()),
                target: Some("ENG-4411".to_owned()),
            }),
        },
        ChannelNode {
            id: channel_id(4),
            origin: OriginKind::Declared,
            detection: DetectionKind::Observed,
            policy: PolicyKind::Unreviewed,
            shape: ChannelShape::Pattern(ResourcePattern::UrlPrefix {
                host: host("github.com"),
                path_prefix: "/acme/ops-notes".to_owned(),
            }),
        },
        ChannelNode {
            id: channel_id(5),
            origin: OriginKind::Declared,
            detection: DetectionKind::Unused,
            policy: PolicyKind::Sanctioned,
            shape: ChannelShape::Pattern(ResourcePattern::Host(host("wiki.internal"))),
        },
    ]
}

/// Writes by the sender and reads by the reader of every channel-routed
/// edge, two accesses per transmission on each side.
fn accesses() -> Vec<AccessEdge> {
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
        .map(|((agent, channel, op), n)| AccessEdge {
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

pub fn bipartite_view() -> BipartiteView {
    let direct: Vec<WeightedEdge> = edges()
        .into_iter()
        .filter(|e| route_kind(&e.route) != RouteKind::Channel)
        .collect();
    let total: u64 = direct.iter().map(|e| e.stats.transmissions.get()).sum();
    let direct = direct
        .into_iter()
        .map(|e| WeightedEdge {
            share: Share::new(e.stats.transmissions.get() as f64 / total as f64)
                .expect("share in [0, 1]"),
            ..e
        })
        .collect();
    BipartiteView::new(
        window(),
        Weighting::Transmissions,
        TopicModelVersion(3),
        agents(),
        channel_nodes(),
        accesses(),
        direct,
        watermark(),
    )
    .expect("every endpoint has a node")
}

/// 96 buckets of 15 minutes over [`window`], busier in working hours.
pub fn timeline() -> (TimeWindow, Timeline) {
    let width = 15 * 60_000_000;
    let mut rng = Rng(0x7131_E11E);
    let buckets = (0..96u64)
        .map(|i| {
            let hour = i / 4;
            let base: u64 = match hour {
                0..=5 => 3,
                6..=8 => 9 + (hour - 6) * 6,
                9..=17 => 26 + (hour % 3) * 4,
                18..=20 => 16 - (hour - 18) * 4,
                _ => 5,
            };
            let transmissions = base + rng.below(base / 2 + 3);
            let burst = if (54..=57).contains(&i) { 22 } else { 0 };
            let transmissions = transmissions + burst;
            TimelineBucket {
                bucket: TimeWindow::new(ts(i * width), ts((i + 1) * width)).expect("bucket"),
                transmissions,
                matched_bytes: transmissions * (480 + rng.below(420)),
            }
        })
        .collect();
    (
        window(),
        Timeline {
            bucket_width: Duration::from_secs(15 * 60),
            buckets,
            watermark: watermark(),
        },
    )
}

const TOPIC_LABELS: [&str; 6] = [
    "deploy pipeline failures",
    "PR review feedback",
    "API rate limits",
    "release notes drafting",
    "credential rotation",
    "flaky integration tests",
];

/// Cluster centres per topic, in projection units.
const CENTRES: [(f64, f64); 6] = [
    (-4.2, 2.6),
    (1.8, 4.1),
    (4.6, -0.4),
    (-1.2, -3.9),
    (-5.1, -1.8),
    (2.4, -4.6),
];

/// 640 points in six topic clusters plus outliers, with tables of eight
/// agents, four channels and six topics.
pub fn projection() -> (ProjectionPoints, ProjectionTables) {
    let agents = agents();
    let table_agents: Vec<&AgentSummary> = agents.iter().take(8).collect();
    let channels: Vec<ChannelId> = (1..=4).map(channel_id).collect();
    let topics: Vec<TopicId> = (1..=6).map(|n| TopicId::from_ulid(ulid(0x70, n))).collect();
    let mut rng = Rng(0x00DE_51C7);
    let n = 640;
    let mut transmissions = Vec::with_capacity(n);
    let mut xs = Vec::with_capacity(n);
    let mut ys = Vec::with_capacity(n);
    let mut categories = Vec::with_capacity(n);
    for i in 0..n {
        let outlier = rng.below(100) < 7;
        let topic = (!outlier).then(|| u32::try_from(rng.below(6)).expect("small"));
        let (cx, cy, spread) = match topic {
            Some(t) => {
                let (x, y) = CENTRES[t as usize];
                (x, y, 0.75)
            }
            None => (0.0, 0.0, 3.6),
        };
        xs.push((cx + rng.normal() * spread) as f32);
        ys.push((cy + rng.normal() * spread) as f32);
        let route = match rng.below(10) {
            0..=3 => RouteKind::Channel,
            4..=6 => RouteKind::Delegation,
            7..=8 => RouteKind::Direct,
            _ => RouteKind::Unobserved,
        };
        // Each topic has a preferred sender, so colouring by sender shows
        // structure too.
        let sender = match topic {
            Some(t) if rng.below(3) > 0 => t % 8,
            _ => u32::try_from(rng.below(8)).expect("small"),
        };
        let reader = (sender + 1 + u32::try_from(rng.below(7)).expect("small")) % 8;
        categories.push(PointCategories {
            sender,
            reader,
            route,
            channel: (route == RouteKind::Channel)
                .then(|| u32::try_from(rng.below(4)).expect("small")),
            topic,
        });
        transmissions.push(TransmissionId::from_ulid(ulid(
            0x7E,
            u128::try_from(i).expect("small"),
        )));
    }
    let meta = projection_meta(ProjectionId::from_ulid(ulid(0x9F, 1)));
    let points = ProjectionPoints::new(
        meta,
        transmissions,
        xs,
        ys,
        categories,
        table_agents.iter().map(|a| a.id).collect(),
        channels,
        topics,
    )
    .expect("fixture projection is consistent");
    let tables = ProjectionTables::new(
        &points,
        table_agents.iter().map(|a| agent_name(a)).collect(),
        channel_nodes()
            .iter()
            .take(4)
            .map(super::names::channel_node_name)
            .collect(),
        TOPIC_LABELS.iter().map(|l| Some((*l).to_owned())).collect(),
    )
    .expect("one name per entry");
    (points, tables)
}

fn projection_meta(id: ProjectionId) -> ProjectionMeta {
    ProjectionMeta {
        id,
        scope: Scope {
            window: window(),
            topic_version: TopicModelVersion(3),
            filter: ViewFilter::default(),
        },
        params: ProjectionParams::new(
            NonZeroU16::new(15).expect("non-zero"),
            0.1,
            42,
            NonZeroU32::new(5000).expect("non-zero"),
        )
        .expect("min_dist in range"),
        embedding_model: EmbeddingModel {
            name: "bge-small-en-v1.5".to_owned(),
            dimension: NonZeroU16::new(384).expect("non-zero"),
        },
        fitted_at: ts(23 * HOUR_MICROS + 40 * 60_000_000),
    }
}

pub fn empty_projection() -> ProjectionPoints {
    ProjectionPoints::new(
        projection_meta(ProjectionId::from_ulid(ulid(0x9F, 2))),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
    )
    .expect("empty projection is consistent")
}

fn json(value: &impl serde::Serialize) -> Vec<u8> {
    let mut text = serde_json::to_string_pretty(value).expect("payloads serialize");
    text.push('\n');
    text.into_bytes()
}

/// The fixture files, by name.
fn fixture_files() -> Vec<(&'static str, Vec<u8>)> {
    let (window, timeline) = timeline();
    let (points, tables) = projection();
    vec![
        (
            "topology-agents.json",
            json(&TopologyPayload::agents(&topology_view())),
        ),
        (
            "topology-channels.json",
            json(&TopologyPayload::channels(&bipartite_view())),
        ),
        (
            "topology-empty.json",
            json(&TopologyPayload::agents(&empty_topology_view())),
        ),
        (
            "timeline.json",
            json(&TimelinePayload::new(window.into(), &timeline)),
        ),
        (
            "projection.bin",
            encode(&points, &tables).expect("fixture projection encodes"),
        ),
    ]
}

#[test]
fn element_fixtures_match_committed_files() {
    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("elements/test/fixtures");
    let update = std::env::var_os("CT_UPDATE_FIXTURES").is_some();
    for (name, bytes) in fixture_files() {
        let path = dir.join(name);
        if update {
            std::fs::write(&path, &bytes).expect("write fixture");
            continue;
        }
        let committed = std::fs::read(&path).unwrap_or_default();
        assert!(
            committed == bytes,
            "{} is stale; regenerate with CT_UPDATE_FIXTURES=1 cargo test element_fixtures",
            path.display()
        );
    }
}
