//! Hand-built spec and contract values for the data tests, and the element
//! fixture files generated from them.
//!
//! `ui/elements/test/fixtures/` holds the payloads these values encode to;
//! the TypeScript tests and the demo page read those files, so they parse
//! exactly what Rust emits. `element_fixtures_match_committed_files` fails
//! when they drift; regenerate with
//! `CT_UPDATE_FIXTURES=1 cargo test element_fixtures`.
//!
//! Everything is deterministic: a fixed seed, integer arithmetic and no
//! transcendental functions, so the bytes are the same on every platform.

use std::num::{NonZeroU16, NonZeroU32, NonZeroU64};
use std::path::PathBuf;

use crosstalk_spec::aggregates::edge::RouteKind;
use crosstalk_spec::aggregates::node::AgentNode;
use crosstalk_spec::aggregates::topic::{EmbeddingModel, TopicModelVersion};
use crosstalk_spec::aggregates::watermark::{Watermark, Watermarked};
use crosstalk_spec::ids::{AgentId, ChannelId, TopicId, TransmissionId};
use crosstalk_spec::support::{TimeWindow, Timestamp};

use super::names::shape_name;
use super::projection::format::{ProjectionTables, encode};
use super::timeline::TimelinePayload;
use super::topology::TopologyPayload;
use crate::components::agent_node_name;
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
fn watermark() -> Watermark {
    Watermark(ts(23 * HOUR_MICROS + 22 * 60_000_000 + 30_000_000))
}

fn watermarked<T>(value: T) -> Watermarked<T> {
    Watermarked {
        watermark: watermark(),
        value,
    }
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

fn non_zero(n: u64) -> NonZeroU64 {
    NonZeroU64::new(n).expect("fixture counts are positive")
}

mod graphs;

use graphs::{agents, channel_specs};
pub use graphs::{bipartite_graph, channel_names, empty_topology_graph, timeline, topology_graph};

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
    let table_agents: Vec<&AgentNode> = agents.iter().take(8).collect();
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
        table_agents.iter().map(|a| agent_node_name(a)).collect(),
        channel_specs()
            .iter()
            .take(4)
            .map(|(_, _, _, _, shape)| shape_name(shape))
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
    let (window, transmissions, matched_bytes) = timeline();
    let (points, tables) = projection();
    vec![
        (
            "topology-agents.json",
            json(&TopologyPayload::agents(&topology_graph())),
        ),
        (
            "topology-channels.json",
            json(&TopologyPayload::channels(
                &bipartite_graph(),
                &channel_names(),
            )),
        ),
        (
            "topology-empty.json",
            json(&TopologyPayload::agents(&empty_topology_graph())),
        ),
        (
            "timeline.json",
            json(
                &TimelinePayload::new(window.into(), &transmissions, &matched_bytes)
                    .expect("one grid"),
            ),
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
