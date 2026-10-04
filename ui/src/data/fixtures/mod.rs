//! Hand-built spec values for the data tests, and the element fixture files
//! generated from them.
//!
//! `ui/elements/test/fixtures/` holds the payloads these values encode to;
//! the TypeScript tests and the demo page read those files, so they parse
//! exactly what Rust emits. `element_fixtures_match_committed_files` fails
//! when they drift; regenerate with
//! `CT_UPDATE_FIXTURES=1 cargo test element_fixtures`.
//!
//! Everything is deterministic: a fixed seed, integer arithmetic and no
//! transcendental functions, so the bytes are the same on every platform.

use std::collections::HashMap;
use std::num::{NonZeroU16, NonZeroU64};
use std::path::PathBuf;

use crosstalk_spec::aggregates::edge::{RouteKind, TopologyFilter};
use crosstalk_spec::aggregates::node::AgentNode;
use crosstalk_spec::aggregates::projection::frame::{FrameHeader, ProjectionFrame};
use crosstalk_spec::aggregates::projection::{
    Fitted, ProjectedPoint, Projection, ProjectionInfo, ProjectionLimit, ProjectionParams,
    ProjectionSpec, ProjectionStatus,
};
use crosstalk_spec::aggregates::topic::{EmbeddingModel, TopicModelVersion};
use crosstalk_spec::aggregates::watermark::{Watermark, Watermarked};
use crosstalk_spec::ids::{AgentId, ChannelId, OperatorId, TopicId, TransmissionId};
use crosstalk_spec::support::{TimeWindow, Timestamp};

use super::names::shape_name;
use super::projection::format::{PayloadPoints, ProjectionTables, encode};
use super::timeline::TimelinePayload;
use super::topology::TopologyPayload;
use crate::components::agent_node_name;
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

/// 640 points in six topic clusters plus outliers, over eight agents, four
/// channels and six topics: the stored projection, the channels of its
/// channel-routed points, and the payload's points and names.
pub fn projection() -> (Projection, HashMap<TransmissionId, ChannelId>) {
    let agents = agents();
    let table_agents: Vec<&AgentNode> = agents.iter().take(8).collect();
    let channels: Vec<ChannelId> = (1..=4).map(channel_id).collect();
    let topics: Vec<TopicId> = (1..=6).map(|n| TopicId::from_ulid(ulid(0x70, n))).collect();
    let mut rng = Rng(0x00DE_51C7);
    let n = 640;
    let mut points = Vec::with_capacity(n);
    let mut routes = HashMap::new();
    for i in 0..n {
        let outlier = rng.below(100) < 7;
        let topic = (!outlier).then(|| usize::try_from(rng.below(6)).expect("small"));
        let (cx, cy, spread) = match topic {
            Some(t) => {
                let (x, y) = CENTRES[t];
                (x, y, 0.75)
            }
            None => (0.0, 0.0, 3.6),
        };
        let x = (cx + rng.normal() * spread) as f32;
        let y = (cy + rng.normal() * spread) as f32;
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
            _ => usize::try_from(rng.below(8)).expect("small"),
        };
        let reader = (sender + 1 + usize::try_from(rng.below(7)).expect("small")) % 8;
        let transmission = TransmissionId::from_ulid(ulid(0x7E, u128::try_from(i).expect("small")));
        if route == RouteKind::Channel {
            let channel = usize::try_from(rng.below(4)).expect("small");
            routes.insert(transmission, channels[channel]);
        }
        points.push(ProjectedPoint {
            transmission,
            from: table_agents[sender].id,
            to: table_agents[reader].id,
            route,
            topic: topic.map(|t| topics[t]),
            confirmed_at: ts(u64::try_from(i).expect("small") * 60_000_000),
            x,
            y,
        });
    }
    let projection = stored(ProjectionId::from_ulid(ulid(0x9F, 1)), &points);
    (projection, routes)
}

/// The payload's points and names for [`projection`].
pub fn payload() -> (Projection, PayloadPoints, ProjectionTables) {
    let (projection, routes) = projection();
    let points = PayloadPoints::new(&projection, &routes);
    let agents = agents();
    let agent_names: HashMap<AgentId, String> =
        agents.iter().map(|a| (a.id, agent_node_name(a))).collect();
    let channel_names: HashMap<ChannelId, String> = channel_specs()
        .iter()
        .map(|(n, _, _, _, shape)| (channel_id(*n), shape_name(shape)))
        .collect();
    let labels: HashMap<TopicId, &str> = (1..=6)
        .map(|n| TopicId::from_ulid(ulid(0x70, n)))
        .zip(TOPIC_LABELS)
        .collect();
    let tables = ProjectionTables::new(
        &points,
        points
            .agents()
            .iter()
            .map(|id| agent_names[id].clone())
            .collect(),
        points
            .channels()
            .iter()
            .map(|id| channel_names[id].clone())
            .collect(),
        points
            .topics()
            .iter()
            .map(|id| labels.get(id).map(|l| (*l).to_owned()))
            .collect(),
    )
    .expect("one name per entry");
    (projection, points, tables)
}

/// A ready projection `id` of `points`, fitted at 23:40 under version 3.
fn stored(id: ProjectionId, points: &[ProjectedPoint]) -> Projection {
    let params = ProjectionParams::new(ProjectionLimit::new(5000).expect("limit"), 15, 100, 42)
        .expect("params");
    let spec = ProjectionSpec::new(
        window(),
        TopologyFilter::default(),
        TopicModelVersion(3),
        params,
        EmbeddingModel {
            name: "bge-small-en-v1.5".to_owned(),
            dimension: NonZeroU16::new(384).expect("non-zero"),
        },
    );
    let matching = u64::try_from(points.len()).expect("small");
    let fitted = Fitted {
        started_at: ts(23 * HOUR_MICROS + 39 * 60_000_000),
        fitted_at: ts(23 * HOUR_MICROS + 40 * 60_000_000),
        watermark: watermark(),
        matching,
        points: u32::try_from(points.len()).expect("small"),
    };
    let header = FrameHeader {
        projection: id,
        topic_version: spec.topic_version(),
        watermark: watermark(),
        limit: params.limit(),
        matching,
    };
    let frame = ProjectionFrame::from_points(header, points).expect("frame");
    let info = ProjectionInfo::new(
        id,
        spec,
        OperatorId::from_ulid(ulid(0x0F, 1)),
        ts(23 * HOUR_MICROS + 38 * 60_000_000),
        ProjectionStatus::Ready(fitted),
    )
    .expect("info");
    Projection::new(info, frame).expect("projection")
}

pub fn empty_projection() -> Projection {
    stored(ProjectionId::from_ulid(ulid(0x9F, 2)), &[])
}

fn json(value: &impl serde::Serialize) -> Vec<u8> {
    let mut text = serde_json::to_string_pretty(value).expect("payloads serialize");
    text.push('\n');
    text.into_bytes()
}

/// The fixture files, by name.
fn fixture_files() -> Vec<(&'static str, Vec<u8>)> {
    let (window, transmissions, matched_bytes) = timeline();
    let (projection, points, tables) = payload();
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
            encode(&projection, &points, &tables).expect("fixture projection encodes"),
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
