//! A small world for the edge store tests: a catalog, the directories, node
//! facts and a store with 10 µs buckets.

use std::num::NonZeroU64;

use crosstalk_spec::aggregates::edge::{TopologyFilter, TopologyGraph, Weighting};
use crosstalk_spec::aggregates::topic::TopicModelVersion;
use crosstalk_spec::derived::flow::transmission::{Classification, Route};
use crosstalk_spec::events::insight::ClassificationCause;
use crosstalk_spec::interfaces::l7_topology::{EdgeContribution, EdgeStore};
use crosstalk_spec::support::TimeWindow;

use crate::analysis::aliases::StaticDirectory;
use crate::analysis::catalog::InMemoryTopicCatalog;
use crate::analysis::support::ManualClock;
use crate::analysis::tests::support::fit_ready;
use crate::model::build::{
    agent, bucket_width, catalog, timing, topic_id, transmission, ts, window,
};
use crate::topology::env::{Env, StaticNodes};
use crate::topology::store::{Activation, EdgeStoreConfig, InMemoryEdgeStore};

pub type TestEnv = Env<InMemoryTopicCatalog, StaticDirectory, StaticNodes>;
pub type Store = InMemoryEdgeStore<TestEnv>;

pub const WIDTH: u64 = 10;
pub const SETTLE: u64 = 20;

pub struct World {
    pub catalog: InMemoryTopicCatalog,
    pub directory: StaticDirectory,
    pub nodes: StaticNodes,
    pub store: Store,
}

pub fn world() -> World {
    let catalog = catalog(3, 0.5, ManualClock::at(ts(0))).unwrap();
    let directory = StaticDirectory::new();
    let nodes = StaticNodes::new();
    let env = Env {
        topics: catalog.clone(),
        directory: directory.clone(),
        nodes: nodes.clone(),
    };
    let config = EdgeStoreConfig {
        bucket_width: bucket_width(WIDTH),
        timing: timing(SETTLE).unwrap(),
    };
    World {
        catalog,
        directory,
        nodes,
        store: InMemoryEdgeStore::new(config, env),
    }
}

/// Transmission `n` from agent `from` to agent `to` at `at`, `bytes`
/// matched, classified under `version` into `topic`.
#[allow(clippy::too_many_arguments)]
pub fn contribution(
    n: u64,
    from: u64,
    to: u64,
    route: Route,
    at: u64,
    bytes: u64,
    version: u32,
    topic: Option<u64>,
) -> EdgeContribution {
    EdgeContribution {
        transmission: transmission(n),
        from: agent(from),
        to: agent(to),
        route,
        at: ts(at),
        matched_bytes: NonZeroU64::new(bytes).unwrap(),
        classification: Classification {
            version: TopicModelVersion(version),
            topic: topic.map(topic_id),
            watched: false,
        },
    }
}

/// An outlier under version 0.
pub fn plain(n: u64, from: u64, to: u64, route: Route, at: u64, bytes: u64) -> EdgeContribution {
    contribution(n, from, to, route, at, bytes, 0, None)
}

pub fn all() -> TimeWindow {
    window(0, 1_000).unwrap()
}

pub async fn graph(world: &World, window: TimeWindow, filter: &TopologyFilter) -> TopologyGraph {
    world
        .store
        .graph(window, Weighting::Transmissions, filter)
        .await
        .unwrap()
        .value
}

/// `(from, to, transmissions, bytes)` per edge, routes left out.
pub fn edge_counts(graph: &TopologyGraph) -> Vec<(u64, u64, u64, u64)> {
    let number =
        |id: crosstalk_spec::ids::AgentId| u64::try_from(id.as_ulid() - (1u128 << 100)).unwrap();
    graph
        .edges()
        .iter()
        .map(|edge| {
            (
                number(edge.from),
                number(edge.to),
                edge.stats.transmissions.get(),
                edge.stats.matched_bytes.get(),
            )
        })
        .collect()
}

/// Fit version `n` in the catalog with topics `topics`, re-classify the
/// given contributions under it with cause `Refit`, and activate it in the
/// store and then the catalog.
pub fn refit(
    world: &World,
    at: u64,
    topics: &[u64],
    contributions: &[EdgeContribution],
) -> TopicModelVersion {
    let directions: Vec<(u64, [f32; 3])> = topics
        .iter()
        .enumerate()
        .map(|(index, id)| (*id, [1.0, index as f32, 0.5]))
        .collect();
    let version = fit_ready(&world.catalog, at, &directions);
    for contribution in contributions {
        let _ = world
            .store
            .apply_classified(contribution, ClassificationCause::Refit);
    }
    world
        .store
        .version_ready(version, u64::try_from(contributions.len()).unwrap());
    assert!(matches!(
        world.store.activate_if_complete(version),
        Ok(Activation::Switched { .. })
    ));
    world.catalog.activated(version, ts(at + 3)).unwrap();
    version
}
