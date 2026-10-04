//! Property tests of the Postgres store: proptest generates a scene
//! (contributions, merges, supersessions, verdicts, accesses), the driver
//! loads it into a fresh store and each property checks what the store
//! then returns against an explicit oracle. Every case runs on its own
//! runtime and small pool over one test database per property, emptied
//! between cases; without `TEST_DATABASE_URL` each property passes with a
//! skip line.

mod graph;
mod series;
mod versions;

use std::collections::HashMap;
use std::num::NonZeroU64;

use crosstalk_memory::model::build::{access, agent, channel, resource, transmission, ts};
use crosstalk_spec::aggregates::edge::{RouteKind, TopologyFilter, TopologyGraph, Weighting};
use crosstalk_spec::aggregates::filter::{FalseDetections, FilterSubject};
use crosstalk_spec::aggregates::topic::TopicModelVersion;
use crosstalk_spec::derived::flow::access::AccessKind;
use crosstalk_spec::derived::flow::transmission::{Classification, DelegationDirection, Route};
use crosstalk_spec::derived::flow::verdict::{Verdict, VerdictRevision};
use crosstalk_spec::events::insight::ClassificationCause;
use crosstalk_spec::ids::{AgentId, ChannelId, TopicId};
use crosstalk_spec::interfaces::l3_reconstruction::AgentDirectory;
use crosstalk_spec::interfaces::l7_topology::{AccessContribution, EdgeContribution, EdgeStore};
use crosstalk_spec::support::TimeWindow;
use proptest::prelude::*;
use proptest::test_runner::{Config, TestCaseError, TestError, TestRunner};

use crate::env::EnvAliases;
use crate::tests::support::{World, database, small_pool, world};

/// How many cases each property runs: every case is a fresh store on a
/// real database, so fewer than an in-memory property.
pub const CASES: u32 = 10;

/// One generated transmission: sender, reader, route, time, bytes.
#[derive(Debug, Clone, Copy)]
pub struct Sent {
    pub from: u64,
    pub to: u64,
    pub route: u8,
    pub at: u64,
    pub bytes: u64,
}

/// What a case loads into the store.
#[derive(Debug, Clone)]
pub struct Scene {
    /// Transmission `i + 1` is `sent[i]`, classified under version 0.
    pub sent: Vec<Sent>,
    pub merges: Vec<(u64, u64)>,
    pub supersessions: Vec<(u64, u64)>,
    /// Transmissions judged `FalseDetection` (by index into `sent`).
    pub false_detections: Vec<usize>,
    /// Accesses: agent, resource, write, time.
    pub accesses: Vec<(u64, u64, bool, u64)>,
}

pub fn route(n: u8) -> Route {
    match n % 6 {
        4 => Route::Delegation(DelegationDirection::ChildToParent),
        5 => Route::Unobserved,
        k => Route::Channel(channel(u64::from(k))),
    }
}

pub fn sent() -> impl Strategy<Value = Sent> {
    (0u64..5, 0u64..5, 0u8..6, 0u64..200, 1u64..10).prop_map(|(from, to, route, at, bytes)| Sent {
        from,
        to,
        route,
        at,
        bytes,
    })
}

pub fn scene() -> impl Strategy<Value = Scene> {
    (
        prop::collection::vec(sent(), 1..14),
        prop::collection::vec((0u64..5, 0u64..5), 0..3),
        prop::collection::vec((0u64..4, 0u64..4), 0..2),
        prop::collection::vec(0usize..14, 0..4),
        prop::collection::vec((0u64..5, 0u64..4, any::<bool>(), 0u64..200), 0..8),
    )
        .prop_map(
            |(sent, merges, supersessions, false_detections, accesses)| Scene {
                sent,
                merges,
                supersessions,
                false_detections,
                accesses,
            },
        )
}

/// An aligned window of whole 10 µs buckets.
pub fn aligned_window() -> impl Strategy<Value = TimeWindow> {
    (0u64..20, 1u64..21).prop_map(|(start, buckets)| window(start * 10, (start + buckets) * 10))
}

pub fn window(start: u64, end: u64) -> TimeWindow {
    crate::tests::support::window(start, end)
}

/// Transmission `i + 1` as a contribution under `version`.
pub fn contribution(
    index: usize,
    sent: Sent,
    version: TopicModelVersion,
    topic: Option<TopicId>,
) -> EdgeContribution {
    EdgeContribution {
        transmission: transmission(index as u64 + 1),
        from: agent(sent.from),
        to: agent(sent.to),
        route: route(sent.route),
        at: ts(sent.at),
        matched_bytes: NonZeroU64::new(sent.bytes).unwrap_or(NonZeroU64::MIN),
        classification: Classification {
            version,
            topic,
            watched: false,
        },
        cause: ClassificationCause::Confirmation,
    }
}

/// Load `scene` into `world`: every contribution (self-edges refused),
/// the merges and supersessions the directory accepts, the verdicts and
/// the accesses.
pub async fn load(world: &mut World, scene: &Scene) -> Result<(), TestCaseError> {
    for (index, one) in scene.sent.iter().enumerate() {
        let applied = world
            .store
            .apply(&contribution(index, *one, TopicModelVersion(0), None))
            .await;
        if one.from != one.to {
            applied.map_err(|error| TestCaseError::fail(format!("apply: {error:?}")))?;
        }
    }
    for (from, into) in &scene.merges {
        let _ = world.directory.merge(agent(*from), agent(*into));
    }
    for (old, by) in &scene.supersessions {
        let _ = world.directory.supersede(channel(*old), channel(*by));
    }
    let revision = VerdictRevision::new(std::num::NonZeroU32::MIN);
    for index in &scene.false_detections {
        world
            .store
            .judge(
                transmission(*index as u64 + 1),
                Some(Verdict::FalseDetection),
                revision,
            )
            .await
            .map_err(|error| TestCaseError::fail(format!("judge: {error:?}")))?;
    }
    for (n, (who, what, write, at)) in scene.accesses.iter().enumerate() {
        let one = AccessContribution {
            access: access(n as u64 + 1),
            agent: agent(*who),
            resource: resource(*what),
            op: if *write {
                AccessKind::Write
            } else {
                AccessKind::Read
            },
            at: ts(*at),
        };
        world
            .store
            .apply_access(&one)
            .await
            .map_err(|error| TestCaseError::fail(format!("apply access: {error:?}")))?;
    }
    Ok(())
}

/// The reference fold of `topology.graph.matches-fold-model` over the
/// scene's version-0 contributions: per (from, to, route) after
/// resolution, the transmissions and bytes `filter` admits in `window`.
pub fn fold(
    world: &World,
    scene: &Scene,
    window: TimeWindow,
    filter: &TopologyFilter,
) -> Vec<(AgentId, AgentId, Route, u64, u64)> {
    let env = world.store.env();
    let aliases = EnvAliases(env);
    let mut sums: HashMap<(AgentId, AgentId, Route), (u64, u64)> = HashMap::new();
    for (index, one) in scene.sent.iter().enumerate() {
        if !window.contains(ts(one.at)) {
            continue;
        }
        let from = AgentDirectory::canonical(&world.directory, agent(one.from));
        let to = AgentDirectory::canonical(&world.directory, agent(one.to));
        let routed = route(one.route).resolved(aliases);
        let subject = FilterSubject {
            from,
            to,
            route: &routed,
            topic: None,
            false_detection: scene.false_detections.contains(&index),
        };
        if filter.admits(&subject, aliases) {
            let entry = sums.entry((from, to, routed)).or_default();
            entry.0 += 1;
            entry.1 += one.bytes;
        }
    }
    let mut edges: Vec<_> = sums
        .into_iter()
        .map(|((from, to, route), (n, bytes))| (from, to, route, n, bytes))
        .collect();
    edges.sort_by_key(|edge| (edge.0, edge.1, crate::store::fold_route_key(&edge.2)));
    edges
}

/// A graph's edges as the fold lists them.
pub fn listed(graph: &TopologyGraph) -> Vec<(AgentId, AgentId, Route, u64, u64)> {
    let mut edges: Vec<_> = graph
        .edges()
        .iter()
        .map(|edge| {
            (
                edge.from,
                edge.to,
                edge.route.clone(),
                edge.stats.transmissions.get(),
                edge.stats.matched_bytes.get(),
            )
        })
        .collect();
    edges.sort_by_key(|edge| (edge.0, edge.1, crate::store::fold_route_key(&edge.2)));
    edges
}

pub async fn read(
    world: &World,
    window: TimeWindow,
    weighting: Weighting,
    filter: &TopologyFilter,
) -> Result<TopologyGraph, TestCaseError> {
    world
        .store
        .graph(window, weighting, filter)
        .await
        .map(|read| read.value)
        .map_err(|error| TestCaseError::fail(format!("graph: {error:?}")))
}

/// A filter from generated choices.
pub fn filter() -> impl Strategy<Value = TopologyFilter> {
    (
        prop::collection::vec(0u64..5, 0..2),
        prop::collection::vec(0u64..4, 0..2),
        prop::collection::vec(0u8..4, 0..2),
        any::<bool>(),
    )
        .prop_map(|(agents, channels, kinds, exclude)| TopologyFilter {
            agents: agents.into_iter().map(agent).collect(),
            channels: channels.into_iter().map(channel).collect(),
            route_kinds: kinds
                .into_iter()
                .map(|k| match k {
                    0 => RouteKind::Channel,
                    1 => RouteKind::Delegation,
                    2 => RouteKind::Direct,
                    _ => RouteKind::Unobserved,
                })
                .collect(),
            false_detections: if exclude {
                FalseDetections::Exclude
            } else {
                FalseDetections::Include
            },
            ..TopologyFilter::default()
        })
}

/// Run `body` on `CASES` inputs from `strategy`, each on a fresh store
/// over the property's own test database.
pub fn check<S, F>(test: &str, strategy: S, body: F)
where
    S: Strategy,
    S::Value: std::fmt::Debug,
    F: AsyncFn(&mut World, &S::Value) -> Result<(), TestCaseError>,
{
    let setup = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .expect("a runtime");
    let Some(db) = setup.block_on(database(test)) else {
        return;
    };
    let options = db.url().connect_options().clone();
    let mut runner = TestRunner::new(Config {
        cases: CASES,
        failure_persistence: None,
        ..Config::default()
    });
    let result = runner.run(&strategy, |input| {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|error| TestCaseError::fail(format!("no runtime: {error}")))?;
        runtime.block_on(async {
            let pool = small_pool(options.clone(), 2).await;
            crate::tests::support::reset(&pool).await;
            let mut world = world(pool);
            body(&mut world, &input).await
        })
    });
    setup.block_on(db.close()).expect("drop the database");
    match result {
        Ok(()) => {}
        Err(TestError::Fail(reason, minimal)) => panic!("{reason}\nminimal input: {minimal:#?}"),
        Err(TestError::Abort(reason)) => panic!("aborted: {reason}"),
    }
}

/// `Ok` when `condition` holds, else the failure `what` describes.
pub fn ensure(condition: bool, what: impl FnOnce() -> String) -> Result<(), TestCaseError> {
    if condition {
        Ok(())
    } else {
        Err(TestCaseError::fail(what()))
    }
}

/// The canonical channel of `id` in `world`.
pub fn canonical_channel(world: &World, id: ChannelId) -> ChannelId {
    crosstalk_spec::interfaces::l5_flow::ChannelDirectory::canonical(&world.directory, id)
}
