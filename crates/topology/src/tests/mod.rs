//! Tests of the store on fixed inputs. Those that need the database run
//! against `TEST_DATABASE_URL` and pass with a skip line without it; the
//! window and grid checks refuse before any query, so they run on a pool
//! that never connects.

pub mod support;

use std::num::NonZeroU64;

use crosstalk_memory::model::build::{agent, bucket_width, channel, transmission, ts};
use crosstalk_memory::model::topology::catalog_ready;
use crosstalk_spec::aggregates::edge::{
    EdgeSelector, RouteKind, TopicSlot, TopologyFilter, Weighting,
};
use crosstalk_spec::aggregates::filter::FalseDetections;
use crosstalk_spec::aggregates::series::{SeriesGrid, SeriesGrouping, SeriesStep};
use crosstalk_spec::aggregates::topic::TopicModelVersion;
use crosstalk_spec::derived::flow::access::AccessKind;
use crosstalk_spec::derived::flow::transmission::{DelegationDirection, DirectCarrier, Route};
use crosstalk_spec::derived::flow::verdict::{Observed, Verdict, VerdictRevision};
use crosstalk_spec::events::insight::ClassificationCause;
use crosstalk_spec::ids::AccessId;
use crosstalk_spec::interfaces::l6_analysis::lifecycle::TopicLifecycle;
use crosstalk_spec::interfaces::l7_topology::{
    AccessContribution, Activation, EdgeContribution, EdgeError, EdgeQueryError, EdgeStore,
};
use crosstalk_spec::paging::{EdgeTransmissionList, PageRequest, PageSize};
use sqlx::PgPool;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};

use self::support::{World, contribution, database, edge_counts, frontier, graph, window, world};
use crate::store::bucket_of;

/// How many rows `table` holds.
pub async fn count(pool: &PgPool, table: &'static str) -> i64 {
    let sql = match table {
        "contributions" => "SELECT count(*) FROM topology.contributions",
        "edge_buckets" => "SELECT count(*) FROM topology.edge_buckets",
        "accesses" => "SELECT count(*) FROM topology.accesses",
        "access_buckets" => "SELECT count(*) FROM topology.access_buckets",
        "outbox" => "SELECT count(*) FROM topology.outbox",
        "refit_processed" => "SELECT count(*) FROM topology.refit_processed",
        other => panic!("no such table {other}"),
    };
    sqlx::query_scalar(sql)
        .fetch_one(pool)
        .await
        .expect("a count")
}

/// The edge buckets as stored: (version, bucket start, transmissions,
/// bytes), sorted.
pub async fn buckets(pool: &PgPool) -> Vec<(i64, i64, i64, i64)> {
    sqlx::query_as(
        "SELECT version, bucket_start, transmissions, matched_bytes FROM topology.edge_buckets \
         ORDER BY version, bucket_start, from_agent, to_agent, route, topic",
    )
    .fetch_all(pool)
    .await
    .expect("the buckets")
}

/// A store on a pool that never connects: for checks that refuse before
/// any query.
fn offline() -> support::Store {
    let pool = PgPoolOptions::new().connect_lazy_with(PgConnectOptions::new());
    world(pool).store
}

/// A re-fit: the catalog fits and makes ready version `n` (no topics), and
/// the store gets one refit classification per listed contribution and the
/// version's ready count.
pub async fn refit(
    world: &mut World,
    at: u64,
    contributions: &[EdgeContribution],
) -> TopicModelVersion {
    let version = catalog_ready(&mut world.catalog, Vec::new(), ts(at))
        .await
        .expect("a ready version");
    for one in contributions {
        let refit = EdgeContribution {
            cause: ClassificationCause::Refit,
            classification: crosstalk_spec::derived::flow::transmission::Classification {
                version,
                ..one.classification
            },
            ..one.clone()
        };
        let _ = world.store.apply(&refit).await;
    }
    let count = u64::try_from(contributions.len()).expect("a count");
    world
        .store
        .version_ready(version, count)
        .await
        .expect("version ready");
    version
}

/// Activate `version` in the store and then in the catalog, as the
/// pipeline does (`TopicVersionActivated` marks it active).
pub async fn activate(world: &mut World, version: TopicModelVersion, at: u64) -> Activation {
    let activation = world.store.activate(version).await.expect("activate");
    if let Activation::Switched { .. } = activation {
        world
            .catalog
            .mark_active(version, ts(at))
            .await
            .expect("catalog activation");
    }
    activation
}

#[test]
fn bucket_of_puts_a_boundary_instant_in_the_bucket_it_starts() {
    let width = bucket_width(10);
    assert_eq!(bucket_of(width, ts(10)), Some(window(10, 20)));
    assert_eq!(bucket_of(width, ts(19)), Some(window(10, 20)));
    assert_eq!(bucket_of(width, ts(9)), Some(window(0, 10)));
    assert_eq!(bucket_of(width, ts(0)), Some(window(0, 10)));
}

/// topology.apply.aligned-bucket
#[tokio::test(flavor = "multi_thread")]
async fn apply_buckets_boundary_instant_into_next_bucket() {
    let Some(db) = database("apply_buckets_boundary_instant_into_next_bucket").await else {
        return;
    };
    let mut world = world(db.pool().clone());
    for (n, at, bucket) in [
        (1, 10, window(10, 20)),
        (2, 19, window(10, 20)),
        (3, 9, window(0, 10)),
        (4, 20, window(20, 30)),
    ] {
        let one = contribution(n, 1, 2, Route::Unobserved, at, 3);
        let key = world.store.apply(&one).await.expect("applied");
        assert_eq!(key.bucket(), bucket, "contribution at {at}");
        assert_eq!(key.from(), one.from);
        assert_eq!(key.to(), one.to);
        assert_eq!(key.route(), &one.route);
        assert_eq!(
            key.topic(),
            TopicSlot {
                version: TopicModelVersion(0),
                topic: None
            }
        );
    }
    db.close().await.expect("drop the database");
}

/// topology.apply.rejects-self-edge
#[tokio::test(flavor = "multi_thread")]
async fn apply_self_edge_errors_and_stores_nothing() {
    let Some(db) = database("apply_self_edge_errors_and_stores_nothing").await else {
        return;
    };
    let mut world = world(db.pool().clone());
    let pool = db.pool();
    let one = contribution(1, 4, 4, Route::Unobserved, 5, 7);
    assert_eq!(world.store.apply(&one).await, Err(EdgeError::SelfEdge));
    assert_eq!(world.store.apply(&one).await, Err(EdgeError::SelfEdge));
    assert_eq!(count(pool, "contributions").await, 0);
    assert_eq!(count(pool, "edge_buckets").await, 0);
    assert_eq!(count(pool, "outbox").await, 0);
    assert!(
        graph(&world.store, window(0, 100), &TopologyFilter::default())
            .await
            .edges()
            .is_empty()
    );
    db.close().await.expect("drop the database");
}

/// topology.filter.route-kind-membership
#[tokio::test(flavor = "multi_thread")]
async fn route_kind_filter_maps_each_route_variant() {
    let Some(db) = database("route_kind_filter_maps_each_route_variant").await else {
        return;
    };
    let mut world = world(db.pool().clone());
    let routes = [
        (RouteKind::Channel, Route::Channel(channel(1))),
        (
            RouteKind::Delegation,
            Route::Delegation(DelegationDirection::ParentToChild),
        ),
        (
            RouteKind::Direct,
            Route::Direct(DirectCarrier::SystemPrompt),
        ),
        (RouteKind::Unobserved, Route::Unobserved),
    ];
    for (n, (_, route)) in (1u64..).zip(&routes) {
        world
            .store
            .apply(&contribution(n, 1, 2, route.clone(), 5, 1))
            .await
            .expect("applied");
    }
    for (kind, route) in &routes {
        let filter = TopologyFilter {
            route_kinds: vec![*kind],
            ..TopologyFilter::default()
        };
        let graph = graph(&world.store, window(0, 10), &filter).await;
        let got: Vec<&Route> = graph.edges().iter().map(|edge| &edge.route).collect();
        assert_eq!(got, vec![route], "route kind {kind:?}");
    }
    let all = graph(&world.store, window(0, 10), &TopologyFilter::default()).await;
    assert_eq!(all.edges().len(), 4);
    db.close().await.expect("drop the database");
}

/// topology.filter.agent-membership
#[tokio::test(flavor = "multi_thread")]
async fn agent_filter_resolves_merged_ids() {
    let Some(db) = database("agent_filter_resolves_merged_ids").await else {
        return;
    };
    let mut world = world(db.pool().clone());
    for (n, from, to) in [(1, 1, 3), (2, 2, 3), (3, 4, 5)] {
        world
            .store
            .apply(&contribution(n, from, to, Route::Unobserved, 5, 2))
            .await
            .expect("applied");
    }
    world.directory.merge(agent(1), agent(2)).expect("a merge");
    // Agent 1 was merged into 2: listing 1 selects every edge touching 2.
    let filter = TopologyFilter {
        agents: vec![agent(1)],
        ..TopologyFilter::default()
    };
    let graph = graph(&world.store, window(0, 10), &filter).await;
    assert_eq!(edge_counts(&graph), vec![(agent(2), agent(3), 2, 4)]);
    db.close().await.expect("drop the database");
}

/// topology.filter.channel-membership
#[tokio::test(flavor = "multi_thread")]
async fn channel_filter_excludes_non_channel_routes() {
    let Some(db) = database("channel_filter_excludes_non_channel_routes").await else {
        return;
    };
    let mut world = world(db.pool().clone());
    let applied = [
        (1, Route::Channel(channel(1))),
        (2, Route::Channel(channel(2))),
        (3, Route::Unobserved),
        (4, Route::Delegation(DelegationDirection::ChildToParent)),
        (5, Route::Channel(channel(3))),
    ];
    for (n, route) in &applied {
        world
            .store
            .apply(&contribution(*n, 1, 2, route.clone(), 5, 1))
            .await
            .expect("applied");
    }
    // Channel 3 is superseded by channel 1: it counts as channel 1.
    world
        .directory
        .supersede(channel(3), channel(1))
        .expect("a supersession");
    let filter = TopologyFilter {
        channels: vec![channel(3)],
        ..TopologyFilter::default()
    };
    let graph = graph(&world.store, window(0, 10), &filter).await;
    let routes: Vec<(&Route, u64)> = graph
        .edges()
        .iter()
        .map(|edge| (&edge.route, edge.stats.transmissions.get()))
        .collect();
    assert_eq!(routes, vec![(&Route::Channel(channel(1)), 2)]);
    db.close().await.expect("drop the database");
}

/// topology.graph.rejects-unaligned-window
#[tokio::test(flavor = "multi_thread")]
async fn graph_rejects_window_cutting_a_bucket() {
    let store = offline();
    for cut in [window(3, 20), window(0, 15), window(5, 7)] {
        let read = store
            .graph(cut, Weighting::Transmissions, &TopologyFilter::default())
            .await;
        assert_eq!(read.err(), Some(EdgeQueryError::UnalignedWindow), "{cut:?}");
        let read = store.totals(cut, &TopologyFilter::default()).await;
        assert_eq!(read.err(), Some(EdgeQueryError::UnalignedWindow), "{cut:?}");
        let read = store
            .channel_topology(cut, Weighting::Transmissions, &TopologyFilter::default())
            .await;
        assert_eq!(read.err(), Some(EdgeQueryError::UnalignedWindow), "{cut:?}");
    }
}

/// topology.series.rejects-bucket-width-mismatch
#[tokio::test(flavor = "multi_thread")]
async fn series_rejects_grid_for_other_bucket_width() {
    let store = offline();
    let step =
        SeriesStep::new(bucket_width(20), NonZeroU64::new(40).expect("non-zero")).expect("a step");
    let grid = SeriesGrid::new(window(0, 80), step).expect("a grid");
    let read = store
        .series(
            grid,
            Weighting::Transmissions,
            SeriesGrouping::Total,
            &TopologyFilter::default(),
        )
        .await;
    assert_eq!(
        read.err(),
        Some(EdgeQueryError::BucketWidthMismatch {
            store: bucket_width(10),
            grid: bucket_width(20),
        })
    );
}

/// topology.version.initial-zero
#[tokio::test(flavor = "multi_thread")]
async fn fresh_store_graph_reports_version_zero() {
    let Some(db) = database("fresh_store_graph_reports_version_zero").await else {
        return;
    };
    let mut world = world(db.pool().clone());
    let empty = graph(&world.store, window(0, 100), &TopologyFilter::default()).await;
    assert_eq!(empty.topic_version(), TopicModelVersion(0));
    world
        .store
        .apply(&contribution(1, 1, 2, Route::Unobserved, 5, 1))
        .await
        .expect("applied");
    let one = graph(&world.store, window(0, 100), &TopologyFilter::default()).await;
    assert_eq!(one.topic_version(), TopicModelVersion(0));
    assert_eq!(one.edges().len(), 1);
    db.close().await.expect("drop the database");
}

/// topology.verdict.buckets-untouched
#[tokio::test(flavor = "multi_thread")]
async fn judge_leaves_buckets() {
    let Some(db) = database("judge_leaves_buckets").await else {
        return;
    };
    let mut world = world(db.pool().clone());
    for (n, at) in [(1, 5), (2, 6), (3, 15)] {
        world
            .store
            .apply(&contribution(n, 1, 2, Route::Unobserved, at, 4))
            .await
            .expect("applied");
    }
    let stored = buckets(db.pool()).await;
    let before = graph(&world.store, window(0, 20), &TopologyFilter::default()).await;
    let revision = VerdictRevision::new(std::num::NonZeroU32::MIN);
    let judged = world
        .store
        .judge(transmission(1), Some(Verdict::FalseDetection), revision)
        .await;
    assert_eq!(judged, Ok(Observed::Newer));
    // A verdict for a transmission never applied is kept too.
    assert_eq!(
        world
            .store
            .judge(transmission(9), Some(Verdict::Genuine), revision)
            .await,
        Ok(Observed::Newer)
    );
    assert_eq!(
        world.store.judge(transmission(1), None, revision).await,
        Ok(Observed::Stale)
    );
    assert_eq!(buckets(db.pool()).await, stored);
    let after = graph(&world.store, window(0, 20), &TopologyFilter::default()).await;
    assert_eq!(after, before);
    let exclude = TopologyFilter {
        false_detections: FalseDetections::Exclude,
        ..TopologyFilter::default()
    };
    let excluded = graph(&world.store, window(0, 20), &exclude).await;
    assert_eq!(edge_counts(&excluded), vec![(agent(1), agent(2), 2, 8)]);
    db.close().await.expect("drop the database");
}

/// topology.retention.drop-refuses-in-use
#[tokio::test(flavor = "multi_thread")]
async fn drop_version_refuses_active_and_newer() {
    let Some(db) = database("drop_version_refuses_active_and_newer").await else {
        return;
    };
    let mut world = world(db.pool().clone());
    let one = contribution(1, 1, 2, Route::Unobserved, 5, 4);
    world.store.apply(&one).await.expect("applied");
    let v1 = refit(&mut world, 100, std::slice::from_ref(&one)).await;
    let stored = buckets(db.pool()).await;
    for version in [TopicModelVersion(0), v1, TopicModelVersion(7)] {
        assert_eq!(
            world.store.drop_version(version).await,
            Err(EdgeError::VersionInUse { version }),
            "{version:?}"
        );
    }
    assert_eq!(buckets(db.pool()).await, stored);
    assert_eq!(count(db.pool(), "contributions").await, 2);
    db.close().await.expect("drop the database");
}

/// topology.watermark.rejects-late
#[tokio::test(flavor = "multi_thread")]
async fn apply_into_final_bucket_is_late() {
    let Some(db) = database("apply_into_final_bucket_is_late").await else {
        return;
    };
    let mut world = world(db.pool().clone());
    world
        .store
        .apply(&contribution(1, 1, 2, Route::Unobserved, 12, 4))
        .await
        .expect("applied");
    // Ticked through 60, settle_after 20: the watermark is 40.
    let advanced = world.store.advance_watermark(frontier(60)).await;
    assert_eq!(
        advanced,
        Ok(Some(crosstalk_spec::support::Watermark(ts(40))))
    );
    let stored = buckets(db.pool()).await;
    let late = world
        .store
        .apply(&contribution(2, 1, 2, Route::Unobserved, 15, 4))
        .await;
    assert_eq!(
        late,
        Err(EdgeError::LateContribution {
            bucket: window(10, 20),
            watermark: crosstalk_spec::support::Watermark(ts(40)),
        })
    );
    let edge_late = world
        .store
        .apply(&contribution(3, 1, 2, Route::Unobserved, 39, 4))
        .await;
    assert!(matches!(edge_late, Err(EdgeError::LateContribution { .. })));
    assert_eq!(buckets(db.pool()).await, stored);
    assert_eq!(count(db.pool(), "contributions").await, 1);
    // The bucket holding the watermark is not final.
    world
        .store
        .apply(&contribution(4, 1, 2, Route::Unobserved, 40, 4))
        .await
        .expect("not late");
    // A redelivery of an applied contribution is not late either.
    world
        .store
        .apply(&contribution(1, 1, 2, Route::Unobserved, 12, 4))
        .await
        .expect("a redelivery");
    db.close().await.expect("drop the database");
}

fn access(n: u128, agent_n: u64, resource: u64, op: AccessKind, at: u64) -> AccessContribution {
    AccessContribution {
        access: AccessId::from_ulid(n),
        agent: agent(agent_n),
        resource: crosstalk_memory::model::build::resource(resource),
        op,
        at: ts(at),
    }
}

/// topology.access.apply-idempotent
#[tokio::test(flavor = "multi_thread")]
async fn apply_access_twice_counts_once() {
    let Some(db) = database("apply_access_twice_counts_once").await else {
        return;
    };
    let mut world = world(db.pool().clone());
    let first = access(1, 1, 1, AccessKind::Write, 12);
    let edge = world.store.apply_access(&first).await.expect("applied");
    assert_eq!(edge.accesses.get(), 1);
    assert_eq!(edge.bucket, window(10, 20));
    let again = world.store.apply_access(&first).await.expect("redelivered");
    assert_eq!(again, edge);
    let second = access(2, 1, 1, AccessKind::Write, 18);
    assert_eq!(
        world
            .store
            .apply_access(&second)
            .await
            .expect("applied")
            .accesses
            .get(),
        2
    );
    let read = access(3, 1, 1, AccessKind::Read, 18);
    assert_eq!(
        world
            .store
            .apply_access(&read)
            .await
            .expect("applied")
            .accesses
            .get(),
        1
    );
    assert_eq!(count(db.pool(), "accesses").await, 3);
    assert_eq!(count(db.pool(), "access_buckets").await, 2);
    db.close().await.expect("drop the database");
}

/// topology.access.apply-idempotent, under concurrent redelivery.
#[tokio::test(flavor = "multi_thread")]
async fn pg_apply_access_idempotent() {
    let Some(db) = database("pg_apply_access_idempotent").await else {
        return;
    };
    let world = world(db.pool().clone());
    let one = access(1, 1, 1, AccessKind::Read, 3);
    let mut tasks = Vec::new();
    for _ in 0..8 {
        let mut store = world.store.clone();
        tasks.push(tokio::spawn(async move { store.apply_access(&one).await }));
    }
    for task in tasks {
        let edge = task.await.expect("joined").expect("applied");
        assert_eq!(edge.accesses.get(), 1);
    }
    let stored: i64 =
        sqlx::query_scalar("SELECT sum(accesses)::bigint FROM topology.access_buckets")
            .fetch_one(db.pool())
            .await
            .expect("a sum");
    assert_eq!(stored, 1);
    db.close().await.expect("drop the database");
}

/// topology.version-ready.first-count-kept
#[tokio::test(flavor = "multi_thread")]
async fn version_ready_keeps_first_count() {
    let Some(db) = database("version_ready_keeps_first_count").await else {
        return;
    };
    let mut world = world(db.pool().clone());
    let applied: Vec<EdgeContribution> = (1..=2)
        .map(|n| contribution(n, 1, 2, Route::Unobserved, 5, 1))
        .collect();
    for one in &applied {
        world.store.apply(one).await.expect("applied");
    }
    // Ready with 2, then a redelivery claiming 5: the first count is kept,
    // so the two refit classifications complete the version.
    let v1 = refit(&mut world, 100, &applied).await;
    world
        .store
        .version_ready(v1, 5)
        .await
        .expect("redelivered ready");
    assert_eq!(
        activate(&mut world, v1, 200).await,
        Activation::Switched {
            version: v1,
            previous: TopicModelVersion(0)
        }
    );
    assert_eq!(activate(&mut world, v1, 201).await, Activation::Ignored);
    world
        .store
        .drop_version(TopicModelVersion(0))
        .await
        .expect("dropped");
    assert_eq!(
        world.store.version_ready(TopicModelVersion(0), 1).await,
        Err(EdgeError::VersionNotRetained {
            version: TopicModelVersion(0)
        })
    );
    db.close().await.expect("drop the database");
}

/// A redelivered drill-down page and a cursor for another request.
#[tokio::test(flavor = "multi_thread")]
async fn drill_cursor_is_bound_to_its_request() {
    let Some(db) = database("drill_cursor_is_bound_to_its_request").await else {
        return;
    };
    let mut world = world(db.pool().clone());
    for n in 1..=3 {
        world
            .store
            .apply(&contribution(n, 1, 2, Route::Unobserved, n, 1))
            .await
            .expect("applied");
    }
    let edge = EdgeSelector::new(agent(1), agent(2), Route::Unobserved).expect("an edge");
    let size = PageSize::new(2).expect("a size");
    let first: PageRequest<EdgeTransmissionList> = PageRequest { size, after: None };
    let page = world
        .store
        .transmissions(&edge, window(0, 10), &TopologyFilter::default(), &first)
        .await
        .expect("a page");
    let (items, next) = page.value.page.into_parts();
    let rows: Vec<_> = items.iter().map(|row| row.transmission).collect();
    assert_eq!(rows, vec![transmission(3), transmission(2)]);
    let next = PageRequest {
        size,
        after: Some(next.expect("more to follow")),
    };
    let other = world
        .store
        .transmissions(&edge, window(0, 20), &TopologyFilter::default(), &next)
        .await;
    assert_eq!(other.err(), Some(EdgeQueryError::InvalidCursor));
    let rest = world
        .store
        .transmissions(&edge, window(0, 10), &TopologyFilter::default(), &next)
        .await
        .expect("the last page");
    let (items, next) = rest.value.page.into_parts();
    assert_eq!(
        items.iter().map(|row| row.transmission).collect::<Vec<_>>(),
        vec![transmission(1)]
    );
    assert!(next.is_none());
    db.close().await.expect("drop the database");
}
