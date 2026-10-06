//! One behaviour each, against a real Postgres.

use std::sync::Mutex;

use crosstalk_memory::model::build::{agent, transmission, ts};
use crosstalk_spec::aggregates::edge::{EdgeSelector, TopologyFilter, Weighting};
use crosstalk_spec::aggregates::filter::{TopicVersionSelector, VersionUnavailable};
use crosstalk_spec::aggregates::topic::TopicModelVersion;
use crosstalk_spec::derived::flow::transmission::Route;
use crosstalk_spec::events::BusEvent;
use crosstalk_spec::events::changed::Changed;
use crosstalk_spec::events::insight::InsightEvent;
use crosstalk_spec::interfaces::l7_topology::{Activation, EdgeError, EdgeQueryError, EdgeStore};
use crosstalk_spec::paging::{EdgeTransmissionList, PageRequest, PageSize};
use crosstalk_spec::support::Watermark;

use crate::integration::outbox_ids;
use crate::outbox::{Announce, drain};
use crate::tests::support::{contribution, database, edge_counts, frontier, graph, window, world};
use crate::tests::{activate, buckets, count, refit};

/// Every event announced, in order.
#[derive(Debug, Default)]
struct Recorder {
    events: Mutex<Vec<BusEvent>>,
}

impl Announce for Recorder {
    async fn announce(
        &self,
        envelope: crosstalk_spec::events::Envelope,
    ) -> Result<(), crosstalk_spec::interfaces::l2_transport::BusError> {
        self.events
            .lock()
            .expect("not poisoned")
            .push(envelope.event);
        Ok(())
    }
}

/// topology.apply.idempotent
#[tokio::test(flavor = "multi_thread")]
async fn pg_apply_redelivery_counts_once() {
    let Some(db) = database("pg_apply_redelivery_counts_once").await else {
        return;
    };
    let mut world = world(db.pool().clone());
    let one = contribution(
        1,
        1,
        2,
        crosstalk_spec::derived::flow::transmission::Route::Unobserved,
        5,
        9,
    );
    let key = world.store.apply(&one).await.expect("applied");
    let stored = buckets(db.pool()).await;
    for _ in 0..3 {
        assert_eq!(world.store.apply(&one).await, Ok(key.clone()));
    }
    assert_eq!(buckets(db.pool()).await, stored);
    assert_eq!(stored, vec![(0, 0, 1, 9)]);
    // One traffic row: only the first apply changed a bucket.
    assert_eq!(count(db.pool(), "outbox").await, 1);
    db.close().await.expect("drop the database");
}

/// topology.apply.no-lost-updates
#[tokio::test(flavor = "multi_thread")]
async fn pg_concurrent_applies_same_bucket() {
    let Some(db) = database("pg_concurrent_applies_same_bucket").await else {
        return;
    };
    let world = world(db.pool().clone());
    let mut tasks = Vec::new();
    for n in 1..=24u64 {
        let mut store = world.store.clone();
        tasks.push(tokio::spawn(async move {
            store
                .apply(&contribution(n, 1, 2, Route::Unobserved, n % 10, n))
                .await
        }));
    }
    for task in tasks {
        task.await.expect("joined").expect("applied");
    }
    let bytes: i64 = (1..=24).sum();
    assert_eq!(buckets(db.pool()).await, vec![(0, 0, 24, bytes)]);
    let graph = graph(&world.store, window(0, 10), &TopologyFilter::default()).await;
    assert_eq!(
        edge_counts(&graph),
        vec![(
            agent(1),
            agent(2),
            24,
            u64::try_from(bytes).expect("positive")
        )]
    );
    db.close().await.expect("drop the database");
}

/// topology.graph.reflects-completed-applies: an apply that returned is in
/// the next graph, with no refresh in between.
#[tokio::test(flavor = "multi_thread")]
async fn pg_graph_reads_apply_immediately() {
    let Some(db) = database("pg_graph_reads_apply_immediately").await else {
        return;
    };
    let mut world = world(db.pool().clone());
    for n in 1..=3u64 {
        world
            .store
            .apply(&contribution(n, 1, 2, Route::Unobserved, 10 * n, 2))
            .await
            .expect("applied");
        let graph = graph(&world.store, window(0, 100), &TopologyFilter::default()).await;
        assert_eq!(edge_counts(&graph), vec![(agent(1), agent(2), n, 2 * n)]);
    }
    db.close().await.expect("drop the database");
}

/// topology.graph.reflects-completed-applies: a contribution into a bucket
/// already read (and, for a version not yet activated, one the watermark
/// has passed) counts in the next read.
#[tokio::test(flavor = "multi_thread")]
async fn pg_late_contribution_into_read_bucket_counts() {
    let Some(db) = database("pg_late_contribution_into_read_bucket_counts").await else {
        return;
    };
    let mut world = world(db.pool().clone());
    let first = contribution(1, 1, 2, Route::Unobserved, 3, 1);
    world.store.apply(&first).await.expect("applied");
    let read = graph(&world.store, window(0, 10), &TopologyFilter::default()).await;
    assert_eq!(edge_counts(&read), vec![(agent(1), agent(2), 1, 1)]);
    world
        .store
        .apply(&contribution(2, 1, 2, Route::Unobserved, 7, 1))
        .await
        .expect("applied");
    let read = graph(&world.store, window(0, 10), &TopologyFilter::default()).await;
    assert_eq!(edge_counts(&read), vec![(agent(1), agent(2), 2, 2)]);
    // The watermark passes the bucket; a re-fit's classification into it
    // still lands (its version is not activated), and counts once active.
    world
        .store
        .advance_watermark(frontier(60))
        .await
        .expect("advanced");
    let v1 = refit(
        &mut world,
        100,
        &[first, contribution(2, 1, 2, Route::Unobserved, 7, 1)],
    )
    .await;
    assert!(matches!(
        activate(&mut world, v1, 200).await,
        Activation::Switched { .. }
    ));
    let read = graph(&world.store, window(0, 10), &TopologyFilter::default()).await;
    assert_eq!(read.topic_version(), v1);
    assert_eq!(edge_counts(&read), vec![(agent(1), agent(2), 2, 2)]);
    db.close().await.expect("drop the database");
}

/// topology.graph.stats-monotone
#[tokio::test(flavor = "multi_thread")]
async fn pg_graph_stats_never_decrease() {
    let Some(db) = database("pg_graph_stats_never_decrease").await else {
        return;
    };
    let mut world = world(db.pool().clone());
    let mut previous = Vec::new();
    for n in 1..=12u64 {
        let one = contribution(n, n % 3, 1 + n % 4, Route::Unobserved, (n * 7) % 50, n);
        let _ = world.store.apply(&one).await;
        // A redelivery between reads changes nothing.
        let _ = world.store.apply(&one).await;
        let current =
            edge_counts(&graph(&world.store, window(0, 50), &TopologyFilter::default()).await);
        for (from, to, transmissions, bytes) in &previous {
            let later = current
                .iter()
                .find(|edge| edge.0 == *from && edge.1 == *to)
                .expect("an edge never disappears");
            assert!(later.2 >= *transmissions && later.3 >= *bytes);
        }
        previous = current;
    }
    db.close().await.expect("drop the database");
}

/// topology.transmissions.match-graph
#[tokio::test(flavor = "multi_thread")]
async fn pg_edge_transmissions_match_graph() {
    let Some(db) = database("pg_edge_transmissions_match_graph").await else {
        return;
    };
    let mut world = world(db.pool().clone());
    for n in 1..=9u64 {
        let (from, to) = if n % 3 == 0 { (5, 6) } else { (1, 2) };
        world
            .store
            .apply(&contribution(n, from, to, Route::Unobserved, n * 3, n))
            .await
            .expect("applied");
    }
    world.directory.merge(agent(5), agent(1)).expect("a merge");
    let read = graph(&world.store, window(0, 30), &TopologyFilter::default()).await;
    for edge in read.edges() {
        let selector = EdgeSelector::new(edge.from, edge.to, edge.route.clone()).expect("an edge");
        let mut request: PageRequest<EdgeTransmissionList> = PageRequest {
            size: PageSize::new(2).expect("a size"),
            after: None,
        };
        let mut listed = Vec::new();
        loop {
            let page = world
                .store
                .transmissions(
                    &selector,
                    window(0, 30),
                    &TopologyFilter::default(),
                    &request,
                )
                .await
                .expect("a page");
            let (items, next) = page.value.page.into_parts();
            listed.extend(items);
            match next {
                Some(cursor) => request.after = Some(cursor),
                None => break,
            }
        }
        let transmissions = u64::try_from(listed.len()).expect("a count");
        let bytes: u64 = listed.iter().map(|row| row.matched_bytes.get()).sum();
        assert_eq!(transmissions, edge.stats.transmissions.get());
        assert_eq!(bytes, edge.stats.matched_bytes.get());
        let mut ids: Vec<_> = listed.iter().map(|row| row.transmission).collect();
        ids.dedup();
        assert_eq!(ids.len(), listed.len(), "each transmission once");
    }
    // 1 → 2 and 5 → 6 (5 now 1): two edges, every transmission listed.
    assert_eq!(read.edges().len(), 2);
    let _ = transmission(0);
    db.close().await.expect("drop the database");
}

/// topology.retention.drop-removes-all
#[tokio::test(flavor = "multi_thread")]
async fn pg_drop_version_removes_buckets() {
    let Some(db) = database("pg_drop_version_removes_buckets").await else {
        return;
    };
    let mut world = world(db.pool().clone());
    let applied: Vec<_> = (1..=4u64)
        .map(|n| contribution(n, 1, 2, Route::Unobserved, n * 5, 1))
        .collect();
    for one in &applied {
        world.store.apply(one).await.expect("applied");
    }
    let edge = EdgeSelector::new(agent(1), agent(2), Route::Unobserved).expect("an edge");
    let pinned = TopologyFilter {
        topic_version: TopicVersionSelector::Pinned(TopicModelVersion(0)),
        ..TopologyFilter::default()
    };
    let size = PageSize::new(1).expect("a size");
    let first = world
        .store
        .transmissions(
            &edge,
            window(0, 30),
            &pinned,
            &PageRequest { size, after: None },
        )
        .await
        .expect("a first page");
    let cursor = first.value.page.next().cloned().expect("more to follow");
    let v1 = refit(&mut world, 100, &applied).await;
    assert!(matches!(
        activate(&mut world, v1, 200).await,
        Activation::Switched { .. }
    ));
    world
        .store
        .drop_version(TopicModelVersion(0))
        .await
        .expect("dropped");
    world
        .store
        .drop_version(TopicModelVersion(0))
        .await
        .expect("dropping twice is a no-op");
    let left: i64 = sqlx::query_scalar(
        "SELECT (SELECT count(*) FROM topology.edge_buckets WHERE version = 0) \
         + (SELECT count(*) FROM topology.contributions WHERE version = 0) \
         + (SELECT count(*) FROM topology.refit_processed WHERE version = 0)",
    )
    .fetch_one(db.pool())
    .await
    .expect("a count");
    assert_eq!(left, 0);
    let gone = Err(EdgeError::VersionNotRetained {
        version: TopicModelVersion(0),
    });
    assert_eq!(world.store.apply(&applied[0]).await.map(|_| ()), gone);
    let not_retained =
        EdgeQueryError::Version(VersionUnavailable::NotRetained(TopicModelVersion(0)));
    let read = world
        .store
        .graph(window(0, 30), Weighting::Transmissions, &pinned)
        .await;
    assert_eq!(read.err(), Some(not_retained.clone()));
    let next = world
        .store
        .transmissions(
            &edge,
            window(0, 30),
            &pinned,
            &PageRequest {
                size,
                after: Some(cursor),
            },
        )
        .await;
    assert_eq!(next.err(), Some(not_retained));
    // The active version still has every bucket.
    let current = graph(&world.store, window(0, 30), &TopologyFilter::default()).await;
    assert_eq!(edge_counts(&current), vec![(agent(1), agent(2), 4, 4)]);
    db.close().await.expect("drop the database");
}

/// The outbox: activation and watermark events are relayed after commit,
/// in commit order, and the outbox is empty afterwards; traffic rows are
/// drained and coalesced.
#[tokio::test(flavor = "multi_thread")]
async fn pg_outbox_relays_committed_events_in_order() {
    let Some(db) = database("pg_outbox_relays_committed_events_in_order").await else {
        return;
    };
    let mut world = world(db.pool().clone());
    let one = contribution(1, 1, 2, Route::Unobserved, 5, 1);
    world.store.apply(&one).await.expect("applied");
    world
        .store
        .advance_watermark(frontier(40))
        .await
        .expect("advanced");
    let v1 = refit(&mut world, 100, std::slice::from_ref(&one)).await;
    assert!(matches!(
        activate(&mut world, v1, 200).await,
        Activation::Switched { .. }
    ));
    world
        .store
        .advance_watermark(frontier(60))
        .await
        .expect("advanced");
    // Not later: nothing published.
    assert_eq!(world.store.advance_watermark(frontier(50)).await, Ok(None));
    let recorder = Recorder::default();
    let mut ids = outbox_ids(1);
    let removed = drain(db.pool(), &mut ids, &recorder)
        .await
        .expect("drained");
    assert_eq!(removed, 7, "two traffic rows and five events");
    let events = std::mem::take(&mut *recorder.events.lock().expect("not poisoned"));
    let twenty = Watermark(ts(20));
    let forty = Watermark(ts(40));
    assert_eq!(
        events,
        vec![
            BusEvent::Insight(InsightEvent::WatermarkAdvanced(twenty)),
            BusEvent::Changed(Changed::Watermark(twenty)),
            BusEvent::Insight(InsightEvent::TopicVersionActivated {
                version: v1,
                previous: TopicModelVersion(0),
            }),
            BusEvent::Insight(InsightEvent::WatermarkAdvanced(forty)),
            BusEvent::Changed(Changed::Watermark(forty)),
        ]
    );
    assert_eq!(count(db.pool(), "outbox").await, 0);
    assert_eq!(
        drain(db.pool(), &mut ids, &recorder)
            .await
            .expect("drained"),
        0
    );
    db.close().await.expect("drop the database");
}

/// The watermark is persisted with the buckets: a store rebuilt on the same
/// database reads what the last one exposed, and never lowers it.
#[tokio::test(flavor = "multi_thread")]
async fn pg_watermark_survives_restart() {
    let Some(db) = database("pg_watermark_survives_restart").await else {
        return;
    };
    let mut first = world(db.pool().clone());
    first
        .store
        .advance_watermark(frontier(70))
        .await
        .expect("advanced");
    drop(first);
    let mut second = world(db.pool().clone());
    assert_eq!(second.store.watermark().await, Ok(Watermark(ts(50))));
    assert_eq!(second.store.advance_watermark(frontier(30)).await, Ok(None));
    assert_eq!(second.store.watermark().await, Ok(Watermark(ts(50))));
    db.close().await.expect("drop the database");
}
