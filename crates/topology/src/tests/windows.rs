//! Windows past the `bigint` time columns (`crosstalk_store::ColumnWindow`):
//! a window ending past `i64::MAX` reads what a bounded window covering the
//! same buckets and contributions does, a drill-down's cursors still bound
//! to it; one starting past the column reads an empty graph, zero series and
//! an empty drill-down, not an error.

use std::num::NonZeroU64;

use crosstalk_memory::model::build::{agent, bucket_width};
use crosstalk_spec::aggregates::edge::{EdgeSelector, TopologyFilter, Weighting};
use crosstalk_spec::aggregates::series::{SeriesGrid, SeriesGrouping, SeriesGroups, SeriesStep};
use crosstalk_spec::derived::flow::access::AccessKind;
use crosstalk_spec::derived::flow::transmission::{DirectCarrier, Route};
use crosstalk_spec::ids::{AccessId, TransmissionId};
use crosstalk_spec::interfaces::l7_topology::{AccessContribution, EdgeStore};
use crosstalk_spec::paging::{EdgeTransmissionList, PageRequest, PageSize};
use crosstalk_spec::support::TimeWindow;

use super::support::{Store, contribution, database, window, world};

/// The first bucket boundary (10 µs buckets) a `bigint` column cannot hold.
const PAST: u64 = 9_223_372_036_854_775_810;
/// The last bucket boundary below `u64::MAX`.
const LAST: u64 = u64::MAX - 5;
/// An instant after every fixture, within the column.
const AFTER: u64 = 1_000;

fn access(n: u128, agent_n: u64, op: AccessKind, at: u64) -> AccessContribution {
    AccessContribution {
        access: AccessId::from_ulid(0xACC0 | n),
        agent: agent(agent_n),
        resource: crosstalk_memory::model::build::resource(1),
        op,
        at: crosstalk_memory::model::build::ts(at),
    }
}

/// Everything the window-shaped reads of `store` return for `window`,
/// without the window itself (which differs between the windows compared).
async fn reads(store: &Store, window: TimeWindow) -> String {
    let filter = TopologyFilter::default();
    let graph = store
        .graph(window, Weighting::Transmissions, &filter)
        .await
        .expect("a graph")
        .value;
    let bipartite = store
        .channel_topology(window, Weighting::MatchedBytes, &filter)
        .await
        .expect("a channel topology")
        .value;
    let traffic = store
        .agent_traffic(window, &[agent(1), agent(2), agent(3)])
        .await
        .expect("agent traffic")
        .value;
    format!(
        "{:?}\n{:?}\n{:?}\n{:?}\n{:?}\n{traffic:?}",
        graph.nodes(),
        graph.edges(),
        bipartite.nodes(),
        bipartite.accesses(),
        bipartite.transmissions(),
    )
}

/// Every row of the drill-down of 1 → 2 in `window`, in pages of `size`.
async fn drilled(store: &Store, window: TimeWindow, size: u16) -> Vec<TransmissionId> {
    let edge = EdgeSelector::new(agent(1), agent(2), Route::Unobserved).expect("an edge");
    let size = PageSize::new(size).expect("a size");
    let mut request: PageRequest<EdgeTransmissionList> = PageRequest { size, after: None };
    let mut rows = Vec::new();
    loop {
        let page = store
            .transmissions(&edge, window, &TopologyFilter::default(), &request)
            .await
            .expect("a page");
        let (items, next) = page.value.page.into_parts();
        rows.extend(items.iter().map(|row| row.transmission));
        match next {
            Some(next) => request.after = Some(next),
            None => return rows,
        }
    }
}

/// The total series of `store` over `points` steps of `step` µs from
/// `start`.
async fn series(store: &Store, start: u64, step: u64, points: u64) -> Vec<u64> {
    let step = SeriesStep::new(bucket_width(10), NonZeroU64::new(step).expect("non-zero"))
        .expect("a step");
    let grid = SeriesGrid::new(window(start, start + step.as_micros().get() * points), step)
        .expect("a grid");
    let series = store
        .series(
            grid,
            Weighting::Transmissions,
            SeriesGrouping::Total,
            &TopologyFilter::default(),
        )
        .await
        .expect("a series")
        .value;
    match series.groups() {
        SeriesGroups::Total(values) => values.clone(),
        other => panic!("not a total series: {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn pg_reads_take_an_end_past_the_column_as_unbounded() {
    let Some(db) = database("pg_reads_take_an_end_past_the_column_as_unbounded").await else {
        return;
    };
    let mut world = world(db.pool().clone());
    for (n, from, to, route, at) in [
        (1, 1, 2, Route::Unobserved, 5),
        (2, 1, 2, Route::Unobserved, 15),
        (3, 1, 2, Route::Unobserved, 15),
        (4, 1, 3, Route::Direct(DirectCarrier::UserTurn), 25),
    ] {
        world
            .store
            .apply(&contribution(n, from, to, route, at, n))
            .await
            .expect("applied");
    }
    for (n, agent_n, op, at) in [(1, 1, AccessKind::Write, 12), (2, 2, AccessKind::Read, 27)] {
        world
            .store
            .apply_access(&access(n, agent_n, op, at))
            .await
            .expect("applied");
    }
    let store = &world.store;
    for start in [0, 10] {
        let want = reads(store, window(start, AFTER)).await;
        let drill = drilled(store, window(start, AFTER), 100).await;
        assert!(!drill.is_empty(), "drilled from {start}");
        for end in [PAST, LAST] {
            assert_eq!(
                reads(store, window(start, end)).await,
                want,
                "{start}..{end}"
            );
        }
        for size in [1, 100] {
            assert_eq!(
                drilled(store, window(start, u64::MAX), size).await,
                drill,
                "drilled from {start} at page size {size}"
            );
        }
    }
    // 9_224 steps of 10^15 µs end past the column; 9_000 do not.
    let step = 1_000_000_000_000_000;
    let bounded = series(store, 0, step, 9_000).await;
    assert_ne!(bounded[0], 0, "a series");
    let unbounded = series(store, 0, step, 9_224).await;
    assert_eq!(unbounded[..9_000], bounded[..]);
    assert!(unbounded[9_000..].iter().all(|value| *value == 0));
    // One step longer than the column: every row in its one point.
    let whole = series(store, 0, PAST, 1).await;
    assert_eq!(whole, vec![bounded.iter().sum::<u64>()]);

    // Past the column: what an empty window inside it reads.
    let empty = reads(store, window(AFTER, AFTER + 10)).await;
    for (start, end) in [(PAST, PAST + 10), (PAST, LAST), (LAST - 10, LAST)] {
        assert_eq!(
            reads(store, window(start, end)).await,
            empty,
            "{start}..{end}"
        );
        assert_eq!(drilled(store, window(start, end), 100).await, Vec::new());
        assert_eq!(series(store, start, 10, 1).await, vec![0], "{start}..{end}");
    }
    assert_eq!(
        drilled(store, window(u64::MAX - 1, u64::MAX), 100).await,
        Vec::new()
    );
    db.close().await.expect("drop the database");
}
