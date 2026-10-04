//! Series against the graph, and their algebra.

use crosstalk_spec::aggregates::edge::{RouteKind, TopologyFilter, TopologyGraph, Weighting};
use crosstalk_spec::aggregates::series::{
    SeriesGrid, SeriesGrouping, SeriesGroups, SeriesStep, TopologySeries,
};
use crosstalk_spec::derived::flow::transmission::{DirectCarrier, Route};
use crosstalk_spec::interfaces::l7_topology::{EdgeContribution, EdgeQueryError, EdgeStore};
use crosstalk_spec::support::TimeWindow;

use super::support::{WIDTH, World, contribution, plain, refit, world};
use crate::model::build::{bucket_width, non_zero, topic_id, window};

fn grid(start: u64, end: u64, step: u64) -> SeriesGrid {
    SeriesGrid::new(
        window(start, end).unwrap(),
        SeriesStep::new(bucket_width(WIDTH), non_zero(step)).unwrap(),
    )
    .unwrap()
}

async fn series(
    world: &World,
    grid: SeriesGrid,
    weighting: Weighting,
    grouping: SeriesGrouping,
) -> TopologySeries {
    world
        .store
        .series(grid, weighting, grouping, &TopologyFilter::default())
        .await
        .unwrap()
        .value
}

async fn graph(world: &World, window: TimeWindow, weighting: Weighting) -> TopologyGraph {
    world
        .store
        .graph(window, weighting, &TopologyFilter::default())
        .await
        .unwrap()
        .value
}

fn traffic() -> Vec<EdgeContribution> {
    vec![
        plain(1, 1, 2, Route::Unobserved, 12, 3),
        plain(2, 1, 2, Route::Unobserved, 27, 5),
        plain(3, 2, 3, Route::Direct(DirectCarrier::UserTurn), 33, 7),
        plain(4, 2, 3, Route::Unobserved, 48, 2),
        plain(5, 3, 3, Route::Unobserved, 48, 2),
    ]
}

async fn loaded() -> World {
    let mut world = world();
    for one in traffic() {
        let _ = world.store.apply(&one).await;
    }
    world
}

#[tokio::test]
async fn series_rejects_grid_for_other_bucket_width() {
    // topology.series.rejects-bucket-width-mismatch
    let world = loaded().await;
    let other = SeriesGrid::new(
        window(0, 40).unwrap(),
        SeriesStep::new(bucket_width(20), non_zero(20)).unwrap(),
    )
    .unwrap();
    assert_eq!(
        world
            .store
            .series(
                other,
                Weighting::Transmissions,
                SeriesGrouping::Total,
                &TopologyFilter::default()
            )
            .await,
        Err(EdgeQueryError::BucketWidthMismatch {
            store: bucket_width(WIDTH),
            grid: bucket_width(20)
        })
    );
}

#[tokio::test]
async fn series_matches_reference_fold() {
    // topology.series.matches-fold-model, on fixed inputs
    let world = loaded().await;
    let total = series(
        &world,
        grid(0, 60, 20),
        Weighting::Transmissions,
        SeriesGrouping::Total,
    )
    .await;
    assert_eq!(*total.groups(), SeriesGroups::Total(vec![1, 2, 1]));
    let bytes = series(
        &world,
        grid(0, 60, 20),
        Weighting::MatchedBytes,
        SeriesGrouping::Total,
    )
    .await;
    assert_eq!(*bytes.groups(), SeriesGroups::Total(vec![3, 12, 2]));
    let by_kind = series(
        &world,
        grid(0, 60, 20),
        Weighting::Transmissions,
        SeriesGrouping::RouteKind,
    )
    .await;
    let SeriesGroups::ByRouteKind(kinds) = by_kind.groups() else {
        panic!("grouped by route kind");
    };
    let kinds: Vec<_> = kinds
        .iter()
        .map(|one| (one.key, one.values.clone()))
        .collect();
    assert_eq!(
        kinds,
        vec![
            (RouteKind::Direct, vec![0, 1, 0]),
            (RouteKind::Unobserved, vec![1, 1, 1])
        ]
    );
}

#[tokio::test]
async fn edge_series_sum_to_graph_edge_stats_and_total() {
    // topology.series.edge-sums-match-graph and total-matches-graph
    let world = loaded().await;
    for weighting in [Weighting::Transmissions, Weighting::MatchedBytes] {
        let by_edge = series(&world, grid(10, 50, 10), weighting, SeriesGrouping::Edge).await;
        let graph = graph(&world, window(10, 50).unwrap(), weighting).await;
        let SeriesGroups::ByEdge(edges) = by_edge.groups() else {
            panic!("grouped by edge");
        };
        assert_eq!(edges.len(), graph.edges().len());
        for (series, edge) in edges.iter().zip(graph.edges()) {
            assert_eq!(
                (series.key.from, series.key.to, &series.key.route),
                (edge.from, edge.to, &edge.route)
            );
            assert_eq!(series.sum(), weighting.stat(edge.stats).get());
        }
        for grouping in [
            SeriesGrouping::Total,
            SeriesGrouping::Topic,
            SeriesGrouping::RouteKind,
            SeriesGrouping::Edge,
        ] {
            let grouped = series(&world, grid(10, 50, 10), weighting, grouping).await;
            assert_eq!(grouped.total(), graph.total());
            assert_eq!(grouped.topic_version(), graph.topic_version());
        }
    }
}

#[tokio::test]
async fn series_concatenate_over_adjacent_grids_and_coarsen() {
    // topology.series.concatenates and step-coarsening
    let world = loaded().await;
    let whole = series(
        &world,
        grid(0, 60, 10),
        Weighting::Transmissions,
        SeriesGrouping::Total,
    )
    .await;
    let first = series(
        &world,
        grid(0, 30, 10),
        Weighting::Transmissions,
        SeriesGrouping::Total,
    )
    .await;
    let second = series(
        &world,
        grid(30, 60, 10),
        Weighting::Transmissions,
        SeriesGrouping::Total,
    )
    .await;
    let (SeriesGroups::Total(whole), SeriesGroups::Total(first), SeriesGroups::Total(second)) =
        (whole.groups(), first.groups(), second.groups())
    else {
        panic!("total series");
    };
    let joined: Vec<u64> = first.iter().chain(second).copied().collect();
    assert_eq!(*whole, joined);
    let coarse = series(
        &world,
        grid(0, 60, 30),
        Weighting::Transmissions,
        SeriesGrouping::Total,
    )
    .await;
    let SeriesGroups::Total(coarse) = coarse.groups() else {
        panic!("total series");
    };
    let summed: Vec<u64> = whole.chunks(3).map(|run| run.iter().sum()).collect();
    assert_eq!(*coarse, summed);
}

#[tokio::test]
async fn topic_series_follow_the_resolved_version() {
    let mut world = loaded().await;
    let refits = vec![
        contribution(1, 1, 2, Route::Unobserved, 12, 3, 1, Some(11)),
        contribution(2, 1, 2, Route::Unobserved, 27, 5, 1, Some(12)),
        contribution(
            3,
            2,
            3,
            Route::Direct(DirectCarrier::UserTurn),
            33,
            7,
            1,
            None,
        ),
    ];
    let v1 = refit(&mut world, 100, &[11, 12], &refits).await;
    let by_topic = series(
        &world,
        grid(0, 60, 20),
        Weighting::Transmissions,
        SeriesGrouping::Topic,
    )
    .await;
    assert_eq!(by_topic.topic_version(), v1);
    let SeriesGroups::ByTopic(topics) = by_topic.groups() else {
        panic!("grouped by topic");
    };
    let topics: Vec<_> = topics
        .iter()
        .map(|one| (one.key, one.values.clone()))
        .collect();
    assert_eq!(
        topics,
        vec![
            (None, vec![0, 1, 0]),
            (Some(topic_id(11)), vec![1, 0, 0]),
            (Some(topic_id(12)), vec![0, 1, 0]),
        ]
    );
}
