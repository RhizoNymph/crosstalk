use std::num::NonZeroU64;

use crate::aggregates::edge::{
    EdgeStats, RouteKind, TopologyGraph, TopologyGraphParts, WeightedEdge, Weighting,
};
use crate::aggregates::series::{
    BucketWidth, InvalidGrid, InvalidSeries, InvalidStep, Series, SeriesEdge, SeriesGrid,
    SeriesGrouping, SeriesGroups, SeriesStep, TopologySeries,
};
use crate::aggregates::topic::TopicModelVersion;
use crate::derived::flow::transmission::{DelegationDirection, DirectCarrier, Route};
use crate::ids::TopicId;
use crate::support::{Share, TimeWindow};
use crate::tests::fixtures::{agent, agent_node, at, channel};

fn n(value: u64) -> NonZeroU64 {
    NonZeroU64::new(value).expect("fixture values are non-zero")
}

/// 10-microsecond buckets.
fn width() -> BucketWidth {
    BucketWidth::from_micros(n(10))
}

fn step(micros: u64) -> SeriesStep {
    SeriesStep::new(width(), n(micros)).expect("multiple of the bucket width")
}

fn window(start: u64, end: u64) -> TimeWindow {
    TimeWindow::new(at(start), at(end)).expect("start < end")
}

/// Four points of 30 microseconds from 60.
fn grid() -> SeriesGrid {
    SeriesGrid::new(window(60, 180), step(30)).expect("aligned, whole steps")
}

fn series_of(groups: SeriesGroups) -> Result<TopologySeries, InvalidSeries> {
    TopologySeries::new(
        grid(),
        Weighting::Transmissions,
        TopicModelVersion(1),
        groups,
    )
}

fn edge(from: u128, to: u128) -> SeriesEdge {
    SeriesEdge {
        from: agent(from),
        to: agent(to),
        route: Route::Channel(channel(1)),
    }
}

#[test]
fn bucket_width_boundaries_are_multiples_from_the_epoch() {
    assert!(width().is_boundary(at(0)));
    assert!(width().is_boundary(at(40)));
    assert!(!width().is_boundary(at(45)));
}

#[test]
fn step_accepts_bucket_multiples() {
    let step = step(30);
    assert_eq!(step.as_micros(), n(30));
    assert_eq!(step.buckets_per_step(), n(3));
    assert_eq!(
        SeriesStep::new(width(), n(10)).map(SeriesStep::buckets_per_step),
        Ok(n(1))
    );
}

#[test]
fn step_rejects_non_multiple_of_bucket() {
    assert_eq!(
        SeriesStep::new(width(), n(25)),
        Err(InvalidStep::NotBucketMultiple {
            bucket: n(10),
            step: n(25)
        })
    );
    assert!(SeriesStep::new(width(), n(5)).is_err());
}

#[test]
fn grid_rejects_unaligned_start() {
    assert_eq!(
        SeriesGrid::new(window(65, 185), step(30)),
        Err(InvalidGrid::UnalignedStart)
    );
}

#[test]
fn grid_rejects_partial_last_step() {
    assert_eq!(
        SeriesGrid::new(window(60, 170), step(30)),
        Err(InvalidGrid::PartialStep {
            remainder_micros: 20
        })
    );
    // Aligned to buckets but not to the step.
    assert!(SeriesGrid::new(window(60, 100), step(30)).is_err());
}

#[test]
fn grid_bounds_point_count() {
    let max = u64::from(SeriesGrid::MAX_POINTS);
    let at_max = SeriesGrid::new(window(0, max * 10), step(10)).expect("exactly MAX_POINTS");
    assert_eq!(at_max.points().get(), SeriesGrid::MAX_POINTS);
    assert_eq!(
        SeriesGrid::new(window(0, (max + 1) * 10), step(10)),
        Err(InvalidGrid::TooManyPoints { points: max + 1 })
    );
}

#[test]
fn grid_points_tile_the_window() {
    let grid = grid();
    assert_eq!(grid.points().get(), 4);
    let windows: Vec<TimeWindow> = grid.point_windows().collect();
    assert_eq!(
        windows,
        vec![
            window(60, 90),
            window(90, 120),
            window(120, 150),
            window(150, 180)
        ]
    );
    assert_eq!(grid.point_window(4), None);
    assert!(windows.iter().all(|w| width().is_boundary(w.start())));
}

#[test]
fn series_rejects_wrong_point_count() {
    assert_eq!(
        series_of(SeriesGroups::Total(vec![1, 2, 3])),
        Err(InvalidSeries::WrongPointCount {
            expected: 4,
            got: 3
        })
    );
    assert_eq!(
        series_of(SeriesGroups::ByRouteKind(vec![Series {
            key: RouteKind::Direct,
            values: vec![1, 0, 0, 0, 0],
        }])),
        Err(InvalidSeries::WrongPointCount {
            expected: 4,
            got: 5
        })
    );
}

#[test]
fn series_total_may_be_all_zero() {
    let series = series_of(SeriesGroups::Total(vec![0; 4])).expect("total of nothing");
    assert_eq!(series.total(), 0);
    assert_eq!(series.groups().grouping(), SeriesGrouping::Total);
}

#[test]
fn series_rejects_duplicate_keys() {
    let topic = Some(TopicId::from_ulid(7));
    assert_eq!(
        series_of(SeriesGroups::ByTopic(vec![
            Series {
                key: topic,
                values: vec![1, 0, 0, 0],
            },
            Series {
                key: topic,
                values: vec![0, 1, 0, 0],
            },
        ])),
        Err(InvalidSeries::DuplicateKey)
    );
}

#[test]
fn series_rejects_all_zero_group() {
    assert_eq!(
        series_of(SeriesGroups::ByTopic(vec![Series {
            key: None,
            values: vec![0; 4],
        }])),
        Err(InvalidSeries::ZeroSeries)
    );
}

#[test]
fn series_rejects_self_edge() {
    assert_eq!(
        series_of(SeriesGroups::ByEdge(vec![Series {
            key: edge(1, 1),
            values: vec![1, 0, 0, 0],
        }])),
        Err(InvalidSeries::SelfEdge)
    );
}

#[test]
fn series_total_sums_every_group() {
    let series = series_of(SeriesGroups::ByTopic(vec![
        Series {
            key: Some(TopicId::from_ulid(1)),
            values: vec![1, 0, 2, 0],
        },
        Series {
            key: None,
            values: vec![0, 0, 0, 4],
        },
    ]))
    .expect("valid grouped series");
    assert_eq!(series.total(), 7);
    assert_eq!(series.groups().grouping(), SeriesGrouping::Topic);
    assert_eq!(series.topic_version(), TopicModelVersion(1));
}

#[test]
fn edge_series_sum_to_graph_stats_and_total() {
    let stats = |transmissions, matched_bytes| EdgeStats {
        transmissions: n(transmissions),
        matched_bytes: n(matched_bytes),
    };
    let graph = TopologyGraph::new(TopologyGraphParts {
        window: grid().window(),
        weighting: Weighting::MatchedBytes,
        topic_version: TopicModelVersion(1),
        nodes: vec![agent_node(1, 1, 2), agent_node(2, 2, 1)],
        edges: vec![
            WeightedEdge {
                from: agent(1),
                to: agent(2),
                route: Route::Channel(channel(1)),
                stats: stats(2, 30),
                share: Share::new(0.75).expect("in range"),
            },
            WeightedEdge {
                from: agent(2),
                to: agent(1),
                route: Route::Channel(channel(1)),
                stats: stats(1, 10),
                share: Share::new(0.25).expect("in range"),
            },
        ],
    })
    .expect("a valid graph");
    let series = TopologySeries::new(
        grid(),
        Weighting::MatchedBytes,
        TopicModelVersion(1),
        SeriesGroups::ByEdge(vec![
            Series {
                key: edge(1, 2),
                values: vec![10, 0, 20, 0],
            },
            Series {
                key: edge(2, 1),
                values: vec![0, 10, 0, 0],
            },
        ]),
    )
    .expect("valid edge series");
    assert_eq!(graph.total(), 40);
    assert_eq!(series.total(), graph.total());
    let SeriesGroups::ByEdge(edges) = series.groups() else {
        panic!("grouped by edge");
    };
    for (one, graph_edge) in edges.iter().zip(graph.edges()) {
        assert_eq!(one.sum(), graph.weighting().stat(graph_edge.stats).get());
    }
}

#[test]
fn weighting_picks_its_stat() {
    let stats = EdgeStats {
        transmissions: n(3),
        matched_bytes: n(120),
    };
    assert_eq!(Weighting::Transmissions.stat(stats), n(3));
    assert_eq!(Weighting::MatchedBytes.stat(stats), n(120));
}

#[test]
fn route_kind_of_each_route() {
    assert_eq!(
        RouteKind::of(&Route::Channel(channel(1))),
        RouteKind::Channel
    );
    assert_eq!(
        RouteKind::of(&Route::Delegation(DelegationDirection::ParentToChild)),
        RouteKind::Delegation
    );
    assert_eq!(
        RouteKind::of(&Route::Direct(DirectCarrier::UserTurn)),
        RouteKind::Direct
    );
    assert_eq!(RouteKind::of(&Route::Unobserved), RouteKind::Unobserved);
}
