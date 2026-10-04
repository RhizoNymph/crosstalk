//! Series and the overview agree with the graph; every linked view refuses
//! what the spec refuses (permission, unaligned windows, another bucket
//! width, versions it cannot be computed under).

use std::collections::BTreeSet;
use std::num::NonZeroU64;

use crosstalk_spec::aggregates::edge::{EdgeTotals, Weighting};
use crosstalk_spec::aggregates::series::{
    BucketWidth, SeriesGrid, SeriesGrouping, SeriesGroups, SeriesStep,
};
use crosstalk_spec::aggregates::topic::TopicModelVersion;
use crosstalk_spec::derived::flow::transmission::Route;
use crosstalk_spec::interfaces::l8_surface::{
    AlertFilter, AlertStateKind, InputError, Permission, PolicyKind, QueryError,
};
use crosstalk_spec::support::TimeWindow;

use super::super::clock::{MINUTE, NOW, START};
use super::{caller, collect, day, first, graph_of, researcher, shared, week};
use crate::url::scope::{Scope, ViewFilter};
use crosstalk_spec::interfaces::l8_surface::QueryApi;
use crosstalk_spec::interfaces::l8_surface::lists::ChannelFilter;
use crosstalk_spec::interfaces::l8_surface::lists::SearchMode;

use super::reads_support::*;

#[tokio::test]
async fn series_totals_match_the_graph() {
    let b = shared();
    let c = researcher();
    for scope in [day(), week()] {
        let filter = scope.topology_filter();
        for weighting in [Weighting::Transmissions, Weighting::MatchedBytes] {
            let graph = graph_of(b, &c, &scope, weighting).await.expect("topology");
            let grid = grid(scope.window, 8);
            assert_eq!(grid.window(), scope.window, "8 steps divide the window");
            let series = b
                .series(&c, grid, weighting, SeriesGrouping::Total, &filter)
                .await
                .expect("series");
            assert_eq!(series.value.total(), graph.value.total());
            assert_eq!(series.value.topic_version(), graph.value.topic_version);
            assert_eq!(series.watermark, graph.watermark);
            let windows: Vec<TimeWindow> = series.value.grid().point_windows().collect();
            assert_eq!(
                windows.first().map(|w| w.start()),
                Some(scope.window.start())
            );
            assert_eq!(windows.last().map(|w| w.end()), Some(scope.window.end()));
        }
    }
}

#[tokio::test]
async fn grouped_series_sum_to_the_graph_and_its_edges() {
    let b = shared();
    let c = researcher();
    let scope = week();
    let filter = scope.topology_filter();
    let graph = graph_of(b, &c, &scope, Weighting::MatchedBytes)
        .await
        .expect("topology");
    for grouping in [
        SeriesGrouping::Topic,
        SeriesGrouping::RouteKind,
        SeriesGrouping::Edge,
    ] {
        let series = b
            .series(
                &c,
                grid(scope.window, 14),
                Weighting::MatchedBytes,
                grouping,
                &filter,
            )
            .await
            .expect("series");
        assert_eq!(series.value.groups().grouping(), grouping);
        assert_eq!(series.value.total(), graph.value.total(), "{grouping:?}");
    }
    let by_edge = b
        .series(
            &c,
            grid(scope.window, 14),
            Weighting::MatchedBytes,
            SeriesGrouping::Edge,
            &filter,
        )
        .await
        .expect("series");
    let SeriesGroups::ByEdge(series) = by_edge.value.groups() else {
        panic!("edge series")
    };
    assert_eq!(series.len(), graph.value.edges.len());
    for edge in &graph.value.edges {
        let one = series
            .iter()
            .find(|s| s.key.from == edge.from && s.key.to == edge.to && s.key.route == edge.route)
            .expect("one series per edge");
        assert_eq!(one.sum(), edge.stats.matched_bytes.get());
    }
}

#[tokio::test]
async fn a_coarser_step_sums_the_finer_points() {
    let b = shared();
    let c = researcher();
    let scope = day();
    let filter = scope.topology_filter();
    let totals = async |points: u32| -> Vec<u64> {
        let series = b
            .series(
                &c,
                grid(scope.window, points),
                Weighting::Transmissions,
                SeriesGrouping::Total,
                &filter,
            )
            .await
            .expect("series");
        match series.value.groups() {
            SeriesGroups::Total(values) => values.clone(),
            other => panic!("{other:?}"),
        }
    };
    let fine = totals(48).await;
    let coarse = totals(24).await;
    let summed: Vec<u64> = fine.chunks(2).map(|pair| pair.iter().sum()).collect();
    assert_eq!(coarse, summed);
}

#[tokio::test]
async fn a_grid_for_another_bucket_width_is_refused() {
    let minute = BucketWidth::from_micros(NonZeroU64::new(MINUTE).expect("minute"));
    let step = SeriesStep::new(minute, NonZeroU64::new(60 * MINUTE).expect("hour")).expect("step");
    let grid = SeriesGrid::new(day().window, step).expect("grid");
    assert_eq!(
        shared()
            .series(
                &researcher(),
                grid,
                Weighting::Transmissions,
                SeriesGrouping::Total,
                &day().topology_filter(),
            )
            .await
            .err(),
        Some(QueryError::InvalidInput(InputError::BucketWidthMismatch))
    );
}

#[tokio::test]
async fn unaligned_windows_are_refused() {
    let b = shared();
    let c = researcher();
    let window = TimeWindow::new(START, super::super::clock::minus(NOW, MINUTE)).expect("window");
    let filter = week().topology_filter();
    let expected = Some(QueryError::InvalidInput(InputError::UnalignedWindow));
    assert_eq!(
        b.topology(&c, window, Weighting::Transmissions, &filter)
            .await
            .err(),
        expected
    );
    assert_eq!(
        b.channel_topology(&c, window, Weighting::Transmissions, &filter)
            .await
            .err(),
        expected
    );
    assert_eq!(b.overview(&c, window, &filter).await.err(), expected);
}

#[tokio::test]
async fn every_linked_view_needs_view() {
    let b = shared();
    let content_only = caller(&[Permission::Content]);
    let scope = week();
    let filter = scope.topology_filter();
    let forbidden = Some(QueryError::Forbidden {
        missing: Permission::View,
    });
    assert_eq!(b.watermark(&content_only).await.err(), forbidden);
    assert_eq!(
        b.channel_topology(
            &content_only,
            scope.window,
            Weighting::Transmissions,
            &filter
        )
        .await
        .err(),
        forbidden
    );
    assert_eq!(
        b.overview(&content_only, scope.window, &filter).await.err(),
        forbidden
    );
    assert_eq!(
        b.series(
            &content_only,
            grid(scope.window, 4),
            Weighting::Transmissions,
            SeriesGrouping::Total,
            &filter,
        )
        .await
        .err(),
        forbidden
    );
}

#[tokio::test]
async fn unknown_topic_versions_are_typed_errors() {
    let b = shared();
    let c = researcher();
    let version = TopicModelVersion(9);
    let scope = Scope {
        topic_version: version,
        ..week()
    };
    let filter = scope.topology_filter();
    // A linked view resolves the filter's version: an unknown one is
    // `NotFound`.
    let unknown = Some(QueryError::NotFound);
    assert_eq!(
        graph_of(b, &c, &scope, Weighting::Transmissions)
            .await
            .err(),
        unknown
    );
    assert_eq!(
        b.channel_topology(&c, scope.window, Weighting::Transmissions, &filter)
            .await
            .err(),
        unknown
    );
    assert_eq!(
        b.series(
            &c,
            grid(scope.window, 4),
            Weighting::Transmissions,
            SeriesGrouping::Total,
            &filter,
        )
        .await
        .err(),
        unknown
    );
    assert_eq!(b.overview(&c, scope.window, &filter).await.err(), unknown);
    let edge = crosstalk_spec::aggregates::edge::EdgeSelector::new(
        agent("cc1"),
        agent("pi1"),
        crosstalk_spec::derived::flow::transmission::Route::Unobserved,
    )
    .expect("edge");
    assert_eq!(
        b.edge_transmissions(&c, &edge, scope.window, &filter, &first(5))
            .await
            .err(),
        unknown
    );
    let any = crosstalk_spec::interfaces::l8_surface::summary::TransmissionSelection::new(vec![
        b.world.transmissions[0].transmission.id,
    ])
    .expect("selection");
    assert_eq!(
        b.transmissions_by_id(
            &c,
            &any,
            crosstalk_spec::aggregates::filter::TopicVersionSelector::Pinned(version),
            &first(5)
        )
        .await
        .err(),
        unknown
    );
    assert_eq!(
        search_in(
            b,
            &c,
            &search("deploy", SearchMode::Text),
            &scope,
            &first(5)
        )
        .await
        .err(),
        unknown
    );
    assert_eq!(
        b.fit_projection(&c, scope.window, &filter, params(1, 10))
            .await
            .err(),
        unknown
    );
    // The catalog's reads: an unknown version is `NotFound` too.
    assert_eq!(
        b.topic_sizes(&c, Some(version), Some(scope.window))
            .await
            .err(),
        unknown
    );
    assert_eq!(
        b.topics(
            &c,
            crosstalk_spec::aggregates::filter::TopicVersionSelector::Pinned(version),
            &first(5)
        )
        .await
        .err(),
        unknown
    );
    assert_eq!(b.topic_lineage(&c, version).await.err(), unknown);
}

#[tokio::test]
async fn the_overview_counts_the_graph_and_the_queues() {
    let b = shared();
    let c = researcher();
    for scope in [day(), week()] {
        let filter = scope.topology_filter();
        let overview = b
            .overview(&c, scope.window, &filter)
            .await
            .expect("overview");
        let graph = graph_of(b, &c, &scope, Weighting::MatchedBytes)
            .await
            .expect("topology");
        assert_eq!(overview.value.activity, EdgeTotals::of(&graph.value));
        assert_eq!(overview.watermark, graph.watermark);
        let routed: BTreeSet<_> = graph
            .value
            .edges
            .iter()
            .filter_map(|e| match e.route {
                Route::Channel(channel) => Some(channel),
                _ => None,
            })
            .collect();
        assert_eq!(overview.value.activity.active_channels, routed.len() as u64);
    }
    // Queues are what the lists show, whatever the window or filter.
    let open = collect(500, async |p| {
        b.alerts(
            &c,
            &AlertFilter {
                states: vec![AlertStateKind::Open],
                channel: None,
            },
            &p,
        )
        .await
    })
    .await;
    let review = collect(500, async |p| {
        b.channels(
            &c,
            &ChannelFilter {
                policies: vec![PolicyKind::Unreviewed],
                ..ChannelFilter::default()
            },
            &p,
        )
        .await
        .map(|rows| rows.value)
    })
    .await;
    assert!(!open.is_empty() && !review.is_empty());
    let narrow = with(ViewFilter {
        agents: vec![agent("cc0")],
        ..Default::default()
    });
    for scope in [day(), narrow] {
        let queues = b
            .overview(&c, scope.window, &scope.topology_filter())
            .await
            .expect("overview")
            .value
            .queues;
        assert_eq!(queues.open_alerts, open.len() as u64);
        assert_eq!(queues.unreviewed_channels, review.len() as u64);
    }
}
