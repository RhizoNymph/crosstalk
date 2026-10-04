//! Series and the overview agree with the graph, and every watermarked
//! read carries the watermark the surface reports.

use std::collections::BTreeSet;
use std::num::NonZeroU64;

use crosstalk_spec::aggregates::edge::{EdgeTotals, TopologyFilter, Weighting};
use crosstalk_spec::aggregates::filter::UnconfirmedChannels;
use crosstalk_spec::aggregates::series::{
    BucketWidth, SeriesGrid, SeriesGrouping, SeriesGroups, SeriesStep, TopologySeries,
};
use crosstalk_spec::derived::flow::channel::confirmation::{Confirmation, ListingKind};
use crosstalk_spec::derived::flow::transmission::Route;
use crosstalk_spec::interfaces::l8_surface::lists::ChannelFilter;
use crosstalk_spec::interfaces::l8_surface::{
    AlertFilter, AlertStateKind, InputError, PolicyKind, QueryApi, QueryError,
};
use crosstalk_spec::support::{TimeWindow, Timestamp};

use crate::harness::Harness;
use crate::scenario::named::{hijacked_wiki, merges, suspected};
use crate::support::reads::{alerts, channel_rows, graph};
use crate::support::windows::{grid, points, window};
use crate::support::{World, first};

async fn series<H: Harness>(
    w: &World<'_, H>,
    grid: SeriesGrid,
    weighting: Weighting,
    grouping: SeriesGrouping,
) -> crosstalk_spec::aggregates::watermark::Watermarked<TopologySeries> {
    w.backend
        .series(
            &w.lead,
            grid,
            weighting,
            grouping,
            &TopologyFilter::default(),
        )
        .await
        .unwrap_or_else(|e| panic!("series: {e:?}"))
}

/// A series' total is the graph's over the grid's window, under the same
/// version and watermark, and its points tile the window (INV-446,
/// INV-433, INV-438).
pub async fn series_totals_match_the_graph<H: Harness>(h: &H) {
    let w = World::everything(h).await;
    for span in [w.day(), w.extent] {
        for weighting in [Weighting::Transmissions, Weighting::MatchedBytes] {
            let g = grid(w.bucket, span, points(8));
            let s = series(&w, g, weighting, SeriesGrouping::Total).await;
            let topology = w
                .backend
                .topology(&w.lead, g.window(), weighting, &TopologyFilter::default())
                .await
                .expect("topology");
            assert_eq!(s.value.total(), topology.value.total());
            assert_eq!(s.value.topic_version(), topology.value.topic_version);
            assert_eq!(s.watermark, topology.watermark);
            let windows: Vec<TimeWindow> = s.value.grid().point_windows().collect();
            assert_eq!(windows.first().map(|p| p.start()), Some(g.window().start()));
            assert_eq!(windows.last().map(|p| p.end()), Some(g.window().end()));
            assert!(windows.windows(2).all(|p| p[0].end() == p[1].start()));
        }
    }
}

/// Grouped by topic, route kind or edge, a series sums to the graph; by
/// edge there is one series per graph edge summing to it (INV-437).
pub async fn grouped_series_sum_to_the_graph_and_its_edges<H: Harness>(h: &H) {
    let w = World::everything(h).await;
    let g = grid(w.bucket, w.extent, points(14));
    let topology = graph(
        &w.backend,
        &w.lead,
        g.window(),
        Weighting::MatchedBytes,
        &TopologyFilter::default(),
    )
    .await;
    for grouping in [
        SeriesGrouping::Topic,
        SeriesGrouping::RouteKind,
        SeriesGrouping::Edge,
    ] {
        let s = series(&w, g, Weighting::MatchedBytes, grouping).await;
        assert_eq!(s.value.groups().grouping(), grouping);
        assert_eq!(s.value.total(), topology.total(), "{grouping:?}");
    }
    let by_edge = series(&w, g, Weighting::MatchedBytes, SeriesGrouping::Edge).await;
    let SeriesGroups::ByEdge(edges) = by_edge.value.groups() else {
        panic!("edge series")
    };
    assert_eq!(edges.len(), topology.edges.len());
    for edge in &topology.edges {
        let one = edges
            .iter()
            .find(|s| s.key.from == edge.from && s.key.to == edge.to && s.key.route == edge.route)
            .expect("one series per edge");
        assert_eq!(one.sum(), edge.stats.matched_bytes.get());
    }
}

/// A step twice as long sums each pair of the finer points (INV-445).
pub async fn a_coarser_step_sums_the_finer_points<H: Harness>(h: &H) {
    let w = World::everything(h).await;
    let width = w.bucket.as_micros().get();
    let extent_buckets = (w.extent.end().as_micros() - w.extent.start().as_micros()) / width;
    let fine_buckets = extent_buckets.div_ceil(48).max(1);
    let fine = width * fine_buckets;
    let end = w.extent.end();
    let span = window(
        Timestamp::from_micros(end.as_micros().saturating_sub(48 * fine)),
        end,
    );
    let step = |micros: u64| {
        NonZeroU64::new(micros)
            .and_then(|m| SeriesStep::new(w.bucket, m).ok())
            .expect("step")
    };
    let totals = async |micros: u64| -> Vec<u64> {
        let g = SeriesGrid::new(span, step(micros)).expect("grid");
        match series(&w, g, Weighting::Transmissions, SeriesGrouping::Total)
            .await
            .value
            .groups()
        {
            SeriesGroups::Total(values) => values.clone(),
            other => panic!("{other:?}"),
        }
    };
    let fine_points = totals(fine).await;
    let coarse = totals(2 * fine).await;
    let summed: Vec<u64> = fine_points
        .chunks(2)
        .map(|pair| pair.iter().sum())
        .collect();
    assert_eq!(coarse, summed);
}

/// A grid built for another bucket width is refused (INV-443).
pub async fn a_grid_for_another_bucket_width_is_refused<H: Harness>(h: &H) {
    let w = World::everything(h).await;
    let other =
        BucketWidth::from_micros(NonZeroU64::new(w.bucket.as_micros().get() * 2).expect("width"));
    let width = other.as_micros().get();
    let start = Timestamp::from_micros(w.extent.start().as_micros().div_ceil(width) * width);
    let span = window(
        start,
        Timestamp::from_micros(start.as_micros() + 12 * width),
    );
    let step = SeriesStep::new(other, other.as_micros()).expect("step");
    let g = SeriesGrid::new(span, step).expect("grid");
    assert_eq!(
        w.backend
            .series(
                &w.lead,
                g,
                Weighting::Transmissions,
                SeriesGrouping::Total,
                &TopologyFilter::default()
            )
            .await
            .err(),
        Some(QueryError::InvalidInput(InputError::BucketWidthMismatch))
    );
}

/// The overview's activity is `EdgeTotals::of` the graph for the same
/// window and filter, under its watermark (INV-710); its active channels
/// are the channels some edge is routed through (INV-743).
pub async fn the_overview_counts_the_graph<H: Harness>(h: &H) {
    let w = World::everything(h).await;
    for span in [w.day(), w.extent] {
        let overview = w
            .backend
            .overview(&w.lead, span, &TopologyFilter::default())
            .await
            .expect("overview");
        let topology = w
            .backend
            .topology(
                &w.lead,
                span,
                Weighting::MatchedBytes,
                &TopologyFilter::default(),
            )
            .await
            .expect("topology");
        assert_eq!(overview.value.activity, EdgeTotals::of(&topology.value));
        assert_eq!(overview.watermark, topology.watermark);
        let routed: BTreeSet<_> = topology
            .value
            .edges
            .iter()
            .filter_map(|e| match e.route {
                Route::Channel(c) => Some(c),
                _ => None,
            })
            .collect();
        assert_eq!(overview.value.activity.active_channels, routed.len() as u64);
    }
}

/// The overview's queues are what the lists show, whatever the window or
/// the filter (but `unconfirmed_channels`): open shown alerts, unreviewed
/// listed channels, unconfirmed channels (INV-760).
pub async fn the_overview_queues_are_the_lists<H: Harness>(h: &H) {
    let w = World::everything(h).await;
    let open = alerts(
        &w.backend,
        &w.lead,
        &AlertFilter {
            states: vec![AlertStateKind::Open],
            channel: None,
        },
    )
    .await;
    let review = channel_rows(
        &w.backend,
        &w.lead,
        &ChannelFilter {
            policies: vec![PolicyKind::Unreviewed],
            ..ChannelFilter::default()
        },
    )
    .await;
    let unconfirmed = channel_rows(
        &w.backend,
        &w.lead,
        &ChannelFilter {
            listings: vec![ListingKind::Unconfirmed],
            ..ChannelFilter::default()
        },
    )
    .await;
    assert!(!open.is_empty() && !review.is_empty() && !unconfirmed.is_empty());
    let narrow = TopologyFilter {
        agents: vec![w.id(merges::CANONICAL)],
        ..TopologyFilter::default()
    };
    for (span, filter) in [(w.day(), TopologyFilter::default()), (w.extent, narrow)] {
        let queues = w
            .backend
            .overview(&w.lead, span, &filter)
            .await
            .expect("overview")
            .value
            .queues;
        assert_eq!(queues.open_alerts, open.len() as u64);
        assert_eq!(queues.unreviewed_channels, review.len() as u64);
        assert_eq!(queues.unconfirmed_channels, Some(unconfirmed.len() as u64));
    }
}

/// Confirmed only leaves unconfirmed channels out of the queues: their
/// count is `None` (not zero) and the unreviewed ones leave the review
/// queue; open alerts are untouched (INV-760).
pub async fn overview_queues_honour_confirmed_only<H: Harness>(h: &H) {
    let w = World::of(h, suspected::scenario()).await;
    let read = async |unconfirmed| {
        w.backend
            .overview(
                &w.lead,
                w.extent,
                &TopologyFilter {
                    unconfirmed_channels: unconfirmed,
                    ..TopologyFilter::default()
                },
            )
            .await
            .expect("overview")
            .value
            .queues
    };
    let include = read(UnconfirmedChannels::Include).await;
    let exclude = read(UnconfirmedChannels::Exclude).await;
    assert!(include.unconfirmed_channels.is_some_and(|n| n > 0));
    assert_eq!(exclude.unconfirmed_channels, None);
    let unreviewed_unconfirmed = channel_rows(
        &w.backend,
        &w.lead,
        &ChannelFilter {
            listings: vec![ListingKind::Unconfirmed],
            policies: vec![PolicyKind::Unreviewed],
            ..ChannelFilter::default()
        },
    )
    .await;
    assert!(
        unreviewed_unconfirmed
            .iter()
            .all(|r| r.confirmation() == Some(Confirmation::Unconfirmed))
    );
    assert_eq!(
        exclude.unreviewed_channels + unreviewed_unconfirmed.len() as u64,
        include.unreviewed_channels
    );
    assert_eq!(exclude.open_alerts, include.open_alerts);
}

/// Every watermarked read in a quiet world carries the watermark
/// `watermark` reports, on a bucket boundary (INV-579, INV-594, INV-588).
pub async fn watermarked_reads_carry_the_watermark<H: Harness>(h: &H) {
    let w = World::everything(h).await;
    let mark = w.backend.watermark(&w.lead).await.expect("watermark");
    assert!(w.bucket.is_boundary(mark.at()), "{mark:?}");
    let f = TopologyFilter::default();
    let wiki = w.id(hijacked_wiki::WIKI);
    let b = &w.backend;
    let c = &w.lead;
    let marks = [
        b.topology(c, w.extent, Weighting::Transmissions, &f)
            .await
            .map(|x| x.watermark),
        b.channel_topology(c, w.extent, Weighting::Transmissions, &f)
            .await
            .map(|x| x.watermark),
        b.overview(c, w.extent, &f).await.map(|x| x.watermark),
        b.series(
            c,
            grid(w.bucket, w.extent, points(4)),
            Weighting::Transmissions,
            SeriesGrouping::Total,
            &f,
        )
        .await
        .map(|x| x.watermark),
        b.channels(c, &ChannelFilter::default(), &first(5))
            .await
            .map(|x| x.watermark),
        b.channel(c, wiki, None)
            .await
            .map(|x| x.expect("wiki").watermark),
        b.channel_resources(c, wiki, w.extent, &first(5))
            .await
            .map(|x| x.watermark),
        b.agents(c, &Default::default(), w.extent, &first(5))
            .await
            .map(|x| x.watermark),
        b.topic_sizes(c, None, Some(w.extent))
            .await
            .map(|x| x.watermark),
    ];
    for (i, read) in marks.into_iter().enumerate() {
        assert_eq!(read, Ok(mark), "read {i}");
    }
}
