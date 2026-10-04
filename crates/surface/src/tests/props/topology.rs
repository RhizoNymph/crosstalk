//! Properties of the linked views over L7: topology and series are the edge
//! store's answers, and every topic a response names is of its version.

use std::collections::BTreeSet;
use std::num::NonZeroU64;

use crosstalk_spec::aggregates::edge::{RouteKind, TopologyFilter, Weighting};
use crosstalk_spec::aggregates::filter::TopicVersionSelector;
use crosstalk_spec::aggregates::series::{SeriesGrouping, SeriesGroups};
use crosstalk_spec::aggregates::topic::TopicModelVersion;
use crosstalk_spec::derived::flow::transmission::{
    Classification, DelegationDirection, DirectCarrier, Route,
};
use crosstalk_spec::events::insight::ClassificationCause;
use crosstalk_spec::ids::{AgentId, TopicId, TransmissionId};
use crosstalk_spec::interfaces::l6_analysis::lifecycle::TopicLifecycle;
use crosstalk_spec::interfaces::l7_topology::{EdgeContribution, EdgeStore};
use crosstalk_spec::interfaces::l8_surface::{QueryApi, QueryError};
use crosstalk_spec::support::{TimeWindow, Timestamp};
use proptest::collection::vec;
use proptest::sample::select;

use super::{ensure, equal, property};
use crate::tests::page;
use crate::tests::world::{Fixture, WIDTH, Who, grid_over, minute};

fn agent(n: u8) -> AgentId {
    AgentId::from_ulid(0xA700 + u128::from(n))
}

fn route(n: u8) -> Route {
    match n % 3 {
        0 => Route::Unobserved,
        1 => Route::Delegation(DelegationDirection::ParentToChild),
        _ => Route::Direct(DirectCarrier::UserTurn),
    }
}

/// `(from, to, route, minute, bytes)`; the reader is never the sender.
fn contributions() -> impl proptest::strategy::Strategy<Value = Vec<(u8, u8, u8, u64, u64)>> {
    vec((0_u8..4, 1_u8..4, 0_u8..3, 0_u64..10, 1_u64..500), 0..12)
}

async fn apply(
    fixture: &Fixture,
    contributions: &[(u8, u8, u8, u64, u64)],
    version: TopicModelVersion,
    topics: &[TopicId],
) -> Result<(), String> {
    let mut edges = fixture.world.edges.clone();
    for (n, (from, step, kind, at, bytes)) in contributions.iter().enumerate() {
        let to = (from + step) % 4;
        let topic = (!topics.is_empty())
            .then(|| topics[n % topics.len()])
            .filter(|_| n % 3 != 0);
        let contribution = EdgeContribution {
            transmission: TransmissionId::from_ulid(0x7700 + n as u128),
            from: agent(*from),
            to: agent(to),
            route: route(*kind),
            at: Timestamp::from_micros(minute(*at).as_micros() + n as u64),
            matched_bytes: NonZeroU64::new(*bytes).unwrap_or(NonZeroU64::MIN),
            classification: Classification {
                version,
                topic,
                watched: false,
            },
            cause: ClassificationCause::Confirmation,
        };
        edges
            .apply(&contribution)
            .await
            .map_err(|error| format!("apply: {error:?}"))?;
    }
    Ok(())
}

fn window(start: u64, length: u64, unaligned: bool) -> Option<TimeWindow> {
    let shift = if unaligned { 1 } else { 0 };
    TimeWindow::new(
        Timestamp::from_micros(minute(start).as_micros() + shift),
        minute(start + length),
    )
    .ok()
}

const KINDS: [RouteKind; 3] = [
    RouteKind::Unobserved,
    RouteKind::Delegation,
    RouteKind::Direct,
];

/// INV-382: `topology` is exactly `EdgeStore::graph`, errors mapped by
/// their `From`, `UnalignedWindow` included.
#[test]
fn prop_topology_matches_in_memory_edge_store() {
    let query = (
        0_u64..8,
        1_u64..10,
        proptest::bool::weighted(0.2),
        vec(select(KINDS.to_vec()), 0..2),
        select(vec![Weighting::Transmissions, Weighting::MatchedBytes]),
    );
    property(
        48,
        (contributions(), query),
        |(contributions, (start, length, unaligned, kinds, weighting))| async move {
            let fixture = Fixture::new().await;
            apply(&fixture, &contributions, TopicModelVersion(0), &[]).await?;
            let Some(window) = window(start, length, unaligned) else {
                return Ok(());
            };
            let filter = TopologyFilter {
                route_kinds: kinds,
                ..TopologyFilter::default()
            };
            let viewer = fixture.caller(Who::Viewer).await;
            let surfaced = fixture
                .surface
                .topology(&viewer, window, weighting, &filter)
                .await;
            let direct = fixture
                .world
                .edges
                .graph(window, weighting, &filter)
                .await
                .map_err(QueryError::from);
            equal("topology", &surfaced, &direct)
        },
    );
}

/// INV-433: `series` is exactly `EdgeStore::series`, watermark included.
#[test]
fn prop_series_matches_in_memory_edge_store() {
    let grouping = select(vec![
        SeriesGrouping::Total,
        SeriesGrouping::Topic,
        SeriesGrouping::RouteKind,
        SeriesGrouping::Edge,
    ]);
    let query = (0_u64..5, 1_u64..6, grouping, 0_u64..12);
    property(
        48,
        (contributions(), query),
        |(contributions, (start, length, grouping, watermark))| async move {
            let fixture = Fixture::new().await;
            apply(&fixture, &contributions, TopicModelVersion(0), &[]).await?;
            fixture.watermark(minute(watermark)).await;
            let grid = grid_over(start, start + length);
            let viewer = fixture.caller(Who::Viewer).await;
            let filter = TopologyFilter::default();
            let surfaced = fixture
                .surface
                .series(&viewer, grid, Weighting::Transmissions, grouping, &filter)
                .await;
            let direct = fixture
                .world
                .edges
                .series(grid, Weighting::Transmissions, grouping, &filter)
                .await
                .map_err(QueryError::from);
            equal("series", &surfaced, &direct)
        },
    );
}

/// Fit version 1 with `topics`, make its edge buckets complete with
/// `contributions` classified under it by the re-fit, and activate it in
/// the edge store and the catalog.
async fn activate_version(
    fixture: &Fixture,
    topics: &[u64],
    contributions: &[(u8, u8, u8, u64, u64)],
) -> Result<(TopicModelVersion, Vec<TopicId>), String> {
    let version = fixture.fit(minute(0), topics, false).await;
    let ids: Vec<TopicId> = topics
        .iter()
        .map(|n| crosstalk_memory::model::build::topic_id(*n))
        .collect();
    let mut edges = fixture.world.edges.clone();
    edges
        .version_ready(version, contributions.len() as u64)
        .await
        .map_err(|error| format!("ready: {error:?}"))?;
    for (n, (from, step, kind, at, bytes)) in contributions.iter().enumerate() {
        let contribution = EdgeContribution {
            transmission: TransmissionId::from_ulid(0x7700 + n as u128),
            from: agent(*from),
            to: agent((from + step) % 4),
            route: route(*kind),
            at: Timestamp::from_micros(minute(*at).as_micros() + n as u64),
            matched_bytes: NonZeroU64::new(*bytes).unwrap_or(NonZeroU64::MIN),
            classification: Classification {
                version,
                topic: (n % 3 != 0).then(|| ids[n % ids.len()]),
                watched: false,
            },
            cause: ClassificationCause::Refit,
        };
        edges
            .apply(&contribution)
            .await
            .map_err(|error| format!("apply: {error:?}"))?;
    }
    edges
        .activate(version)
        .await
        .map_err(|error| format!("activate: {error:?}"))?;
    let mut catalog = fixture.world.catalog.clone();
    catalog
        .mark_active(version, minute(1))
        .await
        .map_err(|error| format!("mark active: {error:?}"))?;
    Ok((version, ids))
}

/// INV-646: every topic id a series, edge drill-down or topics response
/// names belongs to the version that response reports.
#[test]
fn prop_response_topics_belong_to_reported_version() {
    property(24, contributions(), |contributions| async move {
        let fixture = Fixture::new().await;
        let (version, topics) = activate_version(&fixture, &[71, 72, 73], &contributions).await?;
        let in_version: BTreeSet<TopicId> = topics.iter().copied().collect();
        let reader = fixture.caller(Who::Reader).await;
        let filter = TopologyFilter::default();
        let series = fixture
            .surface
            .series(
                &reader,
                grid_over(0, 10),
                Weighting::Transmissions,
                SeriesGrouping::Topic,
                &filter,
            )
            .await
            .map_err(|error| format!("series: {error:?}"))?;
        equal("series version", &series.value.topic_version(), &version)?;
        if let SeriesGroups::ByTopic(groups) = series.value.groups() {
            for group in groups {
                ensure(
                    group.key.is_none_or(|topic| in_version.contains(&topic)),
                    || format!("series names {:?}", group.key),
                )?;
            }
        }
        let graph = fixture
            .surface
            .topology(
                &reader,
                crate::tests::world::minutes(0, 10),
                Weighting::Transmissions,
                &filter,
            )
            .await
            .map_err(|error| format!("topology: {error:?}"))?;
        equal("graph version", &graph.value.topic_version(), &version)?;
        for edge in graph.value.edges() {
            let Ok(selector) = crosstalk_spec::aggregates::edge::EdgeSelector::new(
                edge.from,
                edge.to,
                edge.route.clone(),
            ) else {
                continue;
            };
            let rows = fixture
                .surface
                .edge_transmissions(
                    &reader,
                    &selector,
                    crate::tests::world::minutes(0, 10),
                    &filter,
                    &page(100),
                )
                .await
                .map_err(|error| format!("drill-down: {error:?}"))?;
            equal("drill-down version", &rows.value.topic_version, &version)?;
            for row in rows.value.page.items() {
                ensure(
                    row.topic.is_none_or(|topic| in_version.contains(&topic)),
                    || format!("row names {:?}", row.topic),
                )?;
            }
        }
        let listed = fixture
            .surface
            .topics(&reader, TopicVersionSelector::Current, &page(100))
            .await
            .map_err(|error| format!("topics: {error:?}"))?;
        equal("topics version", &listed.version, &version)?;
        for topic in listed.page.items() {
            equal("topic's version", &topic.version, &version)?;
        }
        let _ = WIDTH;
        Ok(())
    });
}
