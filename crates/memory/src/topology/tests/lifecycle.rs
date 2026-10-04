//! Activation, retention, the watermark and late contributions.

use crosstalk_spec::aggregates::edge::{EdgeSelector, TopologyFilter, Weighting};
use crosstalk_spec::aggregates::filter::{TopicVersionSelector, VersionUnavailable};
use crosstalk_spec::aggregates::series::{SeriesGrid, SeriesGrouping, SeriesStep};
use crosstalk_spec::aggregates::topic::TopicModelVersion;
use crosstalk_spec::aggregates::watermark::{PipelineFrontier, Watermark};
use crosstalk_spec::derived::flow::transmission::Route;
use crosstalk_spec::events::changed::Changed;
use crosstalk_spec::events::insight::{ClassificationCause, InsightEvent};
use crosstalk_spec::interfaces::l7_topology::{
    EdgeError, EdgeQueryError, EdgeStore, FrontierSource,
};
use crosstalk_spec::paging::{PageRequest, PageSize};

use super::support::{SETTLE, WIDTH, all, contribution, edge_counts, graph, plain, refit, world};
use crate::analysis::support::Published;
use crate::analysis::tests::support::fit_ready;
use crate::model::build::{agent, bucket_width, non_zero, ts, window};
use crate::topology::store::{Activation, ManualFrontier};

fn frontier(ticked: u64, pending: Option<u64>) -> PipelineFrontier {
    PipelineFrontier {
        ticked_through: ts(ticked),
        oldest_pending: pending.map(ts),
    }
}

#[tokio::test]
async fn activation_waits_for_ready_count_of_processed_classifications() {
    // topology.version.activate-after-complete and publishes-activated
    let mut world = world();
    let version = fit_ready(&world.catalog, 1, &[(11, [1.0, 0.0, 0.0])]);
    assert_eq!(
        world.store.activate_if_complete(version),
        Ok(Activation::Pending)
    );
    world.store.version_ready(version, 2);
    // A confirmation under the version does not count.
    world
        .store
        .apply_classified(
            &contribution(9, 1, 2, Route::Unobserved, 20, 1, 1, Some(11)),
            ClassificationCause::Confirmation,
        )
        .unwrap();
    world
        .store
        .apply_classified(
            &contribution(1, 1, 2, Route::Unobserved, 20, 1, 1, Some(11)),
            ClassificationCause::Refit,
        )
        .unwrap();
    // A redelivery counts once.
    world
        .store
        .apply_classified(
            &contribution(1, 1, 2, Route::Unobserved, 20, 1, 1, Some(11)),
            ClassificationCause::Refit,
        )
        .unwrap();
    assert_eq!(
        world.store.activate_if_complete(version),
        Ok(Activation::Pending)
    );
    // A self-edge rejection is processed too.
    assert_eq!(
        world.store.apply_classified(
            &contribution(2, 3, 3, Route::Unobserved, 20, 1, 1, Some(11)),
            ClassificationCause::Refit
        ),
        Err(EdgeError::SelfEdge)
    );
    world.store.drain_published();
    assert_eq!(
        world.store.activate_if_complete(version),
        Ok(Activation::Switched {
            version,
            previous: TopicModelVersion(0)
        })
    );
    assert_eq!(
        world.store.drain_published(),
        vec![Published::Insight(InsightEvent::TopicVersionActivated {
            version,
            previous: TopicModelVersion(0)
        })]
    );
    // Once, and never for an older version.
    assert_eq!(
        world.store.activate_if_complete(version),
        Ok(Activation::Ignored)
    );
    assert_eq!(
        world.store.activate_if_complete(TopicModelVersion(0)),
        Ok(Activation::Ignored)
    );
    world.store.activate(version).await.unwrap();
    assert!(world.store.drain_published().is_empty());
    assert_eq!(world.store.active_version(), version);
}

#[tokio::test]
async fn activate_keeps_every_version_and_graph_reads_one() {
    // topology.retention.activate-drops-nothing, version.single-version and
    // version.keeps-current-and-pending
    let mut world = world();
    world
        .store
        .apply(&plain(1, 1, 2, Route::Unobserved, 20, 4))
        .await
        .unwrap();
    world
        .store
        .apply(&plain(2, 2, 3, Route::Unobserved, 20, 4))
        .await
        .unwrap();
    let v1 = refit(
        &world,
        100,
        &[11],
        &[contribution(1, 1, 2, Route::Unobserved, 20, 4, 1, Some(11))],
    );
    assert_eq!(world.store.contributions().len(), 3);
    let current = graph(&world, all(), &TopologyFilter::default()).await;
    assert_eq!(current.topic_version(), v1);
    assert_eq!(edge_counts(&current), vec![(1, 2, 1, 4)]);
    let pinned = TopologyFilter {
        topic_version: TopicVersionSelector::Pinned(TopicModelVersion(0)),
        ..TopologyFilter::default()
    };
    let old = graph(&world, all(), &pinned).await;
    assert_eq!(old.topic_version(), TopicModelVersion(0));
    assert_eq!(edge_counts(&old), vec![(1, 2, 1, 4), (2, 3, 1, 4)]);
}

#[tokio::test]
async fn drop_version_refuses_active_and_newer() {
    // topology.retention.drop-refuses-in-use
    let mut world = world();
    world
        .store
        .apply(&plain(1, 1, 2, Route::Unobserved, 20, 4))
        .await
        .unwrap();
    assert_eq!(
        world.store.drop_version(TopicModelVersion(0)).await,
        Err(EdgeError::VersionInUse {
            version: TopicModelVersion(0)
        })
    );
    assert_eq!(
        world.store.drop_version(TopicModelVersion(4)).await,
        Err(EdgeError::VersionInUse {
            version: TopicModelVersion(4)
        })
    );
    assert_eq!(world.store.contributions().len(), 1);
}

#[tokio::test]
async fn dropped_version_is_gone() {
    // topology.retention.drop-removes-all
    let mut world = world();
    world
        .store
        .apply(&plain(1, 1, 2, Route::Unobserved, 20, 4))
        .await
        .unwrap();
    refit(
        &world,
        100,
        &[11],
        &[contribution(1, 1, 2, Route::Unobserved, 20, 4, 1, Some(11))],
    );
    world
        .store
        .drop_version(TopicModelVersion(0))
        .await
        .unwrap();
    world
        .store
        .drop_version(TopicModelVersion(0))
        .await
        .unwrap();
    assert!(
        world
            .store
            .contributions()
            .iter()
            .all(|one| one.classification.version != TopicModelVersion(0))
    );
    let zero = TopicModelVersion(0);
    assert_eq!(
        world
            .store
            .apply(&plain(5, 1, 2, Route::Unobserved, 20, 4))
            .await,
        Err(EdgeError::VersionNotRetained { version: zero })
    );
    let pinned = TopologyFilter {
        topic_version: TopicVersionSelector::Pinned(zero),
        ..TopologyFilter::default()
    };
    let not_retained = EdgeQueryError::Version(VersionUnavailable::NotRetained(zero));
    assert_eq!(
        world
            .store
            .graph(all(), Weighting::Transmissions, &pinned)
            .await
            .map(|_| ()),
        Err(not_retained.clone())
    );
    let grid = SeriesGrid::new(
        all(),
        SeriesStep::new(bucket_width(WIDTH), non_zero(100)).unwrap(),
    )
    .unwrap();
    assert_eq!(
        world
            .store
            .series(
                grid,
                Weighting::Transmissions,
                SeriesGrouping::Total,
                &pinned
            )
            .await
            .map(|_| ()),
        Err(not_retained.clone())
    );
    let edge = EdgeSelector::new(agent(1), agent(2), Route::Unobserved).unwrap();
    let page = PageRequest {
        size: PageSize::new(5).unwrap(),
        after: None,
    };
    assert_eq!(
        world
            .store
            .transmissions(&edge, all(), &pinned, &page)
            .await
            .map(|_| ()),
        Err(not_retained)
    );
}

#[tokio::test]
async fn advance_watermark_exposes_settled() {
    // topology.watermark.exposed-is-settled, monotone and publishes-advance
    let mut world = world();
    assert_eq!(world.store.watermark().await, Ok(Watermark(ts(0))));
    // ticked 75 - settle 20 = 55, aligned down to 50.
    let advanced = world
        .store
        .advance_watermark(frontier(75, None))
        .await
        .unwrap();
    assert_eq!(advanced, Some(Watermark(ts(50))));
    assert_eq!(world.store.watermark().await, Ok(Watermark(ts(50))));
    // A pending input holds it back: never lowered.
    assert_eq!(
        world.store.advance_watermark(frontier(200, Some(33))).await,
        Ok(None)
    );
    assert_eq!(world.store.watermark().await, Ok(Watermark(ts(50))));
    // The same value again does not advance.
    assert_eq!(
        world.store.advance_watermark(frontier(79, None)).await,
        Ok(None)
    );
    assert_eq!(
        world
            .store
            .advance_watermark(frontier(200, Some(SETTLE + 97)))
            .await,
        Ok(Some(Watermark(ts(110))))
    );
    let published = world.store.drain_published();
    assert_eq!(
        published,
        vec![
            Published::Insight(InsightEvent::WatermarkAdvanced(Watermark(ts(50)))),
            Published::Changed(Changed::Watermark(Watermark(ts(50)))),
            Published::Insight(InsightEvent::WatermarkAdvanced(Watermark(ts(110)))),
            Published::Changed(Changed::Watermark(Watermark(ts(110)))),
        ]
    );
    // Every read reports the watermark it read first.
    let read = world
        .store
        .graph(all(), Weighting::Transmissions, &TopologyFilter::default())
        .await
        .unwrap();
    assert_eq!(read.watermark, Watermark(ts(110)));
}

#[tokio::test]
async fn apply_into_final_bucket_is_late() {
    // topology.watermark.rejects-late
    let mut world = world();
    world
        .store
        .advance_watermark(frontier(70, None))
        .await
        .unwrap();
    // The watermark is 50: the bucket [40, 50) is final, [50, 60) is not.
    assert_eq!(
        world
            .store
            .apply(&plain(1, 1, 2, Route::Unobserved, 45, 4))
            .await,
        Err(EdgeError::LateContribution {
            bucket: window(40, 50).unwrap(),
            watermark: Watermark(ts(50))
        })
    );
    assert!(world.store.contributions().is_empty());
    assert!(
        world
            .store
            .apply(&plain(2, 1, 2, Route::Unobserved, 50, 4))
            .await
            .is_ok()
    );
    // A version never activated is not final yet: its rebuild may fill old
    // buckets.
    let version = fit_ready(&world.catalog, 1, &[(11, [1.0, 0.0, 0.0])]);
    assert!(
        world
            .store
            .apply_classified(
                &contribution(1, 1, 2, Route::Unobserved, 45, 4, version.0, Some(11)),
                ClassificationCause::Refit
            )
            .is_ok()
    );
}

#[tokio::test]
async fn manual_frontier_reports_what_was_set() {
    let frontier_source = ManualFrontier::new(frontier(10, None));
    frontier_source.set(frontier(20, Some(5)));
    assert_eq!(frontier_source.frontier().await, Ok(frontier(20, Some(5))));
}
