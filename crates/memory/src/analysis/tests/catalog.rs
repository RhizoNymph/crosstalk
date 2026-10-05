//! `InMemoryTopicCatalog`: versions, sizes, lineage, retention and pins.

use std::num::NonZeroU16;

use crosstalk_spec::aggregates::edge::EdgeStats;
use crosstalk_spec::aggregates::retention::{Pin, PinChange, Retention};
use crosstalk_spec::aggregates::topic::{EmbeddingModel, TopicModelVersion};
use crosstalk_spec::aggregates::topic_history::{TopicVersionStatus, TopicVersionStatusKind};
use crosstalk_spec::events::BusEvent;
use crosstalk_spec::events::changed::Changed;
use crosstalk_spec::events::insight::InsightEvent;
use crosstalk_spec::interfaces::l6_analysis::lifecycle::{
    CatalogActivation, StoredAssignment, TopicLifecycle, TopicLifecycleError,
};
use crosstalk_spec::interfaces::l6_analysis::{CatalogError, TopicCatalog};
use crosstalk_spec::paging::{PageRequest, PageSize, TopicList};
use crosstalk_spec::support::Change;
use tokio::sync::mpsc::UnboundedReceiver;

use super::support::{at, fit_active, fit_ready, model};
use crate::analysis::catalog::{InMemoryTopicCatalog, TopicVersions};
use crate::model::build::{
    agent, catalog, non_zero, operator, topic, topic_id, transmission, ts, unit, window,
};
use crate::support::{Outbox, drain};

fn new_catalog(keep_last: u32) -> (InMemoryTopicCatalog, UnboundedReceiver<BusEvent>) {
    let (outbox, events) = Outbox::channel();
    (catalog(keep_last, 0.5, outbox).unwrap(), events)
}

fn assigned(topic: Option<u64>, at_micros: u64, bytes: u64) -> StoredAssignment {
    StoredAssignment {
        topic: topic.map(topic_id),
        confirmed_at: ts(at_micros),
        matched_bytes: non_zero(bytes),
        from: agent(1),
        to: agent(2),
    }
}

fn stats(transmissions: u64, bytes: u64) -> Option<EdgeStats> {
    Some(EdgeStats {
        transmissions: non_zero(transmissions),
        matched_bytes: non_zero(bytes),
    })
}

fn page(size: u16) -> PageRequest<TopicList> {
    PageRequest {
        size: PageSize::new(size).unwrap(),
        after: None,
    }
}

#[tokio::test]
async fn fresh_catalog_has_version_zero_active_and_unfitted() {
    let (catalog, _events) = new_catalog(2);
    let history = catalog.versions().await.unwrap();
    assert_eq!(history.versions().len(), 1);
    assert_eq!(history.active().version(), TopicModelVersion(0));
    let sizes = catalog.sizes(TopicModelVersion(0), None).await.unwrap();
    assert!(sizes.topics().is_empty());
    assert_eq!(sizes.outliers(), None);
}

#[tokio::test]
async fn sizes_count_assignments_per_topic_and_window() {
    // analysis.sizes.match-cross-agent-assignments
    let (mut catalog, _events) = new_catalog(2);
    let v1 = fit_ready(
        &mut catalog,
        10,
        &[
            (1, [1.0, 0.0, 0.0]),
            (2, [0.0, 1.0, 0.0]),
            (3, [0.0, 0.0, 1.0]),
        ],
    )
    .await;
    catalog
        .assign(transmission(1), v1, assigned(Some(1), 100, 10))
        .await
        .unwrap();
    catalog
        .assign(transmission(2), v1, assigned(Some(1), 200, 5))
        .await
        .unwrap();
    catalog
        .assign(transmission(3), v1, assigned(Some(2), 300, 7))
        .await
        .unwrap();
    catalog
        .assign(transmission(4), v1, assigned(None, 150, 2))
        .await
        .unwrap();

    let all = catalog.sizes(v1, None).await.unwrap();
    let by_topic: Vec<_> = all
        .topics()
        .iter()
        .map(|size| (size.topic, size.stats))
        .collect();
    assert_eq!(
        by_topic,
        vec![
            (topic_id(1), stats(2, 15)),
            (topic_id(2), stats(1, 7)),
            (topic_id(3), None),
        ]
    );
    assert_eq!(all.outliers(), stats(1, 2));
    assert_eq!(all.window(), None);

    let windowed = catalog.sizes(v1, window(150, 250)).await.unwrap();
    let by_topic: Vec<_> = windowed
        .topics()
        .iter()
        .map(|size| (size.topic, size.stats))
        .collect();
    assert_eq!(
        by_topic,
        vec![
            (topic_id(1), stats(1, 5)),
            (topic_id(2), None),
            (topic_id(3), None)
        ]
    );
    assert_eq!(windowed.outliers(), stats(1, 2));
}

#[tokio::test]
async fn sizes_of_fitting_version_rejected() {
    // analysis.sizes.rejects-unready
    let (mut catalog, _events) = new_catalog(2);
    let fitting = catalog.begin_fit(at(5)).await.unwrap();
    assert_eq!(
        catalog.sizes(fitting, None).await,
        Err(CatalogError::StillFitting(fitting))
    );
    let topics = catalog.topics(fitting, &page(10)).await;
    assert_eq!(topics, Err(CatalogError::StillFitting(fitting)));
}

#[tokio::test]
async fn sizes_of_unknown_version_rejected() {
    // analysis.sizes.rejects-unready
    let (catalog, _events) = new_catalog(2);
    let unknown = TopicModelVersion(7);
    assert_eq!(
        catalog.sizes(unknown, None).await,
        Err(CatalogError::UnknownVersion(unknown))
    );
    assert_eq!(
        catalog.lineage(unknown).await,
        Err(CatalogError::UnknownVersion(unknown))
    );
}

/// Versions 1, 2 and 3 activated in turn under keep-last 2: version 1 (and
/// version 0) are dropped by the third activation.
async fn three_activations(catalog: &mut InMemoryTopicCatalog) -> [TopicModelVersion; 3] {
    let v1 = fit_active(catalog, 10, &[(1, [1.0, 0.0, 0.0])]).await;
    catalog
        .assign(transmission(1), v1, assigned(Some(1), 100, 4))
        .await
        .unwrap();
    catalog
        .assign(transmission(2), v1, assigned(None, 120, 6))
        .await
        .unwrap();
    let v2 = fit_active(catalog, 20, &[(2, [1.0, 0.1, 0.0])]).await;
    let v3 = fit_active(catalog, 30, &[(3, [0.0, 1.0, 0.0])]).await;
    [v1, v2, v3]
}

#[tokio::test]
async fn windowed_sizes_of_dropped_version_rejected() {
    // analysis.sizes.rejects-unready
    let (mut catalog, _events) = new_catalog(2);
    let [v1, ..] = three_activations(&mut catalog).await;
    assert!(!catalog.retains(v1));
    assert_eq!(
        catalog.sizes(v1, window(0, 1_000)).await,
        Err(CatalogError::VersionNotRetained(v1))
    );
}

#[tokio::test]
async fn dropped_version_keeps_all_time_sizes_only() {
    // analysis.retention.dropped-sizes-frozen
    let (mut catalog, _events) = new_catalog(2);
    let v1 = fit_active(&mut catalog, 10, &[(1, [1.0, 0.0, 0.0])]).await;
    catalog
        .assign(transmission(1), v1, assigned(Some(1), 100, 4))
        .await
        .unwrap();
    catalog
        .assign(transmission(2), v1, assigned(None, 120, 6))
        .await
        .unwrap();
    let before = catalog.sizes(v1, None).await.unwrap();
    fit_active(&mut catalog, 20, &[(2, [1.0, 0.1, 0.0])]).await;
    fit_active(&mut catalog, 30, &[(3, [0.0, 1.0, 0.0])]).await;
    let history = catalog.versions().await.unwrap();
    assert!(matches!(
        history.get(v1).unwrap().retention(),
        Retention::Dropped { .. }
    ));
    assert_eq!(catalog.sizes(v1, None).await.unwrap(), before);
    assert_eq!(catalog.assignment(v1, transmission(1)), None);
}

#[tokio::test]
async fn drop_keeps_topics_and_lineage() {
    // analysis.retention.topics-and-lineage-kept
    let (mut catalog, _events) = new_catalog(2);
    let v1 = fit_active(&mut catalog, 10, &[(1, [1.0, 0.0, 0.0])]).await;
    let topics_before = catalog.topics(v1, &page(10)).await.unwrap();
    let from_v1 = {
        fit_active(&mut catalog, 20, &[(2, [1.0, 0.1, 0.0])]).await;
        catalog.lineage(v1).await.unwrap()
    };
    let into_v1 = catalog.lineage(TopicModelVersion(0)).await.unwrap();
    fit_active(&mut catalog, 30, &[(3, [0.0, 1.0, 0.0])]).await;
    assert!(!catalog.retains(v1));
    assert_eq!(catalog.topics(v1, &page(10)).await.unwrap(), topics_before);
    assert_eq!(catalog.lineage(v1).await.unwrap(), from_v1);
    assert_eq!(
        catalog.lineage(TopicModelVersion(0)).await.unwrap(),
        into_v1
    );
}

#[tokio::test]
async fn catalog_pin_rejects_unknown_fitting_and_dropped() {
    // analysis.retention.catalog-pin-errors
    let (mut catalog, _events) = new_catalog(2);
    let [v1, v2, v3] = three_activations(&mut catalog).await;
    let pin = Pin {
        by: operator(1),
        at: at(1_000),
    };
    let fitting = catalog.begin_fit(at(900)).await.unwrap();
    let before = catalog.versions().await.unwrap();
    assert_eq!(
        catalog.pin(TopicModelVersion(99), pin).await,
        Err(CatalogError::UnknownVersion(TopicModelVersion(99)))
    );
    assert_eq!(
        catalog.pin(fitting, pin).await,
        Err(CatalogError::StillFitting(fitting))
    );
    assert_eq!(
        catalog.pin(v1, pin).await,
        Err(CatalogError::VersionNotRetained(v1))
    );
    assert_eq!(catalog.versions().await.unwrap(), before);
    assert_eq!(catalog.pin(v2, pin).await, Ok(PinChange::Changed));
    assert_eq!(catalog.pin(v2, pin).await, Ok(PinChange::Unchanged));
    assert_eq!(catalog.pin(v3, pin).await, Ok(PinChange::Changed));
}

#[tokio::test]
async fn pinned_version_survives_retention_until_unpinned() {
    // analysis.retention.pin-drop-serialized and enforced-on-change (unpin)
    let (mut catalog, mut events) = new_catalog(2);
    let v1 = fit_active(&mut catalog, 10, &[(1, [1.0, 0.0, 0.0])]).await;
    catalog
        .pin(
            v1,
            Pin {
                by: operator(1),
                at: at(14),
            },
        )
        .await
        .unwrap();
    fit_active(&mut catalog, 20, &[(2, [1.0, 0.1, 0.0])]).await;
    fit_active(&mut catalog, 30, &[(3, [0.0, 1.0, 0.0])]).await;
    assert!(catalog.retains(v1), "a pinned version is never dropped");
    drain(&mut events);
    assert_eq!(catalog.unpin(v1, at(500)).await, Ok(PinChange::Changed));
    assert!(!catalog.retains(v1), "the unpin enforced retention");
    let history = catalog.versions().await.unwrap();
    assert_eq!(
        history.get(v1).unwrap().retention(),
        Retention::Dropped { at: at(500) }
    );
    let published = drain(&mut events);
    assert!(
        published.contains(&BusEvent::Insight(InsightEvent::TopicVersionDropped {
            version: v1
        }))
    );
    // A retried unpin of the dropped version is unchanged.
    assert_eq!(catalog.unpin(v1, at(500)).await, Ok(PinChange::Unchanged));
}

#[tokio::test]
async fn retention_enforced_after_activation_unpin_and_start() {
    // analysis.retention.enforced-on-change
    let (mut catalog, mut events) = new_catalog(2);
    let v1 = fit_active(&mut catalog, 10, &[(1, [1.0, 0.0, 0.0])]).await;
    let v2 = fit_ready(&mut catalog, 20, &[(2, [1.0, 0.1, 0.0])]).await;
    drain(&mut events);
    let activated = catalog.mark_active(v2, at(30)).await.unwrap();
    // Version 0 falls out of the two most recent activated versions.
    assert_eq!(
        activated,
        CatalogActivation::Switched {
            superseded: vec![v1],
            dropped: vec![TopicModelVersion(0)],
        }
    );
    let dropped_events: Vec<_> = drain(&mut events)
        .into_iter()
        .filter(|event| {
            matches!(
                event,
                BusEvent::Insight(InsightEvent::TopicVersionDropped { .. })
            )
        })
        .collect();
    assert_eq!(
        dropped_events,
        vec![BusEvent::Insight(InsightEvent::TopicVersionDropped {
            version: TopicModelVersion(0)
        })]
    );
    // Starting again finds nothing more to drop, and publishes nothing.
    assert_eq!(catalog.enforce_retention(at(40)).await, Ok(Vec::new()));
    assert!(drain(&mut events).is_empty());
    assert_eq!(catalog.enforce_retention(at(41)).await, Ok(Vec::new()));
}

#[tokio::test]
async fn catalog_follows_version_activated() {
    // analysis.version.active-follows-topology
    let (mut catalog, _events) = new_catalog(5);
    let v1 = fit_ready(&mut catalog, 10, &[(1, [1.0, 0.0, 0.0])]).await;
    let v2 = fit_ready(&mut catalog, 20, &[(2, [0.0, 1.0, 0.0])]).await;
    // v2 overtakes v1 before v1 is ever activated.
    catalog.mark_active(v2, at(30)).await.unwrap();
    let history = catalog.versions().await.unwrap();
    assert_eq!(history.active().version(), v2);
    assert_eq!(
        *history.active().status(),
        TopicVersionStatus::Active {
            fit: match history.get(v2).unwrap().status() {
                TopicVersionStatus::Active { fit, .. } => *fit,
                _ => unreachable!(),
            },
            activated_at: at(30),
        }
    );
    for old in [TopicModelVersion(0), v1] {
        match history.get(old).unwrap().status() {
            TopicVersionStatus::Superseded {
                by, superseded_at, ..
            } => {
                assert_eq!(*by, v2);
                assert_eq!(*superseded_at, at(30));
            }
            other => panic!("{old:?} should be superseded, is {other:?}"),
        }
    }
    match history.get(v1).unwrap().status() {
        TopicVersionStatus::Superseded { activated_at, .. } => assert_eq!(*activated_at, None),
        _ => unreachable!(),
    }
    // A redelivered or older activation is ignored.
    assert_eq!(
        catalog.mark_active(v2, at(40)).await,
        Ok(CatalogActivation::Ignored)
    );
    assert_eq!(
        catalog.mark_active(v1, at(40)).await,
        Ok(CatalogActivation::Ignored)
    );
}

#[tokio::test]
async fn failed_fit_removes_its_version() {
    // analysis.version.failed-fit-leaves-none
    let (mut catalog, _events) = new_catalog(2);
    let failed = catalog.begin_fit(at(10)).await.unwrap();
    catalog.fail_fit(failed).await.unwrap();
    let history = catalog.versions().await.unwrap();
    assert!(history.get(failed).is_none());
    let next = catalog.begin_fit(at(20)).await.unwrap();
    assert!(next > failed, "a failed fit's number is not reused");
    // Only one fit at a time.
    assert_eq!(
        catalog.begin_fit(at(21)).await,
        Err(TopicLifecycleError::FitInProgress(next))
    );
}

#[tokio::test]
async fn version_ready_only_after_its_fit_returned() {
    // analysis.version.ready-after-event: ready_at is the event's time
    let (mut catalog, mut events) = new_catalog(2);
    let version = catalog.begin_fit(at(10)).await.unwrap();
    assert_eq!(
        catalog.mark_ready(version, at(11)).await,
        Err(TopicLifecycleError::FitNotReturned(version))
    );
    catalog
        .complete_fit(
            version,
            vec![topic(
                topic_id(1),
                version,
                unit(&model(), 1.0, 0.0, 0.0).unwrap(),
                at(12),
            )],
            at(12),
        )
        .await
        .unwrap();
    let history = catalog.versions().await.unwrap();
    assert_eq!(
        history.get(version).unwrap().status().kind(),
        TopicVersionStatusKind::Fitting
    );
    catalog.mark_ready(version, at(13)).await.unwrap();
    match catalog
        .versions()
        .await
        .unwrap()
        .get(version)
        .unwrap()
        .status()
    {
        TopicVersionStatus::Ready { fit } => {
            assert_eq!(fit.ready_at, at(13));
            assert_eq!(fit.fitted_at, at(12));
            assert_eq!(fit.started_at, at(10));
            assert_eq!(fit.topics, 1);
        }
        other => panic!("expected ready, got {other:?}"),
    }
    assert!(drain(&mut events).contains(&BusEvent::Changed(Changed::TopicVersion(version))));
}

#[tokio::test]
async fn lineage_best_is_most_similar_centroid() {
    // analysis.lineage.best-is-argmax, ties to the lower id
    let (mut catalog, _events) = new_catalog(2);
    let v1 = fit_active(
        &mut catalog,
        10,
        &[(1, [1.0, 0.0, 0.0]), (2, [0.0, 1.0, 0.0])],
    )
    .await;
    // Topics 12 and 11 are equally similar to topic 1; 11 is the lower id.
    let v2 = fit_ready(
        &mut catalog,
        20,
        &[
            (12, [1.0, 1.0, 0.0]),
            (11, [1.0, -1.0, 0.0]),
            (13, [0.0, 0.0, 1.0]),
        ],
    )
    .await;
    let lineage = catalog.lineage(v1).await.unwrap().unwrap();
    assert_eq!(lineage.from(), v1);
    assert_eq!(lineage.to(), v2);
    let one = lineage.entry(topic_id(1)).unwrap();
    assert_eq!(one.best().unwrap().topic, topic_id(11));
    let two = lineage.entry(topic_id(2)).unwrap();
    assert_eq!(two.best().unwrap().topic, topic_id(12));
    // The newest version has no successor yet.
    assert_eq!(catalog.lineage(v2).await.unwrap(), None);
}

#[tokio::test]
async fn lineage_links_consecutive_versions() {
    // analysis.lineage.covers-predecessor: one entry per topic of `from`,
    // and `to` is the next version in the history (a failed fit between
    // them leaves no version).
    let (mut catalog, _events) = new_catalog(2);
    let v1 = fit_active(
        &mut catalog,
        10,
        &[(1, [1.0, 0.0, 0.0]), (2, [0.0, 1.0, 0.0])],
    )
    .await;
    let failed = catalog.begin_fit(at(15)).await.unwrap();
    catalog.fail_fit(failed).await.unwrap();
    let v3 = fit_ready(&mut catalog, 20, &[(3, [1.0, 0.0, 0.0])]).await;
    let lineage = catalog.lineage(v1).await.unwrap().unwrap();
    assert_eq!(lineage.to(), v3);
    let entries: Vec<_> = lineage
        .entries()
        .iter()
        .map(|entry| entry.topic())
        .collect();
    assert_eq!(entries, vec![topic_id(1), topic_id(2)]);
    // Version 0 has no topics, so its lineage to version 1 is empty.
    let from_zero = catalog
        .lineage(TopicModelVersion(0))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(from_zero.to(), v1);
    assert!(from_zero.entries().is_empty());
}

#[tokio::test]
async fn lineage_others_are_topics_above_floor() {
    // analysis.lineage.others-above-floor (floor 0.5)
    let (mut catalog, _events) = new_catalog(2);
    let v1 = fit_active(&mut catalog, 10, &[(1, [1.0, 0.0, 0.0])]).await;
    fit_ready(
        &mut catalog,
        20,
        &[
            (11, [1.0, 0.0, 0.0]),
            (12, [1.0, 1.0, 0.0]),
            (13, [0.0, 1.0, 0.0]),
        ],
    )
    .await;
    let lineage = catalog.lineage(v1).await.unwrap().unwrap();
    let entry = lineage.entry(topic_id(1)).unwrap();
    assert_eq!(entry.best().unwrap().topic, topic_id(11));
    // cos 45° ≈ 0.707 reaches the floor; cos 90° = 0 does not.
    let others: Vec<_> = entry.others().iter().map(|link| link.topic).collect();
    assert_eq!(others, vec![topic_id(12)]);
}

#[tokio::test]
async fn assignment_store_rejects_duplicate_transmission_version() {
    // analysis.topic.one-assignment-per-version
    let (mut catalog, _events) = new_catalog(2);
    let v1 = fit_ready(&mut catalog, 10, &[(1, [1.0, 0.0, 0.0])]).await;
    let first = assigned(Some(1), 100, 3);
    assert_eq!(
        catalog.assign(transmission(1), v1, first).await,
        Ok(Change::Applied)
    );
    assert_eq!(
        catalog.assign(transmission(1), v1, first).await,
        Ok(Change::Unchanged)
    );
    assert_eq!(
        catalog
            .assign(transmission(1), v1, assigned(None, 100, 3))
            .await,
        Err(TopicLifecycleError::Conflicting {
            transmission: transmission(1),
            version: v1
        })
    );
    // A topic of another version is refused.
    assert_eq!(
        catalog
            .assign(transmission(2), v1, assigned(Some(9), 100, 3))
            .await,
        Err(TopicLifecycleError::ForeignTopic {
            topic: topic_id(9),
            version: v1
        })
    );
    // Version 0 takes outliers only.
    assert_eq!(
        catalog
            .assign(transmission(3), TopicModelVersion(0), assigned(None, 1, 1))
            .await,
        Ok(Change::Applied)
    );
}

#[tokio::test]
async fn topics_page_newest_id_first_with_cursor_bound_to_version() {
    let (mut catalog, _events) = new_catalog(5);
    let v1 = fit_active(
        &mut catalog,
        10,
        &[
            (1, [1.0, 0.0, 0.0]),
            (2, [0.0, 1.0, 0.0]),
            (3, [0.0, 0.0, 1.0]),
        ],
    )
    .await;
    let v2 = fit_ready(&mut catalog, 20, &[(4, [1.0, 0.0, 0.0])]).await;
    let first = catalog.topics(v1, &page(2)).await.unwrap();
    let ids: Vec<_> = first.items().iter().map(|topic| topic.id).collect();
    assert_eq!(ids, vec![topic_id(3), topic_id(2)]);
    let next = PageRequest {
        size: PageSize::new(2).unwrap(),
        after: first.next().cloned(),
    };
    let second = catalog.topics(v1, &next).await.unwrap();
    let ids: Vec<_> = second.items().iter().map(|topic| topic.id).collect();
    assert_eq!(ids, vec![topic_id(1)]);
    assert!(second.next().is_none());
    // The cursor is bound to its version.
    assert_eq!(
        catalog.topics(v2, &next).await,
        Err(CatalogError::InvalidCursor)
    );
}

#[tokio::test]
async fn status_and_retention_changes_are_announced() {
    // analysis.topic.change-announced (catalog half)
    let (mut catalog, mut events) = new_catalog(2);
    let v1 = fit_ready(&mut catalog, 10, &[(1, [1.0, 0.0, 0.0])]).await;
    catalog.mark_active(v1, at(20)).await.unwrap();
    catalog
        .pin(
            v1,
            Pin {
                by: operator(1),
                at: at(21),
            },
        )
        .await
        .unwrap();
    let changed: Vec<_> = drain(&mut events)
        .into_iter()
        .filter_map(|event| match event {
            BusEvent::Changed(Changed::TopicVersion(version)) => Some(version),
            _ => None,
        })
        .collect();
    // ready v1, active v1, superseded v0, pinned v1.
    assert_eq!(changed, vec![v1, v1, TopicModelVersion(0), v1]);
}

#[tokio::test]
async fn topic_versions_reads_history_and_topics() {
    let (mut catalog, _events) = new_catalog(2);
    let v1 = fit_ready(
        &mut catalog,
        10,
        &[(2, [1.0, 0.0, 0.0]), (1, [0.0, 1.0, 0.0])],
    )
    .await;
    assert_eq!(catalog.topic_ids(v1), vec![topic_id(1), topic_id(2)]);
    assert_eq!(catalog.version_of(topic_id(2)), Some(v1));
    assert_eq!(catalog.version_of(topic_id(3)), None);
    assert_eq!(TopicVersions::history(&catalog).versions().len(), 2);
}

#[tokio::test]
async fn fit_returned_rejects_topics_of_another_model_version() {
    let (mut catalog, _events) = new_catalog(2);
    let version = catalog.begin_fit(at(10)).await.unwrap();
    let other = EmbeddingModel {
        name: "other".to_owned(),
        dimension: NonZeroU16::new(3).unwrap(),
    };
    let foreign = topic(
        topic_id(1),
        TopicModelVersion(9),
        unit(&other, 1.0, 0.0, 0.0).unwrap(),
        at(11),
    );
    assert_eq!(
        catalog.complete_fit(version, vec![foreign], at(11)).await,
        Err(TopicLifecycleError::ForeignTopic {
            topic: topic_id(1),
            version
        })
    );
}

#[tokio::test]
async fn sizes_leave_out_transmissions_within_one_agent() {
    // analysis.sizes.match-cross-agent-assignments: an assignment whose
    // sender has since been merged into its reader counts nowhere, and the
    // unmerge counts it again.
    use crate::analysis::aliases::StaticDirectory;

    let directory = StaticDirectory::new();
    let (catalog, _events) = new_catalog(2);
    let mut catalog = catalog.with_agents(directory.clone());
    let v1 = fit_ready(&mut catalog, 10, &[(1, [1.0, 0.0, 0.0])]).await;
    catalog
        .assign(transmission(1), v1, assigned(Some(1), 100, 10))
        .await
        .unwrap();
    let within = StoredAssignment {
        from: agent(3),
        ..assigned(Some(1), 110, 5)
    };
    catalog.assign(transmission(2), v1, within).await.unwrap();
    let count = |sizes: crosstalk_spec::aggregates::topic_history::TopicSizes| {
        sizes
            .topics()
            .iter()
            .map(|size| size.stats)
            .collect::<Vec<_>>()
    };
    assert_eq!(
        count(catalog.sizes(v1, None).await.unwrap()),
        vec![stats(2, 15)]
    );
    directory.merge(agent(3), agent(2)).unwrap();
    assert_eq!(
        count(catalog.sizes(v1, None).await.unwrap()),
        vec![stats(1, 10)]
    );
    assert_eq!(
        count(catalog.sizes(v1, window(0, 200)).await.unwrap()),
        vec![stats(1, 10)]
    );
    directory.unmerge(agent(3));
    assert_eq!(
        count(catalog.sizes(v1, None).await.unwrap()),
        vec![stats(2, 15)]
    );
}
