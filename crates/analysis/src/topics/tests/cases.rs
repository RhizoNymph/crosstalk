//! Focused cases of [`PgTopicCatalog`](super::super::PgTopicCatalog):
//! redelivered assignments, restarts, retention drops and their events,
//! concurrent fits, and sizes under merges.

use crosstalk_memory::analysis::aliases::StaticDirectory;
use crosstalk_memory::model::build::{agent, non_zero, transmission, ts, window};
use crosstalk_spec::aggregates::retention::{Pin, PinChange};
use crosstalk_spec::aggregates::topic::TopicModelVersion;
use crosstalk_spec::batch::IdBatch;
use crosstalk_spec::events::BusEvent;
use crosstalk_spec::events::changed::Changed;
use crosstalk_spec::events::insight::InsightEvent;
use crosstalk_spec::interfaces::l6_analysis::lifecycle::{
    CatalogActivation, StoredAssignment, TopicLifecycle, TopicLifecycleError,
};
use crosstalk_spec::interfaces::l6_analysis::{CatalogError, TopicCatalog};
use crosstalk_spec::support::Change;

use super::{TestCatalog, catalog, drain, topic_of};
use crate::pg::testing::database;

fn assigned(topic: Option<crosstalk_spec::ids::TopicId>, at: u64, bytes: u64) -> StoredAssignment {
    StoredAssignment {
        topic,
        confirmed_at: ts(at),
        matched_bytes: non_zero(bytes),
        from: agent(1),
        to: agent(2),
    }
}

/// Fit, ready and activate the next version at `at` with one topic per
/// direction. Returns the version.
async fn activate(
    catalog: &mut TestCatalog,
    at: u64,
    directions: &[(f32, f32, f32)],
) -> TopicModelVersion {
    let version = catalog
        .begin_fit(ts(at))
        .await
        .unwrap_or_else(|error| panic!("begin_fit: {error:?}"));
    let topics = directions
        .iter()
        .enumerate()
        .map(|(k, xyz)| topic_of(version, u8::try_from(k).unwrap_or(0), *xyz, ts(at)))
        .collect();
    let completed = catalog.complete_fit(version, topics, ts(at)).await;
    assert!(completed.is_ok(), "{completed:?}");
    assert_eq!(catalog.mark_ready(version, ts(at)).await, Ok(()));
    let activated = catalog.mark_active(version, ts(at)).await;
    assert!(
        matches!(activated, Ok(CatalogActivation::Switched { .. })),
        "{activated:?}"
    );
    version
}

#[tokio::test(flavor = "multi_thread")]
async fn redelivered_assignment_is_unchanged_and_a_different_one_conflicts() {
    let Some(db) = database("redelivered_assignment_is_unchanged").await else {
        return;
    };
    let (mut catalog, _events) = catalog(db.pool().clone(), StaticDirectory::new()).await;
    let v0 = TopicModelVersion(0);
    let first = assigned(None, 5, 10);
    assert_eq!(
        catalog.assign(transmission(1), v0, first).await,
        Ok(Change::Applied)
    );
    assert_eq!(
        catalog.assign(transmission(1), v0, first).await,
        Ok(Change::Unchanged)
    );
    assert_eq!(
        catalog
            .assign(transmission(1), v0, assigned(None, 5, 11))
            .await,
        Err(TopicLifecycleError::Conflicting {
            transmission: transmission(1),
            version: v0,
        })
    );
    let sizes = catalog.sizes(v0, None).await;
    let outliers = sizes.map(|sizes| sizes.outliers());
    assert_eq!(
        outliers.map(|stats| stats.map(|stats| (stats.transmissions, stats.matched_bytes))),
        Ok(Some((non_zero(1), non_zero(10))))
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_restart_keeps_versions_topics_lineage_assignments_and_numbers() {
    let Some(db) = database("a_restart_keeps_the_catalog").await else {
        return;
    };
    let (mut first, _events) = catalog(db.pool().clone(), StaticDirectory::new()).await;
    let v1 = activate(&mut first, 10, &[(1.0, 0.0, 0.0), (0.0, 1.0, 0.0)]).await;
    let topic = topic_of(v1, 0, (1.0, 0.0, 0.0), ts(10)).id;
    assert_eq!(
        first
            .assign(transmission(7), v1, assigned(Some(topic), 12, 3))
            .await,
        Ok(Change::Applied)
    );
    // A fit that fails keeps its number.
    let failed = first.begin_fit(ts(20)).await;
    assert_eq!(failed, Ok(TopicModelVersion(2)));
    assert_eq!(first.fail_fit(TopicModelVersion(2)).await, Ok(()));
    let history = first.versions().await;
    let sizes = first.sizes(v1, None).await;
    let lineage = first.lineage(TopicModelVersion(0)).await;
    drop(first);

    // A new process over the same database, started later.
    let (mut second, _events) = catalog(db.pool().clone(), StaticDirectory::new()).await;
    assert_eq!(second.versions().await, history);
    assert_eq!(second.sizes(v1, None).await, sizes);
    assert_eq!(second.lineage(TopicModelVersion(0)).await, lineage);
    let ids = IdBatch::new(vec![transmission(7)]).unwrap_or_else(|error| panic!("{error:?}"));
    let stored = second.assignments(v1, &ids).await;
    assert_eq!(
        stored.map(|stored| stored.get(&transmission(7)).copied()),
        Ok(Some(Some(topic)))
    );
    assert_eq!(
        second
            .assign(transmission(7), v1, assigned(Some(topic), 12, 3))
            .await,
        Ok(Change::Unchanged)
    );
    assert_eq!(second.begin_fit(ts(30)).await, Ok(TopicModelVersion(3)));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_drop_freezes_sizes_deletes_assignments_and_publishes_its_events_once() {
    let Some(db) = database("a_drop_freezes_sizes").await else {
        return;
    };
    let (mut catalog, mut events) = catalog(db.pool().clone(), StaticDirectory::new()).await;
    let v0 = TopicModelVersion(0);
    assert_eq!(
        catalog
            .assign(transmission(1), v0, assigned(None, 5, 4))
            .await,
        Ok(Change::Applied)
    );
    let before = catalog.sizes(v0, None).await;
    let v1 = activate(&mut catalog, 10, &[(1.0, 0.0, 0.0)]).await;
    drain(&mut events);
    // Keeping the two newest activated versions, activating v2 drops v0.
    let version = catalog
        .begin_fit(ts(20))
        .await
        .unwrap_or_else(|error| panic!("{error:?}"));
    let topics = vec![topic_of(version, 0, (0.0, 1.0, 0.0), ts(20))];
    assert!(catalog.complete_fit(version, topics, ts(20)).await.is_ok());
    assert_eq!(catalog.mark_ready(version, ts(20)).await, Ok(()));
    assert_eq!(
        catalog.mark_active(version, ts(20)).await,
        Ok(CatalogActivation::Switched {
            superseded: vec![v1],
            dropped: vec![v0],
        })
    );
    assert_eq!(catalog.sizes(v0, None).await, before);
    assert_eq!(
        catalog.sizes(v0, window(0, 100)).await,
        Err(CatalogError::VersionNotRetained(v0))
    );
    let ids = IdBatch::new(vec![transmission(1)]).unwrap_or_else(|error| panic!("{error:?}"));
    assert_eq!(
        catalog.assignments(v0, &ids).await,
        Ok(std::collections::BTreeMap::new())
    );
    assert_eq!(
        catalog
            .assign(transmission(1), v0, assigned(None, 5, 4))
            .await,
        Err(TopicLifecycleError::VersionNotRetained(v0))
    );
    let published = drain(&mut events);
    let dropped: Vec<_> = published
        .iter()
        .filter(|event| {
            matches!(
                event,
                BusEvent::Insight(InsightEvent::TopicVersionDropped { .. })
            )
        })
        .collect();
    assert_eq!(
        dropped,
        vec![&BusEvent::Insight(InsightEvent::TopicVersionDropped {
            version: v0
        })]
    );
    assert!(published.contains(&BusEvent::Changed(Changed::TopicVersion(v0))));
    // Retention again finds nothing more to drop and publishes nothing.
    assert_eq!(catalog.enforce_retention(ts(30)).await, Ok(Vec::new()));
    assert_eq!(drain(&mut events), Vec::new());
}

#[tokio::test(flavor = "multi_thread")]
async fn a_pin_survives_and_an_unpin_drops_in_one_call() {
    let Some(db) = database("a_pin_survives_and_an_unpin_drops").await else {
        return;
    };
    let (mut catalog, _events) = catalog(db.pool().clone(), StaticDirectory::new()).await;
    let v0 = TopicModelVersion(0);
    let pin = Pin {
        by: crosstalk_memory::model::build::operator(1),
        at: ts(1),
    };
    assert_eq!(catalog.pin(v0, pin).await, Ok(PinChange::Changed));
    assert_eq!(catalog.pin(v0, pin).await, Ok(PinChange::Unchanged));
    activate(&mut catalog, 10, &[(1.0, 0.0, 0.0)]).await;
    activate(&mut catalog, 20, &[(0.0, 1.0, 0.0)]).await;
    let retained = catalog
        .versions()
        .await
        .map(|history| history.get(v0).map(|info| info.retention().is_retained()));
    assert_eq!(retained, Ok(Some(true)));
    assert_eq!(catalog.unpin(v0, ts(30)).await, Ok(PinChange::Changed));
    let retained = catalog
        .versions()
        .await
        .map(|history| history.get(v0).map(|info| info.retention().is_retained()));
    assert_eq!(retained, Ok(Some(false)));
    assert_eq!(catalog.unpin(v0, ts(31)).await, Ok(PinChange::Unchanged));
}

#[tokio::test(flavor = "multi_thread")]
async fn concurrent_begin_fits_start_one_fit() {
    let Some(db) = database("concurrent_begin_fits_start_one_fit").await else {
        return;
    };
    let (catalog, _events) = catalog(db.pool().clone(), StaticDirectory::new()).await;
    let (mut a, mut b) = (catalog.clone(), catalog.clone());
    let (first, second) = tokio::join!(a.begin_fit(ts(1)), b.begin_fit(ts(1)));
    let started: Vec<_> = [&first, &second]
        .into_iter()
        .filter(|result| result.is_ok())
        .collect();
    assert_eq!(
        started,
        vec![&Ok(TopicModelVersion(1))],
        "{first:?} {second:?}"
    );
    assert!(
        [first, second].contains(&Err(TopicLifecycleError::FitInProgress(TopicModelVersion(
            1
        )))),
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn sizes_leave_out_transmissions_whose_agents_merged() {
    let Some(db) = database("sizes_leave_out_merged_agents").await else {
        return;
    };
    let agents = StaticDirectory::new();
    let (mut catalog, _events) = catalog(db.pool().clone(), agents.clone()).await;
    let v0 = TopicModelVersion(0);
    assert_eq!(
        catalog
            .assign(transmission(1), v0, assigned(None, 5, 4))
            .await,
        Ok(Change::Applied)
    );
    let counted = |sizes: Result<_, CatalogError>| {
        sizes.map(
            |sizes: crosstalk_spec::aggregates::topic_history::TopicSizes| {
                sizes.outliers().map(|stats| stats.transmissions.get())
            },
        )
    };
    assert_eq!(counted(catalog.sizes(v0, None).await), Ok(Some(1)));
    assert!(agents.merge(agent(1), agent(2)).is_ok());
    assert_eq!(counted(catalog.sizes(v0, None).await), Ok(None));
}
