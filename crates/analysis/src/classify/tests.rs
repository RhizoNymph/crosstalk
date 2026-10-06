//! The classification step on redelivery: the same envelope (id and
//! event) whatever happened between two deliveries of one confirmation,
//! and one assignment and one saved classification. Over the memory
//! reference stores, and over [`PgTopicCatalog`](crate::topics::PgTopicCatalog)
//! when `TEST_DATABASE_URL` is configured.

use std::num::NonZeroU64;

use crosstalk_memory::analysis::aliases::StaticDirectory;
use crosstalk_memory::analysis::catalog::InMemoryTopicCatalog;
use crosstalk_memory::flow::MemoryVerdicts;
use crosstalk_memory::model::build::{catalog as memory_catalog, test_model, unit};
use crosstalk_memory::support::Outbox;
use crosstalk_spec::aggregates::topic::{Topic, TopicModelVersion};
use crosstalk_spec::batch::IdBatch;
use crosstalk_spec::derived::flow::transmission::{
    Classification, Route, Transmission, TransmissionState,
};
use crosstalk_spec::events::changed::Changed;
use crosstalk_spec::events::detect::DetectEvent;
use crosstalk_spec::events::insight::InsightEvent;
use crosstalk_spec::events::{BusEvent, Envelope};
use crosstalk_spec::ids::{EventId, TopicId, TransmissionId};
use crosstalk_spec::interfaces::l5_flow::transmissions::TransmissionStore;
use crosstalk_spec::interfaces::l6_analysis::TopicCatalog;
use crosstalk_spec::interfaces::l6_analysis::lifecycle::TopicLifecycle;
use crosstalk_spec::support::Timestamp;
use crosstalk_testkit::build::TransmissionBuilder;
use crosstalk_testkit::ids::Ids;
use crosstalk_testkit::time::T0;

use super::{CLASSIFIED_LABEL, Classifier};
use crate::pg::testing::database;
use crate::topics::tests::catalog as pg_catalog;

const MINUTE: u64 = 60_000_000;

fn later(minutes: u64) -> Timestamp {
    Timestamp::from_micros(T0.as_micros() + minutes * MINUTE)
}

/// A stored confirmed transmission and the delivery that announces it.
struct Confirmation {
    stored: Transmission,
    delivery: Envelope,
}

fn confirmation(seed: u32) -> Confirmation {
    let mut ids = Ids::seeded(seed);
    let (from, to, channel) = (ids.agent(), ids.agent(), ids.channel());
    let Ok(stored) = TransmissionBuilder::new(&mut ids)
        .between(from, to)
        .channel(channel)
        .opened_at(T0)
        .confirmed()
        .build()
    else {
        panic!("transmission fixture");
    };
    let at = stored
        .state
        .confirmed()
        .map(|confirmed| confirmed.at())
        .unwrap_or_else(|| panic!("not confirmed"));
    let delivery = Envelope {
        id: ids.event(),
        at,
        event: BusEvent::Detect(DetectEvent::TransmissionConfirmed {
            transmission: stored.id,
            from,
            to,
            route: Route::Channel(channel),
            at,
            matched_bytes: NonZeroU64::new(42).unwrap_or(NonZeroU64::MIN),
        }),
    };
    Confirmation { stored, delivery }
}

/// Fit, ready and activate the next version at `at` with one topic.
async fn activate_next<C: TopicCatalog + TopicLifecycle>(
    catalog: &mut C,
    at: Timestamp,
) -> TopicModelVersion {
    let version = catalog
        .begin_fit(at)
        .await
        .unwrap_or_else(|error| panic!("begin_fit: {error:?}"));
    let model = test_model("classify");
    let centroid = unit(&model, 1.0, 0.0, 0.0).unwrap_or_else(|| panic!("centroid"));
    let topic = Topic {
        id: TopicId::from_ulid(u128::from(version.0) << 90 | 1),
        version,
        label: "topic".to_owned(),
        terms: Vec::new(),
        centroid,
        fitted_at: at,
    };
    let completed = catalog.complete_fit(version, vec![topic], at).await;
    assert!(completed.is_ok(), "{completed:?}");
    assert_eq!(catalog.mark_ready(version, at).await, Ok(()));
    let activated = catalog.mark_active(version, at).await;
    assert!(activated.is_ok(), "{activated:?}");
    version
}

fn memory_stores() -> (InMemoryTopicCatalog, MemoryVerdicts) {
    let catalog = memory_catalog(4, 0.5, Outbox::none()).unwrap_or_else(|| panic!("catalog"));
    (catalog, MemoryVerdicts::new(Outbox::none()))
}

fn classified_version(envelope: &Envelope) -> Option<TopicModelVersion> {
    match &envelope.event {
        BusEvent::Insight(InsightEvent::TransmissionClassified { classification, .. }) => {
            Some(classification.version)
        }
        _ => None,
    }
}

async fn saved_classification<T: TransmissionStore>(
    transmissions: &T,
    id: TransmissionId,
) -> Option<Classification> {
    match transmissions.transmission(id).await {
        Ok(Some(Transmission {
            state: TransmissionState::Classified { classification, .. },
            ..
        })) => Some(classification),
        _ => None,
    }
}

/// Which of `versions` hold an assignment of `id`.
async fn assigned_under<C: TopicCatalog>(
    catalog: &C,
    id: TransmissionId,
    versions: &[TopicModelVersion],
) -> Vec<TopicModelVersion> {
    let ids = IdBatch::new(vec![id]).unwrap_or_else(|error| panic!("{error:?}"));
    let mut out = Vec::new();
    for version in versions {
        let stored = catalog.assignments(*version, &ids).await;
        if stored.is_ok_and(|stored| stored.contains_key(&id)) {
            out.push(*version);
        }
    }
    out
}

/// The scenario every store runs: deliver, activate a newer version,
/// redeliver; then the same envelope, one assignment, one saved
/// classification.
async fn redelivery_after_a_version_change<C, T>(catalog: C, mut transmissions: T, seed: u32)
where
    C: TopicCatalog + TopicLifecycle + Clone + Send,
    T: TransmissionStore + Clone + Send,
{
    let Confirmation { stored, delivery } = confirmation(seed);
    let id = stored.id;
    assert_eq!(transmissions.save(stored).await, Ok(()));
    let mut classifier = Classifier::new(catalog.clone(), transmissions.clone());
    let first = classifier.classify(&delivery).await;
    let Ok(Some(first)) = first else {
        panic!("first delivery: {first:?}");
    };
    assert_eq!(first.id, EventId::derive(delivery.id, CLASSIFIED_LABEL, 0));
    assert_eq!(first.at, delivery.at);
    assert_eq!(classified_version(&first), Some(TopicModelVersion(0)));

    let mut catalog = catalog;
    let newer = activate_next(&mut catalog, later(10)).await;
    let second = classifier.classify(&delivery).await;
    assert_eq!(second, Ok(Some(first.clone())));
    let third = Classifier::new(catalog.clone(), transmissions.clone())
        .classify(&delivery)
        .await;
    assert_eq!(third, Ok(Some(first)));
    assert_eq!(
        assigned_under(&catalog, id, &[TopicModelVersion(0), newer]).await,
        vec![TopicModelVersion(0)]
    );
    assert_eq!(
        saved_classification(&transmissions, id)
            .await
            .map(|saved| saved.version),
        Some(TopicModelVersion(0))
    );
}

#[tokio::test]
async fn redelivery_republishes_the_same_envelope_after_a_version_change() {
    let (catalog, transmissions) = memory_stores();
    redelivery_after_a_version_change(catalog, transmissions, 1).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn pg_redelivery_republishes_the_same_envelope_after_a_version_change() {
    let Some(db) = database("pg_classifier_redelivery").await else {
        return;
    };
    let (catalog, _events) = pg_catalog(db.pool().clone(), StaticDirectory::new()).await;
    redelivery_after_a_version_change(catalog, MemoryVerdicts::new(Outbox::none()), 2).await;
}

/// A crash after the decision was saved and before the assignment: the
/// redelivery assigns under the saved version, not the newer active one.
#[tokio::test]
async fn a_crash_after_the_save_assigns_under_the_saved_version() {
    let (mut catalog, mut transmissions) = memory_stores();
    let Confirmation {
        mut stored,
        delivery,
    } = confirmation(3);
    let id = stored.id;
    let Some(confirmed) = stored.state.confirmed().cloned() else {
        panic!("not confirmed");
    };
    let saved = Classification {
        version: TopicModelVersion(0),
        topic: None,
        watched: false,
    };
    stored.state = TransmissionState::Classified {
        confirmed,
        classification: saved.clone(),
    };
    assert_eq!(transmissions.save(stored).await, Ok(()));
    let newer = activate_next(&mut catalog, later(10)).await;

    let mut classifier = Classifier::new(catalog.clone(), transmissions.clone());
    let published = classifier.classify(&delivery).await;
    let Ok(Some(published)) = published else {
        panic!("{published:?}");
    };
    assert_eq!(classified_version(&published), Some(TopicModelVersion(0)));
    assert_eq!(
        assigned_under(&catalog, id, &[TopicModelVersion(0), newer]).await,
        vec![TopicModelVersion(0)]
    );
    assert_eq!(saved_classification(&transmissions, id).await, Some(saved));
}

#[tokio::test]
async fn a_fresh_confirmation_is_saved_classified_under_the_active_version() {
    let (mut catalog, transmissions) = memory_stores();
    let newer = activate_next(&mut catalog, later(1)).await;
    let Confirmation { stored, delivery } = confirmation(4);
    let id = stored.id;
    let mut store = transmissions.clone();
    assert_eq!(store.save(stored).await, Ok(()));
    let mut classifier = Classifier::new(catalog.clone(), transmissions.clone());
    let published = classifier.classify(&delivery).await;
    assert_eq!(
        published.map(|envelope| envelope.as_ref().and_then(classified_version)),
        Ok(Some(newer))
    );
    assert_eq!(
        saved_classification(&transmissions, id)
            .await
            .map(|saved| saved.version),
        Some(newer)
    );
    assert_eq!(
        assigned_under(&catalog, id, &[TopicModelVersion(0), newer]).await,
        vec![newer]
    );
}

#[tokio::test]
async fn other_subjects_publish_nothing_and_distinct_deliveries_get_distinct_ids() {
    let (catalog, transmissions) = memory_stores();
    let mut classifier = Classifier::new(catalog, transmissions.clone());
    let a = confirmation(5);
    let b = confirmation(6);
    let mut store = transmissions;
    assert_eq!(store.save(a.stored.clone()).await, Ok(()));
    assert_eq!(store.save(b.stored.clone()).await, Ok(()));
    let first = classifier.classify(&a.delivery).await;
    let second = classifier.classify(&b.delivery).await;
    let (Ok(Some(first)), Ok(Some(second))) = (first, second) else {
        panic!("classify");
    };
    assert_ne!(first.id, second.id);
    let other = Envelope {
        id: a.delivery.id,
        at: a.delivery.at,
        event: BusEvent::Changed(Changed::TopicVersion(TopicModelVersion(0))),
    };
    assert_eq!(classifier.classify(&other).await, Ok(None));
}
