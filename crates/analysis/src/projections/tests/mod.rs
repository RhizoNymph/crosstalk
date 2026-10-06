//! Tests of [`PgProjectionStore`](super::PgProjectionStore) against
//! Postgres: the memory crate's projection harness, and focused cases for
//! concurrency and restarts. Every test is skipped when
//! `TEST_DATABASE_URL` is not configured.

use std::sync::Arc;
use std::time::Duration;

use crosstalk_memory::analysis::projection::ProjectionConfig as ReferenceConfig;
use crosstalk_memory::model::HarnessConfig;
use crosstalk_memory::model::analysis::{check_projection_store, harness_model};
use crosstalk_memory::model::build::{operator, projection, ts, window};
use crosstalk_spec::aggregates::filter::TopologyFilter;
use crosstalk_spec::aggregates::projection::{
    ProjectionInfo, ProjectionLimit, ProjectionParams, ProjectionSpec, ProjectionStatusKind,
};
use crosstalk_spec::aggregates::topic::TopicModelVersion;
use crosstalk_spec::interfaces::l6_analysis::{ProjectionStore, ProjectionStoreError};
use crosstalk_store::DatabaseUrl;
use sqlx::PgPool;

use super::{PgProjectionStore, ProjectionParts, ProjectionStoreConfig};
use crate::pg::testing::{CURSOR_KEY, DiscardSink, case_pool, database, off_runtime, retry};

type Subject = PgProjectionStore<DiscardSink>;

fn config_of(reference: ReferenceConfig) -> ProjectionStoreConfig {
    ProjectionStoreConfig {
        lease: reference.lease,
        frame_retention: reference.frame_retention,
    }
}

async fn open(pool: PgPool, config: ProjectionStoreConfig) -> Subject {
    PgProjectionStore::open(
        pool,
        config,
        ProjectionParts {
            sink: Arc::new(DiscardSink),
            cursor_key: CURSOR_KEY,
            retry: retry(),
        },
    )
    .await
    .unwrap_or_else(|error| panic!("opening the projection store: {error}"))
}

/// A fresh store over emptied tables for one harness case.
async fn make(url: DatabaseUrl, reference: ReferenceConfig) -> Subject {
    open(case_pool(&url).await, config_of(reference)).await
}

#[tokio::test(flavor = "multi_thread")]
async fn projection_store_matches_the_reference() {
    let Some(db) = database("projection_store_matches_the_reference").await else {
        return;
    };
    let url = db.url().clone();
    let result = off_runtime(move || {
        check_projection_store(
            HarnessConfig {
                cases: 24,
                max_ops: 40,
            },
            move |config| make(url.clone(), config),
        )
    });
    if let Err(mismatch) = result {
        panic!("{mismatch}");
    }
}

fn queued(job: u64, at: u64) -> ProjectionInfo {
    let params = ProjectionLimit::new(5)
        .ok()
        .and_then(|limit| ProjectionParams::new(limit, 2, 100, 3).ok())
        .unwrap_or_else(|| panic!("params"));
    let spec = ProjectionSpec::new(
        window(0, 1_000).unwrap_or_else(|| panic!("window")),
        TopologyFilter::default(),
        TopicModelVersion(0),
        params,
        harness_model(),
    );
    ProjectionInfo::queued(projection(job), spec, operator(1), ts(at))
}

fn test_config() -> ProjectionStoreConfig {
    ProjectionStoreConfig {
        lease: Duration::from_micros(50),
        frame_retention: Duration::from_micros(500),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn concurrent_enqueues_never_pass_the_pending_bound() {
    let Some(db) = database("concurrent_enqueues_never_pass_the_bound").await else {
        return;
    };
    let store = open(db.pool().clone(), test_config()).await;
    let mut tasks = Vec::new();
    for job in 0..20u64 {
        let mut store = store.clone();
        tasks.push(tokio::spawn(async move {
            store.enqueue(queued(job, 100 + job)).await
        }));
    }
    let mut accepted = 0u32;
    for task in tasks {
        match task.await {
            Ok(Ok(())) => accepted += 1,
            Ok(Err(ProjectionStoreError::QueueFull)) => {}
            other => panic!("enqueue: {other:?}"),
        }
    }
    assert_eq!(accepted, Subject::MAX_PENDING);
}

#[tokio::test(flavor = "multi_thread")]
async fn concurrent_claims_take_distinct_jobs_oldest_first() {
    let Some(db) = database("concurrent_claims_take_distinct_jobs").await else {
        return;
    };
    let mut store = open(db.pool().clone(), test_config()).await;
    for job in 0..4u64 {
        assert_eq!(store.enqueue(queued(job, 100 - job)).await, Ok(()));
    }
    let (mut a, mut b) = (store.clone(), store.clone());
    let (first, second) = tokio::join!(a.claim(ts(200)), b.claim(ts(200)));
    let mut claimed: Vec<_> = [first, second]
        .into_iter()
        .map(|claim| match claim {
            Ok(Some(info)) => info.id(),
            other => panic!("claim: {other:?}"),
        })
        .collect();
    claimed.sort();
    // The two oldest requests: jobs 3 and 2.
    assert_eq!(claimed, vec![projection(2), projection(3)]);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_restart_keeps_jobs_and_requeues_a_lapsed_claim() {
    let Some(db) = database("a_restart_keeps_jobs").await else {
        return;
    };
    let mut first = open(db.pool().clone(), test_config()).await;
    assert_eq!(first.enqueue(queued(1, 100)).await, Ok(()));
    let claimed = first.claim(ts(110)).await;
    assert!(matches!(claimed, Ok(Some(_))), "{claimed:?}");
    drop(first);
    // The fitter died with the claim; a new process requeues it once the
    // lease lapsed and claims it again.
    let mut second = open(db.pool().clone(), test_config()).await;
    let fitting = second.info(projection(1)).await;
    assert_eq!(
        fitting.map(|info| info.map(|info| info.status().kind())),
        Ok(Some(ProjectionStatusKind::Fitting))
    );
    assert_eq!(second.requeue_lapsed(ts(160)).await, Ok(0));
    assert_eq!(second.requeue_lapsed(ts(161)).await, Ok(1));
    let again = second.claim(ts(170)).await;
    assert_eq!(
        again.map(|info| info.map(|info| info.id())),
        Ok(Some(projection(1)))
    );
}
