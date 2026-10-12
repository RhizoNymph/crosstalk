//! The pipeline lock across a silent network partition: the holder must
//! report the lock possibly lost within a bound strictly shorter than the
//! server's reaping of the partitioned session, since after that reap
//! another process may take the lock (INV-1236).

use std::time::Duration;

use crosstalk_store::TestDb;
use crosstalk_testkit::db_link::DbLink;
use tokio::time::Instant;

use super::{database, migrated, ops, pipeline_start, start_probe, stop_pipeline, within};
use crate::gateway::postgres::{self, LOST_REPORTED, Late};
use crate::live::recovery::{LockState, PipelinePhase};
use crate::store::lock::{
    DETECTION, LockLoss, PING_TIMEOUT, PipelineLock, SERVER_REAP, TAKE_TIMEOUT, TakeError,
};

/// The detection bound under test: a ping's interval plus a ping's deadline.
const BOUND: Duration = DETECTION;

/// Timer and scheduling slack on top of a bound.
const SLACK: Duration = Duration::from_secs(1);

/// A link in front of `db`'s server.
async fn link_to(db: &TestDb) -> DbLink {
    let options = db.url().connect_options();
    DbLink::start(options.get_host(), options.get_port())
        .await
        .expect("the db link")
}

/// A lock session whose link is silently partitioned (nothing answered,
/// nothing reset) is reported lost within the bound, and the timed-out
/// connection is dropped: once the link heals the server ends the session
/// and another session takes the lock.
#[tokio::test(flavor = "multi_thread")]
async fn pg_a_silently_partitioned_lock_session_is_lost_within_the_bound() {
    let Some(db) =
        database("pg_a_silently_partitioned_lock_session_is_lost_within_the_bound").await
    else {
        return;
    };
    let link = link_to(&db).await;
    let url = db.url().with_address(&link.host(), link.port());
    let mut lock = PipelineLock::try_take(&url)
        .await
        .expect("connects through the link")
        .expect("takes the lock");

    link.partition();
    let partitioned = Instant::now();
    let why = tokio::time::timeout(BOUND + SLACK, lock.lost())
        .await
        .unwrap_or_else(|_| {
            panic!(
                "a silently partitioned lock session was not reported lost within {:?}",
                BOUND + SLACK
            )
        });
    let detected = partitioned.elapsed();
    tracing::info!(?detected, reason = why.reason(), %why, "lock loss detected");
    assert_eq!(
        why,
        LockLoss::PingTimedOut {
            after: PING_TIMEOUT
        },
        "a silent partition times a ping out"
    );
    assert!(BOUND < SERVER_REAP);
    assert!(
        detected <= BOUND + SLACK,
        "detected after {detected:?}: {why}"
    );

    // The holder dropped its connection; healed, the close crosses, the
    // server ends the session and the lock is free again.
    link.restore();
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let taken = PipelineLock::try_take(db.url())
            .await
            .expect("connects directly");
        if taken.is_some() {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "the partitioned session still holds the lock after the link healed"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    drop(lock);
    drop(link);
    db.close().await.expect("drops");
}

/// Taking the lock across a silent partition fails within
/// [`TAKE_TIMEOUT`] instead of hanging the pipeline's start.
#[tokio::test(flavor = "multi_thread")]
async fn pg_taking_the_lock_across_a_silent_partition_times_out() {
    let Some(db) = database("pg_taking_the_lock_across_a_silent_partition_times_out").await else {
        return;
    };
    let link = link_to(&db).await;
    let url = db.url().with_address(&link.host(), link.port());
    link.partition();
    let started = Instant::now();
    let taken = tokio::time::timeout(TAKE_TIMEOUT + SLACK, PipelineLock::try_take(&url))
        .await
        .expect("taking the lock is bounded");
    let took = started.elapsed();
    assert!(
        matches!(taken, Err(TakeError::TimedOut { after }) if after == TAKE_TIMEOUT),
        "{taken:?}"
    );
    tracing::info!(?took, "taking the lock timed out");
    drop(link);
    db.close().await.expect("drops");
}

/// The node0 bench's bug, end to end in the pipeline task: a running
/// pipeline whose lock session is silently partitioned reports
/// `pipeline_lock: lost` and stops within [`LOST_REPORTED`], instead of
/// reading `held` and `running` until the link heals.
#[tokio::test(flavor = "multi_thread")]
async fn pg_a_running_pipeline_reports_its_partitioned_lock_lost_within_the_bound() {
    let Some(db) =
        migrated("pg_a_running_pipeline_reports_its_partitioned_lock_lost_within_the_bound").await
    else {
        return;
    };
    let link = link_to(&db).await;
    let dir = tempfile::tempdir().expect("a temp dir");
    let late = Late::default();
    let (mut start, _gate, spool, _bus) = pipeline_start(&db, dir.path(), late.clone()).await;
    // Only the lock's own session crosses the link; the pool goes direct.
    start.url = db.url().with_address(&link.host(), link.port());
    let ops = ops(&start, spool, &late);
    let mut status = start.status.reader();
    let probe = start_probe(&start);
    let task = postgres::spawn_pipeline(start);
    within(
        "the pipeline running",
        status.wait_until(|status| status.phase == PipelinePhase::Running),
        &probe,
    )
    .await
    .expect("the reporter lives");

    link.partition();
    let partitioned = Instant::now();
    let lost = tokio::time::timeout(
        LOST_REPORTED + SLACK,
        status.wait_until(|status| status.lock == LockState::Lost),
    )
    .await
    .unwrap_or_else(|_| {
        panic!(
            "pipeline_lock still not lost {:?} after a silent partition",
            LOST_REPORTED + SLACK
        )
    })
    .expect("the reporter lives");
    let reported = partitioned.elapsed();
    tracing::info!(?reported, pipeline = %lost.pipeline_text(), "lock loss reported");
    assert!(
        matches!(&lost.phase, PipelinePhase::Stopped(why) if why.contains("did not answer a ping")),
        "{:?}",
        lost.phase
    );
    let ready = ops.readiness().await;
    assert!(!ready.ready, "{ready:?}");
    assert_eq!(ready.pipeline_lock.as_deref(), Some("lost"));

    stop_pipeline(task, &probe).await;
    drop(link);
    db.close().await.expect("drops");
}
