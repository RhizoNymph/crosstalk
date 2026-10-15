//! The pipeline fence: a new holder of the pipeline lock bumps the epoch,
//! then waits until every connection of an earlier pipeline incarnation (in
//! any process) is gone, and an incarnation whose epoch is no longer the
//! current one can never get a connection again
//! (`transport.pipeline-lock.no-write-from-an-older-epoch`).

use std::num::NonZeroU32;
use std::time::Duration;

use crosstalk_spec::interfaces::l2_transport::EventBus;
use crosstalk_store::sqlx::postgres::PgConnection;
use crosstalk_store::sqlx::{self, Connection, PgPool};
use crosstalk_store::{PoolSettings, StoreConfig, TestDb};
use tokio::sync::watch;
use tokio::time::Instant;

use super::{migrated, ops, pipeline_start, start_probe, stop_pipeline, within};
use crate::gateway::postgres::{self, Late, application_name, incarnation_pool};
use crate::live::recovery::{LockState, PipelinePhase};
use crate::ops::Phase;
use crate::store::fence::{Epoch, FENCE_POLL, FenceError, join};
use crate::store::lazy_pool;
use crate::store::lock::{PIPELINE_FENCE, PipelineLock, TAKE_TIMEOUT};

/// Timer and scheduling slack on a bound.
const SLACK: Duration = Duration::from_secs(2);

/// A template pool on `db` whose acquires give up after `acquire`: a
/// fenced pool's acquire retries its refused connects until then.
fn template(db: &TestDb, acquire: Duration) -> PgPool {
    let settings =
        PoolSettings::new(NonZeroU32::MIN.saturating_add(3), 0, acquire).expect("pool settings");
    lazy_pool(&StoreConfig::new(db.url().clone(), settings))
}

/// Take the pipeline lock, retrying while the last holder's session is
/// still being released by the server.
async fn take(db: &TestDb) -> PipelineLock {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if let Some(lock) = PipelineLock::try_take(db.url()).await.expect("connects") {
            return lock;
        }
        assert!(Instant::now() < deadline, "the lock never came free");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// The stored epoch, read over the test's own pool.
async fn stored_epoch(db: &TestDb) -> i64 {
    let (epoch,): (i64,) = sqlx::query_as("SELECT epoch FROM transport.pipeline_epoch")
        .fetch_one(db.pool())
        .await
        .expect("reads the epoch");
    epoch
}

/// How many sessions named `application` hold the pipeline fence, in
/// `mode` (`ShareLock` or `ExclusiveLock`).
async fn fence_holders(db: &TestDb, application: &str, mode: &str) -> i64 {
    let (count,): (i64,) = sqlx::query_as(
        "SELECT count(*) FROM pg_locks l JOIN pg_stat_activity a ON a.pid = l.pid \
         WHERE l.locktype = 'advisory' AND l.granted AND l.mode = $3 \
           AND l.classid::bigint = $1 >> 32 AND l.objid::bigint = $1 & 4294967295 \
           AND l.objsubid = 1 AND a.application_name = $2",
    )
    .bind(PIPELINE_FENCE)
    .bind(application)
    .bind(mode)
    .fetch_one(db.pool())
    .await
    .expect("reads pg_locks");
    count
}

/// How many sessions hold the pipeline fence exclusively.
async fn fence_exclusive(db: &TestDb) -> i64 {
    let (count,): (i64,) = sqlx::query_as(
        "SELECT count(*) FROM pg_locks l \
         WHERE l.locktype = 'advisory' AND l.granted AND l.mode = 'ExclusiveLock' \
           AND l.classid::bigint = $1 >> 32 AND l.objid::bigint = $1 & 4294967295 \
           AND l.objsubid = 1",
    )
    .bind(PIPELINE_FENCE)
    .fetch_one(db.pool())
    .await
    .expect("reads pg_locks");
    count
}

/// A session of its own on `db`.
async fn session(db: &TestDb) -> PgConnection {
    PgConnection::connect_with(db.url().connect_options())
        .await
        .expect("connects")
}

/// Each take bumps the stored epoch by one and the lock reports it; an
/// incarnation's pool of the current epoch connects, each connection
/// holding the fence shared. Once a newer epoch is taken, that pool can
/// never connect again (its connects are refused until its acquire gives
/// up), and joining the fence with the old epoch is refused as
/// `Fenced { mine, current }`. While a new holder holds the fence
/// exclusively, a join is refused as `Fencing`.
#[tokio::test(flavor = "multi_thread")]
async fn pg_a_fenced_incarnation_cannot_connect_after_a_newer_epoch() {
    let Some(db) = migrated("pg_a_fenced_incarnation_cannot_connect_after_a_newer_epoch").await
    else {
        return;
    };
    assert_eq!(stored_epoch(&db).await, 0, "the migration seeds epoch 0");
    let first = take(&db).await;
    let mine = first.epoch();
    assert_eq!(mine, Epoch::new(1));
    assert_eq!(stored_epoch(&db).await, 1);

    let name = "crosstalk-fence-test-old";
    let pool = incarnation_pool(&template(&db, Duration::from_secs(2)), 2, name, mine);
    let connection = pool.acquire().await.expect("the current epoch connects");
    assert_eq!(fence_holders(&db, name, "ShareLock").await, 1);
    // Closed, not returned: the next take need not wait for it.
    connection.close().await.expect("closes");
    drop(first);

    let second = take(&db).await;
    let current = second.epoch();
    assert_eq!(current, mine.next());
    assert_eq!(stored_epoch(&db).await, 2);

    let refused = tokio::time::timeout(Duration::from_secs(2) + SLACK, pool.acquire())
        .await
        .expect("the acquire gives up within its timeout");
    assert!(
        matches!(refused, Err(sqlx::Error::PoolTimedOut)),
        "a fenced pool got a connection: {refused:?}"
    );
    assert_eq!(fence_holders(&db, name, "ShareLock").await, 0);

    let mut stale = session(&db).await;
    let joined = join(&mut stale, mine).await;
    assert!(
        matches!(joined, Err(FenceError::Fenced { mine: m, current: c }) if m == mine && c == current),
        "{joined:?}"
    );
    drop(stale);

    // A new holder mid-take holds the fence exclusively: every join is
    // refused, the current epoch's too.
    let mut fencing = session(&db).await;
    let (held,): (bool,) = sqlx::query_as("SELECT pg_try_advisory_lock($1)")
        .bind(PIPELINE_FENCE)
        .fetch_one(&mut fencing)
        .await
        .expect("takes the fence");
    assert!(held);
    let mut joining = session(&db).await;
    let joined = join(&mut joining, current).await;
    assert!(
        matches!(joined, Err(FenceError::Fencing { mine: m, .. }) if m == current),
        "{joined:?}"
    );
    drop(fencing);
    drop(joining);

    pool.close().await;
    drop(second);
    db.close().await.expect("drops");
}

/// A new holder's take waits while a connection of an older incarnation
/// holds the fence shared, longer than [`TAKE_TIMEOUT`] (the fence wait
/// never counts against it and never gives up), reporting its epoch as it
/// starts waiting; once that connection closes the take completes within
/// a poll and some slack.
#[tokio::test(flavor = "multi_thread")]
async fn pg_a_take_waits_for_an_older_connection_and_completes_once_it_closes() {
    let Some(db) =
        migrated("pg_a_take_waits_for_an_older_connection_and_completes_once_it_closes").await
    else {
        return;
    };
    let first = take(&db).await;
    let old = first.epoch();
    let name = "crosstalk-fence-test-older";
    let pool = incarnation_pool(&template(&db, Duration::from_secs(10)), 2, name, old);
    let older = pool.acquire().await.expect("the current epoch connects");
    drop(first);

    let (reported_tx, mut reported) = watch::channel(None);
    let url = db.url().clone();
    let taking = tokio::spawn(async move {
        loop {
            let reported_tx = reported_tx.clone();
            let taken = PipelineLock::try_take_reporting(&url, move |epoch| {
                let _ = reported_tx.send(Some(epoch));
            })
            .await
            .expect("the take never fails while it waits");
            if let Some(lock) = taken {
                return lock;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    });

    let fencing = tokio::time::timeout(Duration::from_secs(30), reported.wait_for(Option::is_some))
        .await
        .expect("the take reports its fence wait")
        .expect("the taker lives")
        .expect("an epoch");
    assert_eq!(fencing, old.next());
    let waiting_since = Instant::now();
    tokio::time::sleep(TAKE_TIMEOUT + Duration::from_secs(1)).await;
    assert!(
        !taking.is_finished(),
        "the take finished while an older connection held the fence"
    );
    assert_eq!(fence_holders(&db, name, "ShareLock").await, 1);

    older.close().await.expect("closes");
    let closed = Instant::now();
    let lock = tokio::time::timeout(FENCE_POLL + SLACK, taking)
        .await
        .expect("the take completes promptly once the connection closed")
        .expect("the taker");
    eprintln!(
        "the take waited {:?}, completing {:?} after the older connection closed",
        waiting_since.elapsed(),
        closed.elapsed()
    );
    assert_eq!(lock.epoch(), fencing);
    assert_eq!(
        stored_epoch(&db).await,
        i64::try_from(fencing.get()).expect("fits")
    );
    assert_eq!(fence_holders(&db, name, "ShareLock").await, 0);
    assert_eq!(
        fence_exclusive(&db).await,
        0,
        "the new holder released the exclusive fence"
    );
    pool.close().await;
    drop(lock);
    db.close().await.expect("drops");
}

/// The pipeline task, taking the lock while an older incarnation's
/// connection still holds the fence, reports `fencing` (the lock held,
/// the new epoch in the report): `/readyz` 200 degraded, `pipeline:
/// fencing`, recovery pending. Once the connection closes the incarnation
/// starts under the new epoch, its sessions named for it and each holding
/// the fence shared.
#[tokio::test(flavor = "multi_thread")]
async fn pg_the_pipeline_reports_fencing_while_an_older_connection_lives() {
    let Some(db) =
        migrated("pg_the_pipeline_reports_fencing_while_an_older_connection_lives").await
    else {
        return;
    };
    let first = take(&db).await;
    let old = first.epoch();
    let name = "crosstalk-fence-test-previous-owner";
    let older_pool = incarnation_pool(&template(&db, Duration::from_secs(10)), 2, name, old);
    let older = older_pool.acquire().await.expect("connects");
    drop(first);

    let dir = tempfile::tempdir().expect("a temp dir");
    let late = Late::default();
    let (start, _gate, spool, _bus) = pipeline_start(&db, dir.path(), late.clone()).await;
    let ops = ops(&start, spool, &late);
    let mut status = start.status.reader();
    let probe = start_probe(&start);
    let task = postgres::spawn_pipeline(start);
    let fencing = within(
        "the pipeline fencing",
        status.wait_until(|status| status.phase == PipelinePhase::Fencing),
        &probe,
    )
    .await
    .expect("the reporter lives");
    assert_eq!(fencing.lock, LockState::Held, "{fencing:?}");
    assert_eq!(fencing.report.epoch, old.next(), "{fencing:?}");
    assert_eq!(fencing.pipeline_text(), "fencing");
    let ready = ops.readiness().await;
    assert!(ready.ready, "200 while fencing: {ready:?}");
    assert_eq!(ready.status, Phase::Degraded, "{ready:?}");
    assert_eq!(ready.pipeline.as_deref(), Some("fencing"));
    assert_eq!(ready.recovery.as_deref(), Some("pending"));
    assert_eq!(ready.pipeline_lock.as_deref(), Some("held"));
    let health = ops.health();
    assert_eq!(
        health.recovery.map(|recovery| recovery.epoch),
        Some(old.next()),
        "/healthz reports the epoch being fenced for"
    );
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(
        status.snapshot().phase,
        PipelinePhase::Fencing,
        "still fencing while the older connection lives"
    );

    older.close().await.expect("closes");
    let running = within(
        "the pipeline running after the fence",
        status.wait_until(|status| status.phase == PipelinePhase::Running),
        &probe,
    )
    .await
    .expect("the reporter lives");
    assert_eq!(running.report.epoch, old.next());
    assert_eq!(running.report.incarnation, 1);
    let current = application_name(1, old.next());
    assert!(
        current.ends_with(&format!("-1-e{}", old.next().get())),
        "{current}"
    );
    assert!(
        fence_holders(&db, &current, "ShareLock").await >= 1,
        "the running incarnation's sessions hold the fence shared"
    );
    stop_pipeline(task, &probe).await;
    older_pool.close().await;
    db.close().await.expect("drops");
}

/// Capture's publisher pool (and the process's template pool) never join
/// the fence (INV-1237): while a newer epoch is being fenced for and the
/// fence is held exclusively, capture's bus publishes and the template
/// pool answers, and none of their sessions holds the fence; an older
/// incarnation's pool is refused meanwhile.
#[tokio::test(flavor = "multi_thread")]
async fn pg_the_publisher_pool_is_unaffected_by_the_fence() {
    let Some(db) = migrated("pg_the_publisher_pool_is_unaffected_by_the_fence").await else {
        return;
    };
    let dir = tempfile::tempdir().expect("a temp dir");
    let (start, _gate, _spool, capture_bus) =
        pipeline_start(&db, dir.path(), Late::default()).await;
    let lock = take(&db).await;
    let old = lock.epoch();
    drop(lock);
    let newer = take(&db).await;
    assert_eq!(newer.epoch(), old.next());
    let mut fencing = session(&db).await;
    let (held,): (bool,) = sqlx::query_as("SELECT pg_try_advisory_lock($1)")
        .bind(PIPELINE_FENCE)
        .fetch_one(&mut fencing)
        .await
        .expect("takes the fence");
    assert!(held, "the fence is free once the take completed");

    tokio::time::timeout(
        Duration::from_secs(10),
        capture_bus.publish(super::envelope(
            0xFE7CE,
            crosstalk_spec::support::Timestamp::from_micros(super::T0),
        )),
    )
    .await
    .expect("capture publishes while the fence is held exclusively")
    .expect("published");
    let (answer,): (i32,) = tokio::time::timeout(
        Duration::from_secs(10),
        sqlx::query_as("SELECT 1").fetch_one(&start.pool),
    )
    .await
    .expect("the template pool answers while the fence is held exclusively")
    .expect("connects");
    assert_eq!(answer, 1);
    let capture_name = format!("crosstalk-capture-{}", std::process::id());
    assert_eq!(fence_holders(&db, &capture_name, "ShareLock").await, 0);
    assert_eq!(fence_holders(&db, "crosstalk", "ShareLock").await, 0);

    let older = incarnation_pool(
        &template(&db, Duration::from_secs(1)),
        2,
        "crosstalk-fence-test-stale",
        old,
    );
    assert!(
        tokio::time::timeout(Duration::from_secs(1) + SLACK, older.acquire())
            .await
            .expect("bounded")
            .is_err(),
        "the older incarnation is refused meanwhile"
    );
    drop(fencing);
    older.close().await;
    capture_bus.shutdown();
    drop(newer);
    db.close().await.expect("drops");
}
