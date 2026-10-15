//! The capture gate's watch over Postgres
//! (`ingress.capture.gate-opens-once-groups-exist`): the group check finds
//! exactly the pipeline groups whose rows exist and is bounded by the
//! pipeline's timeouts; a process without the lock publishes directly once
//! another process subscribed every group, and on a fresh database it
//! spools until the groups exist.

use std::num::NonZeroU32;
use std::time::Duration;

use crosstalk_spec::events::{Envelope, Subject};
use crosstalk_spec::interfaces::l2_transport::{EventBus, RetryPolicy};
use crosstalk_spec::support::Timestamp;
use crosstalk_store::sqlx;
use crosstalk_store::{DbFailure, TestDb, TimeoutKind};
use crosstalk_transport::SpoolState;
use tokio::time::Instant;

use super::{
    Probe, T0, bus_config, envelope, migrated, pipeline_pool, pipeline_start, start_probe,
    stop_pipeline, within,
};
use crate::gateway::postgres::gate_watch::{
    GATE_POLL, GroupCheck, GroupCheckError, GroupsFound, PgGroupCheck,
};
use crate::gateway::postgres::{self, Late};
use crate::live::pg::bus_clock;
use crate::live::pg::groups::{ExchangeLogGroup, PipelineGroups};
use crate::live::recovery::{LockState, PipelinePhase};
use crate::spool::Gate;
use crate::store::lock::PipelineLock;
use crate::store::timeouts::PipelineTimeouts;

/// Timer and scheduling slack on a bound.
const SLACK: Duration = Duration::from_secs(2);

/// How long a non-holder's gate may take to open once every group exists:
/// the look at start or the next poll, the look's own round trips.
const GATE_OPENS: Duration = GATE_POLL.saturating_add(SLACK);

fn retry() -> RetryPolicy {
    RetryPolicy::new(
        NonZeroU32::MIN.saturating_add(4),
        Duration::from_millis(10),
        Duration::from_millis(100),
    )
    .expect("a retry policy")
}

/// Whether the log holds `envelope`, and how many times.
async fn in_log(db: &TestDb, envelope: &Envelope) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM transport.events WHERE id = $1")
        .bind(envelope.id.ulid_text())
        .fetch_one(db.pool())
        .await
        .expect("reads the log")
}

async fn group_rows(db: &TestDb) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM transport.groups")
        .fetch_one(db.pool())
        .await
        .expect("reads the groups")
}

/// Wait up to `bound` until `gate` is open; `false` past it.
async fn opens_within(gate: &Gate, bound: Duration) -> bool {
    let deadline = Instant::now() + bound;
    while !gate.is_open() {
        if Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    true
}

/// The check names exactly the pipeline groups with no row, in the list's
/// order, and finds them all once each was subscribed (by any bus).
#[tokio::test(flavor = "multi_thread")]
async fn pg_the_group_check_finds_only_the_groups_that_exist() {
    let Some(db) = migrated("pg_the_group_check_finds_only_the_groups_that_exist").await else {
        return;
    };
    let names = PipelineGroups::new(ExchangeLogGroup::Subscribed).names();
    let check = PgGroupCheck::new(pipeline_pool(&db), PipelineTimeouts::default());
    assert_eq!(
        check.found(&names).await.expect("the check answers"),
        crate::gateway::postgres::gate_watch::missing_from(names.clone()),
        "a fresh database has none of the groups"
    );

    let bus = crosstalk_transport::PgBus::new(pipeline_pool(&db), bus_clock(), bus_config())
        .expect("a bus");
    let groups = PipelineGroups::new(ExchangeLogGroup::Subscribed).groups();
    let (first, rest) = groups.split_at(2);
    let mut kept = Vec::new();
    for group in first {
        kept.push(
            bus.subscribe(&[Subject::Changed], group.clone(), retry())
                .await
                .expect("subscribes"),
        );
    }
    match check.found(&names).await.expect("the check answers") {
        GroupsFound::Missing(missing) => assert_eq!(
            missing.names(),
            rest.iter().map(|group| group.0.clone()).collect::<Vec<_>>(),
            "the groups not yet subscribed, in order"
        ),
        GroupsFound::All => panic!("two of the groups exist, not all"),
    }
    for group in rest {
        kept.push(
            bus.subscribe(&[Subject::Changed], group.clone(), retry())
                .await
                .expect("subscribes"),
        );
    }
    assert_eq!(
        check.found(&names).await.expect("the check answers"),
        GroupsFound::All
    );
    drop(kept);
    bus.shutdown();
    db.close().await.expect("drops");
}

/// With `transport.groups` locked elsewhere, the check returns at the
/// pipeline's lock bound (a lock timeout), not when the blocker lets go.
#[tokio::test(flavor = "multi_thread")]
async fn pg_a_blocked_group_check_returns_at_its_lock_bound() {
    let Some(db) = migrated("pg_a_blocked_group_check_returns_at_its_lock_bound").await else {
        return;
    };
    let timeouts = PipelineTimeouts::new(
        Duration::from_secs(2),
        Duration::from_millis(300),
        Duration::from_secs(10),
    )
    .expect("valid bounds");
    let check = PgGroupCheck::new(pipeline_pool(&db), timeouts);
    let names = PipelineGroups::new(ExchangeLogGroup::Subscribed).names();
    let mut blocker = db.pool().begin().await.expect("begins");
    sqlx::query("LOCK TABLE transport.groups IN ACCESS EXCLUSIVE MODE")
        .execute(&mut *blocker)
        .await
        .expect("the blocker locks the groups");
    let began = Instant::now();
    let found = check.found(&names).await;
    let took = began.elapsed();
    assert_eq!(
        found,
        Err(GroupCheckError::Failed(DbFailure::TimedOut(
            TimeoutKind::Lock
        ))),
        "after {took:?}"
    );
    assert!(
        took < Duration::from_millis(300) + SLACK,
        "the blocked check returned after {took:?}, past the 300 ms lock bound"
    );
    blocker.rollback().await.expect("rolls back");
    db.close().await.expect("drops");
}

/// The regression (node0 two-gateway fault run): a process that starts
/// while another holds the lock, on a database whose groups exist, opens
/// its gate within a poll and publishes its captures directly to the log;
/// nothing waits in its spool for the lock.
#[tokio::test(flavor = "multi_thread")]
async fn pg_a_non_holder_publishes_directly_once_the_groups_exist() {
    let Some(db) = migrated("pg_a_non_holder_publishes_directly_once_the_groups_exist").await
    else {
        return;
    };
    let owner_dir = tempfile::tempdir().expect("a temp dir");
    let (owner, _owner_gate, _owner_spool, _owner_bus) =
        pipeline_start(&db, owner_dir.path(), Late::default()).await;
    let mut owner_status = owner.status.reader();
    let owner_probe = start_probe(&owner);
    let owner_task = postgres::spawn_pipeline(owner);
    within(
        "the owner running",
        owner_status.wait_until(|status| status.phase == PipelinePhase::Running),
        &owner_probe,
    )
    .await
    .expect("the reporter lives");

    let standby_dir = tempfile::tempdir().expect("a temp dir");
    let (standby, gate, spool, _bus) =
        pipeline_start(&db, standby_dir.path(), Late::default()).await;
    let mut status = standby.status.reader();
    let probe = start_probe(&standby);
    let began = Instant::now();
    let task = postgres::spawn_pipeline(standby);
    assert!(
        opens_within(&gate, GATE_OPENS).await,
        "the standby's gate stayed closed for {GATE_OPENS:?} with every group present; status \
         {:?}\n{}",
        status.snapshot(),
        probe.diagnosis().await
    );
    eprintln!(
        "the standby's gate opened {:?} after its start",
        began.elapsed()
    );
    let standing_by = within(
        "the standby seeing the lock",
        status.wait_until(|status| status.lock == LockState::HeldElsewhere),
        &probe,
    )
    .await
    .expect("the reporter lives");
    assert_eq!(standing_by.phase, PipelinePhase::WaitingForLock);

    let captured = envelope(0x6A7E_0001, Timestamp::from_micros(T0));
    spool.publish(captured.clone()).await.expect("published");
    let stats = spool.stats();
    assert_eq!(
        stats.appended, 0,
        "nothing went through the spool: {stats:?}"
    );
    assert_eq!(stats.state, SpoolState::Direct, "{stats:?}");
    assert_eq!(in_log(&db, &captured).await, 1, "the capture is in the log");
    assert_eq!(
        status.snapshot().lock,
        LockState::HeldElsewhere,
        "the standby never took the lock"
    );

    stop_pipeline(task, &probe).await;
    stop_pipeline(owner_task, &owner_probe).await;
    db.close().await.expect("drops");
}

/// A fresh database: while another session holds the lock and no group
/// exists, a non-holder's gate stays closed through several polls and its
/// captures wait in the spool; once it takes the lock its own subscribe
/// opens the gate and the spool drains to the log once.
#[tokio::test(flavor = "multi_thread")]
async fn pg_a_non_holder_on_a_fresh_database_spools_until_the_groups_exist() {
    let Some(db) =
        migrated("pg_a_non_holder_on_a_fresh_database_spools_until_the_groups_exist").await
    else {
        return;
    };
    let rival = PipelineLock::try_take(db.url())
        .await
        .expect("connects")
        .expect("the rival takes the lock");
    let dir = tempfile::tempdir().expect("a temp dir");
    let (start, gate, spool, _bus) = pipeline_start(&db, dir.path(), Late::default()).await;
    let mut status = start.status.reader();
    let probe: Probe = start_probe(&start);
    let task = postgres::spawn_pipeline(start);
    within(
        "the process seeing the lock",
        status.wait_until(|status| status.lock == LockState::HeldElsewhere),
        &probe,
    )
    .await
    .expect("the reporter lives");

    let captured = envelope(0x6A7E_0002, Timestamp::from_micros(T0));
    spool.publish(captured.clone()).await.expect("spooled");
    tokio::time::sleep(GATE_POLL * 3 + SLACK).await;
    assert!(!gate.is_open(), "the gate opened with no group present");
    let stats = spool.stats();
    assert_eq!(
        stats.records, 1,
        "the capture waits in the spool: {stats:?}"
    );
    assert_eq!(stats.state, SpoolState::Spooling, "{stats:?}");
    assert_eq!(group_rows(&db).await, 0, "no group exists yet");
    assert_eq!(in_log(&db, &captured).await, 0, "nothing reached the log");

    drop(rival);
    within(
        "the process taking the lock and running",
        status.wait_until(|status| status.phase == PipelinePhase::Running),
        &probe,
    )
    .await
    .expect("the reporter lives");
    assert!(gate.is_open(), "the pipeline's subscribe opened the gate");
    within(
        "the spool's drain",
        async {
            while spool.stats().records > 0 {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        },
        &probe,
    )
    .await;
    assert_eq!(in_log(&db, &captured).await, 1, "drained to the log once");

    stop_pipeline(task, &probe).await;
    db.close().await.expect("drops");
}
