//! The pipeline's Postgres waits are bounded: every connection of a
//! pipeline incarnation runs with `statement_timeout` and `lock_timeout`
//! (the store section's `pipeline_timeouts`), so a server that answers but
//! stalls a statement (a row or table lock held elsewhere, a stuck vacuum,
//! disk pressure) cancels it instead of hanging a stage or the reaper; the
//! cancel is transient and the work is retried. Capture's publisher pool
//! runs without them, and `/healthz` bounds its bus read itself
//! (`transport.pipeline.statements-bounded`).

use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::Duration;

use crosstalk_memory::support::ManualClock;
use crosstalk_spec::events::ingest::IngestEvent;
use crosstalk_spec::events::{BusEvent, Envelope, Subject};
use crosstalk_spec::ids::EventId;
use crosstalk_spec::interfaces::l2_transport::{
    ConsumerGroup, EventBus, RetryPolicy, Subscription,
};
use crosstalk_spec::support::{Clock, Timestamp};
use crosstalk_store::sqlx::{self, Connection, PgPool};
use crosstalk_store::{DbFailure, TestDb, TimeoutKind, classify, statement_timeouts};
use crosstalk_transport::{NonZeroDuration, PgBus, PgBusConfig, SpoolingBus};
use tokio::time::Instant;

use super::{T0, migrated, ops, pipeline_pool, secret, start_probe, stop_pipeline, within};
use crate::config::GatewayConfig;
use crate::gateway::postgres::{
    self, Late, PipelineStart, derived_pool, incarnation_pool, incarnation_pool_bounded,
};
use crate::live::pg::bus_clock;
use crate::live::recovery::PipelinePhase;
use crate::live::{LiveClock, Slot};
use crate::ops::{BusSection, BusUnavailable, HEALTH_BUS_BOUND};
use crate::role::Role;
use crate::spool::{Gate, Gated};
use crate::store::fence::Epoch;
use crate::store::timeouts::PipelineTimeouts;

/// Timer and scheduling slack on a bound.
const SLACK: Duration = Duration::from_secs(2);

/// Short bounds, so a test sees a cancel within a second: the lock wait
/// well below the statement's.
fn short() -> PipelineTimeouts {
    PipelineTimeouts::new(
        Duration::from_secs(2),
        Duration::from_millis(300),
        Duration::from_secs(10),
    )
    .expect("valid bounds")
}

/// A setting as the server shows it on `pool`'s sessions.
async fn show(pool: &PgPool, setting: &'static str) -> String {
    let (value,): (String,) = sqlx::query_as(sqlx::AssertSqlSafe(format!("SHOW {setting}")))
        .fetch_one(pool)
        .await
        .expect("reads a setting");
    value
}

/// Poll `check` until it holds, for at most `limit`; `false` past it.
async fn eventually<F, Fut>(limit: Duration, mut check: F) -> bool
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let deadline = Instant::now() + limit;
    loop {
        if check().await {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// A pipeline incarnation's sessions carry the configured bounds (the
/// defaults when none are configured: 60 s and 15 s); capture's publisher
/// pool carries none (capture is never cancelled by them, and a stalled
/// append spools at the publish's own client-side timeout).
#[tokio::test(flavor = "multi_thread")]
async fn pg_pipeline_pools_carry_the_bounds_and_the_publisher_pool_none() {
    let Some(db) = migrated("pg_pipeline_pools_carry_the_bounds_and_the_publisher_pool_none").await
    else {
        return;
    };
    let template = pipeline_pool(&db);
    let bounded = incarnation_pool_bounded(
        &template,
        2,
        "crosstalk-timeouts-test-bounded",
        Epoch::new(0),
        short(),
    );
    assert_eq!(show(&bounded, "statement_timeout").await, "2s");
    assert_eq!(show(&bounded, "lock_timeout").await, "300ms");
    assert_eq!(
        show(&bounded, "idle_in_transaction_session_timeout").await,
        "30s",
        "the shared session setup still runs"
    );

    let defaults = incarnation_pool(
        &template,
        2,
        "crosstalk-timeouts-test-defaults",
        Epoch::new(0),
    );
    assert_eq!(show(&defaults, "statement_timeout").await, "1min");
    assert_eq!(show(&defaults, "lock_timeout").await, "15s");

    let publisher = derived_pool(&template, 2, "crosstalk-timeouts-test-capture");
    assert_eq!(show(&publisher, "statement_timeout").await, "0");
    assert_eq!(show(&publisher, "lock_timeout").await, "0");
    assert_eq!(
        show(&template, "statement_timeout").await,
        "0",
        "the template pool (probes, the head check) is not bounded either"
    );

    for pool in [bounded, defaults, publisher, template] {
        pool.close().await;
    }
    db.close().await.expect("drops");
}

/// A statement on a pipeline connection that runs past
/// `statement_timeout` is cancelled by the server (SQLSTATE 57014),
/// classified a transient statement timeout, and counted.
#[tokio::test(flavor = "multi_thread")]
async fn pg_a_pipeline_statement_past_its_bound_is_cancelled_as_a_statement_timeout() {
    let Some(db) =
        migrated("pg_a_pipeline_statement_past_its_bound_is_cancelled_as_a_statement_timeout")
            .await
    else {
        return;
    };
    let pool = incarnation_pool_bounded(
        &pipeline_pool(&db),
        2,
        "crosstalk-timeouts-test-statement",
        Epoch::new(0),
        short(),
    );
    let before = statement_timeouts();
    let began = Instant::now();
    let cancelled = sqlx::query("SELECT pg_sleep(30)")
        .execute(&pool)
        .await
        .expect_err("the server cancels the statement");
    let took = began.elapsed();
    assert_eq!(
        classify(&cancelled),
        DbFailure::TimedOut(TimeoutKind::Statement)
    );
    assert!(classify(&cancelled).is_transient());
    assert!(
        took < Duration::from_secs(2) + SLACK,
        "cancelled at its bound, not after {took:?}"
    );
    assert!(statement_timeouts().statement > before.statement);
    // The connection is still usable: the cancel ended the statement only.
    let (one,): (i32,) = sqlx::query_as("SELECT 1")
        .fetch_one(&pool)
        .await
        .expect("the pool still answers");
    assert_eq!(one, 1);
    pool.close().await;
    db.close().await.expect("drops");
}

/// A lock wait on a pipeline connection (here an advisory lock another
/// session holds) past `lock_timeout` is cancelled by the server (SQLSTATE
/// 55P03), classified a transient lock timeout, and counted.
#[tokio::test(flavor = "multi_thread")]
async fn pg_a_pipeline_lock_wait_past_its_bound_is_cancelled_as_a_lock_timeout() {
    let Some(db) =
        migrated("pg_a_pipeline_lock_wait_past_its_bound_is_cancelled_as_a_lock_timeout").await
    else {
        return;
    };
    let pool = incarnation_pool_bounded(
        &pipeline_pool(&db),
        2,
        "crosstalk-timeouts-test-lock",
        Epoch::new(0),
        short(),
    );
    let mut blocker = db.pool().begin().await.expect("begins");
    sqlx::query("SELECT pg_advisory_xact_lock(424243)")
        .execute(&mut *blocker)
        .await
        .expect("the blocker takes the lock");

    let before = statement_timeouts();
    let began = Instant::now();
    let mut tx = pool.begin().await.expect("begins");
    let cancelled = sqlx::query("SELECT pg_advisory_xact_lock(424243)")
        .execute(&mut *tx)
        .await
        .expect_err("the server gives up the lock wait");
    let took = began.elapsed();
    drop(tx);
    assert_eq!(classify(&cancelled), DbFailure::TimedOut(TimeoutKind::Lock));
    assert!(
        took < Duration::from_millis(300) + SLACK,
        "cancelled at the lock bound, not after {took:?}"
    );
    assert!(statement_timeouts().lock > before.lock);

    blocker.commit().await.expect("releases");
    let mut tx = pool.begin().await.expect("begins");
    sqlx::query("SELECT pg_advisory_xact_lock(424243)")
        .execute(&mut *tx)
        .await
        .expect("granted once the blocker released it");
    tx.commit().await.expect("commits");
    pool.close().await;
    db.close().await.expect("drops");
}

/// The pipeline task's start for a gateway config on `db` whose store
/// section sets `pipeline_timeouts` to [`short`]'s.
async fn bounded_pipeline_start(
    db: &TestDb,
    data: &std::path::Path,
    late: Late,
) -> (PipelineStart, Gate, SpoolingBus<Gated>, PgBus) {
    let config = GatewayConfig::from_json(
        &serde_json::json!({
            "ingress": {
                "listen": "127.0.0.1:0",
                "routes": [],
                "secrets": {"current": {"version": 1, "env": "CROSSTALK_SECRET_V1"}},
            },
            "ops": {"listen": "127.0.0.1:0"},
            "store": {
                "pool": {"max_connections": 12},
                "bus": {"poll_micros": 20_000},
                "pipeline_timeouts": {"statement_ms": 2_000, "lock_ms": 300, "retention_ms": 10_000},
            },
            "spool": {"probe_ms": 10},
            "blobs": {"root": data.join("blobs")},
        })
        .to_string(),
    )
    .expect("the config parses");
    assert_eq!(
        config.store.map(|store| store.pipeline_timeouts),
        Some(short())
    );
    let clock: Arc<dyn Clock> = Arc::new(ManualClock::at(Timestamp::from_micros(T0)));
    let blobs = crosstalk_transport::blob::FsBlobStore::open(&config.blobs.root)
        .await
        .expect("blobs");
    let side = postgres::capture_side(&config, Role::Pipeline, pipeline_pool(db), &blobs, &clock)
        .await
        .expect("the capture side");
    let spool = side.spool.clone().expect("a pipeline role spools");
    let start = PipelineStart {
        config: config.clone(),
        role: Role::Pipeline,
        clock: LiveClock::Read(Arc::clone(&clock)),
        blobs,
        pool: side.pool.clone(),
        url: db.url().clone(),
        bus: side.bus.clone(),
        spool: spool.clone(),
        gate: side.gate.clone(),
        pipeline: side.pipeline.clone().expect("a capture pipeline"),
        secret: secret(),
        status: postgres::initial_status(),
        late,
        api: None,
    };
    (start, side.gate, spool, side.bus)
}

/// A count read over the test's own pool.
async fn count(db: &TestDb, sql: &'static str) -> i64 {
    let (count,): (i64,) = sqlx::query_as(sql)
        .fetch_one(db.pool())
        .await
        .expect("counts");
    count
}

/// A stage's store write blocked by a lock held elsewhere (L4 recording an
/// exchange while another session holds `provenance.exchanges`) is
/// cancelled at the configured `lock_timeout`, nacked as a retry (never
/// acked as a refused write: the exchange's record would be lost), and
/// redelivered; once the blocker lets go, the stage completes the write and
/// nothing is dead-lettered. Run through the pipeline task as `serve` runs
/// it, so the bounds come from the config's store section (the default 15 s
/// lock bound would not cancel within the wait below).
#[tokio::test(flavor = "multi_thread")]
async fn pg_a_stage_write_blocked_past_the_lock_timeout_is_retried_and_completes() {
    let Some(db) =
        migrated("pg_a_stage_write_blocked_past_the_lock_timeout_is_retried_and_completes").await
    else {
        return;
    };
    let dir = tempfile::tempdir().expect("a temp dir");
    let late = Late::default();
    let (start, _gate, spool, _bus) = bounded_pipeline_start(&db, dir.path(), late).await;
    let mut status = start.status.reader();
    let probe = start_probe(&start);
    let task = postgres::spawn_pipeline(start);
    within(
        "the pipeline's recovery",
        status.wait_until(|status| status.phase == PipelinePhase::Running),
        &probe,
    )
    .await
    .expect("the reporter lives");

    let mut blocker = db.pool().begin().await.expect("begins");
    sqlx::query("LOCK TABLE provenance.exchanges IN ACCESS EXCLUSIVE MODE")
        .execute(&mut *blocker)
        .await
        .expect("the blocker locks L4's exchanges");
    let before = statement_timeouts();

    let mut ids = crosstalk_testkit::ids::Ids::seeded(7);
    let exchange = crosstalk_testkit::build::ExchangeBuilder::new(&mut ids).build();
    spool
        .publish(Envelope {
            id: EventId::from_ulid(0x7E57),
            at: Timestamp::from_micros(T0),
            event: BusEvent::Ingest(IngestEvent::ExchangeCaptured(Box::new(exchange))),
        })
        .await
        .expect("publishes");

    let group = Slot::L4Provenance.group().0;
    let released = eventually(Duration::from_secs(10), || async {
        l4_row(&db, &group)
            .await
            .is_some_and(|(_, _, error)| error.is_some_and(|error| error.contains("55P03")))
    })
    .await;
    assert!(
        released,
        "L4's blocked write was not cancelled at the configured lock bound and released\n{}",
        diagnosis(&db).await
    );
    assert!(statement_timeouts().lock > before.lock);

    // Held across more rounds than the group has attempts (each round: the
    // 300 ms lock bound, then the stall backoff): a stall spends none.
    let attempts = crate::pipeline::Settings::from_config(
        &GatewayConfig::from_json(
            &serde_json::json!({
                "ingress": {"listen": "127.0.0.1:0", "routes": [],
                            "secrets": {"current": {"version": 1, "env": "CROSSTALK_SECRET_V1"}}},
                "ops": {"listen": "127.0.0.1:0"},
                "blobs": {"root": "/nonexistent/blobs"},
            })
            .to_string(),
        )
        .expect("the config parses"),
    )
    .consumer_retry
    .max_attempts()
    .get();
    let rounds = Duration::from_millis(300) + crate::live::STALL_RETRY;
    tokio::time::sleep(rounds * (attempts + 1)).await;
    let row = l4_row(&db, &group).await;
    assert!(
        row.as_ref().is_some_and(|(attempt, _, _)| *attempt <= 1),
        "a stalled delivery spent attempts or left its group: {row:?}\n{}",
        diagnosis(&db).await
    );
    assert_no_stall_dead_letters(&db, &group).await;

    blocker.commit().await.expect("the blocker lets go");
    let stored = eventually(Duration::from_secs(60), || async {
        count(&db, "SELECT count(*) FROM provenance.exchanges").await > 0
    })
    .await;
    assert!(
        stored,
        "the redelivered exchange was recorded\n{}",
        diagnosis(&db).await
    );
    let settled = eventually(Duration::from_secs(60), || async {
        l4_row(&db, &group).await.is_none()
    })
    .await;
    assert!(
        settled,
        "L4 acked the delivery once its write committed\n{}",
        diagnosis(&db).await
    );
    assert_no_stall_dead_letters(&db, &group).await;

    stop_pipeline(task, &probe).await;
    db.close().await.expect("drops");
}

/// The L4 group's one delivery: attempt, state, last error.
async fn l4_row(db: &TestDb, group: &str) -> Option<(i32, String, Option<String>)> {
    sqlx::query_as(
        "SELECT attempt, state, last_error FROM transport.deliveries WHERE group_name = $1",
    )
    .bind(group)
    .fetch_optional(db.pool())
    .await
    .expect("reads the deliveries")
}

/// Every delivery and dead letter, with the event's subject: what a failed
/// assertion prints (the lib tests install no tracing subscriber).
async fn diagnosis(db: &TestDb) -> String {
    let deliveries: Vec<(String, i64, i32, String, Option<String>, Option<String>)> =
        sqlx::query_as(
            "SELECT d.group_name, d.seq, d.attempt, d.state, d.last_error, e.subject \
             FROM transport.deliveries d LEFT JOIN transport.events e ON e.seq = d.seq \
             ORDER BY d.group_name, d.seq",
        )
        .fetch_all(db.pool())
        .await
        .unwrap_or_default();
    format!(
        "deliveries (group, seq, attempt, state, last_error, subject): {deliveries:?}\n\
         dead letters (group, attempts, last_error, subject): {:?}",
        dead_letter_rows(db).await
    )
}

async fn dead_letter_rows(db: &TestDb) -> Vec<(String, i32, String, Option<String>)> {
    sqlx::query_as(
        "SELECT l.group_name, l.attempts, l.last_error, e.subject \
         FROM transport.dead_letters l LEFT JOIN transport.events e ON e.seq = l.seq \
         ORDER BY l.group_name",
    )
    .fetch_all(db.pool())
    .await
    .expect("reads the dead letters")
}

/// No dead letter is a stall's: none in L4's group (whose write was
/// blocked), none whose last error is a statement or lock timeout. L3
/// dead-letters this test's exchange for a reason of the message's own
/// (its bodies were never stored: `integration::pg_settle_finishes_while_a_delivery_is_retried`
/// relies on the same), which is not a stall and stays allowed.
async fn assert_no_stall_dead_letters(db: &TestDb, l4: &str) {
    let rows = dead_letter_rows(db).await;
    let stalls: Vec<_> = rows
        .iter()
        .filter(|(group, _, error, _)| {
            group == l4 || error.contains("55P03") || error.contains("57014")
        })
        .collect();
    assert!(
        stalls.is_empty(),
        "a stall dead-lettered a delivery: {stalls:?}\nall dead letters (group, attempts, last_error, subject): {rows:?}"
    );
    let others: Vec<_> = rows
        .iter()
        .filter(|(group, _, _, _)| group != &Slot::L3Reconstruct.group().0)
        .collect();
    assert!(
        others.is_empty(),
        "unexpected dead letters: {others:?}\nall: {rows:?}"
    );
}

/// The reaper survives a stalled sweep: with `transport.groups` locked
/// elsewhere, a stale-hold sweep (which joins the groups) returns at the
/// lock bound instead of waiting for the blocker (checked directly, with
/// its result and timing), and the reaper's loop, whose own sweeps keep
/// being cancelled, goes on to take back an expired hold (which touches
/// only the deliveries). Without the bound a sweep would wait as long as
/// the blocker holds, and so would every deadline behind it.
#[tokio::test(flavor = "multi_thread")]
async fn pg_the_reaper_survives_a_stalled_sweep() {
    let Some(db) = migrated("pg_the_reaper_survives_a_stalled_sweep").await else {
        return;
    };
    let pool = incarnation_pool_bounded(
        &pipeline_pool(&db),
        4,
        "crosstalk-timeouts-test-reaper",
        Epoch::new(0),
        short(),
    );
    let ack_timeout = Duration::from_secs(2);
    let bus = PgBus::new(
        pool.clone(),
        bus_clock(),
        PgBusConfig {
            ack_timeout: NonZeroDuration::new(ack_timeout).expect("non-zero"),
            poll: NonZeroDuration::new(Duration::from_millis(20)).expect("non-zero"),
            ..PgBusConfig::default()
        },
    )
    .expect("a bus");
    let retry = RetryPolicy::new(
        NonZeroU32::MIN.saturating_add(4),
        Duration::from_millis(10),
        Duration::from_millis(100),
    )
    .expect("a retry policy");
    let mut subscription = bus
        .subscribe(
            &[Subject::Changed],
            ConsumerGroup("reaped".to_owned()),
            retry,
        )
        .await
        .expect("subscribes");
    bus.publish(super::envelope(0x5EE9, Timestamp::from_micros(T0)))
        .await
        .expect("publishes");
    let delivery = tokio::time::timeout(Duration::from_secs(30), subscription.next())
        .await
        .expect("a delivery in time")
        .expect("the bus runs")
        .expect("decodes");
    assert_eq!(delivery.attempt.get(), 1);

    // Held from now until the end: every sweep waits on it.
    let mut blocker = db.pool().begin().await.expect("begins");
    sqlx::query("LOCK TABLE transport.groups IN ACCESS EXCLUSIVE MODE")
        .execute(&mut *blocker)
        .await
        .expect("the blocker locks the groups");
    let blocked_at = Instant::now();

    // A sweep run now waits on the blocker and returns at the lock bound.
    let began = Instant::now();
    let swept = tokio::time::timeout(Duration::from_secs(30), bus.sweep_stale_holds()).await;
    let took = began.elapsed();
    assert!(
        matches!(
            swept,
            Ok(Err(
                crosstalk_spec::interfaces::l2_transport::BusError::Disconnected
            ))
        ),
        "a blocked sweep returns a timeout (Disconnected) at the lock bound: got {swept:?} after {took:?}"
    );
    assert!(
        took < Duration::from_millis(300) + SLACK,
        "the blocked sweep returned after {took:?}, past the 300 ms lock bound"
    );

    // The reaper's own sweeps (every ack timeout) are cancelled the same way
    // and its loop goes on: it takes the expired hold back, and keeps going
    // round, while every sweep stays blocked.
    let before = statement_timeouts();
    let reaped = eventually(ack_timeout * 5, || async {
        let row: Option<(String, Option<String>)> = sqlx::query_as(
            "SELECT state, last_error FROM transport.deliveries WHERE group_name = 'reaped'",
        )
        .fetch_optional(db.pool())
        .await
        .expect("reads the delivery");
        matches!(row, Some((state, Some(error))) if state == "delayed" && error.starts_with("ack timeout"))
    })
    .await;
    let row: Option<(String, i32, Option<String>)> = sqlx::query_as(
        "SELECT state, attempt, last_error FROM transport.deliveries WHERE group_name = 'reaped'",
    )
    .fetch_optional(db.pool())
    .await
    .expect("reads the delivery");
    assert!(
        reaped,
        "the reaper did not take the expired hold back while its sweeps were blocked: \
         row {row:?}, {:?} after the blocker, reaper {:?}",
        blocked_at.elapsed(),
        bus.reaper()
    );
    // At least one reaper sweep ran while blocked (one every ack timeout).
    let cancelled = eventually(ack_timeout * 2 + SLACK, || async {
        statement_timeouts().lock > before.lock
    })
    .await;
    assert!(
        cancelled,
        "no sweep of the reaper's was cancelled at the lock bound in {:?} of blocking \
         (lock timeouts {:?} -> {:?}); reaper {:?}",
        blocked_at.elapsed(),
        before,
        statement_timeouts(),
        bus.reaper()
    );
    let report = bus.reaper();
    assert!(
        report
            .since_active
            .is_some_and(|since| since < ack_timeout + SLACK),
        "the reaper keeps going round: {report:?}"
    );

    blocker.commit().await.expect("the blocker lets go");
    drop(subscription);
    bus.shutdown();
    pool.close().await;
    db.close().await.expect("drops");
}

/// `/healthz` answers within its bound while its bus read is blocked:
/// capture's bus (whose pool has no server-side bound) reads the groups'
/// statistics, here blocked by a lock on `transport.deliveries`; the
/// report comes back after `HEALTH_BUS_BOUND` with the bus section
/// unavailable, and with the statistics again once the lock is gone.
#[tokio::test(flavor = "multi_thread")]
async fn pg_healthz_answers_within_its_bound_while_group_stats_is_blocked() {
    let Some(db) =
        migrated("pg_healthz_answers_within_its_bound_while_group_stats_is_blocked").await
    else {
        return;
    };
    let dir = tempfile::tempdir().expect("a temp dir");
    let late = Late::default();
    let (start, _gate, spool, _bus) = bounded_pipeline_start(&db, dir.path(), late.clone()).await;
    let ops = ops(&start, spool, &late);
    let read = ops.health_full().await;
    assert!(
        matches!(read.bus, Some(BusSection::Read(_))),
        "{:?}",
        read.bus
    );

    let mut blocker = db.pool().begin().await.expect("begins");
    sqlx::query("LOCK TABLE transport.deliveries IN ACCESS EXCLUSIVE MODE")
        .execute(&mut *blocker)
        .await
        .expect("the blocker locks the deliveries");
    let began = Instant::now();
    let report = tokio::time::timeout(HEALTH_BUS_BOUND + SLACK * 5, ops.health_full())
        .await
        .expect("/healthz never hangs on its bus read");
    let took = began.elapsed();
    assert!(
        took < HEALTH_BUS_BOUND + SLACK,
        "/healthz took {took:?} with its bus read blocked"
    );
    assert!(
        matches!(
            report.bus,
            Some(BusSection::Unavailable {
                unavailable: BusUnavailable::TimedOut { .. }
            })
        ),
        "{:?}",
        report.bus
    );

    blocker.commit().await.expect("the blocker lets go");
    let back = eventually(Duration::from_secs(30), || async {
        matches!(ops.health_full().await.bus, Some(BusSection::Read(_)))
    })
    .await;
    assert!(back, "the bus section returns once the read answers");
    start.spool.close().await;
    db.close().await.expect("drops");
}

/// A connection the incarnation pool hands out after the cancel above still
/// runs with the bounds: they are session settings, set once per
/// connection, not per transaction.
#[tokio::test(flavor = "multi_thread")]
async fn pg_the_bounds_hold_on_every_connection_of_the_pool() {
    let Some(db) = migrated("pg_the_bounds_hold_on_every_connection_of_the_pool").await else {
        return;
    };
    let pool = incarnation_pool_bounded(
        &pipeline_pool(&db),
        3,
        "crosstalk-timeouts-test-every",
        Epoch::new(0),
        short(),
    );
    let mut held = Vec::new();
    for _ in 0..3 {
        held.push(pool.acquire().await.expect("connects"));
    }
    for connection in &mut held {
        let (statement,): (String,) = sqlx::query_as("SHOW statement_timeout")
            .fetch_one(&mut **connection)
            .await
            .expect("reads");
        let (lock,): (String,) = sqlx::query_as("SHOW lock_timeout")
            .fetch_one(&mut **connection)
            .await
            .expect("reads");
        assert_eq!((statement.as_str(), lock.as_str()), ("2s", "300ms"));
    }
    for connection in held {
        connection.detach().close().await.expect("closes");
    }
    pool.close().await;
    db.close().await.expect("drops");
}

/// Inside a transaction a retention delete raises `statement_timeout` to
/// the retention bound (10 s here, against the pipeline's 2 s), so a
/// statement past the pipeline bound but inside the retention bound
/// finishes; restored, the pipeline bound applies again, and a transaction
/// that never raised it keeps the pipeline bound. On a pool without the
/// bounds (capture's) the raise changes nothing.
#[tokio::test(flavor = "multi_thread")]
async fn pg_retention_deletes_run_under_the_longer_bound() {
    let Some(db) = migrated("pg_retention_deletes_run_under_the_longer_bound").await else {
        return;
    };
    let template = pipeline_pool(&db);
    let pool = incarnation_pool_bounded(
        &template,
        2,
        "crosstalk-timeouts-test-retention",
        Epoch::new(0),
        short(),
    );
    let mut tx = pool.begin().await.expect("begins");
    crosstalk_store::raise_for_retention(&mut tx)
        .await
        .expect("raises");
    let (raised,): (String,) = sqlx::query_as("SHOW statement_timeout")
        .fetch_one(&mut *tx)
        .await
        .expect("reads");
    assert_eq!(raised, "10s");
    sqlx::query("SELECT pg_sleep(3)")
        .execute(&mut *tx)
        .await
        .expect("past the pipeline bound, inside the retention bound");
    crosstalk_store::restore_after_retention(&mut tx)
        .await
        .expect("restores");
    let (restored,): (String,) = sqlx::query_as("SHOW statement_timeout")
        .fetch_one(&mut *tx)
        .await
        .expect("reads");
    assert_eq!(restored, "2s");
    let cancelled = sqlx::query("SELECT pg_sleep(3)")
        .execute(&mut *tx)
        .await
        .expect_err("the pipeline bound again");
    assert_eq!(
        classify(&cancelled),
        DbFailure::TimedOut(TimeoutKind::Statement)
    );
    drop(tx);
    assert_eq!(show(&pool, "statement_timeout").await, "2s");

    let publisher = derived_pool(&template, 1, "crosstalk-timeouts-test-retention-capture");
    let mut tx = publisher.begin().await.expect("begins");
    crosstalk_store::raise_for_retention(&mut tx)
        .await
        .expect("a no-op without the settings");
    let (unbounded,): (String,) = sqlx::query_as("SHOW statement_timeout")
        .fetch_one(&mut *tx)
        .await
        .expect("reads");
    assert_eq!(unbounded, "0");
    drop(tx);

    for pool in [pool, publisher, template] {
        pool.close().await;
    }
    db.close().await.expect("drops");
}

/// The bus's hourly log prune is a retention delete: made slower than the
/// pipeline's 2 s statement bound (a statement trigger sleeps 3 s on every
/// delete from the log), it still completes, under the 10 s retention
/// bound, where a plain delete on the same pool is cancelled.
#[tokio::test(flavor = "multi_thread")]
async fn pg_the_bus_prune_runs_under_the_retention_bound() {
    let Some(db) = migrated("pg_the_bus_prune_runs_under_the_retention_bound").await else {
        return;
    };
    for statement in [
        "CREATE FUNCTION public.slow_log_delete() RETURNS trigger LANGUAGE plpgsql AS \
         $$ BEGIN PERFORM pg_sleep(3); RETURN NULL; END $$",
        "CREATE TRIGGER slow_log_delete BEFORE DELETE ON transport.events \
         FOR EACH STATEMENT EXECUTE FUNCTION public.slow_log_delete()",
    ] {
        sqlx::query(statement)
            .execute(db.pool())
            .await
            .expect("installs the slow delete");
    }
    let pool = incarnation_pool_bounded(
        &pipeline_pool(&db),
        4,
        "crosstalk-timeouts-test-prune",
        Epoch::new(0),
        short(),
    );
    let bus = PgBus::new(pool.clone(), bus_clock(), PgBusConfig::default()).expect("a bus");
    bus.publish(super::envelope(0x9A0E, Timestamp::from_micros(T0)))
        .await
        .expect("publishes");

    let pruned = bus
        .prune(
            Timestamp::from_micros(T0 + 3_600_000_000),
            Duration::from_secs(60),
        )
        .await
        .expect("the prune runs under the retention bound");
    assert_eq!(pruned, 1, "no group: the aged entry goes");

    let cancelled = sqlx::query("DELETE FROM transport.events WHERE seq < 0")
        .execute(&pool)
        .await
        .expect_err("a delete outside the retention bound is cancelled");
    assert_eq!(
        classify(&cancelled),
        DbFailure::TimedOut(TimeoutKind::Statement)
    );

    bus.shutdown();
    pool.close().await;
    db.close().await.expect("drops");
}
