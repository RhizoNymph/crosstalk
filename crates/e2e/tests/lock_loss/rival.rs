//! The lock's session ended while the database stays reachable, and a
//! rival takes the lock: the abandoned pipeline writes nothing after its
//! abandon, and the gateway waits for the lock, then resumes.

use std::collections::BTreeSet;
use std::time::Duration;

use crosstalk_gateway::live::recovery::{LockState, PipelinePhase};
use crosstalk_gateway::store::lock::PipelineLock;
use crosstalk_memory::support::ManualClock;
use crosstalk_store::TestDb;
use crosstalk_store::sqlx;
use tokio::time::Instant;

use crate::common::gateway::{run, scenario};
use crate::common::{big_stack, migrated};
use crate::harness::{
    Prober, RESUME, expected, serial, server_now, terminate_lock_session, wait_sessions,
    wait_status,
};

/// What the audit records: every committed write to the tables only the
/// lock's holder writes (the bus's deliveries and groups, the correlator's
/// checkpoint and held writes, the four outboxes).
const AUDITED: [&str; 8] = [
    "transport.deliveries",
    "transport.groups",
    "flow.checkpoints",
    "flow.held_writes",
    "reconstruct.outbox",
    "flow.outbox",
    "analysis.outbox",
    "topology.outbox",
];

/// How long the rival holds the lock.
const RIVAL_HOLDS: Duration = Duration::from_secs(10);

/// A commit-time audit (test only): a deferred constraint trigger on each
/// audited table records the table, the session's `application_name` and
/// `clock_timestamp()` as the writing transaction commits; a rolled-back
/// write records nothing.
async fn install_audit(db: &TestDb) {
    let mut statements = vec![
        "CREATE TABLE public.owner_writes (tbl text NOT NULL, app text NOT NULL, \
         at_micros bigint NOT NULL)"
            .to_owned(),
        "CREATE FUNCTION public.record_owner_write() RETURNS trigger LANGUAGE plpgsql AS $$ \
         BEGIN \
           INSERT INTO public.owner_writes VALUES (TG_TABLE_SCHEMA || '.' || TG_TABLE_NAME, \
             current_setting('application_name'), \
             (extract(epoch FROM clock_timestamp()) * 1000000)::bigint); \
           RETURN NULL; \
         END $$"
            .to_owned(),
    ];
    for table in AUDITED {
        statements.push(format!(
            "CREATE CONSTRAINT TRIGGER owner_write_audit AFTER INSERT OR UPDATE OR DELETE \
             ON {table} DEFERRABLE INITIALLY DEFERRED FOR EACH ROW \
             EXECUTE FUNCTION public.record_owner_write()"
        ));
    }
    for statement in statements {
        sqlx::query(sqlx::AssertSqlSafe(statement))
            .execute(db.pool())
            .await
            .expect("installs the audit");
    }
}

/// The audited writes by `apps` committed in `(after, through]` (server
/// microseconds), by table.
async fn writes(
    db: &TestDb,
    apps: &BTreeSet<String>,
    after: i64,
    through: i64,
) -> Vec<(String, i64)> {
    let apps: Vec<String> = apps.iter().cloned().collect();
    sqlx::query_as(
        "SELECT tbl, count(*) FROM public.owner_writes \
         WHERE app = ANY($1) AND at_micros > $2 AND at_micros <= $3 GROUP BY tbl ORDER BY tbl",
    )
    .bind(&apps)
    .bind(after)
    .bind(through)
    .fetch_all(db.pool())
    .await
    .expect("reads the audit")
}

/// Every `application_name` that wrote an audited table so far.
async fn writers(db: &TestDb) -> BTreeSet<String> {
    let rows: Vec<(String,)> = sqlx::query_as("SELECT DISTINCT app FROM public.owner_writes")
        .fetch_all(db.pool())
        .await
        .expect("reads the audit");
    rows.into_iter().map(|(app,)| app).collect()
}

/// `pg_terminate_backend` on the lock's session, the database reachable
/// throughout; a rival session takes the lock and holds it 10 s while an
/// exchange arrives. The loss is reported, the pipeline abandoned, the
/// loop waits with the lock held elsewhere, then takes it, recovers and
/// drains. No audited write by the abandoned pipeline's sessions commits
/// after its abandon finished, and its sessions are gone; the writes in
/// the window before the loss was reported (the lock already gone) are
/// printed: the dual-writer window a fencing epoch would close.
#[test]
fn a_terminated_lock_session_and_a_rival_holder_leave_no_owner_write_after_the_abandon() {
    let _serial = serial();
    big_stack(|| async {
        const TEST: &str =
            "a_terminated_lock_session_and_a_rival_holder_leave_no_owner_write_after_the_abandon";
        let scenario = scenario();
        let Some(expected) = expected(TEST, &scenario).await else {
            return;
        };
        let Some(db) = migrated(TEST).await else {
            return;
        };
        install_audit(&db).await;
        let run = run(db).await;
        let clock = ManualClock::at(scenario.start);
        let gateway = run.start(&clock).await;
        gateway
            .send(&run.upstream, &scenario.exchanges[..1], 1)
            .await;
        gateway
            .settled(&run.db, "the first exchange's pipeline")
            .await;
        let prober = Prober::start(gateway.running.proxy_addr().expect("role all proxies"));
        let old = writers(&run.db).await;
        assert!(!old.is_empty(), "the first pipeline wrote audited tables");
        eprintln!("the first pipeline's writers: {old:?}");

        let terminated_at = server_now(&run.db).await;
        let pid = terminate_lock_session(&run.db).await;
        let rival_deadline = Instant::now() + Duration::from_secs(30);
        let rival = loop {
            if let Some(lock) = PipelineLock::try_take(run.db.url())
                .await
                .expect("the rival connects")
            {
                break lock;
            }
            assert!(
                Instant::now() < rival_deadline,
                "the rival never took the lock"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        };
        let rival_took = Instant::now();
        eprintln!("terminated the lock's session {pid}; the rival holds the lock");

        wait_status(
            &gateway.running,
            "the lock's loss",
            Duration::from_secs(30),
            |status| matches!(status.lock, LockState::Lost | LockState::HeldElsewhere),
        )
        .await;
        let lost_at = server_now(&run.db).await;
        gateway
            .send(&run.upstream, &scenario.exchanges[1..2], 2)
            .await;
        wait_status(
            &gateway.running,
            "the abandon's end",
            Duration::from_secs(30),
            |status| {
                matches!(
                    status.phase,
                    PipelinePhase::WaitingForDatabase
                        | PipelinePhase::WaitingForLock
                        | PipelinePhase::Recovering(_)
                        | PipelinePhase::Running
                ) && status.lock != LockState::Held
            },
        )
        .await;
        let abandoned_at = server_now(&run.db).await;
        let elsewhere = wait_status(
            &gateway.running,
            "waiting while the rival holds the lock",
            Duration::from_secs(30),
            |status| {
                status.lock == LockState::HeldElsewhere
                    && status.phase == PipelinePhase::WaitingForLock
            },
        )
        .await;
        assert!(
            rival_took.elapsed() < RIVAL_HOLDS,
            "the loop saw the rival only after {:?}",
            rival_took.elapsed()
        );
        eprintln!("held elsewhere: {}", elsewhere.pipeline_text());
        wait_sessions(
            &run.db,
            "the abandoned pipeline's sessions ending",
            Duration::from_secs(10),
            |names| old.iter().all(|app| !names.contains_key(app)),
        )
        .await;
        gateway
            .send(&run.upstream, &scenario.exchanges[2..3], 3)
            .await;
        if let Some(rest) = RIVAL_HOLDS.checked_sub(rival_took.elapsed()) {
            tokio::time::sleep(rest).await;
        }
        let ready = gateway.running.readiness().await;
        assert!(ready.ready, "200 degraded while held elsewhere: {ready:?}");
        assert_eq!(ready.pipeline_lock.as_deref(), Some("held elsewhere"));
        drop(rival);

        wait_status(&gateway.running, "the resume", RESUME, |status| {
            status.lock == LockState::Held && status.phase == PipelinePhase::Running
        })
        .await;
        gateway
            .settled(&run.db, "the drain after the rival let go")
            .await;
        gateway
            .send(&run.upstream, &scenario.exchanges[3..], 1)
            .await;
        gateway
            .settled(&run.db, "the last exchange's pipeline")
            .await;
        let connects = prober.finish().await;
        assert_eq!(connects.refused, 0, "{connects:?}");

        let now = server_now(&run.db).await;
        let before_lost = writes(&run.db, &old, terminated_at, lost_at).await;
        let during_abandon = writes(&run.db, &old, lost_at, abandoned_at).await;
        let after_abandon = writes(&run.db, &old, abandoned_at, now).await;
        let new = writers(&run.db).await;
        eprintln!(
            "owner writes by {old:?}: terminated -> lost {before_lost:?}; lost -> abandoned \
             {during_abandon:?}; after the abandon {after_abandon:?}; writers now {new:?}"
        );
        assert_eq!(
            after_abandon,
            Vec::new(),
            "the abandoned pipeline committed after its abandon"
        );
        assert!(
            new.len() > old.len(),
            "the resumed pipeline writes under a new application_name: {new:?}"
        );
        run.finish(
            gateway,
            &scenario,
            &expected,
            "the run with a terminated lock session and a rival holder",
        )
        .await;
    });
}
