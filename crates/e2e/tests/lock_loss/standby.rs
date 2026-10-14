//! Two gateways on one database: an owner and a standby. The owner's lock
//! is lost (its session terminated while the database stays reachable, or
//! the owner killed outright) and the standby takes over. The fence
//! (`transport.pipeline-lock.no-write-from-an-older-epoch`): the standby's
//! take bumps the epoch and waits until every connection of the old
//! owner's incarnation is gone before its own incarnation starts, so no
//! write of the old epoch commits once the new one runs.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::time::Duration;

use crosstalk_gateway::live::recovery::{LockState, PipelinePhase, PipelineStatus};
use crosstalk_memory::support::ManualClock;
use crosstalk_store::TestDb;
use crosstalk_store::sqlx;
use tokio::time::Instant;

use crate::common::gateway::{Gateway, Run, config, run, scenario, start};
use crate::common::{BIG_STACK, WAIT, big_stack, migrated};
use crate::harness::{
    Prober, RESUME, SLACK, epoch_of, expected, serial, server_now, sessions_by_name,
    terminate_lock_session, wait_sessions, wait_status,
};
use crate::rival::install_audit;

/// How many times the owner's lock session is terminated, at most, until
/// the standby (not the owner, resuming) is the one that takes the lock.
const ROUNDS: usize = 6;

/// How long a standby's take may wait once the lock is free: its retry
/// (2 s), the database probe and head check, the take itself.
const TAKE_OVER: Duration = Duration::from_secs(10);

/// One audited commit: the table, the writer's `application_name`, the
/// server's clock at the commit.
type Write = (String, String, i64);

/// Every audited commit so far, oldest first.
async fn audited(db: &TestDb) -> Vec<Write> {
    sqlx::query_as("SELECT tbl, app, at_micros FROM public.owner_writes ORDER BY at_micros")
        .fetch_all(db.pool())
        .await
        .expect("reads the audit")
}

/// Audit the epoch's bumps too: each take's commit of
/// `transport.pipeline_epoch`, by the taker's lock session.
async fn audit_the_epoch(db: &TestDb) {
    sqlx::query(
        "CREATE CONSTRAINT TRIGGER owner_write_audit AFTER INSERT OR UPDATE OR DELETE \
         ON transport.pipeline_epoch DEFERRABLE INITIALLY DEFERRED FOR EACH ROW \
         EXECUTE FUNCTION public.record_owner_write()",
    )
    .execute(db.pool())
    .await
    .expect("audits the epoch");
}

/// A second data directory beside `run`'s whose blob store is `run`'s (a
/// link to it): the standby's pipeline reads the bodies the owner's
/// capture stored, as two hosts on one shared volume would.
fn standby_data(run: &Run, dir: &Path) -> PathBuf {
    let data = dir.join("standby");
    std::fs::create_dir_all(&data).expect("the standby's data directory");
    std::fs::create_dir_all(run.data.join("blobs")).expect("the owner's blob store");
    std::os::unix::fs::symlink(run.data.join("blobs"), data.join("blobs"))
        .expect("links the blob store");
    data
}

/// The standby: a second gateway on `run`'s database, its own data
/// directory, the same clock. Waits until it stands by: `held
/// elsewhere`, not ready (it never held the lock).
async fn start_standby(run: &Run, data: &Path, clock: &ManualClock) -> Gateway {
    let standby_config = config(&run.upstream.base_url(), data, serde_json::json!({}));
    let standby = Gateway {
        running: start(&standby_config, run.url.clone(), clock).await,
        clock: clock.clone(),
    };
    wait_status(
        &standby.running,
        "the standby standing by",
        Duration::from_secs(30),
        |status| {
            status.lock == LockState::HeldElsewhere && status.phase == PipelinePhase::WaitingForLock
        },
    )
    .await;
    let ready = standby.running.readiness().await;
    assert!(!ready.ready, "a standby is not ready: {ready:?}");
    standby
}

fn status(gateway: &Gateway) -> PipelineStatus {
    gateway
        .running
        .postgres()
        .expect("postgres mode")
        .status
        .snapshot()
}

/// Who took the lock after a loss.
enum Taker {
    Standby(PipelineStatus),
    /// The owner resumed first, under a newer epoch of its own.
    Owner,
}

/// Wait until the standby holds the lock, or the owner holds it again
/// under an epoch past `before`.
async fn taker(owner: &Gateway, standby: &Gateway, before: u64) -> Taker {
    let deadline = Instant::now() + TAKE_OVER + Duration::from_secs(20);
    loop {
        let taken = status(standby);
        if taken.lock == LockState::Held {
            return Taker::Standby(taken);
        }
        let resumed = status(owner);
        if resumed.lock == LockState::Held && resumed.report.epoch.get() > before {
            return Taker::Owner;
        }
        assert!(
            Instant::now() < deadline,
            "nobody took the lock: owner {resumed:?}; standby {taken:?}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// The audited pipeline writes whose incarnation's epoch is below
/// `current`, committed after `after` (server microseconds).
fn older_writes_after(writes: &[Write], current: u64, after: i64) -> Vec<Write> {
    writes
        .iter()
        .filter(|(_, app, at)| *at > after && epoch_of(app).is_some_and(|epoch| epoch < current))
        .cloned()
        .collect()
}

/// The first audited write of the incarnation of `epoch`: when it began
/// writing, by the server's clock.
fn first_write_of(writes: &[Write], epoch: u64) -> Option<i64> {
    writes
        .iter()
        .filter(|(_, app, _)| epoch_of(app) == Some(epoch))
        .map(|(_, _, at)| *at)
        .min()
}

/// The `PingFailed` case the fence closes: the owner's lock session is
/// terminated with the database reachable, so its incarnation's
/// connections live on until its next ping (up to 5 s) while the standby
/// takes the lock at once. The standby's take bumps the epoch and waits
/// for those connections (reported `fencing`); no audited write of the
/// owner's epoch commits after the standby's incarnation began writing.
/// The owner resumes as a standby (`held elsewhere`, 200 degraded),
/// capture on it never stopped (no refused connect, every exchange
/// forwarded and captured), and the standby's pipeline drains it all to
/// the uninterrupted run's answers.
///
/// Who retakes the lock first is a race (the owner's next ping against
/// the standby's next retry); when the owner wins, its lock session is
/// terminated again, up to [`ROUNDS`] times.
#[test]
fn a_standby_takes_over_a_terminated_lock_session_with_no_older_epoch_write() {
    let _serial = serial();
    big_stack(|| async {
        const TEST: &str =
            "a_standby_takes_over_a_terminated_lock_session_with_no_older_epoch_write";
        let scenario = scenario();
        let Some(expected) = expected(TEST, &scenario).await else {
            return;
        };
        let Some(db) = migrated(TEST).await else {
            return;
        };
        install_audit(&db).await;
        audit_the_epoch(&db).await;
        let run = run(db).await;
        let clock = ManualClock::at(scenario.start);
        let owner = run.start(&clock).await;
        owner.send(&run.upstream, &scenario.exchanges[..1], 1).await;
        owner.settled(&run.db, "the first exchange's pipeline").await;
        let dir = tempfile::tempdir().expect("a temp dir");
        let standby = start_standby(&run, &standby_data(&run, dir.path()), &clock).await;
        let prober = Prober::start(owner.running.proxy_addr().expect("role all proxies"));

        let mut captured: u64 = 1;
        let mut round = 0;
        let (terminated_at, took) = loop {
            round += 1;
            assert!(round <= ROUNDS, "the owner retook the lock {ROUNDS} times");
            let before = status(&owner).report.epoch.get();
            let terminated_at = server_now(&run.db).await;
            let pid = terminate_lock_session(&run.db).await;
            // Traffic while the owner's incarnation may still run: its
            // writes race the standby's take.
            if captured < 2 {
                captured += 1;
                owner
                    .send(&run.upstream, &scenario.exchanges[1..2], captured)
                    .await;
            }
            match taker(&owner, &standby, before).await {
                Taker::Standby(took) => {
                    eprintln!(
                        "round {round}: terminated the owner's lock session {pid} (epoch \
                         {before}); the standby took the lock under epoch {}",
                        took.report.epoch
                    );
                    break (terminated_at, took);
                }
                Taker::Owner => {
                    eprintln!("round {round}: the owner retook the lock first; again");
                    wait_status(&owner.running, "the owner running again", RESUME, |status| {
                        status.lock == LockState::Held && status.phase == PipelinePhase::Running
                    })
                    .await;
                }
            }
        };
        let owner_epoch = status(&owner).report.epoch.get();
        let standby_epoch = took.report.epoch.get();
        assert!(
            standby_epoch > owner_epoch,
            "the standby's epoch {standby_epoch} is newer than the owner's {owner_epoch}"
        );

        wait_status(
            &standby.running,
            "the standby running",
            RESUME,
            |status| status.lock == LockState::Held && status.phase == PipelinePhase::Running,
        )
        .await;
        let resumed = wait_status(
            &owner.running,
            "the owner standing by",
            RESUME,
            |status| {
                status.lock == LockState::HeldElsewhere
                    && status.phase == PipelinePhase::WaitingForLock
            },
        )
        .await;
        assert!(resumed.report.lock_losses >= 1, "{resumed:?}");
        let ready = owner.running.readiness().await;
        assert!(ready.ready, "the old owner stays 200 degraded: {ready:?}");
        assert_eq!(ready.pipeline_lock.as_deref(), Some("held elsewhere"));
        assert_ne!(ready.capture.as_deref(), Some("stopped"), "{ready:?}");
        let older_sessions: BTreeSet<String> = sessions_by_name(&run.db)
            .await
            .into_keys()
            .filter(|name| epoch_of(name).is_some_and(|epoch| epoch < standby_epoch))
            .collect();
        assert_eq!(
            older_sessions,
            BTreeSet::new(),
            "an older epoch's session outlived the standby's fence"
        );

        // The rest through the old owner: capture never stopped.
        let all = u64::try_from(scenario.exchanges.len()).expect("fits");
        owner
            .send(
                &run.upstream,
                &scenario.exchanges[usize::try_from(captured).expect("fits")..],
                all,
            )
            .await;
        standby
            .settled(&run.db, "the standby's drain of the owner's captures")
            .await;
        let connects = prober.finish().await;
        assert_eq!(connects.refused, 0, "{connects:?}");
        assert_eq!(connects.failed, 0, "{connects:?}");

        let writes = audited(&run.db).await;
        let began = first_write_of(&writes, standby_epoch)
            .expect("the standby's incarnation wrote audited tables");
        let bumped = writes
            .iter()
            .filter(|(table, _, at)| table == "transport.pipeline_epoch" && *at > terminated_at)
            .map(|(_, _, at)| *at)
            .max();
        let late = older_writes_after(&writes, standby_epoch, began);
        let in_fence = bumped.map(|bumped| older_writes_after(&writes, standby_epoch, bumped));
        eprintln!(
            "terminated at {terminated_at}; the standby's bump at {bumped:?}, its first write \
             at {began} (the fence wait: {:?} us); older-epoch writes between the bump and the \
             standby's start: {in_fence:?}",
            bumped.map(|bumped| began - bumped)
        );
        assert_eq!(
            late,
            Vec::new(),
            "an older epoch's write committed after the standby's incarnation began"
        );

        standby.shutdown().await;
        run.finish(
            owner,
            &scenario,
            &expected,
            "the run handed over to a standby after a terminated lock session",
        )
        .await;
    });
}

/// The owner killed outright (its runtime and every task dropped, its
/// sockets closed, as a crashed process's are): the server ends its
/// sessions, the standby takes the lock at its next retry, and its fence
/// finds no older connection to wait for: it runs promptly, and the run
/// ends as the uninterrupted one.
#[test]
fn a_standby_takes_over_promptly_from_a_killed_owner() {
    let _serial = serial();
    big_stack(|| async {
        const TEST: &str = "a_standby_takes_over_promptly_from_a_killed_owner";
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

        // The owner on a runtime of its own, so a kill drops every one of
        // its tasks at once.
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .thread_stack_size(BIG_STACK)
            .thread_name("killed-owner-worker")
            .enable_all()
            .build()
            .expect("a runtime");
        let running = {
            let config = run.config.clone();
            let url = run.url.clone();
            let clock = clock.clone();
            runtime
                .spawn(async move { start(&config, url, &clock).await })
                .await
                .expect("the owner starts")
        };
        let owner = Gateway {
            running,
            clock: clock.clone(),
        };
        owner.settled(&run.db, "the owner's start").await;
        owner.send(&run.upstream, &scenario.exchanges[..1], 1).await;
        owner.settled(&run.db, "the first exchange's pipeline").await;
        let owner_epoch = status(&owner).report.epoch.get();
        let dir = tempfile::tempdir().expect("a temp dir");
        let standby = start_standby(&run, &standby_data(&run, dir.path()), &clock).await;

        let killed = Instant::now();
        let shut = tokio::task::spawn_blocking(move || {
            runtime.shutdown_timeout(Duration::from_secs(30));
        });
        tokio::time::timeout(WAIT, shut)
            .await
            .expect("the kill finishes")
            .expect("the runtime shuts down");
        drop(owner);

        let fenced = wait_status(
            &standby.running,
            "the standby past its fence",
            TAKE_OVER + SLACK,
            |status| {
                status.lock == LockState::Held
                    && !matches!(
                        status.phase,
                        PipelinePhase::Fencing | PipelinePhase::WaitingForLock
                    )
            },
        )
        .await;
        let took = killed.elapsed();
        eprintln!(
            "the standby took the lock and passed its fence {took:?} after the kill (epoch {})",
            fenced.report.epoch
        );
        assert_eq!(fenced.report.epoch.get(), owner_epoch + 1, "{fenced:?}");
        wait_sessions(
            &run.db,
            "the killed owner's sessions ending",
            Duration::from_secs(10),
            |names| {
                names
                    .keys()
                    .all(|name| epoch_of(name).is_none_or(|epoch| epoch > owner_epoch))
            },
        )
        .await;
        wait_status(&standby.running, "the standby running", RESUME, |status| {
            status.lock == LockState::Held && status.phase == PipelinePhase::Running
        })
        .await;

        let all = u64::try_from(scenario.exchanges.len()).expect("fits");
        standby
            .send(&run.upstream, &scenario.exchanges[1..], all - 1)
            .await;
        standby
            .settled(&run.db, "the standby's pipeline after the kill")
            .await;
        let writes = audited(&run.db).await;
        let began = first_write_of(&writes, owner_epoch + 1).expect("the standby wrote");
        assert_eq!(
            older_writes_after(&writes, owner_epoch + 1, began),
            Vec::new(),
            "the killed owner's epoch wrote after the standby began"
        );
        run.finish(
            standby,
            &scenario,
            &expected,
            "the run handed over to a standby after the owner was killed",
        )
        .await;
    });
}
