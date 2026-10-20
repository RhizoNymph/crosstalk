//! A planned handover (`transport.pipeline-lock.planned-handover-clean`):
//! two gateways on one database, traffic flowing, the owner shut down
//! gracefully (what SIGTERM, `docker stop` or a rolling restart do). The
//! owner stops taking deliveries, finishes or releases what it handles,
//! checkpoints, closes its pipeline pool and only then unlocks the
//! pipeline lock; the standby takes the lock at its next try, finds no
//! older connection to fence, and runs. Nothing is redelivered on a later
//! attempt, nothing dead-lettered, nothing late at L7, and every captured
//! exchange is in the exchange logs exactly once.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::time::Duration;

use crosstalk_gateway::config::exchange_log_path;
use crosstalk_gateway::gateway::postgres::RETRY;
use crosstalk_gateway::live::recovery::{
    HandoverStep, LockState, PipelinePhase, PipelineStatus, StatusReader,
};
use crosstalk_memory::support::ManualClock;
use crosstalk_spec::events::{Envelope, Subject};
use crosstalk_store::TestDb;
use crosstalk_store::sqlx;
use crosstalk_testkit::client::{BodyEnd, HarnessClient};
use crosstalk_testkit::upstream::{Pacing, Reply};
use crosstalk_transport::HANDOVER_REASON;
use tokio::time::Instant;

use crate::common::gateway::{
    Gateway, PREFIX, Run, captured_in_log, config, request, response, run, scenario, start,
};
use crate::common::{WAIT, big_stack, migrated};
use crate::harness::{SLACK, epoch_of, expected, serial, sessions_by_name, wait_status};

/// How long after the owner released the lock the standby may take to
/// hold it: its next try (the pipeline task's retry) and the take itself.
const TAKES_WITHIN: Duration = RETRY.saturating_add(SLACK);

/// How long the upstream holds the in-flight response before its body:
/// past a handover and the standby's next try ([`RETRY`]), within the drain timeout
/// (5 s in the test config).
const STREAM_HOLD: Duration = Duration::from_secs(4);

/// How long after the release the standby may take to run: the take, then
/// its recovery (the owner left nothing held, so nothing to wait for).
const RUNS_WITHIN: Duration = RETRY
    .saturating_add(SLACK)
    .saturating_add(SLACK)
    .saturating_add(SLACK);

/// One audited update of `transport.deliveries`.
#[derive(Debug, Clone, PartialEq, Eq)]
struct DeliveryUpdate {
    id: i64,
    group: String,
    seq: i64,
    old_state: String,
    new_state: String,
    old_attempt: i32,
    new_attempt: i32,
    last_error: Option<String>,
    app: String,
}

/// Record every update of a delivery: takes (to `held`, the attempt
/// counted), releases, nacks and their reasons, with the writer's
/// `application_name`.
async fn audit_deliveries(db: &TestDb) {
    for statement in [
        "CREATE TABLE public.delivery_updates (id bigserial PRIMARY KEY, group_name text NOT NULL, \
         seq bigint NOT NULL, old_state text NOT NULL, new_state text NOT NULL, \
         old_attempt integer NOT NULL, new_attempt integer NOT NULL, last_error text, \
         app text NOT NULL)",
        "CREATE FUNCTION public.record_delivery_update() RETURNS trigger LANGUAGE plpgsql AS $$ \
         BEGIN \
           INSERT INTO public.delivery_updates \
             (group_name, seq, old_state, new_state, old_attempt, new_attempt, last_error, app) \
           VALUES (NEW.group_name, NEW.seq, OLD.state, NEW.state, OLD.attempt, NEW.attempt, \
             NEW.last_error, current_setting('application_name')); \
           RETURN NULL; \
         END $$",
        "CREATE TRIGGER delivery_update_audit AFTER UPDATE ON transport.deliveries \
         FOR EACH ROW EXECUTE FUNCTION public.record_delivery_update()",
    ] {
        sqlx::query(statement)
            .execute(db.pool())
            .await
            .expect("installs the delivery audit");
    }
}

/// [`DeliveryUpdate`]'s columns, in order.
type UpdateRow = (
    i64,
    String,
    i64,
    String,
    String,
    i32,
    i32,
    Option<String>,
    String,
);

async fn delivery_updates(db: &TestDb) -> Vec<DeliveryUpdate> {
    let rows: Vec<UpdateRow> = sqlx::query_as(
        "SELECT id, group_name, seq, old_state, new_state, old_attempt, new_attempt, \
             last_error, app FROM public.delivery_updates ORDER BY id",
    )
    .fetch_all(db.pool())
    .await
    .expect("reads the delivery audit");
    rows.into_iter()
        .map(
            |(id, group, seq, old_state, new_state, old_attempt, new_attempt, last_error, app)| {
                DeliveryUpdate {
                    id,
                    group,
                    seq,
                    old_state,
                    new_state,
                    old_attempt,
                    new_attempt,
                    last_error,
                    app,
                }
            },
        )
        .collect()
}

async fn dead_letters(db: &TestDb) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM transport.dead_letters")
        .fetch_one(db.pool())
        .await
        .expect("counts the dead letters")
}

/// A data directory beside `run`'s sharing its blob store (a link), as two
/// hosts on one shared volume would.
fn standby_data(run: &Run, dir: &Path) -> PathBuf {
    let data = dir.join("standby");
    std::fs::create_dir_all(&data).expect("the standby's data directory");
    std::fs::create_dir_all(run.data.join("blobs")).expect("the owner's blob store");
    std::os::unix::fs::symlink(run.data.join("blobs"), data.join("blobs"))
        .expect("links the blob store");
    data
}

/// The `ExchangeCaptured` ids in the exchange log under `data` (none when
/// it has no log).
async fn logged_exchanges(data: &Path) -> Vec<String> {
    let path = exchange_log_path(data);
    if !path.exists() {
        return Vec::new();
    }
    crosstalk_gateway::log::read(&path)
        .await
        .expect("reads the exchange log")
        .entries
        .into_iter()
        .filter(|envelope: &Envelope| envelope.event.subject() == Subject::ExchangeCaptured)
        .map(|envelope| envelope.id.ulid_text())
        .collect()
}

/// When `reader`'s status first satisfies `ready`, within [`WAIT`].
async fn when(
    mut reader: StatusReader,
    what: &str,
    ready: impl FnMut(&PipelineStatus) -> bool,
) -> (Instant, PipelineStatus) {
    match tokio::time::timeout(WAIT, reader.wait_until(ready)).await {
        Ok(Some(status)) => (Instant::now(), status),
        Ok(None) => panic!("{what}: the status reporter is gone"),
        Err(_) => panic!("{what}: not within {WAIT:?}: {:?}", reader.snapshot()),
    }
}

/// The handover's releases each came back on the attempt they were given
/// back: for every delivery the owner released, the next take counts the
/// same attempt it had when the owner held it.
fn retakes_after_release(
    updates: &[DeliveryUpdate],
) -> Vec<(DeliveryUpdate, Option<DeliveryUpdate>)> {
    let releases: Vec<&DeliveryUpdate> = updates
        .iter()
        .filter(|update| {
            update.last_error.as_deref() == Some(HANDOVER_REASON) && update.new_state == "ready"
        })
        .collect();
    releases
        .into_iter()
        .map(|release| {
            let retake = updates
                .iter()
                .find(|update| {
                    update.id > release.id
                        && update.group == release.group
                        && update.seq == release.seq
                        && update.new_state == "held"
                })
                .cloned();
            (release.clone(), retake)
        })
        .collect()
}

/// Deliveries whose attempt the handover raised, as (the owner's last
/// update, the standby's first take after it): the owner's last word on a
/// delivery was a take it never settled (left held: a recovery would count
/// its attempt again) or the handover's release (its attempt given back),
/// and the standby's first take of it counts a later attempt than the
/// owner's. A delivery the owner nacked is left out: the nack spent that
/// attempt, not the handover; one it acked the standby never takes.
fn raised_across_the_handover(
    updates: &[DeliveryUpdate],
    owner: u64,
    standby: u64,
) -> Vec<(DeliveryUpdate, DeliveryUpdate)> {
    let mut last_by_owner: BTreeMap<(String, i64), &DeliveryUpdate> = BTreeMap::new();
    for update in updates
        .iter()
        .filter(|update| epoch_of(&update.app) == Some(owner))
    {
        last_by_owner.insert((update.group.clone(), update.seq), update);
    }
    last_by_owner
        .into_iter()
        .filter_map(|((group, seq), last)| {
            let given = match (last.new_state.as_str(), last.last_error.as_deref()) {
                ("held", _) => last.new_attempt,
                ("ready", Some(HANDOVER_REASON)) => last.old_attempt,
                _ => return None,
            };
            let retake = updates.iter().find(|update| {
                update.id > last.id
                    && update.group == group
                    && update.seq == seq
                    && update.new_state == "held"
                    && epoch_of(&update.app) == Some(standby)
            })?;
            (retake.new_attempt > given).then(|| (last.clone(), retake.clone()))
        })
        .collect()
}

/// The owner (lock holder) shut down gracefully while the standby waits
/// and traffic flows through both: an exchange in the owner's pipeline as
/// it stops, another captured by the standby during the handover. The
/// owner's handover is clean (every stage finished its delivery, the holds
/// released, its pool closed, the lock unlocked); the standby holds the
/// lock within a retry of the release and runs soon after, with no
/// session of the owner's epoch left and nothing for its recovery to
/// redeliver. Every delivery released came back on the same attempt;
/// nothing was dead-lettered or late at L7; each capture is in the bus log
/// once and in the two exchange logs exactly once together; the run ends
/// as the uninterrupted one.
#[test]
fn a_planned_shutdown_hands_the_pipeline_to_the_standby_cleanly() {
    let _serial = serial();
    big_stack(|| async {
        const TEST: &str = "a_planned_shutdown_hands_the_pipeline_to_the_standby_cleanly";
        let scenario = scenario();
        assert!(
            scenario.exchanges.len() >= 4,
            "the scenario has the exchanges to spread"
        );
        let Some(expected) = expected(TEST, &scenario).await else {
            return;
        };
        let Some(db) = migrated(TEST).await else {
            return;
        };
        audit_deliveries(&db).await;
        let run = run(db).await;
        let clock = ManualClock::at(scenario.start);
        let owner = run.start(&clock).await;
        owner.send(&run.upstream, &scenario.exchanges[..1], 1).await;
        owner
            .settled(&run.db, "the first exchange's pipeline")
            .await;
        let dir = tempfile::tempdir().expect("a temp dir");
        let standby_dir = standby_data(&run, dir.path());
        let standby = Gateway {
            running: start(
                &config(
                    &run.upstream.base_url(),
                    &standby_dir,
                    serde_json::json!({}),
                ),
                run.url.clone(),
                &clock,
            )
            .await,
            clock: clock.clone(),
        };
        wait_status(
            &standby.running,
            "the standby standing by",
            Duration::from_secs(30),
            |status| {
                status.lock == LockState::HeldElsewhere
                    && status.phase == PipelinePhase::WaitingForLock
            },
        )
        .await;
        let owner_epoch = owner
            .running
            .postgres()
            .expect("postgres mode")
            .status
            .snapshot()
            .report
            .epoch
            .get();

        // The second exchange through the owner, its pipeline not awaited:
        // the shutdown finds it in flight.
        owner
            .send(&run.upstream, &scenario.exchanges[1..2], 2)
            .await;
        let owner_status = owner
            .running
            .postgres()
            .expect("postgres mode")
            .status
            .reader();
        let released = tokio::spawn(when(
            owner_status.clone(),
            "the owner releasing the lock",
            |status| status.lock == LockState::Released,
        ));
        let standby_status = standby
            .running
            .postgres()
            .expect("postgres mode")
            .status
            .reader();
        let held = tokio::spawn(when(
            standby_status.clone(),
            "the standby taking the lock",
            |status| status.lock == LockState::Held,
        ));
        let Gateway { running, clock: _ } = owner;
        let began = Instant::now();
        let stopping = tokio::spawn(async move { running.shutdown().await });

        // The third through the standby while the owner hands over: it
        // publishes directly (its gate is open), the new owner takes it.
        standby
            .send(&run.upstream, &scenario.exchanges[2..3], 1)
            .await;

        let report = tokio::time::timeout(WAIT, stopping)
            .await
            .expect("the owner's shutdown finishes")
            .expect("the shutdown's task");
        let stopped = began.elapsed();
        let (released_at, after) = released.await.expect("the owner's status watch");
        let handover = report
            .handover
            .clone()
            .expect("the owner ran an incarnation: handed over");
        eprintln!("the owner stopped in {stopped:?}; its handover: {handover:?}");
        assert!(handover.clean(), "a clean handover: {handover:?}");
        assert_eq!(after.handover(), Some(HandoverStep::Done), "{after:?}");
        assert_eq!(after.report.handover, Some(HandoverStep::Done), "{after:?}");
        assert!(
            report.capture_drained && report.stages_drained && report.log_drained,
            "{report:?}"
        );

        let (held_at, taken) = held.await.expect("the standby's status watch");
        let took = held_at.saturating_duration_since(released_at);
        eprintln!(
            "the standby held the lock {took:?} after the owner released it (epoch {})",
            taken.report.epoch
        );
        assert!(
            took <= TAKES_WITHIN,
            "the standby took the lock {took:?} after the release"
        );
        let standby_epoch = taken.report.epoch.get();
        assert_eq!(standby_epoch, owner_epoch + 1, "{taken:?}");

        let (running_at, running) = when(standby_status.clone(), "the standby running", |status| {
            status.lock == LockState::Held && status.phase == PipelinePhase::Running
        })
        .await;
        let ran = running_at.saturating_duration_since(released_at);
        eprintln!("the standby ran {ran:?} after the release");
        assert!(
            ran <= RUNS_WITHIN,
            "the standby ran {ran:?} after the release"
        );
        let older: BTreeMap<String, i64> = sessions_by_name(&run.db)
            .await
            .into_iter()
            .filter(|(name, _)| epoch_of(name).is_some_and(|epoch| epoch < standby_epoch))
            .collect();
        assert_eq!(
            older,
            BTreeMap::new(),
            "an older epoch's session outlived the handover"
        );
        assert_eq!(
            running.report.deliveries_redelivered, 0,
            "the owner left nothing held for the standby's recovery: {running:?}"
        );

        // The rest through the standby, now the owner.
        let all = scenario.exchanges.len();
        standby
            .send(
                &run.upstream,
                &scenario.exchanges[3..],
                u64::try_from(all - 2).expect("fits"),
            )
            .await;
        standby
            .settled(&run.db, "the standby's pipeline after the handover")
            .await;

        assert_eq!(dead_letters(&run.db).await, 0, "nothing dead-lettered");
        let updates = delivery_updates(&run.db).await;
        let late: Vec<&DeliveryUpdate> = updates
            .iter()
            .filter(|update| {
                update
                    .last_error
                    .as_deref()
                    .is_some_and(|error| error.contains("late contribution"))
            })
            .collect();
        assert_eq!(
            late,
            Vec::<&DeliveryUpdate>::new(),
            "no L7 late contribution"
        );
        for (release, retake) in retakes_after_release(&updates) {
            let retake =
                retake.unwrap_or_else(|| panic!("released but never taken again: {release:?}"));
            assert_eq!(
                retake.new_attempt, release.old_attempt,
                "the handover spent no attempt: released {release:?}, taken again {retake:?}"
            );
            assert_eq!(epoch_of(&release.app), Some(owner_epoch), "{release:?}");
        }
        assert_eq!(
            raised_across_the_handover(&updates, owner_epoch, standby_epoch),
            Vec::new(),
            "a delivery the owner left came back to the standby on a later attempt"
        );

        let mut logged = logged_exchanges(&run.data).await;
        let from_owner = logged.len();
        logged.extend(logged_exchanges(&standby_dir).await);
        eprintln!(
            "exchange logs: {from_owner} line(s) by the owner, {} by the standby",
            logged.len() - from_owner
        );
        let distinct: BTreeSet<&String> = logged.iter().collect();
        assert_eq!(
            logged.len(),
            all,
            "every capture logged once across both logs: {logged:?}"
        );
        assert_eq!(distinct.len(), all, "no capture logged twice: {logged:?}");
        let all = i64::try_from(all).expect("fits");
        assert_eq!(
            captured_in_log(&run.db).await,
            (all, all),
            "every capture in the bus log once"
        );

        run.finish(
            standby,
            &scenario,
            &expected,
            "the run handed over to a standby by a planned shutdown",
        )
        .await;
    });
}

/// The owner shut down with a response still streaming through its proxy:
/// the pipeline is handed over at once, beside the proxy's drain, not
/// after it. The owner releases the lock, and the standby takes it, before
/// that response's body has even begun; the client still gets the whole
/// response unchanged, and its capture (published by the owner after the
/// handover, through capture's own bus) is taken by the standby and in the
/// exchange logs exactly once.
#[test]
fn the_handover_does_not_wait_for_the_proxy_drain() {
    let _serial = serial();
    big_stack(|| async {
        const TEST: &str = "the_handover_does_not_wait_for_the_proxy_drain";
        let scenario = scenario();
        assert!(
            scenario.exchanges.len() >= 3,
            "the scenario has the exchanges to spread"
        );
        let Some(expected) = expected(TEST, &scenario).await else {
            return;
        };
        let Some(db) = migrated(TEST).await else {
            return;
        };
        let run = run(db).await;
        let clock = ManualClock::at(scenario.start);
        let owner = run.start(&clock).await;
        owner.send(&run.upstream, &scenario.exchanges[..1], 1).await;
        owner
            .settled(&run.db, "the first exchange's pipeline")
            .await;
        let dir = tempfile::tempdir().expect("a temp dir");
        let standby_dir = standby_data(&run, dir.path());
        let standby = Gateway {
            running: start(
                &config(
                    &run.upstream.base_url(),
                    &standby_dir,
                    serde_json::json!({}),
                ),
                run.url.clone(),
                &clock,
            )
            .await,
            clock: clock.clone(),
        };
        wait_status(
            &standby.running,
            "the standby standing by",
            Duration::from_secs(30),
            |status| {
                status.lock == LockState::HeldElsewhere
                    && status.phase == PipelinePhase::WaitingForLock
            },
        )
        .await;

        // The second exchange through the owner, its body held back by the
        // upstream: in flight when the shutdown begins.
        let slow = &scenario.exchanges[1];
        clock.set(slow.ended_at);
        let mut reply = Reply::from_response(&response(slow));
        reply.pacing = Pacing {
            first_chunk: STREAM_HOLD,
            between_chunks: Duration::ZERO,
        };
        run.upstream.reply_next(reply).await.expect("scripted");
        let client = HarnessClient::new(owner.running.proxy_addr().expect("role all proxies"))
            .prefix(PREFIX);
        let sent = request(slow);
        let answering = tokio::spawn(async move {
            let answered = client.send(&sent).await.expect("the gateway answers");
            (Instant::now(), answered)
        });
        tokio::time::timeout(WAIT, async {
            while run
                .upstream
                .received()
                .await
                .expect("the upstream log")
                .len()
                < 2
            {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("the upstream got the second request");

        let owner_status = owner
            .running
            .postgres()
            .expect("postgres mode")
            .status
            .reader();
        let released = tokio::spawn(when(
            owner_status,
            "the owner releasing the lock",
            |status| status.lock == LockState::Released,
        ));
        let standby_status = standby
            .running
            .postgres()
            .expect("postgres mode")
            .status
            .reader();
        let held = tokio::spawn(when(
            standby_status.clone(),
            "the standby taking the lock",
            |status| status.lock == LockState::Held,
        ));
        let Gateway { running, clock: _ } = owner;
        let began = Instant::now();
        let stopping = tokio::spawn(async move { running.shutdown().await });

        let (released_at, _) = released.await.expect("the owner's status watch");
        let (held_at, _) = held.await.expect("the standby's status watch");
        let (answered_at, answered) = answering.await.expect("the client's task");
        eprintln!(
            "after the shutdown began: released {:?}, taken {:?}, the response ended {:?}",
            released_at.saturating_duration_since(began),
            held_at.saturating_duration_since(began),
            answered_at.saturating_duration_since(began),
        );
        assert!(
            released_at < answered_at,
            "the owner released the lock only after the proxy's last response ended"
        );
        assert!(
            held_at < answered_at,
            "the standby took the lock only after the proxy's last response ended"
        );
        assert_eq!(answered.end, BodyEnd::Complete, "{}", slow.label);
        assert_eq!(
            answered.differences_from(&response(slow)),
            Vec::new(),
            "{}: the client's response changed",
            slow.label
        );

        let report = tokio::time::timeout(WAIT, stopping)
            .await
            .expect("the owner's shutdown finishes")
            .expect("the shutdown's task");
        let handover = report
            .handover
            .clone()
            .expect("the owner ran an incarnation: handed over");
        assert!(handover.clean(), "a clean handover: {handover:?}");
        assert_eq!(report.proxy.cut, 0, "no connection cut: {report:?}");
        assert!(
            report.capture_drained && report.stages_drained && report.log_drained,
            "{report:?}"
        );

        when(standby_status, "the standby running", |status| {
            status.lock == LockState::Held && status.phase == PipelinePhase::Running
        })
        .await;
        let all = scenario.exchanges.len();
        standby
            .send(
                &run.upstream,
                &scenario.exchanges[2..],
                u64::try_from(all - 2).expect("fits"),
            )
            .await;
        standby
            .settled(&run.db, "the standby's pipeline after the handover")
            .await;

        assert_eq!(dead_letters(&run.db).await, 0, "nothing dead-lettered");
        let mut logged = logged_exchanges(&run.data).await;
        logged.extend(logged_exchanges(&standby_dir).await);
        let distinct: BTreeSet<&String> = logged.iter().collect();
        assert_eq!(
            logged.len(),
            all,
            "every capture logged once across both logs: {logged:?}"
        );
        assert_eq!(distinct.len(), all, "no capture logged twice: {logged:?}");
        let all = i64::try_from(all).expect("fits");
        assert_eq!(
            captured_in_log(&run.db).await,
            (all, all),
            "every capture in the bus log once"
        );

        run.finish(
            standby,
            &scenario,
            &expected,
            "the run handed over while the owner's proxy drained",
        )
        .await;
    });
}
