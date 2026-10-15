//! The capture gate of a gateway that does not hold the pipeline lock
//! (`ingress.capture.gate-opens-once-groups-exist`).
//!
//! The node0 two-gateway fault run: the owner was killed, the standby took
//! the lock, and the owner restarted as a non-holder. Its gate, closed at
//! start, opened only at its own pipeline's subscribe, so every capture it
//! took stayed in its spool (97 exchanges for five minutes) until it took
//! the lock itself, past the watermark. Now a non-holder opens its gate as
//! soon as every pipeline group exists; on a fresh database, where none
//! does, it still spools until its own subscribe creates them.

use std::path::{Path, PathBuf};
use std::time::Duration;

use crosstalk_gateway::config::exchange_log_path;
use crosstalk_gateway::gateway::postgres::gate_watch::GATE_POLL;
use crosstalk_gateway::live::recovery::{LockState, PipelinePhase};
use crosstalk_gateway::store::lock::PipelineLock;
use crosstalk_memory::support::ManualClock;
use crosstalk_spec::events::{Envelope, Subject};
use crosstalk_spec::ids::EventId;
use crosstalk_store::TestDb;
use crosstalk_store::sqlx;
use tokio::time::Instant;

use crate::common::gateway::{Gateway, Run, captured_in_log, config, run, scenario, start, state};
use crate::common::{big_stack, migrated};
use crate::harness::{RESUME, SLACK, expected, serial, wait_status};

/// How long a non-holder's gate may take to open once every group exists:
/// the look at start or the next poll, and the look itself.
const GATE_OPENS: Duration = GATE_POLL.saturating_add(SLACK);

/// A data directory beside `run`'s whose blob store is `run`'s (a link):
/// the owner's pipeline reads the bodies the other gateway's capture
/// stored, as two hosts on one shared volume would.
fn shared_blobs_data(run: &Run, dir: &Path) -> PathBuf {
    let data = dir.join("restarted");
    std::fs::create_dir_all(&data).expect("the data directory");
    std::fs::create_dir_all(run.data.join("blobs")).expect("the owner's blob store");
    std::os::unix::fs::symlink(run.data.join("blobs"), data.join("blobs"))
        .expect("links the blob store");
    data
}

/// Wait up to `bound` until `gateway`'s gate is open, or fail with its
/// state.
async fn gate_opens(gateway: &Gateway, what: &str, bound: Duration) -> Duration {
    let began = Instant::now();
    let pg = gateway.running.postgres().expect("postgres mode");
    while !pg.gate.is_open() {
        if began.elapsed() > bound {
            panic!(
                "{what}: the gate stayed closed for {bound:?}\n{}",
                state(&gateway.running).await
            );
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    began.elapsed()
}

async fn group_rows(db: &TestDb) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM transport.groups")
        .fetch_one(db.pool())
        .await
        .expect("reads the groups")
}

/// The ids of the `ExchangeCaptured` envelopes in the exchange log under
/// `data`.
async fn logged_exchanges(data: &Path) -> Vec<EventId> {
    crosstalk_gateway::log::read(&exchange_log_path(data))
        .await
        .expect("reads the exchange log")
        .entries
        .into_iter()
        .filter(|envelope: &Envelope| envelope.event.subject() == Subject::ExchangeCaptured)
        .map(|envelope| envelope.id)
        .collect()
}

/// The regression: a gateway started while another holds the lock, on a
/// database whose groups exist, opens its gate within a poll of its start
/// and publishes every capture directly (nothing through its spool); the
/// lock holder's exchange log writes them and its pipeline drains them to
/// the uninterrupted run's answers. The second gateway never takes the
/// lock.
#[test]
fn a_gateway_started_beside_a_lock_holder_publishes_its_captures_directly() {
    let _serial = serial();
    big_stack(|| async {
        const TEST: &str = "a_gateway_started_beside_a_lock_holder_publishes_its_captures_directly";
        let scenario = scenario();
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
        let data = shared_blobs_data(&run, dir.path());
        let restarted = Gateway {
            running: start(
                &config(&run.upstream.base_url(), &data, serde_json::json!({})),
                run.url.clone(),
                &clock,
            )
            .await,
            clock: clock.clone(),
        };
        let opened = gate_opens(
            &restarted,
            "the non-holder's gate with every group present",
            GATE_OPENS,
        )
        .await;
        eprintln!("the non-holder's gate opened {opened:?} after its start");
        wait_status(
            &restarted.running,
            "the non-holder standing by",
            Duration::from_secs(30),
            |status| {
                status.lock == LockState::HeldElsewhere
                    && status.phase == PipelinePhase::WaitingForLock
            },
        )
        .await;

        let rest = u64::try_from(scenario.exchanges.len() - 1).expect("fits");
        restarted
            .send(&run.upstream, &scenario.exchanges[1..], rest)
            .await;
        let spool = restarted.running.health().spool.expect("a spool");
        assert_eq!(
            spool.appended, 0,
            "nothing went through the spool: {spool:?}"
        );
        assert_eq!(spool.state, "direct", "{spool:?}");
        owner
            .settled(
                &run.db,
                "the owner's pipeline over the non-holder's captures",
            )
            .await;
        let logged = logged_exchanges(&run.data).await;
        assert_eq!(
            logged.len(),
            scenario.exchanges.len(),
            "the owner's exchange log wrote every capture, the non-holder's included: \
             {logged:?}"
        );
        let after = restarted
            .running
            .postgres()
            .expect("postgres mode")
            .status
            .snapshot();
        assert_eq!(after.lock, LockState::HeldElsewhere, "{after:?}");
        assert_eq!(after.report.incarnation, 0, "it never ran an incarnation");

        restarted.shutdown().await;
        run.finish(
            owner,
            &scenario,
            &expected,
            "the run captured by a gateway beside the lock holder",
        )
        .await;
    });
}

/// A fresh database: another session holds the lock and no group exists.
/// A gateway's gate stays closed through several polls and every capture
/// waits in its spool (none reaches the log); once the lock is free its
/// own pipeline subscribes the groups, which opens the gate, and the spool
/// drains to the uninterrupted run's answers.
#[test]
fn a_gateway_on_a_fresh_database_spools_until_the_groups_exist() {
    let _serial = serial();
    big_stack(|| async {
        const TEST: &str = "a_gateway_on_a_fresh_database_spools_until_the_groups_exist";
        let scenario = scenario();
        let Some(expected) = expected(TEST, &scenario).await else {
            return;
        };
        let Some(db) = migrated(TEST).await else {
            return;
        };
        let rival = PipelineLock::try_take(db.url())
            .await
            .expect("the rival connects")
            .expect("the rival takes the lock");
        let run = run(db).await;
        let clock = ManualClock::at(scenario.start);
        let gateway = Gateway {
            running: start(&run.config, run.url.clone(), &clock).await,
            clock: clock.clone(),
        };
        wait_status(
            &gateway.running,
            "the gateway seeing the rival's lock",
            Duration::from_secs(30),
            |status| status.lock == LockState::HeldElsewhere,
        )
        .await;

        gateway
            .send(&run.upstream, &scenario.exchanges[..2], 2)
            .await;
        tokio::time::sleep(GATE_POLL * 3 + SLACK).await;
        let pg = gateway.running.postgres().expect("postgres mode");
        assert!(!pg.gate.is_open(), "the gate opened with no group present");
        let spool = gateway.running.health().spool.expect("a spool");
        assert_eq!(
            spool.records, 2,
            "the captures wait in the spool: {spool:?}"
        );
        assert_eq!(spool.state, "spooling", "{spool:?}");
        assert_eq!(group_rows(&run.db).await, 0, "no group exists yet");
        assert_eq!(
            captured_in_log(&run.db).await,
            (0, 0),
            "nothing reached the log"
        );

        drop(rival);
        wait_status(
            &gateway.running,
            "the gateway taking the lock",
            RESUME,
            |status| status.lock == LockState::Held && status.phase == PipelinePhase::Running,
        )
        .await;
        assert!(
            pg.gate.is_open(),
            "the pipeline's subscribe opened the gate"
        );
        gateway
            .settled(&run.db, "the drain once the groups exist")
            .await;
        let all = u64::try_from(scenario.exchanges.len()).expect("fits");
        gateway
            .send(&run.upstream, &scenario.exchanges[2..], all)
            .await;
        gateway
            .settled(&run.db, "the rest of the run's pipeline")
            .await;
        run.finish(
            gateway,
            &scenario,
            &expected,
            "the run spooled on a fresh database while the lock was held elsewhere",
        )
        .await;
    });
}
