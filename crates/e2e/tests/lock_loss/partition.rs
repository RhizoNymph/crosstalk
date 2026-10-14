//! The lock's and the pipeline's sessions partitioned or reset while
//! exchanges keep arriving, then healed: the gateway never stops, and the
//! run ends as an uninterrupted one.

use std::time::Duration;

use crosstalk_gateway::gateway::postgres::LOST_REPORTED;
use crosstalk_gateway::live::recovery::{LockState, PipelinePhase};
use crosstalk_gateway::store::lock::SERVER_REAP;
use crosstalk_memory::support::ManualClock;
use tokio::time::Instant;

use crate::common::gateway::{Run, run, scenario};
use crate::common::{big_stack, migrated};
use crate::harness::{
    DRAIN_AFTER_HEAL, LOST_SHUTDOWN_MAX, Prober, RESUME, SLACK, Sampler, expected, serial,
    wait_status,
};

/// How the link fails.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Fault {
    /// Nothing answered, nothing reset: a network partition.
    Silent,
    /// Every connection reset, every new one refused: Postgres stopped.
    Reset,
}

/// A silent partition held past the loss's report and the abandon, then
/// healed.
#[test]
fn a_silent_partition_never_stops_capture_and_resumes_in_process() {
    let _serial = serial();
    big_stack(|| async {
        lose_and_resume(
            "a_silent_partition_never_stops_capture_and_resumes_in_process",
            Fault::Silent,
            LOST_REPORTED + LOST_SHUTDOWN_MAX + Duration::from_secs(5),
        )
        .await;
    });
}

/// Postgres stopped (every session reset, connects refused) past the
/// loss's report and the abandon, then started again.
#[test]
fn a_postgres_stop_never_stops_capture_and_resumes_in_process() {
    let _serial = serial();
    big_stack(|| async {
        lose_and_resume(
            "a_postgres_stop_never_stops_capture_and_resumes_in_process",
            Fault::Reset,
            LOST_REPORTED + LOST_SHUTDOWN_MAX + Duration::from_secs(5),
        )
        .await;
    });
}

/// A silent partition held past the server's reap of a partitioned
/// session, then healed.
#[test]
fn a_partition_past_the_server_reap_never_stops_capture_and_resumes_in_process() {
    let _serial = serial();
    big_stack(|| async {
        lose_and_resume(
            "a_partition_past_the_server_reap_never_stops_capture_and_resumes_in_process",
            Fault::Silent,
            SERVER_REAP + Duration::from_secs(10),
        )
        .await;
    });
}

/// The fault from the first exchange's settle on for `held`, exchanges
/// forwarded during it; then healed and drained. Throughout: no refused
/// connect, every exchange answered unchanged (`Gateway::send`), `/readyz`
/// 200 and never `stopped`.
async fn lose_and_resume(test: &str, fault: Fault, held: Duration) {
    let scenario = scenario();
    let Some(expected) = expected(test, &scenario).await else {
        return;
    };
    let Some(db) = migrated(test).await else {
        return;
    };
    let run: Run = run(db).await;
    let clock = ManualClock::at(scenario.start);
    let gateway = run.start(&clock).await;
    gateway
        .send(&run.upstream, &scenario.exchanges[..1], 1)
        .await;
    gateway
        .settled(&run.db, "the first exchange's pipeline")
        .await;
    let appended_before = gateway.running.health().spool.expect("a spool").appended;

    let prober = Prober::start(gateway.running.proxy_addr().expect("role all proxies"));
    let sampler = Sampler::start(gateway.running.ops_addr());
    let faulted = Instant::now();
    match fault {
        Fault::Silent => run.link.partition(),
        Fault::Reset => run.link.cut(),
    }
    gateway
        .send(&run.upstream, &scenario.exchanges[1..2], 2)
        .await;
    let lost = wait_status(
        &gateway.running,
        "the lock's loss",
        LOST_REPORTED + SLACK + Duration::from_secs(5),
        |status| status.lock == LockState::Lost,
    )
    .await;
    eprintln!(
        "{fault:?}: lock lost after {:?}: {}",
        faulted.elapsed(),
        lost.pipeline_text()
    );
    gateway
        .send(&run.upstream, &scenario.exchanges[2..3], 3)
        .await;
    if let Some(rest) = held.checked_sub(faulted.elapsed()) {
        tokio::time::sleep(rest).await;
    }
    let spool = gateway.running.health().spool.expect("a spool");
    assert_eq!(spool.records, 2, "both exchanges spooled: {spool:?}");
    assert_eq!(spool.state, "spooling", "{spool:?}");

    run.link.restore();
    let healed = Instant::now();
    let resumed = wait_status(&gateway.running, "the resume", RESUME, |status| {
        status.lock == LockState::Held && status.phase == PipelinePhase::Running
    })
    .await;
    let resumed_after = healed.elapsed();
    gateway.settled(&run.db, "the drain after the heal").await;
    let drained_after = healed.elapsed();
    eprintln!(
        "{fault:?} held {held:?}: resumed {resumed_after:?} and drained {drained_after:?} after the heal; report {:?}",
        resumed.report
    );
    assert!(
        drained_after < DRAIN_AFTER_HEAL,
        "the drain after the heal took {drained_after:?}: holds of the abandoned pipeline not ready at once?"
    );
    gateway
        .send(&run.upstream, &scenario.exchanges[3..], 1)
        .await;
    gateway
        .settled(&run.db, "the last exchange's pipeline")
        .await;

    let connects = prober.finish().await;
    let samples = sampler.finish().await;
    eprintln!(
        "{fault:?}: connects {connects:?}; readyz path {:?}",
        samples.path
    );
    assert!(connects.attempts > 100, "{connects:?}");
    assert_eq!(connects.refused, 0, "refused connects: {connects:?}");
    assert_eq!(connects.failed, 0, "failed connects: {connects:?}");
    assert_eq!(samples.unanswered, 0, "{samples:?}");
    assert_eq!(samples.not_ok, Vec::<String>::new(), "readyz not 200");
    assert!(
        !samples.path.iter().any(|step| step.contains("stopped")),
        "the pipeline read stopped: {:?}",
        samples.path
    );
    assert!(
        samples.path.iter().any(|step| step.starts_with("lost")),
        "the loss shows in readyz: {:?}",
        samples.path
    );
    let spool = gateway.running.health().spool.expect("a spool");
    assert_eq!(spool.records, 0, "{spool:?}");
    assert_eq!(spool.state, "direct", "{spool:?}");
    assert_eq!(
        spool.drained, spool.appended,
        "every record drained exactly once: {spool:?}"
    );
    assert!(
        spool.appended >= appended_before + 2,
        "both exchanges went through the spool: {spool:?}"
    );
    run.finish(
        gateway,
        &scenario,
        &expected,
        &format!("the run with the lock lost ({fault:?}, {held:?})"),
    )
    .await;
}

/// A graceful shutdown while the database is silently partitioned
/// returns within `Running::shutdown_bound`: no step waits on the dead
/// link (a pool's close used to wait for every connection to come back,
/// each returning one pinging without a deadline).
#[test]
fn a_shutdown_across_a_partition_returns_within_its_bound() {
    let _serial = serial();
    big_stack(|| async {
        const TEST: &str = "a_shutdown_across_a_partition_returns_within_its_bound";
        let scenario = scenario();
        let Some(db) = migrated(TEST).await else {
            return;
        };
        let run = run(db).await;
        let clock = ManualClock::at(scenario.start);
        let gateway = run.start(&clock).await;
        gateway
            .send(&run.upstream, &scenario.exchanges[..1], 1)
            .await;
        gateway
            .settled(&run.db, "the first exchange's pipeline")
            .await;
        run.link.partition();
        gateway
            .send(&run.upstream, &scenario.exchanges[1..2], 2)
            .await;
        let bound = gateway.running.shutdown_bound();
        let started = Instant::now();
        let stopped = tokio::time::timeout(bound + SLACK, gateway.running.shutdown()).await;
        let took = started.elapsed();
        eprintln!("shutdown across a partition took {took:?} (bound {bound:?})");
        assert!(
            stopped.is_ok(),
            "the shutdown across a partition took over {bound:?}"
        );
        run.link.restore();
        run.db.close().await.expect("drops");
    });
}
