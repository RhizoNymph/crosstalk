//! A database outage through a gateway running exactly as `serve` runs:
//! every gateway task on a runtime from `crosstalk_gateway::runtime::builder`
//! (tokio's default worker count and worker stack size), only its threads
//! named (`serve-runtime-worker`), so a stack overflow on the outage path
//! aborts naming them. The harness itself runs on `common::big_stack`; the
//! gateway's start, sends' capture, the lost lock's abandon, the resume
//! and the stop all run on the serve runtime. Cut once, the lock's loss
//! (the incarnation abandoned, the process spooling on with the database
//! still down), the link restored, the resume in process and the drain,
//! the uninterrupted run's answers. See `common::gateway`.
//! Evidence paths: `crosstalk_e2e::spool_outage_serve_runtime::*`.

#[path = "../common/mod.rs"]
mod common;

use std::time::Duration;

use crosstalk_gateway::gateway::Running;
use crosstalk_gateway::live::recovery::{LockState, PipelinePhase};
use crosstalk_memory::support::ManualClock;
use hyper::StatusCode;
use tokio::runtime::Runtime;

use common::gateway::*;
use common::{WAIT, migrated};

/// Start a gateway of `run` on `runtime`: every task it spawns lives there.
async fn start_on(runtime: &Runtime, run: &Run, clock: &ManualClock) -> Gateway {
    let config = run.config.clone();
    let url = run.url.clone();
    let task_clock = clock.clone();
    let running = tokio::time::timeout(
        WAIT,
        runtime.spawn(async move { start(&config, url, &task_clock).await }),
    )
    .await
    .unwrap_or_else(|_| panic!("the gateway did not start within {WAIT:?}"))
    .expect("the gateway's start task");
    Gateway {
        running,
        clock: clock.clone(),
    }
}

/// Stop `running` gracefully on `runtime`, within its shutdown bound.
async fn stop_on(runtime: &Runtime, running: Running) {
    let bound = running.shutdown_bound();
    let stopped =
        runtime.spawn(async move { tokio::time::timeout(bound, running.shutdown()).await });
    match tokio::time::timeout(WAIT, stopped).await {
        Ok(Ok(Ok(_))) => {}
        Ok(Ok(Err(_))) => panic!("the gateway's shutdown took over {bound:?}"),
        Ok(Err(error)) => panic!("the shutdown task failed: {error}"),
        Err(_) => panic!("the shutdown did not finish within {WAIT:?}"),
    }
}

#[test]
fn database_cut_on_the_serve_runtime_resumes_and_drains_to_the_uninterrupted_answers() {
    common::big_stack(body);
}

async fn body() {
    const TEST: &str =
        "database_cut_on_the_serve_runtime_resumes_and_drains_to_the_uninterrupted_answers";
    let scenario = scenario();
    let Some(expected) = uninterrupted(TEST, &scenario).await else {
        return;
    };
    let Some(db) = migrated(TEST).await else {
        return;
    };
    let serve = crosstalk_gateway::runtime::builder()
        .thread_name("serve-runtime-worker")
        .build()
        .expect("serve's runtime");
    let run = run(db).await;
    let clock = ManualClock::at(scenario.start);

    let gateway = start_on(&serve, &run, &clock).await;
    gateway.settled(&run.db, "the gateway's start").await;
    gateway
        .send(&run.upstream, &scenario.exchanges[..1], 1)
        .await;
    gateway
        .settled(&run.db, "the first exchange's pipeline")
        .await;

    run.link.cut();
    gateway
        .send(&run.upstream, &scenario.exchanges[1..3], 3)
        .await;
    let spool = gateway.running.health().spool.expect("a spool");
    assert_eq!(spool.records, 2, "{spool:?}");
    let mut status = gateway
        .running
        .postgres()
        .expect("postgres mode")
        .status
        .reader();
    within(
        &gateway.running,
        "the pipeline lock's loss",
        status.wait_until(|status| status.lock == LockState::Lost),
    )
    .await
    .expect("the reporter lives");
    gateway
        .readyz_until("spooling while the database is down", |status, body| {
            status == StatusCode::OK
                && body["capture"] == "spooling"
                && body["pipeline"] == "waiting_for_database"
        })
        .await;
    run.link.restore();
    within(
        &gateway.running,
        "the resume after the outage",
        status.wait_until(|status| {
            status.lock == LockState::Held && status.phase == PipelinePhase::Running
        }),
    )
    .await
    .expect("the reporter lives");
    gateway.settled(&run.db, "the drain after the outage").await;
    gateway
        .send(&run.upstream, &scenario.exchanges[3..], 1)
        .await;
    gateway
        .settled(&run.db, "the last exchange's pipeline")
        .await;
    stop_on(&serve, gateway.running).await;
    let shut = tokio::task::spawn_blocking(move || {
        serve.shutdown_timeout(Duration::from_secs(30));
    });
    tokio::time::timeout(WAIT, shut)
        .await
        .expect("the serve runtime stops")
        .expect("the serve runtime's shutdown task");

    let all = i64::try_from(scenario.exchanges.len()).expect("fits");
    assert_eq!(
        captured_in_log(&run.db).await,
        (all, all),
        "every capture in the bus log once"
    );
    let observed = read_back(&run.db, run.data.join("blobs"), &scenario).await;
    for (pass, (observed, expected)) in observed.iter().zip(&expected).enumerate() {
        common::assert_same(
            observed,
            expected,
            &format!(
                "pass {pass}: the run on the serve runtime differs from the uninterrupted one"
            ),
        );
    }
    run.db.close().await.expect("drops");
}
