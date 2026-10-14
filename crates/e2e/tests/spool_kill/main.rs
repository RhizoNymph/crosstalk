//! The gateway killed while its spool holds captures; see
//! `common::gateway`. Evidence paths: `crosstalk_e2e::spool_kill::*`.

#[path = "../common/mod.rs"]
mod common;

use std::time::Duration;

use crosstalk_memory::support::ManualClock;
use crosstalk_testkit::upstream::{FakeUpstream, Script};
use hyper::StatusCode;

use common::gateway::*;
use common::{WAIT, migrated};

/// The gateway killed while its spool holds a capture, restarted with the
/// database still cut (the spool recovered from disk, spooling the next
/// capture behind it), then the link restored: every capture reaches the
/// bus log exactly once, and the run ends as an uninterrupted one.
#[test]
fn gateway_killed_while_spooling_drains_once_after_restart() {
    common::big_stack(body);
}

async fn body() {
    const TEST: &str = "gateway_killed_while_spooling_drains_once_after_restart";
    let scenario = scenario();
    let Some(expected) = uninterrupted(TEST, &scenario).await else {
        return;
    };
    let Some(db) = migrated(TEST).await else {
        return;
    };
    let link = link_to(&db).await;
    let upstream = FakeUpstream::start(Script::new()).await.expect("upstream");
    let dir = tempfile::tempdir().expect("a temp dir");
    let data = dir.path().join("data");
    let clock = ManualClock::at(scenario.start);
    let url = linked_url(&db, &link);
    let gateway_config = config(&upstream.base_url(), &data, serde_json::json!({}));

    // The first gateway runs on a runtime of its own, so a kill can drop
    // every one of its tasks at once, as a crashed process loses them.
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .thread_stack_size(common::BIG_STACK)
        .thread_name("killed-gateway-worker")
        .enable_all()
        .build()
        .expect("a runtime");
    let running = {
        let config = gateway_config.clone();
        let url = url.clone();
        let clock = clock.clone();
        runtime
            .spawn(async move { start(&config, url, &clock).await })
            .await
            .expect("the first gateway starts")
    };
    let first = Gateway {
        running,
        clock: clock.clone(),
    };
    first.settled(&db, "the first gateway's start").await;
    first.send(&upstream, &scenario.exchanges[..1], 1).await;
    first.settled(&db, "the first exchange's pipeline").await;
    link.cut();
    first.send(&upstream, &scenario.exchanges[1..2], 2).await;
    let spool = first.running.health().spool.expect("a spool");
    assert_eq!(spool.records, 1, "spooled before the kill: {spool:?}");

    // Killed: the runtime and every task on it gone, nothing drained.
    let shut = tokio::task::spawn_blocking(move || {
        runtime.shutdown_timeout(Duration::from_secs(30));
    });
    tokio::time::timeout(WAIT, shut)
        .await
        .expect("the kill finishes")
        .expect("the runtime shuts down");
    drop(first);

    // Restarted with the link still cut: the spool's record is recovered,
    // the next capture spools behind it.
    let second = Gateway {
        running: start(&gateway_config, url, &clock).await,
        clock: clock.clone(),
    };
    let recovered = second.running.health().spool.expect("a spool");
    assert_eq!(recovered.records, 1, "recovered from disk: {recovered:?}");
    second
        .readyz_until("the restarted gateway spooling", |status, body| {
            status == StatusCode::OK
                && body["capture"] == "spooling"
                && body["pipeline"] == "waiting_for_database"
        })
        .await;
    second.send(&upstream, &scenario.exchanges[2..3], 1).await;
    let spool = second.running.health().spool.expect("a spool");
    assert_eq!(spool.records, 2, "{spool:?}");

    link.restore();
    second.settled(&db, "the drain after the restart").await;
    second.send(&upstream, &scenario.exchanges[3..], 2).await;
    second.settled(&db, "the last exchange's pipeline").await;
    second.shutdown().await;

    let all = i64::try_from(scenario.exchanges.len()).expect("fits");
    assert_eq!(
        captured_in_log(&db).await,
        (all, all),
        "every capture in the bus log once"
    );
    let observed = read_back(&db, data.join("blobs"), &scenario).await;
    for (pass, (observed, expected)) in observed.iter().zip(&expected).enumerate() {
        common::assert_same(
            observed,
            expected,
            &format!(
                "pass {pass}: the killed and restarted run differs from the uninterrupted one"
            ),
        );
    }
    db.close().await.expect("drops");
}
