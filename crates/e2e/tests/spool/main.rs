//! The publish spool end to end, through the gateway's proxy: the wiki
//! relay sent as HTTP through `serve --role all` in Postgres mode
//! (`gateway::start_on` with `store` and `spool` sections), the database
//! reached through a cuttable `DbLink` so an outage can be cut and
//! restored without touching the server.
//!
//! - `full_spool_refuses_and_proxy_still_forwards` (`transport.spool.bounded`,
//!   INV-1210; no database needed): a spool with room for two captures
//!   holds two, refuses the rest at once (counted `spool_full`), never
//!   holds more than its bound, and the proxy forwards every exchange
//!   unchanged while `/readyz` turns 503.
//! - The outage and kill tests run one per binary, on 16 MiB stacks
//!   (`common::big_stack`), so an abort cannot hide another's result:
//!   `spool_outage` (the database cut mid-stream: the process must restart,
//!   a fresh start drains), `spool_outage_twice` (two outages in a row),
//!   `spool_kill` (the gateway killed while spooling). Their harness is
//!   `common::gateway`.
//!
//! Every gateway runs on a manual clock set to each exchange's end before
//! it is sent, with the flow consumer's periodic tick out of reach (a day),
//! so what a run decides depends on its traffic alone. A gateway mints its
//! ids from OS entropy (`SERVE_IDS`); runs are compared once renamed
//! (`crosstalk_e2e::answers`). The answers are read by a Postgres-mode
//! `Live` process started over the database and the gateway's blob store
//! after the gateway stopped, settled on the same passes in every run.
//!
//! Evidence paths: `crosstalk_e2e::spool::*`.

#[path = "../common/mod.rs"]
mod common;

use std::time::Duration;

use crosstalk_memory::support::ManualClock;
use crosstalk_testkit::upstream::{FakeUpstream, Script};
use hyper::StatusCode;
use tokio::time::Instant;

use common::gateway::*;

/// `transport.spool.bounded` (INV-1210): with the database down and a
/// spool bounded at what two of the scenario's captures take, two are
/// spooled and the other two are refused at once and counted
/// `spool_full`; the spool never holds more than its bound; the proxy
/// forwards and answers every exchange unchanged; `/readyz` is 503
/// `dropping: spool full`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn full_spool_refuses_and_proxy_still_forwards() {
    let scenario = scenario();
    let upstream = FakeUpstream::start(Script::new()).await.expect("upstream");

    // How many bytes the first two captures take: a roomy spool, measured.
    let measured = {
        let dir = tempfile::tempdir().expect("a temp dir");
        let clock = ManualClock::at(scenario.start);
        let gateway = Gateway {
            running: start(
                &config(
                    &upstream.base_url(),
                    &dir.path().join("data"),
                    serde_json::json!({}),
                ),
                UNREACHABLE.to_owned(),
                &clock,
            )
            .await,
            clock,
        };
        gateway.send(&upstream, &scenario.exchanges[..2], 2).await;
        let spool = gateway.running.health().spool.expect("a spool");
        assert_eq!(spool.records, 2, "{spool:?}");
        let bytes = spool.bytes;
        gateway.shutdown().await;
        bytes
    };

    let dir = tempfile::tempdir().expect("a temp dir");
    let clock = ManualClock::at(scenario.start);
    let gateway = Gateway {
        running: start(
            &config(
                &upstream.base_url(),
                &dir.path().join("data"),
                serde_json::json!({"max_bytes": measured, "segment_bytes": measured}),
            ),
            UNREACHABLE.to_owned(),
            &clock,
        )
        .await,
        clock,
    };
    let all = u64::try_from(scenario.exchanges.len()).expect("fits");
    let sent = Instant::now();
    gateway.send(&upstream, &scenario.exchanges, all).await;
    let elapsed = sent.elapsed();
    let running = &gateway.running;
    let health = running.health();
    let spool = health.spool.clone().expect("a spool section");
    assert_eq!(spool.records, 2, "{spool:?}");
    assert!(spool.bytes <= measured, "over its bound: {spool:?}");
    assert_eq!(spool.capture_spool_full, all - 2, "{spool:?}");
    assert_eq!(spool.rejected_full, all - 2, "{spool:?}");
    assert_eq!(health.pipeline.published, 2);
    assert_eq!(health.capture.captured, all);
    assert!(
        elapsed < Duration::from_secs(60),
        "a refusal waits on nothing: {elapsed:?} for {all} exchanges"
    );

    let (status, ready) = json(running, "/readyz").await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{ready}");
    assert_eq!(ready["ready"], false);
    assert_eq!(ready["capture"], "dropping: spool full");

    let text = metrics(running).await;
    has_line(&text, "crosstalk_spool_records 2");
    has_line(&text, &format!("crosstalk_spool_bytes {}", spool.bytes));
    has_line(
        &text,
        &format!(
            "crosstalk_spool_rejected_total{{reason=\"spool_full\"}} {}",
            all - 2
        ),
    );
    has_line(
        &text,
        &format!(
            "crosstalk_capture_uncaptured_total{{reason=\"spool_full\"}} {}",
            all - 2
        ),
    );
    gateway.shutdown().await;
}
