//! A database outage mid-stream through the gateway, one test per binary
//! (an abort cannot hide another test's result); see `common::gateway`.
//! Evidence paths: `crosstalk_e2e::spool_outage::*`.

#[path = "../common/mod.rs"]
mod common;

use crosstalk_memory::support::ManualClock;

use common::gateway::*;
use common::migrated;

/// The database cut mid-stream: forwarding never changes and captures
/// spool; the lost pipeline lock makes the process exit non-zero
/// (`Running::fatal`, what `serve` exits on); a fresh start on the same
/// data directory spools on while the database is down, drains once it is
/// back, and the run ends as an uninterrupted one.
#[test]
fn database_cut_mid_stream_spools_then_drains_to_the_uninterrupted_answers() {
    common::big_stack(body);
}

async fn body() {
    const TEST: &str = "database_cut_mid_stream_spools_then_drains_to_the_uninterrupted_answers";
    let scenario = scenario();
    let Some(expected) = uninterrupted(TEST, &scenario).await else {
        return;
    };
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
    let gateway = run.outage(gateway, &scenario.exchanges[1..3], 1).await;
    gateway
        .send(&run.upstream, &scenario.exchanges[3..], 1)
        .await;
    gateway
        .settled(&run.db, "the last exchange's pipeline")
        .await;
    run.finish(gateway, &scenario, &expected, "the run with an outage")
        .await;
}
