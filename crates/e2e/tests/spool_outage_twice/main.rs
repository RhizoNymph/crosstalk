//! Two database outages in a row through the gateway; see
//! `common::gateway`. Evidence paths: `crosstalk_e2e::spool_outage_twice::*`.

#[path = "../common/mod.rs"]
mod common;

use crosstalk_memory::support::ManualClock;

use common::gateway::*;
use common::migrated;

/// Two outages in a row, each restarting the gateway: the same answers,
/// every capture once.
#[test]
fn database_cut_twice_drains_to_the_uninterrupted_answers() {
    common::big_stack(body);
}

async fn body() {
    const TEST: &str = "database_cut_twice_drains_to_the_uninterrupted_answers";
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
    let gateway = run.outage(gateway, &scenario.exchanges[1..2], 1).await;
    gateway
        .send(&run.upstream, &scenario.exchanges[2..3], 1)
        .await;
    gateway
        .settled(&run.db, "the third exchange's pipeline")
        .await;
    let gateway = run.outage(gateway, &scenario.exchanges[3..], 1).await;
    run.finish(gateway, &scenario, &expected, "the run with two outages")
        .await;
}
