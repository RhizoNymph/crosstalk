//! Integration tests against a real Postgres (`TEST_DATABASE_URL`; each
//! test passes with a skip line without it).
//!
//! `pg_matches_reference` is the model test: crosstalk-memory's
//! `check_edge_store` drives this store and the reference edge store with
//! the same random operations (applies, re-fits, activations, drops,
//! verdicts, accesses, merges, supersessions, watermark advances) and
//! compares every read, and checks every graph against the harness's own
//! fold of `topology.graph.matches-fold-model` and every series total
//! against the graph's. The other tests pin one behaviour each.

mod consumer;
mod outbox;
mod scenarios;

use std::sync::Arc;

use crosstalk_memory::model::HarnessConfig;
use crosstalk_memory::model::topology::check_edge_store;

use crosstalk_memory::model::build::ts;
use crosstalk_memory::support::ManualClock;
use crosstalk_spec::ids::SeededRandom;

use crate::outbox::OutboxIds;
use crate::tests::support::{PgSubject, database, small_pool};

/// Outbox stamps from a fixed seed, at a clock reading `1_000 * seed` µs
/// (so relays built with larger seeds stamp later).
pub(crate) fn outbox_ids(seed: u64) -> OutboxIds {
    let clock = ManualClock::at(ts(1_000_000 + 1_000 * seed));
    OutboxIds::new(Arc::new(clock), SeededRandom::new(seed))
}

/// Run the edge store harness against this store, each case on its own
/// runtime and pool over one test database, emptied between cases.
fn model_test(test: &str, harness: HarnessConfig) {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("a runtime");
    let Some(db) = runtime.block_on(database(test)) else {
        return;
    };
    let options = db.url().connect_options().clone();
    let result = check_edge_store(harness, |config, world| {
        let options = options.clone();
        async move {
            let pool = small_pool(options, 2).await;
            PgSubject::new(pool, config, world).await
        }
    });
    runtime
        .block_on(db.close())
        .expect("drop the test database");
    if let Err(mismatch) = result {
        panic!("{mismatch}");
    }
}

/// `topology.graph.matches-fold-model`, `topology.series.matches-fold-model`
/// and the rest of the edge store's reads, against the reference.
#[test]
fn pg_matches_reference() {
    model_test(
        "pg_matches_reference",
        HarnessConfig {
            cases: 32,
            max_ops: 40,
        },
    );
}

/// topology.outbox.stable-envelope-id: the relay stopped at each crash
/// point (after the stamp, after some publishes, after every publish and
/// before the delete; dropped or failed), then a new relay: each staged
/// event is in the log once, under the id stamped on its row before its
/// first publish ([`outbox`]).
#[tokio::test(flavor = "multi_thread")]
async fn outbox_relay_publishes_each_event_once_under_its_stamped_id() {
    outbox::every_crash_point_publishes_each_event_once().await;
}
