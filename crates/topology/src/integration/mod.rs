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

mod scenarios;

use crosstalk_memory::model::HarnessConfig;
use crosstalk_memory::model::topology::check_edge_store;

use crate::tests::support::{PgSubject, database, small_pool};

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
