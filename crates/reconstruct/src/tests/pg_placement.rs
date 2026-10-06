//! `ExchangePlacements` on `PgConversations` against `MemoryConversations`:
//! the same generated harness scripts, with every third exchange left
//! unthreaded, place every exchange the same way on both stores
//! (`reconstruct.placement.as-threaded`).

use crosstalk_spec::interfaces::l3_reconstruction::{ExchangePlacements, Placement, Threader};
use proptest::test_runner::{Config, TestCaseError, TestRunner};

use super::pg::{close, database, pool_on, truncate};
use super::thread_props::script::{Harness, Mix, script};
use crate::thread::{MemoryConversations, PgConversations};

#[test]
fn pg_placements_agree_with_memory() {
    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(error) => panic!("no runtime: {error}"),
    };
    let Some(db) = runtime.block_on(database("pg_placements_agree_with_memory")) else {
        return;
    };
    let url = db.url().clone();
    let mut runner = TestRunner::new(Config {
        cases: 8,
        failure_persistence: None,
        ..Config::default()
    });
    let result = runner.run(&script(Mix::GENERAL, 20), |ops| {
        let case = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|error| TestCaseError::fail(format!("no runtime: {error}")))?;
        case.block_on(async {
            let pool = pool_on(&url).await;
            truncate(&pool).await;
            let pg = PgConversations::new(pool);
            let memory = MemoryConversations::new();
            let mut on_pg = Harness::new();
            let mut on_memory = Harness::new();
            let mut pg_threader = on_pg.scene.threader_in(pg.clone(), on_pg.clusters());
            let mut memory_threader = on_memory
                .scene
                .threader_in(memory.clone(), on_memory.clusters());
            let fail = |error| TestCaseError::fail(format!("{error:?}"));
            for (index, op) in ops.iter().enumerate() {
                let step = on_pg.step(op).await;
                let same = on_memory.step(op).await;
                let exchange = step.exchange.meta.id;
                if index % 3 != 2 {
                    let outcome = pg_threader
                        .thread(&step.exchange, step.agent)
                        .await
                        .map_err(fail)?;
                    memory_threader
                        .thread(&same.exchange, same.agent)
                        .await
                        .map_err(fail)?;
                    let expected = Some(Placement::of(&outcome));
                    if pg.placement(exchange).await.map_err(fail)? != expected {
                        return Err(TestCaseError::fail(format!(
                            "step {index}: not placed as threaded"
                        )));
                    }
                }
                let on_pg = pg.placement(exchange).await.map_err(fail)?;
                let on_memory = memory.placement(exchange).await.map_err(fail)?;
                if on_pg != on_memory {
                    return Err(TestCaseError::fail(format!(
                        "step {index}:\n  postgres: {on_pg:?}\n  memory:   {on_memory:?}"
                    )));
                }
                if index % 3 == 2 && on_pg.is_some() {
                    return Err(TestCaseError::fail(format!(
                        "step {index}: an unthreaded exchange is placed"
                    )));
                }
            }
            Ok(())
        })
    });
    runtime.block_on(close(db));
    if let Err(error) = result {
        panic!("{error}");
    }
}
