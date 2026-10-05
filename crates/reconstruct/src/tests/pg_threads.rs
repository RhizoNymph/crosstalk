//! `PgConversations` against `MemoryConversations`: the same generated
//! harness scripts give the same outcomes, the same stored conversations
//! and the same transcripts, step by step.

use crosstalk_spec::interfaces::l3_reconstruction::Threader;
use proptest::test_runner::{Config, TestCaseError, TestRunner};

use super::pg::{close, database, pool_on, truncate};
use super::thread_props::oracle::snapshot;
use super::thread_props::script::{Harness, Mix, script};
use crate::thread::{ConversationStore, MemoryConversations, PgConversations};

fn compare(test: &str, mix: Mix, cases: u32, max: usize) {
    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(error) => panic!("no runtime: {error}"),
    };
    let Some(db) = runtime.block_on(database(test)) else {
        return;
    };
    let url = db.url().clone();
    let mut runner = TestRunner::new(Config {
        cases,
        failure_persistence: None,
        ..Config::default()
    });
    let result = runner.run(&script(mix, max), |ops| {
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
            for (index, op) in ops.iter().enumerate() {
                let step = on_pg.step(op).await;
                let same = on_memory.step(op).await;
                let pg_outcome = pg_threader
                    .thread(&step.exchange, step.agent)
                    .await
                    .map_err(|error| TestCaseError::fail(format!("step {index}: {error:?}")))?;
                let memory_outcome = memory_threader
                    .thread(&same.exchange, same.agent)
                    .await
                    .map_err(|error| TestCaseError::fail(format!("step {index}: {error:?}")))?;
                if pg_outcome != memory_outcome {
                    return Err(TestCaseError::fail(format!(
                        "step {index}:\n  postgres: {pg_outcome:?}\n  memory:   {memory_outcome:?}"
                    )));
                }
            }
            let (pg_snapshot, memory_snapshot) = (snapshot(&pg).await?, snapshot(&memory).await?);
            if pg_snapshot != memory_snapshot {
                return Err(TestCaseError::fail("stored conversations differ"));
            }
            for id in pg_snapshot.keys() {
                let pg_transcript = pg
                    .transcript(*id)
                    .await
                    .map_err(|error| TestCaseError::fail(format!("{error:?}")))?;
                let memory_transcript = memory
                    .transcript(*id)
                    .await
                    .map_err(|error| TestCaseError::fail(format!("{error:?}")))?;
                if pg_transcript != memory_transcript {
                    return Err(TestCaseError::fail(format!("transcript of {id:?} differs")));
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

/// The Postgres conversation store agrees with the in-memory one.
#[test]
fn pg_conversations_agree_with_memory() {
    compare("pg_conversations_agree_with_memory", Mix::GENERAL, 6, 20);
}

/// The same, on fork- and compaction-heavy scripts.
#[test]
fn pg_conversations_agree_on_forks_and_compactions() {
    compare(
        "pg_conversations_agree_on_forks_and_compactions",
        Mix::FORKS,
        4,
        16,
    );
    compare(
        "pg_conversations_agree_on_compactions",
        Mix::COMPACTIONS,
        4,
        16,
    );
}
