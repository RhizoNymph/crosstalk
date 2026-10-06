//! `ConversationReads` on `PgConversations`: the scenarios of
//! `conversation_reads`, and every read agreeing with the in-memory store
//! on generated harness scripts. Gated on `TEST_DATABASE_URL`.

use crosstalk_spec::batch::IdBatch;
use crosstalk_spec::interfaces::l3_reconstruction::Threader;
use crosstalk_spec::interfaces::l3_reconstruction::conversations::{
    ConversationQuery, ConversationReads, ReplayFilter,
};
use crosstalk_spec::paging::PageSize;
use proptest::test_runner::{Config, TestCaseError, TestRunner};

use super::conversation_reads::{
    all_turns, carried_over_scenario, filters_scenario, links_scenario, list_order_scenario,
    stored, thread_scenario, traverse, turns_scenario, window, windows_scenario,
};
use super::pg::{close, database, pool_on, truncate};
use super::support::Scene;
use super::thread_props::script::{Harness, Mix, script};
use crate::thread::{ConversationStore, MemoryConversations, PgConversations};

/// The scenario threaded on Postgres, every read checked as on memory.
#[tokio::test(flavor = "multi_thread")]
async fn pg_turn_entries_are_the_exchanges_transcript_entries() {
    let Some(db) = database("pg_turn_entries_are_the_exchanges_transcript_entries").await else {
        return;
    };
    let store = PgConversations::new(db.pool().clone());
    let mut scene = Scene::new();
    let mut threader = scene.threader(store.clone());
    let threaded = thread_scenario(&mut scene, &mut threader).await;
    turns_scenario(&store, &threaded).await;
    windows_scenario(&store, &threaded).await;
    links_scenario(&store, &threaded).await;
    list_order_scenario(&store).await;
    close(db).await;
}

/// INV-1008 on Postgres.
#[tokio::test(flavor = "multi_thread")]
async fn pg_carried_over_survives_a_round_trip() {
    let Some(db) = database("pg_carried_over_survives_a_round_trip").await else {
        return;
    };
    let store = PgConversations::new(db.pool().clone());
    let mut scene = Scene::new();
    let mut threader = scene.threader(store.clone());
    let threaded = thread_scenario(&mut scene, &mut threader).await;
    carried_over_scenario(&store, &threaded).await;
    close(db).await;
}

/// INV-1002 and INV-1016 on Postgres.
#[tokio::test(flavor = "multi_thread")]
async fn pg_list_keeps_exactly_what_the_query_admits() {
    let Some(db) = database("pg_list_keeps_exactly_what_the_query_admits").await else {
        return;
    };
    let store = PgConversations::new(db.pool().clone());
    let mut scene = Scene::new();
    let mut threader = scene.threader(store.clone());
    filters_scenario(&mut scene, &mut threader, &store).await;
    close(db).await;
}

/// INV-1004 on Postgres: a redelivered exchange adds no turn.
#[tokio::test(flavor = "multi_thread")]
async fn pg_rethreading_adds_no_turn() {
    let Some(db) = database("pg_rethreading_adds_no_turn").await else {
        return;
    };
    let store = PgConversations::new(db.pool().clone());
    let mut scene = Scene::new();
    let mut threader = scene.threader(store.clone());
    let agent = scene.ids.agent();
    let (u, a) = (scene.user("task").await, scene.assistant("one").await);
    let first = scene.exchange(vec![u], a);
    let outcome = threader
        .thread(&first, agent)
        .await
        .unwrap_or_else(|error| panic!("threaded: {error:?}"));
    let root = crate::thread::store::outcome_conversation(&outcome);
    let before = all_turns(&store, root).await;
    threader
        .thread(&first, agent)
        .await
        .unwrap_or_else(|error| panic!("again: {error:?}"));
    assert_eq!(all_turns(&store, root).await, before);
    assert_eq!(stored(&store, root).await.turns, 1);
    close(db).await;
}

/// INV-1001, INV-1003, INV-1010, INV-1011 and INV-1022: on generated
/// scripts, every `ConversationReads` read on Postgres equals the
/// in-memory store's after the same threading.
#[test]
fn pg_conversation_reads_agree_with_memory() {
    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(error) => panic!("no runtime: {error}"),
    };
    let Some(db) = runtime.block_on(database("pg_conversation_reads_agree_with_memory")) else {
        return;
    };
    let url = db.url().clone();
    let mut runner = TestRunner::new(Config {
        cases: 16,
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
            let mut exchanges = Vec::new();
            for op in &ops {
                let step = on_pg.step(op).await;
                let same = on_memory.step(op).await;
                let fail = |error| TestCaseError::fail(format!("{error:?}"));
                pg_threader
                    .thread(&step.exchange, step.agent)
                    .await
                    .map_err(fail)?;
                memory_threader
                    .thread(&same.exchange, same.agent)
                    .await
                    .map_err(fail)?;
                exchanges.push(step.exchange.meta.id);
            }
            let differ = |what: &str| Err(TestCaseError::fail(format!("{what} differs")));
            let every = ConversationQuery::default();
            if traverse(&pg, &every, 2).await != traverse(&memory, &every, 2).await {
                return differ("the list");
            }
            let live = ConversationQuery {
                replay: ReplayFilter::Exclude,
                ..ConversationQuery::default()
            };
            if traverse(&pg, &live, 3).await != traverse(&memory, &live, 3).await {
                return differ("the live list");
            }
            let ids = memory
                .conversations()
                .await
                .map_err(|error| TestCaseError::fail(format!("{error:?}")))?;
            for id in ids {
                if ConversationReads::conversation(&pg, id).await
                    != ConversationReads::conversation(&memory, id).await
                {
                    return differ("a conversation");
                }
                if pg.successors(id).await != memory.successors(id).await {
                    return differ("successors");
                }
                for (from, size) in [(0, PageSize::MAX), (1, 2), (3, 1)] {
                    let window = window(from, size);
                    if pg.turns(id, &window).await != memory.turns(id, &window).await {
                        return differ("a turn window");
                    }
                }
                for prefix in 0..8 {
                    if pg.branch_turn(id, prefix).await != memory.branch_turn(id, prefix).await {
                        return differ("a branch turn");
                    }
                }
            }
            let batch = IdBatch::new(exchanges)
                .map_err(|error| TestCaseError::fail(format!("{error:?}")))?;
            if pg.locate(&batch).await != memory.locate(&batch).await {
                return differ("locate");
            }
            Ok(())
        })
    });
    runtime.block_on(close(db));
    if let Err(error) = result {
        panic!("{error}");
    }
}
