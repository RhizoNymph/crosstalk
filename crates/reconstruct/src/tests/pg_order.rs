//! `reconstruct.thread.order-independent` on `PgConversations`: fixed
//! delivery orders (the node0 forensic case, the third exchange after the
//! fourth, among them) store on Postgres what the in-memory store stores
//! for the same exchanges delivered in order.

use crosstalk_spec::ids::AgentId;
use crosstalk_spec::interfaces::l3_reconstruction::Threader;
use crosstalk_spec::interfaces::l3_reconstruction::conversations::ConversationReads;
use crosstalk_spec::observed::exchange::Exchange;
use crosstalk_testkit::build::ExchangeBuilder;
use proptest::test_runner::{Config, TestCaseError, TestRunner};
use sqlx::PgPool;

use super::order_props::{permutation, relay};
use super::order_view::{L3View, view};
use super::pg::{close, database, pool_on, truncate};
use super::support::{Clusters, Scene};
use super::thread_props::script::{Harness, Mix, script};
use crate::thread::{ConversationStore, MemoryConversations, PgConversations};

fn runtime() -> tokio::runtime::Runtime {
    match tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(error) => panic!("no runtime: {error}"),
    }
}

/// `exchanges` threaded in `order` on `store` (emptied first when it is
/// on Postgres), then read back.
async fn delivered<S>(
    scene: &Scene,
    clusters: Clusters,
    store: S,
    exchanges: &[(Exchange, AgentId)],
    order: &[usize],
) -> Result<L3View, String>
where
    S: ConversationStore + ConversationReads + Clone,
{
    let mut threader = scene.threader_in(store.clone(), clusters);
    for index in order {
        let (exchange, agent) = &exchanges[*index];
        threader
            .thread(exchange, *agent)
            .await
            .map_err(|error| format!("exchange {index}: {error:?}"))?;
    }
    view(&store, &mut threader, exchanges)
        .await
        .map_err(|error| format!("{error:?}"))
}

/// Each of `orders` on Postgres stores what in-order delivery stores in
/// memory.
async fn agree(
    pool: &PgPool,
    scene: &Scene,
    clusters: &Clusters,
    exchanges: &[(Exchange, AgentId)],
    orders: &[Vec<usize>],
) -> Result<(), String> {
    let in_order: Vec<usize> = (0..exchanges.len()).collect();
    let expected = delivered(
        scene,
        clusters.clone(),
        MemoryConversations::new(),
        exchanges,
        &in_order,
    )
    .await?;
    for order in orders {
        truncate(pool).await;
        let got = delivered(
            scene,
            clusters.clone(),
            PgConversations::new(pool.clone()),
            exchanges,
            order,
        )
        .await?;
        if got != expected {
            return Err(format!(
                "delivered in {order:?} on Postgres:\n  got      {got:#?}\n  expected {expected:#?}"
            ));
        }
    }
    Ok(())
}

/// The forensic case: B's first exchange redelivered after its second,
/// and the other orders of the relay's four exchanges that move a
/// session's turns around.
#[test]
fn pg_third_exchange_after_the_fourth_stores_the_in_order_conversations() {
    let runtime = runtime();
    let Some(db) = runtime.block_on(database(
        "pg_third_exchange_after_the_fourth_stores_the_in_order_conversations",
    )) else {
        return;
    };
    let result = runtime.block_on(async {
        let mut scene = Scene::new();
        let a = ExchangeBuilder::new(&mut scene.ids).build().meta.client;
        let b = ExchangeBuilder::new(&mut scene.ids).build().meta.client;
        let agents = [scene.ids.agent(), scene.ids.agent()];
        let exchanges: Vec<(Exchange, AgentId)> = relay(&mut scene, &a, &b)
            .await
            .into_iter()
            .zip([agents[0], agents[0], agents[1], agents[1]])
            .collect();
        let orders = vec![
            vec![0, 1, 2, 3],
            vec![0, 1, 3, 2],
            vec![1, 0, 3, 2],
            vec![3, 2, 1, 0],
            vec![2, 3, 0, 1],
        ];
        agree(db.pool(), &scene, &Clusters::default(), &exchanges, &orders).await
    });
    runtime.block_on(close(db));
    if let Err(error) = result {
        panic!("{error}");
    }
}

/// Generated scripts, each delivered reversed and in two seeded orders.
#[test]
fn pg_out_of_order_scripts_store_the_in_order_conversations() {
    let runtime = runtime();
    let Some(db) = runtime.block_on(database(
        "pg_out_of_order_scripts_store_the_in_order_conversations",
    )) else {
        return;
    };
    let url = db.url().clone();
    let mut runner = TestRunner::new(Config {
        cases: 6,
        failure_persistence: None,
        ..Config::default()
    });
    let result = runner.run(&script(Mix::GENERAL, 8), |ops| {
        let case = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|error| TestCaseError::fail(format!("no runtime: {error}")))?;
        case.block_on(async {
            let pool = pool_on(&url).await;
            let mut harness = Harness::new();
            let mut exchanges = Vec::new();
            for op in &ops {
                let step = harness.step(op).await;
                exchanges.push((step.exchange, step.agent));
            }
            let len = exchanges.len();
            let orders = vec![
                (0..len).rev().collect(),
                permutation(len, 0x5EED),
                permutation(len, 0xC0FFEE),
            ];
            agree(
                &pool,
                &harness.scene,
                &harness.clusters(),
                &exchanges,
                &orders,
            )
            .await
            .map_err(TestCaseError::fail)
        })
    });
    runtime.block_on(close(db));
    if let Err(error) = result {
        panic!("{error}");
    }
}
