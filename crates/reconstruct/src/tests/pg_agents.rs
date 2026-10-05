//! `PgAgents` against Postgres: the model test against `crosstalk-memory`'s
//! reference store (same operations, same results, same events, same
//! reads), and the merge log's refusals and records on a real database.

use crosstalk_memory::model::HarnessConfig;
use crosstalk_memory::reconstruct::model::{check_agent_store_with, claim, evidence, label};
use crosstalk_spec::events::BusEvent;
use crosstalk_spec::events::ingest::IngestEvent;
use crosstalk_spec::ids::{AgentId, OperatorId};
use crosstalk_spec::interfaces::l3_reconstruction::agents::{ActivityStore, AgentReads};
use crosstalk_spec::interfaces::l3_reconstruction::lifecycle::{
    AgentLifecycle, AgentOrigin, NewAgent,
};
use crosstalk_spec::interfaces::l3_reconstruction::{
    AgentDirectory, ClaimStore, IdentityResolver, ResolveError,
};
use crosstalk_spec::observed::agent::{AgentState, ClaimSet, MergeAuthor, MergeRequest, MergeVeto};
use crosstalk_spec::support::{Change, NonEmpty, Timestamp};
use crosstalk_testkit::ids::Ids;

use super::pg::{
    CURSOR_KEY, OutboxSink, SeqIds, TestAgents, agents, close, database, pool_on, truncate,
};
use crate::agents::PgAgents;

/// Run the reference harness against `PgAgents` on a fresh database:
/// every case on its own pool, inside the case's runtime, with emptied
/// tables.
fn model_check(test: &str, cases: u32) {
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
    let outcome = check_agent_store_with(HarnessConfig { cases, max_ops: 24 }, |ids, outbox| {
        let url = url.clone();
        async move {
            let pool = pool_on(&url).await;
            truncate(&pool).await;
            match PgAgents::open(pool, OutboxSink(outbox), SeqIds(ids), CURSOR_KEY).await {
                Ok(store) => store,
                Err(error) => panic!("store did not open: {error}"),
            }
        }
    });
    runtime.block_on(close(db));
    if let Err(mismatch) = outcome {
        panic!("{mismatch}");
    }
}

/// Roadmap P4.1: the Postgres agent store agrees with the reference on
/// random operation sequences (results, events, every read).
#[test]
fn pg_agents_agree_with_the_reference_store() {
    model_check("pg_agents_agree_with_the_reference_store", 10);
}

/// `reconstruct.agent-merge.target-not-merged`, checked by the harness on
/// the Postgres store's own records after every step.
#[test]
fn merge_chains_are_flat() {
    model_check("merge_chains_are_flat", 4);
}

/// `reconstruct.agent-state.legal-transitions`, checked by the harness on
/// every state change the Postgres store makes.
#[test]
fn agent_state_changes_follow_lifecycle() {
    model_check("agent_state_changes_follow_lifecycle", 4);
}

/// `reconstruct.agent-merge.record-agreement`, checked by the harness on
/// the Postgres store's agents and merge log.
#[test]
fn merge_log_agrees_with_states() {
    model_check("merge_log_agrees_with_states", 4);
}

/// `reconstruct.directory.canonical-follows-merge`: `canonical` of every
/// id equals the reference's after every step.
#[test]
fn canonical_follows_merge_table() {
    model_check("canonical_follows_merge_table", 4);
}

/// `surface.agent.rows-canonical`: every page of the agents list (filtered
/// and not) equals the reference's, which lists canonical agents only.
#[test]
fn prop_agent_list_matches_merge_table_model() {
    model_check("prop_agent_list_matches_merge_table_model", 4);
}

// ---- integration tests on hand-picked states ------------------------------

fn at(micros: u64) -> Timestamp {
    Timestamp::from_micros(1_000_000 + micros)
}

async fn create(store: &mut TestAgents, id: AgentId, item: u8, origin: AgentOrigin) {
    let created = store
        .create(NewAgent {
            id,
            evidence: NonEmpty::new(evidence(item)),
            parent: None,
            origin,
            label: None,
        })
        .await;
    assert_eq!(created, Ok(()));
}

async fn traffic(store: &mut TestAgents, id: AgentId, item: u8) {
    create(store, id, item, AgentOrigin::Traffic { first_seen: at(1) }).await;
}

fn request(from: AgentId, into: AgentId, by: MergeAuthor) -> MergeRequest {
    match MergeRequest::new(from, into, by) {
        Ok(request) => request,
        Err(error) => panic!("self merge: {error:?}"),
    }
}

async fn state(store: &TestAgents, id: AgentId) -> AgentState {
    let cluster = store
        .cluster(id)
        .await
        .expect("cluster read")
        .expect("stored agent");
    std::iter::once(cluster.agent())
        .chain(cluster.aliases())
        .find(|agent| agent.id == id)
        .map(|agent| agent.state.clone())
        .expect("agent in its cluster")
}

/// `reconstruct.agent-rename.active-only`.
#[tokio::test(flavor = "multi_thread")]
async fn pg_rename_refuses_merged_agent() {
    let Some(db) = database("pg_rename_refuses_merged_agent").await else {
        return;
    };
    let (mut store, recorder) = agents(&db).await;
    let mut ids = Ids::new();
    let (a, b, operator) = (ids.agent(), ids.agent(), ids.operator());
    traffic(&mut store, a, 1).await;
    traffic(&mut store, b, 2).await;
    assert_eq!(
        store.rename(a, label(0), operator).await,
        Ok(Change::Applied)
    );
    store
        .merge(request(a, b, MergeAuthor::Operator(operator)), at(5))
        .await
        .expect("merged");
    recorder.take();
    assert_eq!(
        store.rename(a, label(1), operator).await,
        Err(ResolveError::AgentMerged { agent: a, into: b })
    );
    let cluster = store.cluster(a).await.expect("read").expect("stored");
    assert_eq!(cluster.aliases()[0].label, label(0));
    assert!(recorder.take().is_empty());
    close(db).await;
}

/// `reconstruct.agent-merge.records-restore-data`.
#[tokio::test(flavor = "multi_thread")]
async fn pg_merge_records_prior_and_repointed() {
    let Some(db) = database("pg_merge_records_prior_and_repointed").await else {
        return;
    };
    let (mut store, _) = agents(&db).await;
    let mut ids = Ids::new();
    let (a, b, c) = (ids.agent(), ids.agent(), ids.agent());
    traffic(&mut store, a, 1).await;
    traffic(&mut store, b, 2).await;
    create(&mut store, c, 3, AgentOrigin::Config { at: at(0) }).await;
    // a into b, then b into c: the second merge repoints a.
    store
        .merge(request(a, b, MergeAuthor::Resolver), at(5))
        .await
        .expect("first merge");
    let before = state(&store, b).await;
    let record = store
        .merge(request(b, c, MergeAuthor::Resolver), at(6))
        .await
        .expect("second merge");
    assert_eq!(record.repointed(), &[a]);
    match state(&store, b).await {
        AgentState::Merged(merged) => {
            assert_eq!(merged.merge, record.id());
            assert_eq!(merged.into, c);
            assert_eq!(AgentState::from(merged.prior), before);
        }
        other => panic!("not merged: {other:?}"),
    }
    assert_eq!(store.canonical(a), c);
    close(db).await;
}

/// `reconstruct.agent-merge.canonical-only`.
#[tokio::test(flavor = "multi_thread")]
async fn pg_merge_refuses_merged_agents() {
    let Some(db) = database("pg_merge_refuses_merged_agents").await else {
        return;
    };
    let (mut store, recorder) = agents(&db).await;
    let mut ids = Ids::new();
    let (a, b, c) = (ids.agent(), ids.agent(), ids.agent());
    for (id, item) in [(a, 1), (b, 2), (c, 3)] {
        traffic(&mut store, id, item).await;
    }
    store
        .merge(request(a, b, MergeAuthor::Resolver), at(5))
        .await
        .expect("merged");
    recorder.take();
    assert_eq!(
        store
            .merge(request(a, c, MergeAuthor::Resolver), at(6))
            .await,
        Err(ResolveError::AgentMerged { agent: a, into: b })
    );
    assert_eq!(
        store
            .merge(request(c, a, MergeAuthor::Resolver), at(6))
            .await,
        Err(ResolveError::AgentMerged { agent: a, into: b })
    );
    assert!(recorder.take().is_empty());
    assert_eq!(store.canonical(c), c);
    close(db).await;
}

/// `reconstruct.agent-merge.into-self-refused`.
#[tokio::test(flavor = "multi_thread")]
async fn pg_merge_refuses_one_cluster() {
    let Some(db) = database("pg_merge_refuses_one_cluster").await else {
        return;
    };
    let (mut store, recorder) = agents(&db).await;
    let mut ids = Ids::new();
    let (a, b, c, operator) = (ids.agent(), ids.agent(), ids.agent(), ids.operator());
    for (id, item) in [(a, 1), (b, 2), (c, 3)] {
        traffic(&mut store, id, item).await;
    }
    store
        .merge(request(a, c, MergeAuthor::Resolver), at(5))
        .await
        .expect("a into c");
    store
        .merge(request(b, c, MergeAuthor::Resolver), at(6))
        .await
        .expect("b into c");
    recorder.take();
    for (from, into) in [(a, b), (a, c), (c, a)] {
        assert_eq!(
            store
                .merge(request(from, into, MergeAuthor::Operator(operator)), at(7))
                .await,
            Err(ResolveError::MergeIntoSelf {
                from,
                into,
                canonical: c
            })
        );
    }
    assert!(recorder.take().is_empty());
    close(db).await;
}

/// `reconstruct.agent-unmerge.restores-prior-state`.
#[tokio::test(flavor = "multi_thread")]
async fn pg_unmerge_restores_prior_state() {
    let Some(db) = database("pg_unmerge_restores_prior_state").await else {
        return;
    };
    let (mut store, _) = agents(&db).await;
    let mut ids = Ids::new();
    let (a, b, operator) = (ids.agent(), ids.agent(), ids.operator());
    create(&mut store, a, 1, AgentOrigin::Config { at: at(0) }).await;
    traffic(&mut store, b, 2).await;
    let record = store
        .merge(request(a, b, MergeAuthor::Operator(operator)), at(5))
        .await
        .expect("merged");
    let reversal = store
        .unmerge(record.id(), operator, at(9))
        .await
        .expect("unmerged");
    assert!(reversal.restored.is_empty());
    assert_eq!(state(&store, a).await, AgentState::Registered { at: at(0) });
    assert_eq!(store.canonical(a), a);
    close(db).await;
}

/// `reconstruct.merge-record.revert-once`.
#[tokio::test(flavor = "multi_thread")]
async fn pg_unmerge_twice_conflicts() {
    let Some(db) = database("pg_unmerge_twice_conflicts").await else {
        return;
    };
    let (mut store, recorder) = agents(&db).await;
    let mut ids = Ids::new();
    let (a, b, operator) = (ids.agent(), ids.agent(), ids.operator());
    traffic(&mut store, a, 1).await;
    traffic(&mut store, b, 2).await;
    let record = store
        .merge(request(a, b, MergeAuthor::Resolver), at(5))
        .await
        .expect("merged");
    let first = store
        .unmerge(record.id(), operator, at(6))
        .await
        .expect("first unmerge");
    recorder.take();
    assert_eq!(
        store.unmerge(record.id(), operator, at(7)).await,
        Err(ResolveError::MergeAlreadyReverted(record.id()))
    );
    assert!(recorder.take().is_empty());
    let cluster = store.cluster(a).await.expect("read").expect("stored");
    let stored = cluster
        .merges()
        .iter()
        .find(|stored| stored.id() == record.id())
        .expect("record kept");
    assert_eq!(stored.reverted(), Some(&first));
    close(db).await;
}

/// `reconstruct.merge-veto.blocks-resolver`.
#[tokio::test(flavor = "multi_thread")]
async fn pg_veto_blocks_resolver_merge() {
    let Some(db) = database("pg_veto_blocks_resolver_merge").await else {
        return;
    };
    let (mut store, recorder) = agents(&db).await;
    let mut ids = Ids::new();
    let (a, b, c, operator) = (ids.agent(), ids.agent(), ids.agent(), ids.operator());
    for (id, item) in [(a, 1), (b, 2), (c, 3)] {
        traffic(&mut store, id, item).await;
    }
    let record = store
        .merge(request(a, b, MergeAuthor::Resolver), at(5))
        .await
        .expect("merged");
    let reversal = store
        .unmerge(record.id(), operator, at(6))
        .await
        .expect("unmerged");
    // c joins b's cluster; a veto between a and b still separates a from
    // the whole cluster.
    store
        .merge(request(c, b, MergeAuthor::Resolver), at(7))
        .await
        .expect("c into b");
    recorder.take();
    let veto = MergeVeto::of(
        &store
            .cluster(a)
            .await
            .expect("read")
            .expect("stored")
            .merges()
            .iter()
            .find(|stored| stored.id() == record.id())
            .cloned()
            .expect("record"),
        &reversal,
    );
    assert_eq!(
        store
            .merge(request(a, c, MergeAuthor::Resolver), at(8))
            .await,
        Err(ResolveError::AgentMerged { agent: c, into: b })
    );
    assert_eq!(
        store
            .merge(request(a, b, MergeAuthor::Resolver), at(8))
            .await,
        Err(ResolveError::Vetoed(veto))
    );
    assert!(recorder.take().is_empty());
    assert_eq!(store.canonical(a), a);
    close(db).await;
}

/// `reconstruct.merge-veto.operator-clears`.
#[tokio::test(flavor = "multi_thread")]
async fn pg_operator_merge_clears_veto() {
    let Some(db) = database("pg_operator_merge_clears_veto").await else {
        return;
    };
    let (mut store, _) = agents(&db).await;
    let mut ids = Ids::new();
    let (a, b, operator) = (ids.agent(), ids.agent(), ids.operator());
    traffic(&mut store, a, 1).await;
    traffic(&mut store, b, 2).await;
    let record = store
        .merge(request(a, b, MergeAuthor::Resolver), at(5))
        .await
        .expect("merged");
    store
        .unmerge(record.id(), operator, at(6))
        .await
        .expect("unmerged");
    assert!(matches!(
        store
            .merge(request(a, b, MergeAuthor::Resolver), at(7))
            .await,
        Err(ResolveError::Vetoed(_))
    ));
    store
        .merge(request(a, b, MergeAuthor::Operator(operator)), at(8))
        .await
        .expect("operator merge goes ahead");
    let cluster = store.cluster(b).await.expect("read").expect("stored");
    assert!(cluster.vetoes().is_empty(), "{:?}", cluster.vetoes());
    let (rows,): (i64,) = sqlx::query_as("SELECT count(*) FROM reconstruct.vetoes")
        .fetch_one(db.pool())
        .await
        .expect("count");
    assert_eq!(rows, 0);
    close(db).await;
}

/// `reconstruct.merge-veto.recorded-on-unmerge`.
#[tokio::test(flavor = "multi_thread")]
async fn pg_unmerge_records_veto() {
    let Some(db) = database("pg_unmerge_records_veto").await else {
        return;
    };
    let (mut store, _) = agents(&db).await;
    let mut ids = Ids::new();
    let (a, b, operator) = (ids.agent(), ids.agent(), ids.operator());
    traffic(&mut store, a, 1).await;
    traffic(&mut store, b, 2).await;
    let record = store
        .merge(request(a, b, MergeAuthor::Resolver), at(5))
        .await
        .expect("merged");
    store
        .unmerge(record.id(), operator, at(6))
        .await
        .expect("unmerged");
    let cluster = store.cluster(a).await.expect("read").expect("stored");
    let veto = cluster.vetoes().first().copied().expect("a veto");
    assert_eq!((veto.a(), veto.b()), (a.min(b), a.max(b)));
    assert_eq!(veto.by(), operator);
    assert_eq!(veto.at(), at(6));
    close(db).await;
}

/// `reconstruct.claims.union-over-aliases`.
#[tokio::test(flavor = "multi_thread")]
async fn pg_claims_union_over_aliases() {
    let Some(db) = database("pg_claims_union_over_aliases").await else {
        return;
    };
    let (mut store, _) = agents(&db).await;
    let mut ids = Ids::new();
    let (a, b, operator) = (ids.agent(), ids.agent(), ids.operator());
    traffic(&mut store, a, 1).await;
    traffic(&mut store, b, 2).await;
    ClaimStore::record(&mut store, a, &claim(0), at(3))
        .await
        .expect("claim a");
    ClaimStore::record(&mut store, b, &claim(1), at(4))
        .await
        .expect("claim b");
    ClaimStore::record(&mut store, a, &claim(0), at(2))
        .await
        .expect("older repeat");
    let own_a = store.claims(a).await.expect("claims a");
    let own_b = store.claims(b).await.expect("claims b");
    let record = store
        .merge(request(a, b, MergeAuthor::Resolver), at(5))
        .await
        .expect("merged");
    let union = ClaimSet::union([&own_a, &own_b]);
    assert_eq!(store.claims(a).await, Ok(union.clone()));
    assert_eq!(store.claims(b).await, Ok(union));
    store
        .unmerge(record.id(), operator, at(6))
        .await
        .expect("unmerged");
    assert_eq!(store.claims(a).await, Ok(own_a));
    assert_eq!(store.claims(b).await, Ok(own_b));
    close(db).await;
}

/// `reconstruct.agent-create.traffic-has-last-seen`: an agent created from
/// traffic, or a registered agent's first traffic, is seen at least then,
/// recorded by the same write.
#[tokio::test(flavor = "multi_thread")]
async fn created_from_traffic_is_seen() {
    let Some(db) = database("created_from_traffic_is_seen").await else {
        return;
    };
    let (mut store, _) = agents(&db).await;
    let mut ids = Ids::new();
    let (a, b) = (ids.agent(), ids.agent());
    create(&mut store, a, 1, AgentOrigin::Traffic { first_seen: at(4) }).await;
    assert_eq!(store.last_seen(a).await, Ok(Some(at(4))));
    create(&mut store, b, 2, AgentOrigin::Config { at: at(0) }).await;
    assert_eq!(store.last_seen(b).await, Ok(None));
    store
        .advance(
            b,
            crosstalk_spec::interfaces::l3_reconstruction::lifecycle::Advance::FirstTraffic {
                at: at(7),
            },
        )
        .await
        .expect("first traffic");
    assert_eq!(store.last_seen(b).await, Ok(Some(at(7))));
    assert!(matches!(
        state(&store, b).await,
        AgentState::Provisional { first_seen } if first_seen == at(7)
    ));
    close(db).await;
}

/// The merge table survives a restart: a store opened on the same
/// database loads it into its directory.
#[tokio::test(flavor = "multi_thread")]
async fn reopened_store_loads_the_merge_table() {
    let Some(db) = database("reopened_store_loads_the_merge_table").await else {
        return;
    };
    let (mut store, _) = agents(&db).await;
    let mut ids = Ids::new();
    let (a, b) = (ids.agent(), ids.agent());
    traffic(&mut store, a, 1).await;
    traffic(&mut store, b, 2).await;
    store
        .merge(request(a, b, MergeAuthor::Resolver), at(5))
        .await
        .expect("merged");
    let (reopened, _) = agents(&db).await;
    assert_eq!(reopened.canonical(a), b);
    assert_eq!(reopened.members(a), {
        let mut members = vec![a, b];
        members.sort_unstable();
        members
    });
    close(db).await;
}

/// Events left in the outbox (a sink that failed) are published by
/// `flush_outbox`.
#[tokio::test(flavor = "multi_thread")]
async fn outbox_leftovers_are_flushed() {
    let Some(db) = database("outbox_leftovers_are_flushed").await else {
        return;
    };
    let (store, recorder) = agents(&db).await;
    let event = BusEvent::Ingest(IngestEvent::AgentRenamed {
        agent: Ids::new().agent(),
        label: label(0),
        by: OperatorId::from_ulid(1),
    });
    sqlx::query("INSERT INTO reconstruct.outbox (event) VALUES ($1)")
        .bind(serde_json::to_string(&event).expect("encodes"))
        .execute(db.pool())
        .await
        .expect("inserted");
    assert_eq!(store.flush_outbox().await.expect("flushed"), 1);
    assert_eq!(recorder.take(), vec![event]);
    assert_eq!(store.flush_outbox().await.expect("flushed"), 0);
    close(db).await;
}
