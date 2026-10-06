//! A restart over Postgres loses nothing L4 decided: a new engine over the
//! same database replays every delta's envelopes, reads each recorded
//! exchange's start, and scans an exchange recorded before the restart;
//! and retention ages out token observations as the memory model does.

use crosstalk_spec::events::ingest::ConversationDelta;
use crosstalk_spec::interfaces::l4_provenance::FingerprintIndex;
use crosstalk_testkit::build::exchange::ExchangeBuilder;
use crosstalk_testkit::build::message::{message, tool_result};

use super::{PgWorld, database, lazy_pool, pg_world_on};
use crate::engine::{Processed, Provenance};
use crate::fingerprint::token;
use crate::index::PgFingerprintIndex;
use crate::semantic::DisabledSemanticMatcher;
use crate::store::{PgProvenanceStore, ProvenanceStore};
use crate::tests::fixtures::{RETENTION, Turn, World, at, config, sentence};
use crate::tests::scenarios::originate;
use crate::tests::started::started_at_is_the_recorded_start;

/// `ProvenanceStore::started_at` on Postgres.
#[tokio::test(flavor = "multi_thread")]
async fn pg_started_at_is_the_recorded_start() {
    let Some(db) = database("pg_started_at_is_the_recorded_start").await else {
        return;
    };
    started_at_is_the_recorded_start(&mut PgProvenanceStore::new(db.pool().clone())).await;
    db.close().await.expect("close");
}

/// The process stops after A's text was read and C's exchange was
/// captured but before its delta was scanned. A new engine on new pools
/// over the same database republishes the same envelopes for every
/// delivered delta, reads every start (C's included), and scans C's
/// delta, matching A's span as it would have before the restart.
#[tokio::test(flavor = "multi_thread")]
async fn pg_engine_restart_replays_and_reads_starts() {
    let Some(db) = database("pg_engine_restart_replays_and_reads_starts").await else {
        return;
    };
    let pool = lazy_pool(&db);
    let mut before: PgWorld = pg_world_on(pool.clone(), config());
    let (a, b, c) = (before.agent(), before.agent(), before.agent());
    let text = sentence("restart");
    originate(&mut before, a, &text, 1).await;
    before
        .run(Turn::new(b, at(2)).input(tool_result("call_1", &text)))
        .await;
    let read = message(tool_result("call_2", &format!("again: {text}")));
    before.messages.put(read.clone());
    let captured = ExchangeBuilder::new(&mut before.ids)
        .started_at(at(3))
        .request(vec![read.hash])
        .build();
    before
        .engine
        .record_exchange(&captured)
        .await
        .expect("recorded");
    let pending = ConversationDelta {
        exchange: captured.meta.id,
        agent: c,
        conversation: before.ids.conversation(),
        new_inputs: vec![read.hash],
        new_system: None,
        output: None,
    };
    let ran = before.ran.clone();
    let messages = before.messages.clone();
    drop(before);
    pool.close().await;

    let pool = lazy_pool(&db);
    let mut after = Provenance::new(
        &config(),
        PgFingerprintIndex::new(pool.clone(), config().index().clone()),
        PgProvenanceStore::new(pool.clone()),
        DisabledSemanticMatcher,
        messages,
    );
    for turn in &ran {
        let Processed::Scanned { events } = &turn.processed else {
            panic!("first delivery scanned: {:?}", turn.processed);
        };
        let replayed = after.process(&turn.delta).await.expect("replayed");
        assert_eq!(
            replayed,
            Processed::Replayed {
                events: events.clone()
            }
        );
        let started = after.started_at(turn.delta.exchange).await.expect("read");
        assert!(started.is_some());
    }
    assert_eq!(
        after.started_at(captured.meta.id).await.expect("read"),
        Some(at(3))
    );
    let scanned = after.process(&pending).await.expect("scanned");
    let Processed::Scanned { events } = &scanned else {
        panic!("the captured exchange is scanned after the restart: {scanned:?}");
    };
    assert!(
        events.iter().all(|envelope| envelope.at == at(3)),
        "stamped with the recorded start"
    );
    let matches = after
        .store()
        .exchange_matches(captured.meta.id)
        .await
        .expect("matches");
    assert!(
        matches
            .iter()
            .any(|stored| stored.content.origin_agent() == a),
        "the read after the restart matches A"
    );
    drop(after);
    pool.close().await;
    db.close().await.expect("close");
}

/// What a text's distinct tokens are observed in, before and after
/// retention: A writes a text with a word seen nowhere else, B reads it,
/// then everything ages out.
async fn token_retention<I, S>(world: &mut World<I, DisabledSemanticMatcher, S>) -> Vec<u64>
where
    I: FingerprintIndex + Send + Sync,
    S: ProvenanceStore + Clone + Send + Sync,
{
    let (a, b) = (world.agent(), world.agent());
    let text = "The zephyrquill ledger moved to the blue binder behind the archive shelf.";
    originate(world, a, text, 1).await;
    world
        .run(Turn::new(b, at(2)).input(tool_result("call_1", text)))
        .await;
    let tokens = token::observed(text, world.config.spread().tokens_per_text());
    assert!(!tokens.is_empty());
    let mut counts = Vec::new();
    for now in [at(3), at(1 + RETENTION.as_secs())] {
        for fingerprint in &tokens {
            counts.push(
                world
                    .engine
                    .index()
                    .frequency(*fingerprint, now)
                    .await
                    .expect("frequency"),
            );
        }
    }
    let later = at(2 + RETENTION.as_secs() + 10);
    let expired = world.engine.expire(later).await.expect("expiry");
    counts.push(u64::try_from(expired).expect("small"));
    for fingerprint in &tokens {
        counts.push(
            world
                .engine
                .index()
                .frequency(*fingerprint, later)
                .await
                .expect("frequency"),
        );
    }
    let fresh = tokens.len();
    assert!(
        counts[..fresh].iter().all(|count| *count >= 2),
        "each token observed by the span and the read: {counts:?}"
    );
    assert!(
        counts[2 * fresh + 1..].iter().all(|count| *count == 0),
        "observations aged out: {counts:?}"
    );
    counts
}

/// `provenance.index.retention-bound` for token observations: the spread
/// rule's distinct-token observations age out of Postgres as they do of
/// the memory model, and no observation row is left.
#[tokio::test(flavor = "multi_thread")]
async fn pg_token_observations_age_out_with_memory() {
    let Some(db) = database("pg_token_observations_age_out_with_memory").await else {
        return;
    };
    let mut memory = World::new(config());
    let expected = token_retention(&mut memory).await;
    let pool = lazy_pool(&db);
    let mut pg = pg_world_on(pool.clone(), config());
    let got = token_retention(&mut pg).await;
    assert_eq!(got, expected);
    assert_eq!(
        pg.engine.index().row_counts().await.expect("counts"),
        (0, 0),
        "postings evicted and observations aged out"
    );
    drop(pg);
    pool.close().await;
    db.close().await.expect("close");
}
