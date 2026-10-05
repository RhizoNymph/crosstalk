//! The engine over Postgres: the index and the records.

use crosstalk_spec::derived::provenance::matching::Carrier;
use crosstalk_spec::derived::provenance::span::{Origin, SpanState};
use crosstalk_spec::interfaces::l4_provenance::FingerprintIndex;
use crosstalk_store::TestDb;
use crosstalk_testkit::build::message::{assistant_text, system_text, tool_result, user_text};

use super::{database, lazy_pool};
use crate::engine::Processed;
use crate::fingerprint::{Winnowing, positioned};
use crate::index::PgFingerprintIndex;
use crate::semantic::DisabledSemanticMatcher;
use crate::store::{PgProvenanceStore, ProvenanceStore, ScanFailure, ScanStatus, ScannedAs};
use crate::tests::fixtures::{RETENTION, Turn, World, at, config, sentence};
use crate::tests::scenarios::originate;

type PgWorld = World<PgFingerprintIndex, DisabledSemanticMatcher, PgProvenanceStore>;

fn pg_world(db: &TestDb) -> PgWorld {
    let config = config();
    let index = PgFingerprintIndex::new(lazy_pool(db), config.index().clone());
    let store = PgProvenanceStore::new(lazy_pool(db));
    World::over(config, index, DisabledSemanticMatcher, store)
}

/// The turns both engines run: A writes three texts; B reads one in a tool
/// result, one in a new system prompt; C reproduces the third with no
/// input holding it, and copies B's input.
async fn script<I, S>(world: &mut World<I, DisabledSemanticMatcher, S>) -> Vec<Processed>
where
    I: FingerprintIndex + Send + Sync,
    S: ProvenanceStore + Clone + Send + Sync,
{
    let (a, b, c) = (world.agent(), world.agent(), world.agent());
    let one = sentence("one");
    let two = sentence("two");
    let three = sentence("three");
    let mut processed = Vec::new();
    for (n, text) in [&one, &two, &three].into_iter().enumerate() {
        let ran = world
            .run(Turn::new(a, at(n as u64 + 1)).output(assistant_text(text)))
            .await;
        processed.push(ran.processed);
    }
    let ran = world
        .run(
            Turn::new(b, at(10))
                .system(system_text(&format!("Remember: {two}")))
                .input(tool_result("call_1", &format!("page body: {one}")))
                .output(assistant_text(&format!(
                    "{} and also {one}",
                    sentence("b-own")
                ))),
        )
        .await;
    processed.push(ran.processed);
    let ran = world
        .run(
            Turn::new(c, at(20))
                .input(user_text(&sentence("c-asks")))
                .output(assistant_text(&format!("{three}\n\n{}", sentence("c-own")))),
        )
        .await;
    processed.push(ran.processed);
    processed
}

pub async fn agrees_with_memory() {
    let Some(db) = database("pg_engine_agrees_with_memory").await else {
        return;
    };
    let mut memory = World::new(config());
    let expected = script(&mut memory).await;
    let mut pg = pg_world(&db);
    let got = script(&mut pg).await;
    assert_eq!(got, expected);
    assert!(
        expected.iter().any(|p| !p.events().is_empty()),
        "the script publishes something"
    );
    db.close().await.expect("close");
}

pub async fn redelivery_is_idempotent() {
    let Some(db) = database("pg_redelivered_delta_is_idempotent").await else {
        return;
    };
    let mut world = pg_world(&db);
    let (a, b) = (world.agent(), world.agent());
    let text = sentence("redelivered");
    originate(&mut world, a, &text, 1).await;
    let ran = world
        .run(
            Turn::new(b, at(2))
                .input(tool_result("call_1", &text))
                .output(assistant_text(&sentence("b-writes"))),
        )
        .await;
    let counts = world.engine.index().row_counts().await.expect("counts");
    let spans = world
        .store
        .exchange_spans(ran.exchange)
        .await
        .expect("spans");
    let matches = world.stored_matches(ran.exchange).await;
    let again = world.engine.process(&ran.delta).await.expect("redelivery");
    match (&ran.processed, &again) {
        (Processed::Scanned { events: first }, Processed::Replayed { events: second }) => {
            assert_eq!(first, second, "a redelivery republishes the same envelopes");
        }
        other => panic!("expected scanned then replayed, got {other:?}"),
    }
    assert_eq!(
        world.engine.index().row_counts().await.expect("counts"),
        counts
    );
    assert_eq!(
        world
            .store
            .exchange_spans(ran.exchange)
            .await
            .expect("spans"),
        spans
    );
    assert_eq!(world.stored_matches(ran.exchange).await, matches);
    db.close().await.expect("close");
}

pub async fn scan_status_and_match_reads() {
    let Some(db) = database("pg_store_reads_scan_status_and_matches").await else {
        return;
    };
    let mut world = pg_world(&db);
    let (a, b) = (world.agent(), world.agent());
    let text = sentence("status");
    let span = originate(&mut world, a, &text, 1).await;
    let ran = world
        .run(Turn::new(b, at(2)).input(tool_result("call_1", &text)))
        .await;
    let (record, status) = world
        .store
        .exchange(ran.exchange)
        .await
        .expect("read")
        .expect("recorded");
    assert!(matches!(status, ScanStatus::Indexed { .. }));
    assert_eq!(record.started_at, at(2));
    let input = ran.delta.new_inputs[0];
    let scans = world.store.message_scans(input).await.expect("scans");
    assert_eq!(scans.len(), 1);
    assert_eq!(scans[0].scanned_as, ScannedAs::Input);
    let in_message = world
        .store
        .matches_in_message(input)
        .await
        .expect("matches");
    assert_eq!(in_message.len(), 1);
    assert!(matches!(
        in_message[0].content.carrier(),
        Carrier::ToolResult(_)
    ));
    let of_span = world
        .store
        .matches_of_span(span.span.id)
        .await
        .expect("matches");
    assert_eq!(of_span, in_message);
    let stored = world
        .store
        .span(span.span.id)
        .await
        .expect("read")
        .expect("stored");
    assert!(matches!(stored.span.state, SpanState::Propagated { .. }));
    assert_eq!(stored.span.location, span.span.location);

    // A delta whose body is gone fails for good, and says why.
    let turn = Turn::new(b, at(4)).input(user_text(&sentence("missing")));
    let missing = turn.new_inputs[0].hash;
    let exchange = crosstalk_testkit::build::exchange::ExchangeBuilder::new(&mut world.ids)
        .started_at(at(4))
        .request(vec![missing])
        .build();
    world
        .engine
        .record_exchange(&exchange)
        .await
        .expect("record");
    let delta = crosstalk_spec::events::ingest::ConversationDelta {
        exchange: exchange.meta.id,
        agent: b,
        conversation: world.ids.conversation(),
        new_inputs: vec![missing],
        new_system: None,
        output: None,
    };
    let processed = world.engine.process(&delta).await.expect("process");
    assert_eq!(
        processed,
        Processed::Failed {
            failure: ScanFailure::BodyMissing(missing)
        }
    );
    let (_, status) = world
        .store
        .exchange(exchange.meta.id)
        .await
        .expect("read")
        .expect("recorded");
    assert!(
        matches!(status, ScanStatus::Failed { failure: ScanFailure::BodyMissing(hash), .. } if hash == missing)
    );
    db.close().await.expect("close");
}

pub async fn evicts_expired_spans() {
    let Some(db) = database("pg_index_evicts_expired_spans").await else {
        return;
    };
    let mut world = pg_world(&db);
    let a = world.agent();
    let text = sentence("ephemeral");
    let span = originate(&mut world, a, &text, 1).await;
    let fingerprints = positioned(&Winnowing::new(world.config.winnow()).winnow(&text));
    let hits = world
        .engine
        .index()
        .lookup(&fingerprints, at(2))
        .await
        .expect("lookup");
    assert!(hits.iter().any(|hit| hit.span == span.span.id));
    let past = at(1 + RETENTION.as_secs() + 1);
    let expired = world.engine.expire(past).await.expect("expire");
    assert_eq!(expired, 1);
    let hits = world
        .engine
        .index()
        .lookup(&fingerprints, past)
        .await
        .expect("lookup");
    assert!(hits.is_empty(), "an expired span still has postings");
    assert_eq!(
        world.engine.index().row_counts().await.expect("counts"),
        (0, 0)
    );
    let stored = world
        .store
        .span(span.span.id)
        .await
        .expect("read")
        .expect("stored");
    assert!(matches!(stored.span.state, SpanState::Expired { .. }));
    assert_eq!(stored.span.state.origin(), Some(Origin::Originated));
    let (record, _) = world
        .store
        .exchange(span.span.exchange)
        .await
        .expect("read")
        .expect("recorded");
    assert!(
        record.request.is_empty(),
        "the request list outlived retention"
    );
    db.close().await.expect("close");
}

/// A forwards a page it fetched; B reads the forward; the forwarded span is
/// indexed (state still `Relayed`), matched, then expired, on Postgres as
/// in memory.
async fn forwarding<I, S>(world: &mut World<I, DisabledSemanticMatcher, S>) -> Vec<String>
where
    I: FingerprintIndex + Send + Sync,
    S: ProvenanceStore + Clone + Send + Sync,
{
    let (a, b, c) = (world.agent(), world.agent(), world.agent());
    let page = sentence("forwarded-page");
    let sent = world
        .run(
            Turn::new(a, at(1))
                .input(tool_result("call_1", &page))
                .output(assistant_text(&format!("{}\n\n{page}", sentence("a-note")))),
        )
        .await;
    let read = world.run(Turn::new(b, at(2)).input(user_text(&page))).await;
    let mut seen = Vec::new();
    for record in world
        .store
        .exchange_spans(sent.exchange)
        .await
        .expect("spans")
    {
        seen.push(format!(
            "{:?} {:?} {:?}",
            record.span.state,
            record.forward,
            record.index_seq.is_some()
        ));
    }
    for stored in world.stored_matches(read.exchange).await {
        seen.push(format!(
            "{:?} {:?}",
            stored.content.origin_agent() == a,
            stored.content.carrier()
        ));
    }
    let later = at(1 + RETENTION.as_secs() + 10);
    world.engine.expire(later).await.expect("expiry");
    for record in world
        .store
        .exchange_spans(sent.exchange)
        .await
        .expect("spans")
    {
        seen.push(format!("{:?} {:?}", record.span.state, record.forward));
    }
    let late = world.run(Turn::new(c, later).input(user_text(&page))).await;
    seen.push(format!(
        "{}",
        world.stored_matches(late.exchange).await.len()
    ));
    seen
}

pub async fn forwarded_spans_index_and_expire() {
    let Some(db) = database("pg_forwarded_spans_index_and_expire").await else {
        return;
    };
    let mut memory = World::new(config());
    let expected = forwarding(&mut memory).await;
    assert!(
        expected.iter().any(|line| line.contains("Some(Expired")),
        "{expected:?}"
    );
    let mut pg = pg_world(&db);
    let got = forwarding(&mut pg).await;
    assert_eq!(got, expected);
    db.close().await.expect("close");
}
