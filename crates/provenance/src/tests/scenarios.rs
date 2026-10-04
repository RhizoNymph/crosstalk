//! Hand-picked deltas through the engine: carriers, scanned messages,
//! self-hits, origin agents, the cutoff, observations, semantic fallback.

use crosstalk_spec::derived::provenance::matching::{Carrier, MatchKind};
use crosstalk_spec::derived::provenance::span::{Origin, SpanState};
use crosstalk_spec::ids::{AgentId, SpanId};
use crosstalk_spec::interfaces::l4_provenance::{FingerprintIndex, SemanticHit};
use crosstalk_spec::observed::message::ToolCallId;
use crosstalk_spec::support::{ByteRange, Similarity};
use crosstalk_testkit::build::message::{assistant_text, system_text, tool_result, user_text};

use super::fixtures::{
    FakeSemantic, IndexCall, Recording, Turn, World, at, config, config_with, reference_index,
    sentence,
};
use crate::engine::Processed;
use crate::fingerprint::Winnowing;
use crate::store::{ProvenanceStore, SpanRecord};

/// `agent` writes `text` with no inputs at `seconds`; the span it
/// originated.
pub async fn originate<I, M, S>(
    world: &mut World<I, M, S>,
    agent: AgentId,
    text: &str,
    seconds: u64,
) -> SpanRecord
where
    I: FingerprintIndex + Send + Sync,
    M: crosstalk_spec::interfaces::l4_provenance::SemanticMatcher + Send + Sync,
    S: ProvenanceStore + Clone + Send + Sync,
{
    let ran = world
        .run(Turn::new(agent, at(seconds)).output(assistant_text(text)))
        .await;
    let spans = world
        .store
        .exchange_spans(ran.exchange)
        .await
        .expect("spans");
    spans
        .into_iter()
        .find(|record| record.span.state.origin() == Some(Origin::Originated))
        .expect("an originated span")
}

pub async fn carrier_for_tool_result() {
    let mut world = World::new(config());
    let (a, b) = (world.agent(), world.agent());
    let text = sentence("amber");
    let span = originate(&mut world, a, &text, 1).await;
    let ran = world
        .run(Turn::new(b, at(2)).input(tool_result("call_7", &format!("fetched: {text}"))))
        .await;
    let matches = world.matches_of(ran.exchange);
    assert_eq!(
        matches.len(),
        1,
        "{}",
        super::fixtures::brief_matches(&matches)
    );
    let found = &matches[0].content;
    assert_eq!(found.origin(), span.span.id);
    assert_eq!(
        found.carrier(),
        &Carrier::ToolResult(ToolCallId("call_7".to_owned()))
    );
    assert_eq!(found.read_at().part.message, ran.delta.new_inputs[0]);
}

pub async fn carrier_for_user_turn() {
    let mut world = World::new(config());
    let (a, b) = (world.agent(), world.agent());
    let text = sentence("cobalt");
    originate(&mut world, a, &text, 1).await;
    let ran = world
        .run(Turn::new(b, at(2)).input(user_text(&format!("please use this: {text}"))))
        .await;
    let matches = world.matches_of(ran.exchange);
    assert_eq!(
        matches.len(),
        1,
        "{}",
        super::fixtures::brief_matches(&matches)
    );
    assert_eq!(matches[0].content.carrier(), &Carrier::UserTurn);
}

pub async fn carrier_for_system_prompt() {
    let mut world = World::new(config());
    let (a, b) = (world.agent(), world.agent());
    let text = sentence("saffron");
    originate(&mut world, a, &text, 1).await;
    let ran = world
        .run(Turn::new(b, at(2)).system(system_text(&format!("You know that {text}."))))
        .await;
    let matches = world.matches_of(ran.exchange);
    assert_eq!(
        matches.len(),
        1,
        "{}",
        super::fixtures::brief_matches(&matches)
    );
    assert_eq!(matches[0].content.carrier(), &Carrier::SystemPrompt);
    assert_eq!(
        Some(matches[0].content.read_at().part.message),
        ran.delta.new_system
    );
}

pub async fn carrier_for_reader_output() {
    let mut world = World::new(config());
    let (a, b) = (world.agent(), world.agent());
    let text = sentence("vermilion");
    let span = originate(&mut world, a, &text, 1).await;
    let ran = world
        .run(
            Turn::new(b, at(2))
                .input(user_text("write something about orchards"))
                .output(assistant_text(&format!("Here it is. {text}"))),
        )
        .await;
    let matches = world.matches_of(ran.exchange);
    assert_eq!(
        matches.len(),
        1,
        "{}",
        super::fixtures::brief_matches(&matches)
    );
    let found = &matches[0].content;
    assert_eq!(found.carrier(), &Carrier::ReaderOutput);
    assert_eq!(found.origin(), span.span.id);
    assert_eq!(Some(found.read_at().part.message), ran.delta.output);
    let spans = world
        .store
        .exchange_spans(ran.exchange)
        .await
        .expect("spans");
    let relayed = spans
        .iter()
        .find(|record| {
            matches!(record.span.state, SpanState::Relayed { source: crosstalk_spec::derived::provenance::span::RelaySource::Span(source) } if source == span.span.id)
        })
        .expect("the copied text is relayed from the origin span");
    let range = found.read_at().range;
    let relayed_range = relayed.span.location.range;
    assert!(range.start() >= relayed_range.start() && range.end() <= relayed_range.end());
}

pub async fn origin_agent_taken_from_span() {
    let mut world = World::new(config());
    let (a, b, c) = (world.agent(), world.agent(), world.agent());
    let first = sentence("indigo");
    let second = sentence("ochre");
    let span_a = originate(&mut world, a, &first, 1).await;
    let span_b = originate(&mut world, b, &second, 2).await;
    let ran = world
        .run(
            Turn::new(c, at(3))
                .input(tool_result("call_1", &first))
                .input(tool_result("call_2", &second)),
        )
        .await;
    let matches = world.matches_of(ran.exchange);
    assert_eq!(
        matches.len(),
        2,
        "{}",
        super::fixtures::brief_matches(&matches)
    );
    for stored in matches {
        let record = world
            .store
            .span(stored.content.origin())
            .await
            .expect("span read")
            .expect("origin span stored");
        assert_eq!(stored.content.origin_agent(), record.span.agent);
        assert!(record.span.id == span_a.span.id || record.span.id == span_b.span.id);
    }
}

pub async fn lookups_cover_new_inputs_system_and_output() {
    let mut world = World::<Recording>::with(
        config(),
        Recording::new(reference_index(&config())),
        crate::semantic::DisabledSemanticMatcher,
    );
    let (a, b) = (world.agent(), world.agent());
    let one = sentence("teal");
    let two = sentence("umber");
    let three = sentence("sienna");
    originate(&mut world, a, &one, 1).await;
    originate(&mut world, a, &two, 2).await;
    originate(&mut world, a, &three, 3).await;
    let ran = world
        .run(
            Turn::new(b, at(4))
                .system(system_text(&one))
                .input(tool_result("call_1", &two))
                .output(assistant_text(&three)),
        )
        .await;
    let matches = world.matches_of(ran.exchange);
    let read_messages: Vec<_> = matches
        .iter()
        .map(|stored| stored.content.read_at().part.message)
        .collect();
    assert!(read_messages.contains(&ran.delta.new_system.expect("system")));
    assert!(read_messages.contains(&ran.delta.new_inputs[0]));
    assert!(read_messages.contains(&ran.delta.output.expect("output")));
    assert_eq!(
        matches.len(),
        3,
        "{}",
        super::fixtures::brief_matches(&matches)
    );
}

pub async fn replayed_history_is_not_looked_up() {
    let mut world = World::new(config());
    let (a, b) = (world.agent(), world.agent());
    let text = sentence("russet");
    originate(&mut world, a, &text, 1).await;
    let ran = world
        .run(
            Turn::new(b, at(2))
                .history(tool_result("call_old", &text))
                .input(user_text("thanks, carry on")),
        )
        .await;
    assert!(world.matches_of(ran.exchange).is_empty());
}

pub async fn own_span_in_input_is_skipped() {
    let mut world = World::new(config());
    let a = world.agent();
    let text = sentence("cerulean");
    originate(&mut world, a, &text, 1).await;
    let ran = world
        .run(Turn::new(a, at(2)).input(tool_result("call_1", &text)))
        .await;
    assert!(matches!(ran.processed, Processed::Scanned { .. }));
    assert!(world.matches_of(ran.exchange).is_empty());
}

pub async fn originated_span_fingerprints_are_indexed() {
    let mut world = World::new(config());
    let a = world.agent();
    let text = sentence("viridian");
    let span = originate(&mut world, a, &text, 1).await;
    let winnowing = Winnowing::new(world.config.winnow());
    let fingerprints = crate::fingerprint::positioned(&winnowing.winnow(&text));
    assert!(!fingerprints.is_empty());
    let hits = world
        .engine
        .index()
        .lookup(&fingerprints, at(2))
        .await
        .expect("lookup");
    for fingerprint in &fingerprints {
        assert!(
            hits.iter()
                .any(|hit| hit.fingerprint == fingerprint.fingerprint && hit.span == span.span.id),
            "fingerprint {fingerprint:?} not indexed"
        );
    }
    assert!(matches!(span.span.state, SpanState::Indexed { .. }));
}

/// `count` other texts each containing `text`, observed at `seconds`.
async fn make_frequent<I, M>(world: &mut World<I, M>, text: &str, count: usize, seconds: u64)
where
    I: FingerprintIndex + Send + Sync,
    M: crosstalk_spec::interfaces::l4_provenance::SemanticMatcher + Send + Sync,
{
    for n in 0..count {
        let reader = world.agent();
        world
            .run(Turn::new(reader, at(seconds)).input(user_text(&format!("note {n}: {text}"))))
            .await;
    }
}

pub async fn insert_skips_fingerprints_above_cutoff() {
    let mut world = World::new(config_with(2));
    let boilerplate = "Licensed under the Apache License, Version 2.0";
    make_frequent(&mut world, boilerplate, 3, 1).await;
    let a = world.agent();
    let novel = sentence("chartreuse");
    let ran = world
        .run(Turn::new(a, at(2)).output(assistant_text(&format!("{novel} {boilerplate}"))))
        .await;
    let winnowing = Winnowing::new(world.config.winnow());
    let frequent: Vec<_> = winnowing.winnow(boilerplate);
    let hits = world
        .engine
        .index()
        .lookup(&crate::fingerprint::positioned(&frequent), at(2))
        .await
        .expect("lookup");
    assert!(
        hits.is_empty(),
        "boilerplate fingerprints got postings: {hits:?}"
    );
    let spans = world
        .store
        .exchange_spans(ran.exchange)
        .await
        .expect("spans");
    assert!(
        spans
            .iter()
            .any(|record| record.span.state.origin() == Some(Origin::Originated))
    );
}

pub async fn lookup_ignores_fingerprints_that_crossed_cutoff() {
    let mut world = World::new(config_with(2));
    let a = world.agent();
    let text = sentence("magenta");
    originate(&mut world, a, &text, 1).await;
    make_frequent(&mut world, &text, 3, 2).await;
    let b = world.agent();
    let ran = world
        .run(Turn::new(b, at(3)).input(tool_result("call_1", &text)))
        .await;
    assert!(world.matches_of(ran.exchange).is_empty());
}

pub async fn span_with_one_rare_fingerprint_is_not_common() {
    let mut world = World::new(config_with(1));
    let common = "All rights reserved. Unauthorized copying of this file is prohibited";
    make_frequent(&mut world, common, 2, 1).await;
    let a = world.agent();
    let ran = world
        .run(Turn::new(a, at(2)).output(assistant_text(&format!("{common} zq"))))
        .await;
    let spans = world
        .store
        .exchange_spans(ran.exchange)
        .await
        .expect("spans");
    assert_eq!(spans.len(), 1, "{}", super::fixtures::brief_spans(&spans));
    assert_eq!(spans[0].span.state.origin(), Some(Origin::Originated));
    let ran = world
        .run(Turn::new(a, at(3)).output(assistant_text(common)))
        .await;
    let spans = world
        .store
        .exchange_spans(ran.exchange)
        .await
        .expect("spans");
    assert_eq!(spans.len(), 1, "{}", super::fixtures::brief_spans(&spans));
    assert_eq!(spans[0].span.state.origin(), Some(Origin::Common));
}

pub async fn every_span_origin_is_observed() {
    let mut world = World::<Recording>::with(
        config_with(1),
        Recording::new(reference_index(&config_with(1))),
        crate::semantic::DisabledSemanticMatcher,
    );
    let common = "All rights reserved. Unauthorized copying of this file is prohibited";
    make_frequent(&mut world, common, 2, 1).await;
    let (a, b) = (world.agent(), world.agent());
    let other = sentence("puce");
    originate(&mut world, a, &other, 2).await;
    let copied = sentence("mauve");
    let written = sentence("lilac");
    let output = format!("{written}\n\n{copied}\n\n{common}\n\n{other}");
    let ran = world
        .run(
            Turn::new(b, at(3))
                .input(user_text(&format!("quote: {copied}")))
                .output(assistant_text(&output)),
        )
        .await;
    let spans = world
        .store
        .exchange_spans(ran.exchange)
        .await
        .expect("spans");
    let origins: Vec<_> = spans.iter().filter_map(|r| r.span.state.origin()).collect();
    assert!(
        origins.contains(&Origin::Originated),
        "{}",
        super::fixtures::brief_spans(&spans)
    );
    assert!(
        origins.contains(&Origin::Common),
        "{}",
        super::fixtures::brief_spans(&spans)
    );
    assert!(
        origins.iter().any(|o| matches!(
            o,
            Origin::Relayed(crosstalk_spec::derived::provenance::span::RelaySource::Input(_))
        )),
        "{}",
        super::fixtures::brief_spans(&spans)
    );
    assert!(
        origins.iter().any(|o| matches!(
            o,
            Origin::Relayed(crosstalk_spec::derived::provenance::span::RelaySource::Span(_))
        )),
        "{}",
        super::fixtures::brief_spans(&spans)
    );
    let observed: Vec<Vec<_>> = world
        .engine
        .index()
        .calls()
        .into_iter()
        .filter_map(|call| match call {
            IndexCall::Observe(fingerprints) => Some(fingerprints),
            _ => None,
        })
        .collect();
    let output_message = world.messages_text(ran.delta.output.expect("output"));
    let winnowing = Winnowing::new(world.config.winnow());
    for record in &spans {
        let range = record.span.location.range;
        let text = &output_message[range.start() as usize..range.end() as usize];
        let expected: Vec<_> = winnowing
            .winnow(text)
            .iter()
            .map(|k| k.fingerprint)
            .collect();
        assert!(
            observed.contains(&expected),
            "span {:?} not observed",
            record.span.state
        );
    }
}

pub async fn scanned_input_parts_are_observed() {
    let mut world = World::<Recording>::with(
        config(),
        Recording::new(reference_index(&config())),
        crate::semantic::DisabledSemanticMatcher,
    );
    let a = world.agent();
    let tool_text = sentence("bronze");
    let user = sentence("pewter");
    let system = sentence("copper");
    world
        .run(
            Turn::new(a, at(1))
                .system(system_text(&system))
                .input(tool_result("call_1", &tool_text))
                .input(user_text(&user)),
        )
        .await;
    let observed: Vec<Vec<_>> = world
        .engine
        .index()
        .calls()
        .into_iter()
        .filter_map(|call| match call {
            IndexCall::Observe(fingerprints) => Some(fingerprints),
            _ => None,
        })
        .collect();
    let winnowing = Winnowing::new(world.config.winnow());
    for text in [&tool_text, &user, &system] {
        let mut expected: Vec<_> = winnowing
            .winnow(text)
            .iter()
            .map(|k| k.fingerprint)
            .collect();
        expected.sort_unstable();
        expected.dedup();
        assert!(observed.contains(&expected), "{text:?} not observed");
    }
}

pub async fn fingerprint_match_preempts_semantic() {
    let config = config();
    let index = reference_index(&config);
    let mut world = World::with(config.clone(), index, FakeSemantic::default());
    let (a, b) = (world.agent(), world.agent());
    let text = sentence("jade");
    let paraphrased = sentence("onyx");
    let fingerprinted = originate(&mut world, a, &text, 1).await;
    let paraphrase = originate(&mut world, a, &paraphrased, 2).await;
    let read = format!("result: {text}");
    let range = ByteRange::new(0, u32::try_from(read.len()).expect("short")).expect("range");
    let score = Similarity::new(0.95).expect("score");
    let hits = vec![
        SemanticHit {
            span: fingerprinted.span.id,
            read_range: range,
            score,
        },
        SemanticHit {
            span: paraphrase.span.id,
            read_range: range,
            score,
        },
    ];
    let mut world = rebind_semantic(world, FakeSemantic::returning(hits));
    let ran = world
        .run(Turn::new(b, at(3)).input(tool_result("call_1", &read)))
        .await;
    let matches = world.matches_of(ran.exchange);
    let of = |span: SpanId| -> Vec<MatchKind> {
        matches
            .iter()
            .filter(|stored| stored.content.origin() == span)
            .map(|stored| stored.content.kind().clone())
            .collect()
    };
    let fingerprinted_kinds = of(fingerprinted.span.id);
    assert_eq!(
        fingerprinted_kinds.len(),
        1,
        "{}",
        super::fixtures::brief_matches(&matches)
    );
    assert!(!matches!(fingerprinted_kinds[0], MatchKind::Semantic(_)));
    assert_eq!(of(paraphrase.span.id), vec![MatchKind::Semantic(score)]);
}

pub async fn semantic_lookup_respects_threshold() {
    use crosstalk_spec::interfaces::l4_provenance::SemanticMatcher;
    let threshold = Similarity::new(0.8).expect("threshold");
    let disabled = crate::semantic::DisabledSemanticMatcher;
    assert!(
        disabled
            .lookup("any text at all", threshold)
            .await
            .expect("lookup")
            .is_empty()
    );
    let config = config();
    let index = reference_index(&config);
    let mut world = World::with(config, index, FakeSemantic::default());
    let (a, b) = (world.agent(), world.agent());
    let low = originate(&mut world, a, &sentence("slate"), 1).await;
    let high = originate(&mut world, a, &sentence("flint"), 2).await;
    let read = "a paraphrase of both, sharing no words with either of them".to_owned();
    let range = ByteRange::new(0, u32::try_from(read.len()).expect("short")).expect("range");
    let hits = vec![
        SemanticHit {
            span: low.span.id,
            read_range: range,
            score: Similarity::new(0.5).expect("score"),
        },
        SemanticHit {
            span: high.span.id,
            read_range: range,
            score: Similarity::new(0.9).expect("score"),
        },
    ];
    let mut world = rebind_semantic(world, FakeSemantic::returning(hits));
    let ran = world.run(Turn::new(b, at(3)).input(user_text(&read))).await;
    let matches = world.matches_of(ran.exchange);
    assert_eq!(
        matches.len(),
        1,
        "{}",
        super::fixtures::brief_matches(&matches)
    );
    assert_eq!(matches[0].content.origin(), high.span.id);
    match matches[0].content.kind() {
        MatchKind::Semantic(score) => assert!(*score >= threshold),
        other => panic!("expected a semantic match, got {other:?}"),
    }
}

/// The same world with another semantic matcher.
fn rebind_semantic<I>(
    world: World<I, FakeSemantic>,
    semantic: FakeSemantic,
) -> World<I, FakeSemantic>
where
    I: FingerprintIndex + Send + Sync,
{
    let World {
        engine,
        store,
        messages,
        ids,
        config,
    } = world;
    let index = engine.into_index();
    let engine =
        crate::engine::Provenance::new(&config, index, store.clone(), semantic, messages.clone());
    World {
        engine,
        store,
        messages,
        ids,
        config,
    }
}

pub async fn delta_ends_indexed_or_failed() {
    use crate::store::{ScanFailure, ScanStatus};
    let mut world = World::new(config());
    let (a, b) = (world.agent(), world.agent());
    let text = sentence("terminal");
    let span = originate(&mut world, a, &text, 1).await;
    let (_, status) = world
        .store
        .exchange(span.span.exchange)
        .await
        .expect("read")
        .expect("recorded");
    assert!(matches!(status, ScanStatus::Indexed { .. }));
    let turn = Turn::new(b, at(2))
        .input(tool_result("call_1", &text))
        .output(assistant_text(&sentence("terminal-out")));
    let missing = turn.output.as_ref().expect("output").hash;
    let exchange = crosstalk_testkit::build::exchange::ExchangeBuilder::new(&mut world.ids)
        .started_at(at(2))
        .request(turn.new_inputs.iter().map(|m| m.hash).collect())
        .response(missing)
        .build();
    for input in &turn.new_inputs {
        world.messages.put(input.clone());
    }
    world
        .engine
        .record_exchange(&exchange)
        .await
        .expect("record");
    let delta = crosstalk_spec::events::ingest::ConversationDelta {
        exchange: exchange.meta.id,
        agent: b,
        conversation: world.ids.conversation(),
        new_inputs: turn.new_inputs.iter().map(|m| m.hash).collect(),
        new_system: None,
        output: Some(missing),
    };
    let processed = world.engine.process(&delta).await.expect("process");
    assert_eq!(
        processed,
        Processed::Failed {
            failure: ScanFailure::BodyMissing(missing)
        }
    );
    assert!(processed.events().is_empty());
    let (_, status) = world
        .store
        .exchange(exchange.meta.id)
        .await
        .expect("read")
        .expect("recorded");
    assert!(matches!(status, ScanStatus::Failed { .. }));
    assert!(
        world
            .store
            .exchange_spans(exchange.meta.id)
            .await
            .expect("spans")
            .is_empty()
    );
    assert!(world.matches_of(exchange.meta.id).is_empty());
    let again = world.engine.process(&delta).await.expect("redelivery");
    assert!(again.events().is_empty());
}
