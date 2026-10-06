//! Property tests, the invariant evidence named
//! `crosstalk_provenance::props::<name>`.
//!
//! Fingerprint properties run on generated text; decode properties encode
//! an originated span's text with generated codec chains; the rest run
//! generated multi-agent scenarios ([`generate::scenario`]) through the engine
//! and check every span and match they leave
//! ([`world::Outcome`]).

mod generate;
mod world;

use std::collections::BTreeSet;
use std::num::NonZeroU16;

use crosstalk_spec::derived::provenance::fingerprint::Fingerprint;
use crosstalk_spec::derived::provenance::matching::{Carrier, Codec, MatchKind};
use crosstalk_spec::derived::provenance::span::{Origin, RelaySource, SpanState};
use crosstalk_spec::interfaces::l4_provenance::{FingerprintIndex, Fingerprinter, SemanticHit};
use crosstalk_spec::support::{ByteRange, NonEmpty, Similarity};
use crosstalk_testkit::build::message::{assistant_text, tool_result, user_text};
use proptest::prelude::*;

use self::generate::{codec_chain, encode_chain, long_sentence, scenario, sentence};
use self::world::{Outcome, run};
use crate::config::DecodeLimits;
use crate::decode::DecodePipeline;
use crate::fingerprint::{Winnowing, hash, positioned};
use crate::store::ProvenanceStore;
use crate::tests::fixtures::{
    FakeSemantic, Recording, Turn, World, at, config, config_with, reference_index,
};
use crate::tests::scenarios::originate;
use crate::text::normalize;

fn block_on<F: std::future::Future>(future: F) -> F::Output {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("a runtime")
        .block_on(future)
}

fn cases(n: u32) -> ProptestConfig {
    ProptestConfig {
        cases: n,
        failure_persistence: None,
        ..ProptestConfig::default()
    }
}

fn test_winnowing() -> Winnowing {
    Winnowing::new(config().winnow())
}

fn fingerprint_set(text: &str) -> BTreeSet<Fingerprint> {
    test_winnowing()
        .fingerprints(text)
        .into_iter()
        .map(|p| p.fingerprint)
        .collect()
}

/// Text with mixed case, whitespace runs and non-ASCII characters.
fn messy_text() -> impl Strategy<Value = String> {
    proptest::collection::vec(
        prop_oneof![
            4 => "[a-zA-Z]{1,8}",
            1 => "[ \t\n]{1,4}",
            1 => "[àÉîÕüßİΣ一-龥]{1,3}",
            1 => "[0-9.,;:!?]{1,3}",
        ],
        1..40,
    )
    .prop_map(|pieces| pieces.concat())
}

proptest! {
    #![proptest_config(cases(256))]

    /// `provenance.fingerprint.hashes-k-grams`.
    #[test]
    fn fingerprints_hash_normalized_k_grams(text in messy_text()) {
        let winnowing = test_winnowing();
        let k = winnowing.k();
        let normalized = normalize(&text);
        for positioned in winnowing.fingerprints(&text) {
            let start = normalized
                .iter()
                .position(|c| c.start == positioned.offset)
                .expect("an offset is a normalized character's start");
            let candidates: Vec<u64> = (start..normalized.len())
                .take_while(|i| normalized[*i].start == positioned.offset)
                .filter(|i| i + k <= normalized.len())
                .map(|i| hash::kgram(&normalized[i..i + k].iter().map(|c| c.ch).collect::<Vec<_>>()))
                .collect();
            prop_assert!(candidates.contains(&positioned.fingerprint.0));
        }
    }

    /// `provenance.fingerprint.normalization-invariant`.
    #[test]
    fn fingerprints_ignore_case_and_whitespace(
        words in proptest::collection::vec("[a-zA-Z]{1,9}", 2..30),
        flips in proptest::collection::vec(any::<bool>(), 64),
        spaces in proptest::collection::vec(prop_oneof![
            Just(" "), Just("  "), Just("\t"), Just("\n\n"), Just("\u{a0}"), Just(" \u{2003} "),
        ], 30),
    ) {
        let plain = words.join(" ");
        let mut other = String::new();
        for (i, word) in words.iter().enumerate() {
            if i > 0 {
                other.push_str(spaces[i % spaces.len()]);
            }
            for (j, ch) in word.chars().enumerate() {
                if flips[(i + j) % flips.len()] {
                    other.extend(ch.to_uppercase());
                } else {
                    other.extend(ch.to_lowercase());
                }
            }
        }
        let values = |text: &str| -> Vec<u64> {
            test_winnowing().fingerprints(text).into_iter().map(|p| p.fingerprint.0).collect()
        };
        prop_assert_eq!(values(&plain), values(&other));
    }

    /// `provenance.fingerprint.winnow-guarantee`.
    #[test]
    fn shared_run_of_k_plus_w_minus_1_shares_fingerprint(
        run in "[a-z]([a-z ]{12,40})[a-z]",
        before in messy_text(),
        after in messy_text(),
        other_before in messy_text(),
        other_after in messy_text(),
    ) {
        let winnowing = test_winnowing();
        prop_assume!(crate::text::normalize::normalized_string(&run).chars().count() >= winnowing.guarantee());
        let first = fingerprint_set(&format!("{before}{run}{after}"));
        let second = fingerprint_set(&format!("{other_before}{run}{other_after}"));
        prop_assert!(!first.is_disjoint(&second));
    }

    /// `provenance.fingerprint.shard-is-modulo`.
    #[test]
    fn shard_is_fingerprint_modulo_shards(value in any::<u64>(), shards in 1u16..=u16::MAX) {
        let shards = NonZeroU16::new(shards).expect("non-zero");
        let shard = Fingerprint(value).shard(shards);
        prop_assert_eq!(u64::from(shard), value % u64::from(shards.get()));
        prop_assert!(shard < shards.get());
    }

    /// `provenance.decode.depth-bounded`: no layer, and so no
    /// `MatchKind::Decoded`, is deeper than the configured depth.
    #[test]
    fn decode_depth_never_exceeds_max(
        text in sentence(3, 10),
        chain in proptest::collection::vec(generate::codec(), 0..8),
        noise in messy_text(),
        depth in 1u8..=4,
    ) {
        let encoded = format!("{noise} {} {noise}", encode_chain(&chain, &text));
        let limits = DecodeLimits::new(depth, 32, 16).expect("limits");
        for layer in DecodePipeline::new(limits).layers(&encoded) {
            prop_assert!(layer.chain.len() <= usize::from(depth));
        }
    }
}

proptest! {
    #![proptest_config(cases(64))]

    /// `provenance.decode.codecs-in-decode-order`.
    #[test]
    fn decoded_match_lists_codecs_in_decode_order(text in long_sentence(), chain in codec_chain(3)) {
        prop_assume!(generate::effective(&chain, &text));
        let kind = block_on(encoded_read(&text, &chain));
        let mut expected: Vec<Codec> = chain.clone();
        expected.reverse();
        let expected = NonEmpty::from_vec(expected).expect("a chain is non-empty");
        prop_assert_eq!(kind, Some(MatchKind::Decoded(expected)));
    }

    /// `provenance.decode.encoded-text-matches`.
    #[test]
    fn encoded_span_text_still_matches(text in long_sentence(), chain in codec_chain(3)) {
        prop_assume!(generate::effective(&chain, &text));
        prop_assert!(block_on(encoded_read(&text, &chain)).is_some());
    }

    /// `provenance.index.originated-indexed`.
    #[test]
    fn originated_span_is_found_by_its_own_text(text in long_sentence()) {
        block_on(async {
            let mut world = World::new(config());
            let agent = world.agent();
            let span = originate(&mut world, agent, &text, 1).await;
            let fingerprints = positioned(&test_winnowing().winnow(&text));
            let hits = world.engine.index().lookup(&fingerprints, at(2)).await.expect("lookup");
            assert!(hits.iter().any(|hit| hit.span == span.span.id));
        });
    }

    /// `provenance.match.bytes-within-span`.
    #[test]
    fn exact_matched_bytes_within_origin_span(
        text in long_sentence(),
        prefix in sentence(0, 4),
        repeats in 1usize..3,
        cut in 0usize..30,
    ) {
        block_on(async {
            let mut world = World::new(config());
            let (a, b) = (world.agent(), world.agent());
            let span = originate(&mut world, a, &text, 1).await;
            let start = text.char_indices().nth(cut).map_or(0, |(i, _)| i);
            let read = format!("{prefix} {}", vec![&text[start..]; repeats].join(" "));
            let ran = world.run(Turn::new(b, at(2)).input(tool_result("call", &read))).await;
            for stored in world.matches_of(ran.exchange) {
                if *stored.content.kind() == MatchKind::Exact {
                    assert!(stored.content.matched_bytes() <= span.span.location.range.len());
                }
            }
        });
    }

    /// `provenance.match.reader-output-detected`.
    #[test]
    fn unexplained_output_text_matches_as_reader_output(
        text in long_sentence(),
        before in sentence(0, 5),
        after in sentence(0, 5),
        input in sentence(3, 8),
    ) {
        // `provenance.match.reader-output-strict`: shorter text never
        // yields a `ReaderOutput` match.
        prop_assume!(
            crate::text::normalize::normalized_string(&text).chars().count()
                >= config().reader_output().min_chars()
        );
        block_on(async {
            let mut world = World::new(config());
            let (a, b) = (world.agent(), world.agent());
            let span = originate(&mut world, a, &text, 1).await;
            let ran = world
                .run(Turn::new(b, at(2)).input(user_text(&input)).output(assistant_text(&format!("{before} {text} {after}"))))
                .await;
            let found = world.matches_of(ran.exchange).into_iter().any(|stored| {
                stored.content.origin() == span.span.id && *stored.content.carrier() == Carrier::ReaderOutput
            });
            assert!(found, "no ReaderOutput match");
        });
    }

    /// `provenance.span.originated-absent-from-inputs`: text matching
    /// another agent's indexed span and no input is relayed from it.
    #[test]
    fn output_matching_other_agents_span_is_relayed(text in long_sentence(), own in sentence(4, 8)) {
        block_on(async {
            let mut world = World::new(config());
            let (a, b) = (world.agent(), world.agent());
            let span = originate(&mut world, a, &text, 1).await;
            let ran = world
                .run(Turn::new(b, at(2)).output(assistant_text(&format!("{own}\n\n{text}"))))
                .await;
            let spans = world.store.exchange_spans(ran.exchange).await.expect("spans");
            let relayed = spans.iter().any(|record| {
                record.span.state == SpanState::Relayed { source: RelaySource::Span(span.span.id) }
            });
            assert!(relayed, "{}", crate::tests::fixtures::brief_spans(&spans));
        });
    }

    /// `provenance.semantic.score-above-threshold`.
    #[test]
    fn semantic_hits_meet_threshold(scores in proptest::collection::vec(0.0f32..=1.0, 1..6)) {
        block_on(async {
            let config = config();
            let threshold = config.semantic_threshold();
            let mut world = World::with(config.clone(), reference_index(&config), FakeSemantic::default());
            let (a, b) = (world.agent(), world.agent());
            let mut hits = Vec::new();
            let read = "some paraphrase sharing no words with anything";
            let range = ByteRange::new(0, read.len() as u32).expect("range");
            for (n, score) in scores.iter().enumerate() {
                let span = originate(&mut world, a, &crate::tests::fixtures::sentence(&format!("s{n}")), 1).await;
                hits.push(SemanticHit { span: span.span.id, read_range: range, score: Similarity::new(*score).expect("score") });
            }
            let World { engine, store, messages, ids, config, ran } = world;
            let index = engine.into_index();
            let engine = crate::engine::Provenance::new(&config, index, store.clone(), FakeSemantic::returning(hits), messages.clone());
            let mut world = World { engine, store, messages, ids, config, ran };
            let ran = world.run(Turn::new(b, at(2)).input(user_text(read))).await;
            for stored in world.matches_of(ran.exchange) {
                if let MatchKind::Semantic(score) = stored.content.kind() {
                    assert!(*score >= threshold);
                }
            }
        });
    }

    /// `provenance.index.frequency-counts-observed-texts`: the engine
    /// observes each scanned text once, so a fingerprint's frequency is the
    /// number of live scanned texts containing it.
    #[test]
    fn frequency_matches_observation_model(
        texts in proptest::collection::vec((sentence(3, 8), 0u64..7200), 1..8),
        now in 0u64..9000,
    ) {
        block_on(async {
            let mut world = World::new(config());
            let mut sorted = texts.clone();
            sorted.sort_by_key(|(_, at)| *at);
            for (text, seconds) in &sorted {
                let agent = world.agent();
                world.run(Turn::new(agent, at(*seconds)).input(user_text(text))).await;
            }
            let retention = config().index().retention().as_secs();
            let last = sorted.last().map_or(0, |(_, s)| *s);
            let horizon = now.max(last);
            let now_at = at(horizon);
            let pipeline = DecodePipeline::new(config().decode());
            let observed = |text: &str| -> BTreeSet<Fingerprint> {
                pipeline
                    .layers(text)
                    .iter()
                    .flat_map(|layer| test_winnowing().winnow(layer.text.text()))
                    .map(|kgram| kgram.fingerprint)
                    .collect()
            };
            let mut all = BTreeSet::new();
            for (text, _) in &sorted {
                all.extend(observed(text));
            }
            for fingerprint in all {
                // An observation counts while within retention of `now`; one
                // already out of retention at the last write is gone.
                let expected = sorted
                    .iter()
                    .filter(|(text, seconds)| {
                        seconds + retention >= horizon && observed(text).contains(&fingerprint)
                    })
                    .count() as u64;
                let got = world.engine.index().frequency(fingerprint, now_at).await.expect("frequency");
                assert_eq!(got, expected);
            }
        });
    }

    /// `provenance.span.common-above-cutoff`: a span is `Common` only when
    /// every fingerprint was above the cutoff when it was classified.
    #[test]
    fn common_spans_have_only_frequent_fingerprints(
        boilerplate in sentence(4, 8),
        repeats in 0usize..4,
        extra in sentence(0, 3),
    ) {
        block_on(async {
            let config = config_with(2);
            let mut world = World::with(config.clone(), Recording::new(reference_index(&config)), crate::semantic::DisabledSemanticMatcher);
            for n in 0..repeats {
                let agent = world.agent();
                world.run(Turn::new(agent, at(1)).input(user_text(&format!("copy {n}: {boilerplate}")))).await;
            }
            let before = world.engine.index().calls().len();
            let agent = world.agent();
            let ran = world.run(Turn::new(agent, at(2)).output(assistant_text(&format!("{boilerplate} {extra}")))).await;
            let delta_observations: Vec<Vec<Fingerprint>> = world.engine.index().calls()[before..]
                .iter()
                .filter_map(|call| match call {
                    crate::tests::fixtures::IndexCall::Observe(f) => Some(f.clone()),
                    _ => None,
                })
                .collect();
            let output = world.messages_text(ran.delta.output.expect("output"));
            for record in world.store.exchange_spans(ran.exchange).await.expect("spans") {
                if record.span.state.origin() != Some(Origin::Common) {
                    continue;
                }
                let range = record.span.location.range;
                let text = &output[range.start() as usize..range.end() as usize];
                for fingerprint in fingerprint_set(text) {
                    let after = world.engine.index().frequency(fingerprint, at(2)).await.expect("frequency");
                    let during = delta_observations.iter().filter(|f| f.contains(&fingerprint)).count() as u64;
                    assert!(after - during > config.index().cutoff(), "a common span's fingerprint was not frequent");
                }
            }
        });
    }
}

proptest! {
    #![proptest_config(cases(256))]

    /// `provenance.decode.utf8-lossless`.
    #[test]
    fn decoders_yield_lossless_utf8(
        payload in proptest::collection::vec(any::<u8>(), 8..64),
        text in sentence(2, 6),
    ) {
        use base64::Engine as _;
        use crate::decode::{Base64Decoder, HexDecoder, TextDecoder, UrlDecoder};
        let mut source = payload.clone();
        if payload.len() % 2 == 0 {
            // Half the cases carry text, which is valid UTF-8.
            source = text.clone().into_bytes();
        }
        let valid = std::str::from_utf8(&source).ok().map(str::to_owned);
        let b64 = base64::engine::general_purpose::STANDARD.encode(&source);
        let hex: String = source.iter().map(|b| format!("{b:02x}")).collect();
        let url: String = source.iter().map(|b| format!("%{b:02X}")).collect();
        for (decoded, encoded) in [
            (Base64Decoder::new(16).decode_mapped(&format!("x {b64} y")), &b64),
            (HexDecoder::new(16).decode_mapped(&format!("x {hex} y")), &hex),
        ] {
            let got: Vec<String> = decoded.into_iter().map(|d| d.text.into_text()).collect();
            match &valid {
                Some(text) if encoded.trim_end_matches('=').len() >= 16 => prop_assert_eq!(got, vec![text.clone()]),
                Some(_) => {}
                None => prop_assert!(got.is_empty(), "invalid UTF-8 was decoded"),
            }
        }
        let got: Vec<String> = UrlDecoder
            .decode_mapped(&format!("x {url} y"))
            .into_iter()
            .map(|d| d.text.into_text())
            .collect();
        match &valid {
            Some(text) => prop_assert_eq!(got, vec![format!("x {text} y")]),
            None => prop_assert!(got.is_empty(), "invalid UTF-8 was decoded"),
        }
    }
}

/// A originates `text`; B reads it encoded with `chain` in a tool result.
/// The match's kind, if any.
async fn encoded_read(text: &str, chain: &[Codec]) -> Option<MatchKind> {
    let mut world = World::new(config());
    let (a, b) = (world.agent(), world.agent());
    let span = originate(&mut world, a, text, 1).await;
    let encoded = encode_chain(chain, text);
    let ran = world
        .run(Turn::new(b, at(2)).input(tool_result("call", &format!("payload: {encoded}"))))
        .await;
    world
        .matches_of(ran.exchange)
        .into_iter()
        .find(|stored| stored.content.origin() == span.span.id)
        .map(|stored| stored.content.kind().clone())
}

/// Run a generated scenario and check `check` on its outcome.
fn scenario_property(check: fn(&Outcome)) {
    let mut runner = proptest::test_runner::TestRunner::new(cases(48));
    let result = runner.run(&scenario(), |plans| {
        let outcome = block_on(run(&plans));
        check(&outcome);
        Ok(())
    });
    if let Err(error) = result {
        panic!("{error}");
    }
}

fn on_boundaries(text: &str, range: ByteRange) -> bool {
    let (start, end) = (range.start() as usize, range.end() as usize);
    end <= text.len() && text.is_char_boundary(start) && text.is_char_boundary(end)
}

/// `provenance.span.char-boundaries`.
#[test]
fn segment_spans_on_char_boundaries() {
    scenario_property(|outcome| {
        for record in &outcome.spans {
            let message = outcome
                .message(record.span.location.part.message)
                .expect("stored");
            let text = message
                .part_text(record.span.location.part.index)
                .expect("text part");
            assert!(on_boundaries(&text, record.span.location.range));
        }
    });
}

/// `provenance.span.disjoint`.
#[test]
fn segment_spans_do_not_overlap() {
    scenario_property(|outcome| {
        for (i, a) in outcome.spans.iter().enumerate() {
            for b in &outcome.spans[i + 1..] {
                if a.span.exchange == b.span.exchange
                    && a.span.location.part == b.span.location.part
                {
                    let (ra, rb) = (a.span.location.range, b.span.location.range);
                    assert!(
                        ra.end() <= rb.start() || rb.end() <= ra.start(),
                        "overlapping spans"
                    );
                }
            }
        }
    });
}

/// `provenance.span.within-output-part`.
#[test]
fn segment_spans_lie_within_output_parts() {
    scenario_property(|outcome| {
        for record in &outcome.spans {
            let turn = outcome.turn(record.span.exchange).expect("its turn");
            assert_eq!(
                Some(record.span.location.part.message),
                turn.ran.delta.output
            );
            let output = turn.turn.output.as_ref().expect("an output");
            let text = output
                .part_text(record.span.location.part.index)
                .expect("a text part");
            assert!(record.span.location.range.end() as usize <= text.len());
        }
    });
}

/// `provenance.span.location-indexes-part-text`.
#[test]
fn locations_index_part_text() {
    scenario_property(|outcome| {
        let locations = outcome
            .spans
            .iter()
            .map(|r| r.span.location)
            .chain(outcome.matches.iter().map(|m| m.content.read_at()));
        for location in locations {
            let message = outcome.message(location.part.message).expect("stored");
            let text = message.part_text(location.part.index).expect("a text part");
            assert!(on_boundaries(&text, location.range));
        }
    });
}

/// `provenance.match.read-at-in-reader-exchange`.
#[test]
fn read_at_points_into_reader_delta() {
    scenario_property(|outcome| {
        for stored in &outcome.matches {
            let turn = outcome
                .turn(stored.content.reader_exchange())
                .expect("its turn");
            let delta = &turn.ran.delta;
            let message = stored.content.read_at().part.message;
            let listed = delta.new_inputs.contains(&message)
                || delta.new_system == Some(message)
                || delta.output == Some(message);
            assert!(listed, "a read outside the delta");
            assert!(
                outcome
                    .text_at(
                        stored.content.read_at().part,
                        stored.content.read_at().range
                    )
                    .is_some()
            );
        }
    });
}

/// `provenance.match.reader-output-relayed-span`.
#[test]
fn reader_output_match_agrees_with_relay_span() {
    scenario_property(|outcome| {
        for stored in &outcome.matches {
            if *stored.content.carrier() != Carrier::ReaderOutput {
                continue;
            }
            let read = stored.content.read_at();
            let inside = outcome.spans.iter().any(|record| {
                record.span.exchange == stored.content.reader_exchange()
                    && record.span.location.part == read.part
                    && record.span.location.range.start() <= read.range.start()
                    && read.range.end() <= record.span.location.range.end()
                    && record.span.state
                        == SpanState::Relayed {
                            source: RelaySource::Span(stored.content.origin()),
                        }
            });
            assert!(inside, "a ReaderOutput match outside its relay span");
        }
    });
}

/// `provenance.match.reader-output-unexplained`.
#[test]
fn reader_output_match_absent_from_inputs() {
    scenario_property(|outcome| {
        for stored in &outcome.matches {
            if *stored.content.carrier() != Carrier::ReaderOutput {
                continue;
            }
            let turn = outcome
                .turn(stored.content.reader_exchange())
                .expect("its turn");
            let coverage = outcome.input_coverage(turn);
            let read = stored.content.read_at();
            let text = outcome.text_at(read.part, read.range).expect("read text");
            for kgram in outcome.winnowing.winnow(&text) {
                assert!(
                    !coverage.contains(kgram.fingerprint),
                    "explained by an input"
                );
            }
        }
    });
}

/// `provenance.span.originated-absent-from-inputs`: an originated span
/// shares no fingerprint with the exchange's inputs, raw or decoded, nor
/// with any span indexed before it, except another agent's span it is
/// recorded to coincide with (a coincident template stretch,
/// `provenance.span.coincident-template-originated`).
#[test]
fn originated_span_not_fingerprint_matchable_in_inputs() {
    scenario_property(|outcome| {
        for record in &outcome.spans {
            if record.span.state.origin() != Some(Origin::Originated) {
                continue;
            }
            let turn = outcome.turn(record.span.exchange).expect("its turn");
            let coverage = outcome.input_coverage(turn);
            let fingerprints = outcome.span_fingerprints(&record.span);
            for fingerprint in &fingerprints {
                assert!(
                    !coverage.contains(*fingerprint),
                    "an originated span is in its inputs"
                );
            }
            for earlier in outcome.indexed_before(record.span.exchange) {
                if earlier.span.agent != record.span.agent
                    && outcome
                        .coincidences
                        .contains(&(record.span.id, earlier.span.id))
                {
                    continue;
                }
                let theirs = outcome.span_fingerprints(&earlier.span);
                assert!(
                    fingerprints.is_disjoint(&theirs),
                    "an originated span matches an indexed span"
                );
            }
        }
    });
}

/// `provenance.span.relay-source-contains-text` and
/// `provenance.span.relay-source-exists`.
#[test]
fn relayed_span_source_contains_text() {
    scenario_property(|outcome| {
        for record in &outcome.spans {
            let SpanState::Relayed { source } = record.span.state else {
                continue;
            };
            let text = outcome.span_view(&record.span).expect("span text");
            let turn = outcome.turn(record.span.exchange).expect("its turn");
            match source {
                RelaySource::Input(hash) => {
                    let request = turn.turn.request();
                    let message = request
                        .iter()
                        .find(|m| m.hash == hash)
                        .expect("the source is an input");
                    assert!(
                        outcome.message_contains(message, &text),
                        "the input does not contain the span"
                    );
                }
                RelaySource::Span(span) => {
                    let origin = outcome
                        .spans
                        .iter()
                        .find(|r| r.span.id == span)
                        .expect("the source span is stored");
                    assert!(origin.index_seq.is_some(), "the source span was indexed");
                    let origin_text = outcome.span_view(&origin.span).expect("origin text");
                    let normalized = crate::text::normalize::normalized_string(&origin_text);
                    let needle = crate::text::normalize::normalized_string(&text);
                    assert!(
                        normalized.contains(needle.trim()),
                        "the source span does not contain the span"
                    );
                }
            }
        }
    });
}

/// A regression `decoded_match_lists_codecs_in_decode_order` found: URL
/// encoding touched only the last base64 character, so base64 alone
/// decoded all but one character of the text and covered as many bytes.
#[test]
fn trailing_escape_still_reports_the_full_chain() {
    let text = "aßîîéü aaa aaaaa aaa aaa aaa aaaa bca aulo αδρ";
    let kind = block_on(encoded_read(text, &[Codec::Base64, Codec::UrlEncoding]));
    let expected = NonEmpty::from_vec(vec![Codec::UrlEncoding, Codec::Base64]).expect("two codecs");
    assert_eq!(kind, Some(MatchKind::Decoded(expected)));
}

proptest! {
    #![proptest_config(cases(256))]

    /// `provenance.index.remainder-around-relay-matchable`: in a run of an
    /// originated remainder, a forwarded quote and another remainder, every
    /// context k-gram is posted under the span holding more than half of
    /// its characters, so a k-gram mostly over the quote never goes to an
    /// originated span.
    #[test]
    fn context_kgrams_go_to_the_span_holding_most_of_them(
        before in "[a-z]{1,6}( [a-z]{1,6}){0,3}",
        quote in "[a-z]{1,6}( [a-z]{1,6}){1,6}",
        after in "[a-z]{1,6}( [a-z]{1,6}){0,3}",
    ) {
        use crosstalk_spec::derived::provenance::span::{Span, SpanLocation};
        use crosstalk_spec::ids::{AgentId, ExchangeId, SpanId};
        use crosstalk_spec::observed::message::PartRef;
        use crosstalk_testkit::build::message::message;

        let text = format!("{before} {quote} {after}");
        let output = message(assistant_text(&text));
        let quote_start = before.len() as u32 + 1;
        let quote_end = quote_start + quote.len() as u32;
        let ranges = [
            (0, before.len() as u32, SpanState::Originated),
            (quote_start, quote_end, SpanState::Relayed { source: RelaySource::Input(output.hash) }),
            (quote_end + 1, text.len() as u32, SpanState::Originated),
        ];
        let spans: Vec<Span> = ranges
            .iter()
            .enumerate()
            .map(|(n, (start, end, state))| Span {
                id: SpanId::from_ulid(n as u128 + 1),
                location: SpanLocation {
                    part: PartRef { message: output.hash, index: 0 },
                    range: ByteRange::new(*start, *end).expect("a range"),
                },
                agent: AgentId::from_ulid(1),
                exchange: ExchangeId::from_ulid(1),
                state: state.clone(),
            })
            .collect();
        let scanner = crate::scan::Scanner::new(&config());
        let k = scanner.winnowing().k();
        let context = scanner.context_kgrams(&crate::segment::text_parts(&output), &spans);
        for (id, kgrams) in &context {
            let span = spans.iter().find(|span| span.id == *id).expect("a posted span");
            let range = span.location.range;
            for kgram in kgrams {
                // ASCII with single spaces: a normalized position is a byte.
                let inside = (kgram.position..kgram.position + k)
                    .filter(|at| (range.start() as usize..range.end() as usize).contains(at))
                    .count();
                prop_assert!(2 * inside > k, "a context k-gram posted under a minority span");
            }
        }
    }
}
