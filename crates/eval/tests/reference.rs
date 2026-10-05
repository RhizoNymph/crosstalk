//! The reference matcher on hand-built worlds.

mod common;

use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use common::{calls, dataset, draft, result, says, system, user};
use crosstalk_eval::corpus::{Coverage, Driven, HashedMessage, World, WorldBuilder};
use crosstalk_eval::keys::{AgentKey, WorldKey};
use crosstalk_eval::location::SpanLocationExt;
use crosstalk_eval::predict::reads::Resolved;
use crosstalk_eval::predict::{
    AgentMap, EvidenceClass, PredictedRoute, Prediction, WorldDirectory, from_transmission,
};
use crosstalk_eval::reference::classify::Classified::Match;
use crosstalk_eval::reference::classify::classify;
use crosstalk_eval::reference::decode::decode_candidates;
use crosstalk_eval::reference::fold::{fold, fold_plain, string_codec};
use crosstalk_eval::reference::opaque::{opaque_ranges, segments};
use crosstalk_eval::reference::route::{normalize_path, parse_url};
use crosstalk_eval::reference::shingle::{covered, shingles};
use crosstalk_eval::reference::{ReferenceConfig, ReferenceOutput, run};
use crosstalk_eval::truth::{CarrierKind, Tier};
use crosstalk_spec::derived::flow::resource::{Host, Locator};
use crosstalk_spec::derived::provenance::matching::{Codec, MatchKind};
use crosstalk_spec::support::NonEmpty;

const SENTENCE: &str = "The vendor table has eleven overdue approvals in March";

struct Pair {
    builder: WorldBuilder,
    alice: AgentKey,
    bob: AgentKey,
}

fn pair() -> Pair {
    let mut builder = WorldBuilder::new(dataset(), WorldKey::new("w"));
    let alice = builder
        .agent("alice", Driven::Model, "m")
        .unwrap_or_else(|e| panic!("{e}"));
    let bob = builder
        .agent("bob", Driven::Model, "m")
        .unwrap_or_else(|e| panic!("{e}"));
    Pair {
        builder,
        alice,
        bob,
    }
}

impl Pair {
    /// Alice's exchange at `at`, answering with `response` to a task prompt.
    fn alice_writes(&mut self, at: u64, response: HashedMessage) {
        let request = vec![system("You are Alice."), user("Do the task and report.")];
        self.builder
            .exchange(draft(&self.alice, at, request, response))
            .unwrap_or_else(|e| panic!("{e}"));
    }

    /// Bob's exchange at `at` with `inputs` after his system prompt.
    fn bob_reads(&mut self, at: u64, inputs: Vec<HashedMessage>) {
        let mut request = vec![system("You are Bob.")];
        request.extend(inputs);
        self.builder
            .exchange(draft(&self.bob, at, request, says("Noted.")))
            .unwrap_or_else(|e| panic!("{e}"));
    }

    fn finish(self) -> (World, AgentKey, AgentKey) {
        (
            self.builder.finish(Coverage::Complete {
                tier: Tier::Construction,
            }),
            self.alice,
            self.bob,
        )
    }
}

fn send(content: &str) -> HashedMessage {
    let arguments = serde_json::json!({ "content": content, "message_type": "status" }).to_string();
    calls("call_1", "send_message", &arguments)
}

fn matched(world: &World) -> (ReferenceOutput, Vec<Prediction>) {
    let output = run(world, ReferenceConfig::default()).unwrap_or_else(|e| panic!("{e}"));
    let mut channels = std::collections::BTreeMap::new();
    for transmission in &output.transmissions {
        if let crosstalk_spec::derived::flow::transmission::Route::Channel(id) = transmission.route
        {
            channels.insert(id, output.channels.get(&id).cloned().unwrap_or_default());
        }
    }
    let agents = AgentMap::of_world(world);
    let mut detector = crosstalk_eval::pipeline::ReferenceDetector::default();
    let resolved = crosstalk_eval::pipeline::Detector::detect(&mut detector, world)
        .map(|detection| detection.resolved)
        .unwrap_or_else(|e| panic!("{e}"));
    assert_eq!(resolved, read_back(&output, &channels));
    let directory = WorldDirectory::new(world, &agents, &resolved);
    let predictions = output
        .transmissions
        .iter()
        .flat_map(|t| from_transmission(t, &directory).unwrap_or_else(|e| panic!("{e}")))
        .collect();
    (output, predictions)
}

fn delivered(content: &str) -> HashedMessage {
    user(&format!(
        "[round=1/5][from=alice][type=status]\n\n{content}"
    ))
}

#[test]
fn verbatim_delivery_is_an_exact_match() {
    let mut pair = pair();
    pair.alice_writes(1, send(SENTENCE));
    let message = delivered(SENTENCE);
    pair.bob_reads(2, vec![message.clone()]);
    let (world, alice, bob) = pair.finish();
    let (output, predictions) = matched(&world);
    assert_eq!(output.transmissions.len(), 1);
    assert!(!predictions.is_empty());
    for p in &predictions {
        assert_eq!((&p.from, &p.to), (&alice, &bob));
        assert_eq!(p.class, EvidenceClass::Exact);
        assert_eq!(p.carrier, CarrierKind::UserTurn);
        assert_eq!(p.route, PredictedRoute::Direct);
        assert_eq!(p.read_at.message(), message.hash());
        let text = p.read_at.text(message.message()).unwrap_or_default();
        assert!(SENTENCE.contains(&text), "{text:?}");
    }
}

#[test]
fn escaped_content_is_a_json_string_match() {
    let content = "First line of the \"audit\" result\nsecond line names the owner Omar";
    let mut pair = pair();
    pair.alice_writes(1, send(content));
    pair.bob_reads(2, vec![delivered(content)]);
    let (world, _, _) = pair.finish();
    let (output, predictions) = matched(&world);
    assert!(!predictions.is_empty());
    assert!(
        predictions
            .iter()
            .any(|p| p.class == EvidenceClass::Decoded)
    );
    assert!(
        kinds(&output).contains(&MatchKind::Decoded(NonEmpty::new(Codec::JsonString))),
        "{:?}",
        kinds(&output)
    );
    assert!(!kinds(&output).contains(&MatchKind::Normalized));
}

#[test]
fn case_and_whitespace_differences_are_normalized() {
    let mut pair = pair();
    pair.alice_writes(1, says(SENTENCE));
    pair.bob_reads(2, vec![user(&SENTENCE.to_uppercase().replace(' ', "   "))]);
    let (world, _, _) = pair.finish();
    let (_, predictions) = matched(&world);
    assert_eq!(predictions.len(), 1);
    assert_eq!(predictions[0].class, EvidenceClass::Normalized);
}

#[test]
fn double_escaped_relays_and_yaml_continuations_are_string_decoded() {
    let content = "Quote: \"the budget gap is 30000\" and the vendor risk is high today";
    let once = serde_json::to_string(content).unwrap_or_default();
    let twice = serde_json::to_string(&once).unwrap_or_default();
    let mut pair = pair();
    pair.alice_writes(1, send(content));
    pair.bob_reads(
        2,
        vec![result("call_9", &format!("{{\"raw_log\": {twice}}}"))],
    );
    let yaml = "note: \"Quote: \\\"the budget gap is 30000\\\" and the vendor \\\n    risk is high today\"";
    pair.bob_reads(3, vec![user(yaml)]);
    let (world, _, _) = pair.finish();
    let (output, predictions) = matched(&world);
    let exchanges: std::collections::BTreeSet<_> =
        predictions.iter().map(|p| p.reader_exchange).collect();
    assert_eq!(
        exchanges.len(),
        2,
        "found in the relayed log and in the YAML"
    );
    assert!(
        predictions
            .iter()
            .all(|p| p.class == EvidenceClass::Decoded)
    );
    let found = kinds(&output);
    assert!(found.contains(&MatchKind::Decoded(NonEmpty::new(Codec::JsonString))));
    assert!(found.contains(&MatchKind::Decoded(NonEmpty::new(Codec::YamlString))));
}

/// Every match kind the matcher reported.
fn kinds(output: &ReferenceOutput) -> Vec<MatchKind> {
    output
        .transmissions
        .iter()
        .filter_map(|t| t.state.confirmed())
        .flat_map(|c| c.content().iter().map(|m| m.kind().clone()))
        .collect()
}

/// The matcher's own spans and channels, as the seam should read them back.
fn read_back(
    output: &ReferenceOutput,
    channels: &std::collections::BTreeMap<
        crosstalk_spec::ids::ChannelId,
        Vec<crosstalk_spec::derived::flow::resource::Locator>,
    >,
) -> Resolved {
    use crosstalk_eval::predict::memory::{AccessTable, ChannelTable, SpanTable};
    use crosstalk_eval::predict::reads::{Reads, ready};
    use crosstalk_spec::interfaces::l4_provenance::IndexedSpan;
    let mut spans = SpanTable::default();
    for span in &output.spans {
        spans.insert(
            span.id,
            IndexedSpan {
                exchange: span.exchange,
                author: span.author,
                location: span.location,
            },
        );
    }
    let channels = ChannelTable::new(channels.clone());
    let reads = Reads {
        spans: &spans,
        accesses: &AccessTable::default(),
        channels: &channels,
    };
    ready(Resolved::gather(&output.transmissions, reads))
        .unwrap_or_else(|e| panic!("{e}"))
        .unwrap_or_else(|e| panic!("{e}"))
}

#[test]
fn hits_are_classed_by_the_weakest_transformation() {
    // A span as it sits inside JSON tool-call arguments: escaped.
    let span = r#"Say \"hello\" to\nthe Vendor Desk today"#;
    let plain = fold_plain(span);
    assert_eq!(
        classify(span, &plain, r#"\"hello\" to"#),
        Match(MatchKind::Exact)
    );
    assert_eq!(
        classify(span, &plain, "THE   vendor desk"),
        Match(MatchKind::Normalized),
        "case and whitespace only"
    );
    assert_eq!(
        classify(span, &plain, "\"hello\" to\nthe vendor"),
        Match(MatchKind::Decoded(NonEmpty::new(Codec::JsonString))),
        "delivered unescaped: one level of JSON string decoding"
    );
    let yaml = "a long note that \\\n    continues on the next line";
    assert_eq!(
        classify(
            yaml,
            &fold_plain(yaml),
            "a long note that continues on the next line"
        ),
        Match(MatchKind::Decoded(NonEmpty::new(Codec::YamlString))),
        "an escaped line break is YAML's"
    );
}

#[test]
fn string_codecs_are_told_apart_by_their_escapes() {
    assert_eq!(string_codec(r#"a \"quote\" and é\n"#), Codec::JsonString);
    assert_eq!(string_codec(r"a \\ backslash then x"), Codec::JsonString);
    assert_eq!(string_codec(r"bell \a and \x41"), Codec::YamlString);
    assert_eq!(string_codec("continued \\\n here"), Codec::YamlString);
    assert_eq!(fold_plain("  Mixed\tCASE  text "), "mixed case text ");
}

#[test]
fn encoded_content_is_a_decoded_match() {
    let base64 = STANDARD.encode(SENTENCE);
    let hex: String = SENTENCE.bytes().map(|b| format!("{b:02x}")).collect();
    let url: String = SENTENCE.replace(' ', "%20");
    for (encoded, codec) in [
        (base64, Codec::Base64),
        (hex, Codec::Hex),
        (url, Codec::UrlEncoding),
    ] {
        let mut pair = pair();
        pair.alice_writes(1, says(SENTENCE));
        pair.bob_reads(2, vec![user(&format!("payload: {encoded} end"))]);
        let (world, _, _) = pair.finish();
        let (output, predictions) = matched(&world);
        assert_eq!(predictions.len(), 1, "{codec:?}");
        assert_eq!(predictions[0].class, EvidenceClass::Decoded, "{codec:?}");
        let Some(transmission) = output.transmissions.first() else {
            panic!("transmission")
        };
        let Some(confirmed) = transmission.state.confirmed() else {
            panic!("confirmed")
        };
        let kind = format!("{:?}", confirmed.content().first().kind());
        assert!(kind.contains(&format!("{:?}", codec)), "{kind}");
    }
}

#[test]
fn text_the_writer_read_is_not_originated() {
    let shared = "SELECT request_id FROM procurement_requests WHERE amount >= 30000";
    let mut pair = pair();
    let request = vec![
        system("You are Alice."),
        user("go"),
        result("call_0", shared),
    ];
    pair.builder
        .exchange(draft(
            &pair.alice,
            1,
            request,
            says(&format!("I ran {shared} and it worked.")),
        ))
        .unwrap_or_else(|e| panic!("{e}"));
    pair.bob_reads(2, vec![result("call_5", shared)]);
    let (world, _, _) = pair.finish();
    let (output, predictions) = matched(&world);
    assert!(predictions.is_empty(), "{predictions:?}");
    assert!(
        !output.spans.is_empty(),
        "the novel framing is still a span"
    );
}

#[test]
fn an_agent_never_matches_itself() {
    let mut pair = pair();
    pair.alice_writes(1, says(SENTENCE));
    let request = vec![
        system("You are Alice."),
        user("Do the task and report."),
        says(SENTENCE),
        user(&format!("You said: {SENTENCE}")),
    ];
    pair.builder
        .exchange(draft(&pair.alice, 3, request, says("ok")))
        .unwrap_or_else(|e| panic!("{e}"));
    let (world, _, _) = pair.finish();
    let (_, predictions) = matched(&world);
    assert!(predictions.is_empty());
}

#[test]
fn short_messages_are_below_the_minimum() {
    let mut pair = pair();
    pair.alice_writes(1, send("OK, accept."));
    pair.bob_reads(2, vec![delivered("OK, accept.")]);
    let (world, _, _) = pair.finish();
    assert!(matched(&world).1.is_empty());
}

#[test]
fn only_new_inputs_are_scanned() {
    let mut pair = pair();
    pair.alice_writes(1, send(SENTENCE));
    let message = delivered(SENTENCE);
    pair.bob_reads(2, vec![message.clone()]);
    let request = vec![
        system("You are Bob."),
        message,
        says("Noted."),
        user("next round"),
    ];
    pair.builder
        .exchange(draft(&pair.bob, 3, request, says("done")))
        .unwrap_or_else(|e| panic!("{e}"));
    let (world, _, _) = pair.finish();
    let (output, predictions) = matched(&world);
    assert_eq!(output.transmissions.len(), 1);
    let readers: std::collections::BTreeSet<_> =
        predictions.iter().map(|p| p.reader_exchange).collect();
    assert_eq!(readers.len(), 1);
}

#[test]
fn thought_ids_and_signatures_are_ignored() {
    let id = "call_419236__thought__EsQGCsEGARFNMg9O9j8kD2jcYfVRHLjz4WqB7ShRIyNClWpHxs+Be8XJTaBDb8T34Vq1IBnFWZJEjEUKOVsQ0ODxIsE";
    let signature = STANDARD.encode(SENTENCE);
    let mut pair = pair();
    pair.alice_writes(1, says(&format!("tool ids seen: {id} and {id}")));
    pair.builder
        .exchange(draft(&pair.alice, 2, vec![user("x")], says(SENTENCE)))
        .unwrap_or_else(|e| panic!("{e}"));
    pair.bob_reads(
        3,
        vec![result(
            "call_7",
            &format!("{{\"id\": \"{id}\", \"thought_signature\": \"{signature}\"}}"),
        )],
    );
    let (world, _, _) = pair.finish();
    let (_, predictions) = matched(&world);
    assert!(predictions.is_empty(), "{predictions:?}");
}

#[test]
fn opaque_ranges_cover_ids_and_signature_values() {
    let text = r#"{"id": "call_1__thought__QUJD+/=", "signature": "c2lnbmF0dXJl\"x", "thought_signatures": ["a", "b"], "keep": "this"}"#;
    let ranges = opaque_ranges(text);
    let cut: Vec<&str> = ranges.iter().map(|&(s, e)| &text[s..e]).collect();
    assert_eq!(
        cut,
        vec![
            "call_1__thought__QUJD+/=",
            "\"c2lnbmF0dXJl\\\"x\"",
            "[\"a\", \"b\"]"
        ]
    );
    let kept: String = segments(text).into_iter().map(|(_, piece)| piece).collect();
    assert!(kept.contains("\"keep\": \"this\""));
    assert!(!kept.contains("__thought__"));
    let escaped = r#"[{\"thought_signature\": \"QUJDREVG\", \"tool\": \"x\"}]"#;
    let cut: Vec<&str> = opaque_ranges(escaped)
        .iter()
        .map(|&(s, e)| &escaped[s..e])
        .collect();
    assert_eq!(cut, vec![r#""QUJDREVG\""#]);
}

#[test]
fn channel_reads_route_through_the_resource() {
    let mut pair = pair();
    pair.alice_writes(1, says(SENTENCE));
    let read = calls(
        "call_r",
        "read_file",
        r#"{"path": "/shared/./notes/../notes.md"}"#,
    );
    let mut request = vec![
        system("You are Bob."),
        user("check the notes"),
        read.clone(),
    ];
    request.push(result("call_r", &format!("# notes\n{SENTENCE}\n")));
    pair.builder
        .exchange(draft(&pair.bob, 2, request, says("Noted.")))
        .unwrap_or_else(|e| panic!("{e}"));
    let (world, _, _) = pair.finish();
    let (output, predictions) = matched(&world);
    let resource = Locator::File {
        host: None,
        path: "/shared/notes.md".into(),
    };
    assert_eq!(predictions.len(), 1);
    assert_eq!(predictions[0].carrier, CarrierKind::ToolResult);
    assert_eq!(
        predictions[0].route,
        PredictedRoute::Channel {
            resources: vec![resource.clone()]
        }
    );
    assert_eq!(output.channels.values().next(), Some(&vec![resource]));
}

#[test]
fn tool_results_without_a_resource_are_direct() {
    let mut pair = pair();
    pair.alice_writes(1, says(SENTENCE));
    let request = vec![
        system("You are Bob."),
        calls("call_q", "query_database", r#"{"sql": "SELECT 1"}"#),
        result("call_q", SENTENCE),
    ];
    pair.builder
        .exchange(draft(&pair.bob, 2, request, says("Noted.")))
        .unwrap_or_else(|e| panic!("{e}"));
    let (world, _, _) = pair.finish();
    let (_, predictions) = matched(&world);
    assert_eq!(predictions.len(), 1);
    assert_eq!(predictions[0].route, PredictedRoute::Direct);
    assert_eq!(predictions[0].carrier, CarrierKind::ToolResult);
}

#[test]
fn hits_group_into_one_transmission_per_sender_and_route() {
    let other = "A second independent sentence about quarterly risk reviews";
    let mut pair = pair();
    pair.alice_writes(1, says(&format!("{SENTENCE}. {other}.")));
    pair.bob_reads(2, vec![user(SENTENCE), user(other)]);
    let (world, _, _) = pair.finish();
    let (output, predictions) = matched(&world);
    assert_eq!(output.transmissions.len(), 1);
    assert_eq!(predictions.len(), 2);
    assert!(
        predictions
            .iter()
            .all(|p| p.transmission == predictions[0].transmission)
    );
}

#[test]
fn runs_are_deterministic() {
    let build = || {
        let mut pair = pair();
        pair.alice_writes(1, send(SENTENCE));
        pair.bob_reads(2, vec![delivered(SENTENCE)]);
        pair.finish().0
    };
    let (a, b) = (build(), build());
    assert_eq!(matched(&a).1, matched(&b).1);
}

#[test]
fn fold_unescapes_folds_case_and_collapses_whitespace() {
    let folded = fold("A\\\\\\\"B\\nC  \t D\\u00e9\\ud83d\\ude00 x\\\n   y", 0);
    assert_eq!(folded.text, "a\"b c d\u{e9}\u{1F600} xy");
    assert_eq!(folded.raw_range(0, 1), Some((0, 1)));
    let raw = "Hé  WORLD";
    let folded = fold(raw, 10);
    assert_eq!(folded.text, "hé world");
    assert_eq!(folded.raw_range(4, 9), Some((15, 20)));
}

#[test]
fn shingles_hash_every_window_and_cover_runs() {
    let windows = shingles(b"abcdabcd", 4);
    assert_eq!(windows.len(), 5);
    assert_eq!(windows[0].0, windows[4].0);
    assert_ne!(windows[0].0, windows[1].0);
    assert!(shingles(b"abc", 4).is_empty());
    assert_eq!(covered(&[0, 1, 9], 4), vec![(0, 5), (9, 13)]);
}

#[test]
fn decoding_drops_noise() {
    assert!(decode_candidates("plain words only, nothing encoded here at all", 0, 16).is_empty());
    assert!(decode_candidates("verificationprocessing", 0, 16).is_empty());
    let found = decode_candidates(&format!("x {} y", STANDARD.encode(SENTENCE)), 5, 16);
    assert_eq!(found.len(), 1);
    assert_eq!(found[0].text, SENTENCE);
    assert_eq!(found[0].start, 7);
}

#[test]
fn urls_and_paths_normalize() {
    assert_eq!(
        parse_url("HTTPS://Example.COM:443/a/b?z=1&a=2#frag"),
        Some(Locator::Url {
            scheme: "https".into(),
            host: Host("example.com".into()),
            path: "/a/b".into(),
            query: Some("a=2&z=1".into()),
        })
    );
    assert_eq!(parse_url("ftp://x/y"), None);
    assert_eq!(normalize_path("/a//b/./c/../d"), "/a/b/d");
}

// --- boilerplate: text many agents originate independently ---

const TEMPLATE: &str = "Describe the new page here and add your notes below";

/// A sentence no other writer shares a 24-byte window with: every word
/// carries the writer's own tag.
fn unique_sentence(writer: usize) -> String {
    let tag: String = [writer / 26 % 26, writer % 26]
        .iter()
        .map(|&d| char::from(b'a' + u8::try_from(d).unwrap_or(0)))
        .collect();
    (0..8)
        .map(|word| format!("{tag}note{word}"))
        .collect::<Vec<_>>()
        .join(" ")
}

/// A world where `originators` agents each say `TEMPLATE` plus a unique
/// sentence, then a reader reads one tool result holding `copies` copies of
/// `TEMPLATE` and the first originator's unique sentence.
fn shared_template_world(originators: usize, copies: usize) -> (World, AgentKey, AgentKey) {
    let mut builder = WorldBuilder::new(dataset(), WorldKey::new("w"));
    let mut writers = Vec::new();
    for at in 0..originators {
        let agent = builder
            .agent(&format!("writer{at:03}"), Driven::Model, "m")
            .unwrap_or_else(|e| panic!("{e}"));
        writers.push(agent);
    }
    let reader = builder
        .agent("reader", Driven::Model, "m")
        .unwrap_or_else(|e| panic!("{e}"));
    for (at, writer) in writers.iter().enumerate() {
        let text = format!("{TEMPLATE}\n{}", unique_sentence(at));
        let request = vec![system("You are a writer."), user("Write the page.")];
        builder
            .exchange(draft(writer, at as u64 + 1, request, says(&text)))
            .unwrap_or_else(|e| panic!("{e}"));
    }
    let mut body = vec![TEMPLATE; copies].join("\n");
    body.push('\n');
    body.push_str(&unique_sentence(0));
    let request = vec![
        system("You are the reader."),
        calls(
            "read_1",
            "http_request",
            r#"{"method":"GET","url":"https://wiki.example/Hub"}"#,
        ),
        result("read_1", &body),
    ];
    builder
        .exchange(draft(
            &reader,
            originators as u64 + 1,
            request,
            says("Read it."),
        ))
        .unwrap_or_else(|e| panic!("{e}"));
    let first = writers.swap_remove(0);
    (
        builder.finish(Coverage::Complete {
            tier: Tier::Construction,
        }),
        first,
        reader,
    )
}

#[test]
fn text_many_agents_originate_is_boilerplate() {
    // Without a cutoff every copy matches every originator's span:
    // 74 originators x 400 copies = 29,600 matches in one read.
    let originators = ReferenceConfig::default().max_postings + 24;
    let copies = 400;
    let (world, first, reader) = shared_template_world(originators, copies);
    let config = ReferenceConfig::default();
    assert!(originators > config.max_postings);
    let (output, predictions) = matched(&world);
    assert!(
        output.matches <= 2,
        "boilerplate must not fan out: {} matches",
        output.matches
    );
    // The one sentence only the first writer originated is still found.
    assert_eq!(output.transmissions.len(), 1);
    assert!(
        predictions
            .iter()
            .all(|p| (&p.from, &p.to) == (&first, &reader))
    );
}

#[test]
fn text_a_few_agents_originate_still_matches_each() {
    // At or under the cutoff, each originator's span is still a candidate.
    let originators = ReferenceConfig::default().max_postings;
    let (world, _, _) = shared_template_world(originators, 1);
    let (output, _) = matched(&world);
    assert_eq!(output.transmissions.len(), originators);
}

#[test]
fn matches_grow_linearly_with_the_read_body() {
    // A large read body of boilerplate costs no more matches than a small one.
    let originators = ReferenceConfig::default().max_postings + 24;
    let (small, _, _) = shared_template_world(originators, 10);
    let (large, _, _) = shared_template_world(originators, 4_000);
    let (small, _) = matched(&small);
    let (large, _) = matched(&large);
    assert_eq!(small.matches, large.matches);
}

#[test]
fn unescape_once_undoes_exactly_one_string_level() {
    use crosstalk_eval::reference::fold::unescape_once;
    assert_eq!(
        unescape_once(r#"say \"hi\"\nthen go"#),
        "say \"hi\"\nthen go"
    );
    // An escaped backslash before `n` is a backslash and an `n`, never a
    // line break: one level only.
    assert_eq!(unescape_once(r"a \\n b"), r"a \n b");
    assert_eq!(unescape_once(r#"\\\"quoted\\\""#), r#"\"quoted\""#);
    assert_eq!(unescape_once(r"café 😀"), "café 😀");
    assert_eq!(unescape_once("long \\\n    line"), "long line");
    assert_eq!(
        unescape_once(r"bell \x41 and \q and end\"),
        r"bell A and \q and end\"
    );
}

#[test]
fn two_string_levels_are_out_of_reach() {
    use crosstalk_eval::reference::classify::Classified;
    // The span holds the text raw; the read holds it escaped twice.
    let span = r#"She said "move the meeting" and "bring the ledger" today"#;
    let plain = fold_plain(span);
    // One JSON string level: the contents of the literal, no quotes.
    let escape = |text: &str| {
        let quoted = serde_json::to_string(text).unwrap_or_default();
        quoted[1..quoted.len() - 1].to_owned()
    };
    let once = escape(span);
    let twice = escape(&once);
    assert_eq!(
        classify(span, &plain, &once),
        Match(MatchKind::Decoded(NonEmpty::new(Codec::JsonString))),
        "one level: in reach"
    );
    assert_eq!(
        classify(span, &plain, &twice),
        Classified::TwoStringLevels,
        "two levels: no spec decoder undoes them"
    );
    // A span written inside JSON arguments (escaped once) read escaped
    // twice is one level apart: in reach.
    assert_eq!(
        classify(&once, &fold_plain(&once), &twice),
        Match(MatchKind::Decoded(NonEmpty::new(Codec::JsonString)))
    );
}

#[test]
fn the_matcher_reports_no_match_two_string_levels_apart() {
    // Quotes every few words, so every 24-byte window holds an escape.
    let content = r#""alpha" "bravo" "charlie" "delta" "echo" "foxtrot" "golf" "hotel""#;
    let once = serde_json::to_string(content).unwrap_or_default();
    let twice = serde_json::to_string(&once).unwrap_or_default();
    let mut pair = pair();
    pair.alice_writes(1, says(content));
    pair.bob_reads(2, vec![result("call_9", &format!("{{\"log\": {twice}}}"))]);
    let (world, _, _) = pair.finish();
    let (output, predictions) = matched(&world);
    assert!(predictions.is_empty(), "{predictions:?}");
    assert!(
        output.out_of_reach > 0,
        "the fold found it; the spec cannot"
    );

    // One level apart, the same text is a JSON string match.
    let mut pair = self::pair();
    pair.alice_writes(1, says(content));
    pair.bob_reads(2, vec![result("call_9", &format!("{{\"log\": {once}}}"))]);
    let (world, _, _) = pair.finish();
    let (output, predictions) = matched(&world);
    assert!(!predictions.is_empty());
    assert_eq!(output.out_of_reach, 0);
    assert!(
        kinds(&output)
            .iter()
            .all(|kind| *kind == MatchKind::Decoded(NonEmpty::new(Codec::JsonString))),
        "{:?}",
        kinds(&output)
    );
}

#[test]
fn the_default_boilerplate_cutoff_is_l4s() {
    // L4's `IndexSettings::default().cutoff()` (crates/provenance): a
    // fingerprint in more than 50 live texts is boilerplate.
    assert_eq!(ReferenceConfig::default().max_postings, 50);
    assert_eq!(crosstalk_eval::reference::MAX_POSTINGS, 50);
}

#[test]
fn yaml_single_quotes_and_bridged_ranges_stay_in_reach() {
    use crosstalk_eval::reference::classify::Classified;
    let span = "use the subject 'All messages with Travel Agency' and the body";
    let plain = fold_plain(span);
    // A YAML single-quoted scalar doubles the quote: one YAML level.
    assert_eq!(
        classify(
            span,
            &plain,
            "use the subject ''All\n  messages with Travel Agency'' and the body"
        ),
        Match(MatchKind::Decoded(NonEmpty::new(Codec::YamlString)))
    );
    // A range the fold joined across one character neither side shares is
    // not two string levels: no reading explains it, so it stays in reach.
    let bridged = classify(
        span,
        &plain,
        "use the subject 'All messages with Travel Agency'X and the body",
    );
    assert_ne!(bridged, Classified::TwoStringLevels);
}
