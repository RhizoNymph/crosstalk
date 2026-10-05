//! Match quality on the default shingles (k = 32, w = 16), after the first
//! live evaluation: originated text around a forwarded run, short whole
//! values, the stricter reader-output rules, and forwarded text.

use crosstalk_spec::batch::IdBatch;
use crosstalk_spec::derived::provenance::matching::{Carrier, MatchKind};
use crosstalk_spec::derived::provenance::span::{Origin, RelaySource, SpanState};
use crosstalk_spec::interfaces::l4_provenance::SpanIndex;
use crosstalk_testkit::build::message::{
    assistant, assistant_text, tool_call, tool_result, user_text,
};

use super::fixtures::{RETENTION, Turn, World, at, brief_matches, brief_spans, config};
use crate::config::{IndexSettings, ProvenanceConfig, ShortSpans};
use crate::store::{Forwarding, ProvenanceStore, SpanRecord};

/// The default shingles with the test retention and `cutoff`.
fn real(cutoff: u64) -> ProvenanceConfig {
    ProvenanceConfig::default()
        .with_index(IndexSettings::single_node(cutoff, RETENTION).expect("valid index settings"))
}

async fn spans_of(world: &World, exchange: crosstalk_spec::ids::ExchangeId) -> Vec<SpanRecord> {
    world
        .store
        .exchange_spans(exchange)
        .await
        .expect("spans read")
}

fn text_of(output: &str, record: &SpanRecord) -> String {
    let range = record.span.location.range;
    output[range.start() as usize..range.end() as usize].to_owned()
}

/// `provenance.index.remainder-around-relay-matchable` (forwarding off, the
/// default): Bob's reply quotes
/// a phrase of Alice's message in its middle (SALT exchange
/// `01KDVDNA0C9RCBYW8FYCB7ZR3N`). The quote is forwarded, the remainder
/// after it is shorter than a shingle, and Alice reading the whole reply
/// still matches Bob's originated remainder.
#[tokio::test]
async fn originated_remainder_around_a_relayed_middle_matches() {
    let mut world = World::new(real(50));
    let (alice, bob) = (world.agent(), world.agent());
    let asked = "Bob, I have received your raw_log for task 3-14 and will check it tonight.";
    world
        .run(Turn::new(alice, at(1)).output(assistant_text(asked)))
        .await;
    let reply =
        "Thanks, Alice. I have received your raw_log for task 3-15 and will review it as well.";
    assert!((47..=300).contains(&reply.len()));
    let replied = world
        .run(
            Turn::new(bob, at(2))
                .input(user_text(asked))
                .output(assistant_text(reply)),
        )
        .await;
    let spans = spans_of(&world, replied.exchange).await;
    let forwarded = spans
        .iter()
        .find(|record| record.span.state.is_forwarded())
        .unwrap_or_else(|| panic!("a forwarded quote: {}", brief_spans(&spans)));
    let remainder = spans
        .iter()
        .find(|record| {
            record.span.state.origin() == Some(Origin::Originated)
                && record.span.location.range.start() >= forwarded.span.location.range.end()
        })
        .unwrap_or_else(|| panic!("an originated remainder: {}", brief_spans(&spans)));
    let remainder_text = text_of(reply, remainder);
    assert!(
        remainder_text.len() < 32,
        "the remainder is shorter than a shingle: {remainder_text:?}"
    );

    let read = world
        .run(Turn::new(alice, at(3)).input(user_text(reply)))
        .await;
    let matches = world.matches_of(read.exchange);
    let on_remainder = matches
        .iter()
        .find(|stored| stored.content.origin() == remainder.span.id)
        .unwrap_or_else(|| panic!("no match on the remainder: {}", brief_matches(&matches)));
    assert_eq!(on_remainder.content.origin_agent(), bob);
    assert_eq!(on_remainder.content.carrier(), &Carrier::UserTurn);
    // Forwarding is off by default: the quote itself is not indexed.
    assert!(
        matches
            .iter()
            .all(|stored| stored.content.origin() != forwarded.span.id),
        "{}",
        brief_matches(&matches)
    );
}

/// `provenance.match.short-span-exact`: a whole text part of 24 to 46
/// characters is matched by its exact hash where it is read whole, as
/// `Exact` verbatim and `Normalized` with other case or spacing.
#[tokio::test]
async fn short_whole_value_matches_exact_or_normalized() {
    let mut world = World::new(real(50));
    let (a, b, c) = (world.agent(), world.agent(), world.agent());
    let note = "Note for my partner: IRxSBdcNMr";
    assert!(note.len() < 32);
    world
        .run(Turn::new(a, at(1)).output(assistant_text(note)))
        .await;
    let verbatim = world
        .run(
            Turn::new(b, at(2)).input(tool_result("call_1", &format!("inbox: \"{note}\" (1 new)"))),
        )
        .await;
    let matches = world.matches_of(verbatim.exchange);
    assert_eq!(matches.len(), 1, "{}", brief_matches(&matches));
    assert_eq!(matches[0].content.kind(), &MatchKind::Exact);
    assert_eq!(matches[0].content.origin_agent(), a);
    let read = matches[0].content.read_at().range;
    assert_eq!(read.len().get() as usize, note.len());

    let folded = world
        .run(Turn::new(c, at(3)).input(user_text("NOTE FOR MY   partner: irxsbdcnmr")))
        .await;
    let matches = world.matches_of(folded.exchange);
    assert_eq!(matches.len(), 1, "{}", brief_matches(&matches));
    assert_eq!(matches[0].content.kind(), &MatchKind::Normalized);
}

/// A short value glued to a longer word is not a token run, and a value
/// that is only part of a longer text part has no short-span hash.
#[tokio::test]
async fn short_values_match_only_whole_and_as_token_runs() {
    let mut world = World::new(real(50));
    let (a, b) = (world.agent(), world.agent());
    let note = "ledger rollback at 0400 UTC";
    world
        .run(Turn::new(a, at(1)).output(assistant_text(note)))
        .await;
    let glued = world
        .run(Turn::new(b, at(2)).input(user_text("unledger rollback at 0400 UTCx")))
        .await;
    assert!(world.matches_of(glued.exchange).is_empty());

    let longer = "Plan: ledger rollback at 0400 UTC, then a full reconciliation pass.";
    world
        .run(Turn::new(a, at(3)).output(assistant_text(longer)))
        .await;
    let part = world
        .run(Turn::new(b, at(4)).input(user_text("then a full reconciliation pass")))
        .await;
    assert!(
        world.matches_of(part.exchange).is_empty(),
        "{}",
        brief_matches(&world.matches_of(part.exchange))
    );
}

/// `provenance.match.short-span-exact`: one whole string value of a tool
/// call's arguments takes the path; the locator next to it does not.
#[tokio::test]
async fn short_argument_value_matches() {
    let mut world = World::new(real(50));
    let (a, b) = (world.agent(), world.agent());
    let value = "deploy window moved to friday";
    let call = tool_call(
        "call_1",
        "send_message",
        &serde_json::json!({ "to": "bob", "body": value, "url": "https://chat.example.com/x" }),
    );
    world
        .run(Turn::new(a, at(1)).output(assistant(vec![call])))
        .await;
    let read = world
        .run(Turn::new(b, at(2)).input(tool_result("call_9", &format!("[alice] {value}"))))
        .await;
    let matches = world.matches_of(read.exchange);
    assert_eq!(matches.len(), 1, "{}", brief_matches(&matches));
    assert_eq!(matches[0].content.kind(), &MatchKind::Exact);
}

/// Below the floor nothing is matched; the floor is configurable.
#[tokio::test]
async fn short_floor_is_respected_and_configurable() {
    let mut world = World::new(real(50));
    let (a, b) = (world.agent(), world.agent());
    let tiny = "see you at noon";
    assert!(tiny.len() < 24);
    let wrote = world
        .run(Turn::new(a, at(1)).output(assistant_text(tiny)))
        .await;
    assert!(spans_of(&world, wrote.exchange).await.is_empty());
    let read = world.run(Turn::new(b, at(2)).input(user_text(tiny))).await;
    assert!(world.matches_of(read.exchange).is_empty());

    let strict = real(50).with_short_spans(ShortSpans::new(30, 46).expect("a range"));
    let mut world = World::new(strict);
    let (a, b) = (world.agent(), world.agent());
    let note = "deploy window moved to friday";
    assert!(note.len() < 30);
    world
        .run(Turn::new(a, at(1)).output(assistant_text(note)))
        .await;
    let read = world.run(Turn::new(b, at(2)).input(user_text(note))).await;
    assert!(world.matches_of(read.exchange).is_empty());
}

/// The boilerplate cutoff applies to short values: once more texts than
/// the cutoff were the same whole value, it is `Common` and not matched.
#[tokio::test]
async fn short_values_above_the_cutoff_are_common() {
    let mut world = World::new(real(1));
    let phrase = "Sounds good to me, thanks!";
    for n in 0..2 {
        let agent = world.agent();
        world
            .run(Turn::new(agent, at(1 + n)).output(assistant_text(phrase)))
            .await;
    }
    let last = world.agent();
    let wrote = world
        .run(Turn::new(last, at(3)).output(assistant_text(phrase)))
        .await;
    let spans = spans_of(&world, wrote.exchange).await;
    assert_eq!(spans.len(), 1, "{}", brief_spans(&spans));
    assert_eq!(spans[0].span.state, SpanState::Common);
    let reader = world.agent();
    let read = world
        .run(Turn::new(reader, at(4)).input(user_text(phrase)))
        .await;
    assert!(
        world.matches_of(read.exchange).is_empty(),
        "{}",
        brief_matches(&world.matches_of(read.exchange))
    );
}

/// `provenance.match.reader-output-strict`, length: another agent's text
/// of fewer than 64 normalized characters in a reader's output is relayed
/// from its span but yields no `ReaderOutput` match, while a tool result
/// carrying the same text still matches.
#[tokio::test]
async fn short_reader_output_yields_no_match_but_tool_results_do() {
    let mut world = World::new(config());
    let (a, b, c) = (world.agent(), world.agent(), world.agent());
    let text = "t_date BETWEEN '2025-01-01' AND '2025-06-30' AND x";
    assert!(text.len() < 64);
    let origin = super::scenarios::originate(&mut world, a, text, 1).await;
    let wrote = world
        .run(Turn::new(b, at(2)).output(assistant_text(&format!("SELECT * WHERE {text}"))))
        .await;
    assert!(world.matches_of(wrote.exchange).is_empty());
    let spans = spans_of(&world, wrote.exchange).await;
    assert!(
        spans.iter().any(|r| r.span.state
            == SpanState::Relayed {
                source: RelaySource::Span(origin.span.id)
            }),
        "{}",
        brief_spans(&spans)
    );
    let read = world
        .run(Turn::new(c, at(3)).input(tool_result("call_1", text)))
        .await;
    let matches = world.matches_of(read.exchange);
    assert_eq!(matches.len(), 1, "{}", brief_matches(&matches));
    assert_eq!(matches[0].content.origin(), origin.span.id);
}

/// `provenance.match.cross-agent-spread` with
/// `provenance.match.reader-output-strict`, a broadcast: one agent
/// originates a distinctive message of 64 characters or more; five agents
/// later (beyond the spread window) reproduce it with no observed read.
/// Each copy yields a `ReaderOutput` match to the first writer.
#[tokio::test]
async fn broadcast_copies_match_the_first_writer() {
    let mut world = World::new(real(50));
    let first = world.agent();
    let message =
        "Meet at the old boathouse at dusk; bring the ledger and the brass key, tell no one else.";
    assert!(message.len() >= 64);
    let origin = super::scenarios::originate(&mut world, first, message, 1).await;
    for n in 0..5u64 {
        let copier = world.agent();
        let wrote = world
            .run(
                Turn::new(copier, at(120 + 60 * n))
                    .input(user_text("carry on with your task"))
                    .output(assistant_text(message)),
            )
            .await;
        let matches = world.matches_of(wrote.exchange);
        assert!(
            matches
                .iter()
                .any(|stored| stored.content.origin() == origin.span.id
                    && stored.content.carrier() == &Carrier::ReaderOutput
                    && stored.content.origin_agent() == first),
            "copy {n}: {}",
            brief_matches(&matches)
        );
    }
}

/// `provenance.index.forwarded-indexed`: an agent copies a document from
/// its own tool result into a message; a peer's later read of the message
/// matches the forwarding agent's span, whose state stays `Relayed` from
/// the input, and `SpanIndex::spans` reads it back. After retention it is
/// evicted like an originated span, and still read back.
#[tokio::test]
async fn forwarded_text_is_indexed_under_the_forwarder() {
    let mut world = World::new(real(50).with_forwarding(true));
    let (a, b) = (world.agent(), world.agent());
    let document =
        "Quarterly figures: revenue rose eleven percent while support tickets fell by a third.";
    let forwarded = world
        .run(
            Turn::new(a, at(1))
                .input(tool_result("call_1", document))
                .output(assistant_text(&format!("Forwarding this: {document}"))),
        )
        .await;
    let spans = spans_of(&world, forwarded.exchange).await;
    let span = spans
        .iter()
        .find(|record| record.span.state.is_forwarded())
        .unwrap_or_else(|| panic!("a forwarded span: {}", brief_spans(&spans)))
        .clone();
    assert!(matches!(span.forward, Some(Forwarding::Indexed { .. })));
    assert!(span.index_seq.is_some());

    let read = world
        .run(Turn::new(b, at(2)).input(user_text(&format!("From a: Forwarding this: {document}"))))
        .await;
    let matches = world.matches_of(read.exchange);
    let found = matches
        .iter()
        .find(|stored| stored.content.origin() == span.span.id)
        .unwrap_or_else(|| {
            panic!(
                "no match on the forwarded span: {}",
                brief_matches(&matches)
            )
        });
    assert_eq!(found.content.origin_agent(), a);
    let after = world
        .store
        .span(span.span.id)
        .await
        .expect("read")
        .expect("stored");
    assert!(after.span.state.is_forwarded(), "the state stays relayed");

    let batch = IdBatch::new([span.span.id]).expect("one id");
    let indexed = SpanIndex::spans(&world.store, &batch)
        .await
        .expect("span index read");
    assert_eq!(indexed.get(&span.span.id).map(|s| s.author), Some(a));

    let later = at(1 + RETENTION.as_secs() + 10);
    world.engine.expire(later).await.expect("expiry");
    let expired = world
        .store
        .span(span.span.id)
        .await
        .expect("read")
        .expect("stored");
    assert!(matches!(expired.forward, Some(Forwarding::Expired { .. })));
    let c = world.agent();
    let late = world
        .run(Turn::new(c, later).input(user_text(document)))
        .await;
    assert!(world.matches_of(late.exchange).is_empty());
    let indexed = SpanIndex::spans(&world.store, &batch)
        .await
        .expect("span index read");
    assert!(
        indexed.contains_key(&span.span.id),
        "the record outlives eviction"
    );
}

/// Text relayed from another agent's indexed span is not indexed again,
/// and the span index does not return it.
#[tokio::test]
async fn span_relays_are_not_indexed() {
    let mut world = World::new(config());
    let (a, b) = (world.agent(), world.agent());
    let text = super::fixtures::sentence("cobalt");
    super::scenarios::originate(&mut world, a, &text, 1).await;
    let wrote = world
        .run(Turn::new(b, at(2)).output(assistant_text(&text)))
        .await;
    let spans = spans_of(&world, wrote.exchange).await;
    let relayed = spans
        .iter()
        .find(|r| {
            matches!(
                r.span.state,
                SpanState::Relayed {
                    source: RelaySource::Span(_)
                }
            )
        })
        .unwrap_or_else(|| panic!("{}", brief_spans(&spans)));
    assert_eq!(relayed.index_seq, None);
    assert_eq!(relayed.forward, None);
    let batch = IdBatch::new([relayed.span.id]).expect("one id");
    assert!(
        SpanIndex::spans(&world.store, &batch)
            .await
            .expect("read")
            .is_empty()
    );
}

/// Forwarding off (the default): a forwarded span is neither indexed nor
/// returned by `SpanIndex::spans`, and a peer's read of it matches nothing.
#[tokio::test]
async fn forwarded_text_is_not_indexed_when_forwarding_is_off() {
    let mut world = World::new(real(50));
    let (a, b) = (world.agent(), world.agent());
    let document =
        "Quarterly figures: revenue rose eleven percent while support tickets fell by a third.";
    let forwarded = world
        .run(
            Turn::new(a, at(1))
                .input(tool_result("call_1", document))
                .output(assistant_text(&format!("Forwarding this: {document}"))),
        )
        .await;
    let spans = spans_of(&world, forwarded.exchange).await;
    let span = spans
        .iter()
        .find(|record| record.span.state.is_forwarded())
        .unwrap_or_else(|| panic!("a forwarded span: {}", brief_spans(&spans)))
        .clone();
    assert_eq!(span.forward, None);
    assert_eq!(span.index_seq, None);
    let read = world
        .run(Turn::new(b, at(2)).input(user_text(document)))
        .await;
    assert!(
        world
            .matches_of(read.exchange)
            .iter()
            .all(|stored| stored.content.origin() != span.span.id)
    );
    let batch = IdBatch::new([span.span.id]).expect("one id");
    assert!(
        SpanIndex::spans(&world.store, &batch)
            .await
            .expect("read")
            .is_empty()
    );
}

/// `provenance.match.reader-output-strict`, shaped like the node0 bench's
/// 749 false positives: unrelated agents' outputs sharing three phrase
/// fragments of 34 to 46 bytes give no `ReaderOutput` match.
#[tokio::test]
async fn shared_phrase_fragments_give_no_reader_output_match() {
    let mut world = World::new(real(50));
    let (a, b) = (world.agent(), world.agent());
    let fragments = [
        "start by exploring the repository structure",
        "run the full test suite before committing",
        "check the configuration file for typos now",
    ];
    for fragment in fragments {
        assert!((34..=46).contains(&fragment.len()), "{fragment}");
    }
    let first = format!(
        "Plan for the ledger fix: {}. Then patch the parser; {}, and finally {}.",
        fragments[0], fragments[1], fragments[2]
    );
    let second = format!(
        "Onboarding notes for the wiki: {}; afterwards draft the summary. Also {}. Lastly {}!",
        fragments[0], fragments[1], fragments[2]
    );
    world
        .run(Turn::new(a, at(1)).output(assistant_text(&first)))
        .await;
    let wrote = world
        .run(Turn::new(b, at(2)).output(assistant_text(&second)))
        .await;
    let matches = world.matches_of(wrote.exchange);
    assert!(
        matches
            .iter()
            .all(|stored| stored.content.carrier() != &Carrier::ReaderOutput),
        "{}",
        brief_matches(&matches)
    );
}

/// `provenance.match.cross-agent-spread`, boilerplate: five agents
/// originate the same template within the spread window (no clear first
/// writer), and a reader of it gets no match. The same template written by
/// one agent and copied by others later, beyond the window, still matches
/// its first writer.
#[tokio::test]
async fn template_originated_together_is_not_matched_but_a_broadcast_is() {
    let template = "Please review the plan now!";
    let mut world = World::new(real(50));
    for n in 0..5u64 {
        let agent = world.agent();
        world
            .run(Turn::new(agent, at(1 + n)).output(assistant_text(template)))
            .await;
    }
    let reader = world.agent();
    let read = world
        .run(Turn::new(reader, at(10)).input(user_text(template)))
        .await;
    let matches = world.matches_of(read.exchange);
    assert!(matches.is_empty(), "{}", brief_matches(&matches));

    let mut world = World::new(real(50));
    let first = world.agent();
    world
        .run(Turn::new(first, at(1)).output(assistant_text(template)))
        .await;
    for n in 0..5u64 {
        let agent = world.agent();
        world
            .run(Turn::new(agent, at(200 + n)).output(assistant_text(template)))
            .await;
    }
    let reader = world.agent();
    let read = world
        .run(Turn::new(reader, at(300)).input(user_text(template)))
        .await;
    let matches = world.matches_of(read.exchange);
    assert!(
        matches
            .iter()
            .any(|stored| stored.content.origin_agent() == first),
        "{}",
        brief_matches(&matches)
    );
}

/// The node0 bench's shape: short template fragments (34 to 46 bytes) that
/// several unrelated agents write within the window give no match of any
/// carrier to a reader holding them.
#[tokio::test]
async fn short_template_fragments_from_unrelated_agents_give_no_match() {
    let fragments = [
        "start by exploring the repository structure",
        "run the full test suite before committing",
        "check the configuration file for typos now",
    ];
    let mut world = World::new(real(50));
    for n in 0..4u64 {
        let agent = world.agent();
        for (m, fragment) in fragments.iter().enumerate() {
            world
                .run(Turn::new(agent, at(1 + n * 3 + m as u64)).output(assistant_text(fragment)))
                .await;
        }
    }
    let reader = world.agent();
    for fragment in fragments {
        let read = world
            .run(Turn::new(reader, at(30)).input(user_text(fragment)))
            .await;
        let matches = world.matches_of(read.exchange);
        assert!(
            matches.is_empty(),
            "{fragment}: {}",
            brief_matches(&matches)
        );
    }
}
