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
use crate::config::{IndexSettings, ProvenanceConfig, ReaderOutputRules, ShortSpans};
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

/// `provenance.index.remainder-around-relay-matchable`: Bob's reply quotes
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
    assert!(
        matches
            .iter()
            .any(|stored| stored.content.origin() == forwarded.span.id),
        "the forwarded quote is matched too: {}",
        brief_matches(&matches)
    );
}

/// `provenance.match.short-span-exact`: a whole text part of 16 to 46
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
    assert!(tiny.len() < 16);
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

/// `provenance.match.reader-output-strict`, frequency: text observed in
/// more texts than the reader-output cutoff (5) but fewer than the index
/// cutoff (50) yields no `ReaderOutput` match; a tool result still does.
/// Lowering the rules restores the match.
#[tokio::test]
async fn frequent_reader_output_yields_no_match_but_tool_results_do() {
    let text = super::fixtures::sentence("saffron");
    let run = |rules: ReaderOutputRules| {
        let text = text.clone();
        async move {
            let mut world = World::new(config().with_reader_output(rules));
            let (a, b, c) = (world.agent(), world.agent(), world.agent());
            let origin = super::scenarios::originate(&mut world, a, &text, 1).await;
            for n in 0..6 {
                let reader = world.agent();
                world
                    .run(Turn::new(reader, at(2)).input(user_text(&format!("note {n}: {text}"))))
                    .await;
            }
            let wrote = world
                .run(Turn::new(b, at(3)).output(assistant_text(&text)))
                .await;
            let output = world.matches_of(wrote.exchange);
            let read = world
                .run(Turn::new(c, at(4)).input(tool_result("call_1", &text)))
                .await;
            let tool = world.matches_of(read.exchange);
            assert!(
                tool.iter()
                    .any(|stored| stored.content.origin() == origin.span.id),
                "{}",
                brief_matches(&tool)
            );
            output
                .iter()
                .any(|stored| stored.content.carrier() == &Carrier::ReaderOutput)
        }
    };
    assert!(!run(ReaderOutputRules::default()).await);
    assert!(run(ReaderOutputRules::new(8, 50)).await);
}

/// `provenance.index.forwarded-indexed`: an agent copies a document from
/// its own tool result into a message; a peer's later read of the message
/// matches the forwarding agent's span, whose state stays `Relayed` from
/// the input, and `SpanIndex::spans` reads it back. After retention it is
/// evicted like an originated span, and still read back.
#[tokio::test]
async fn forwarded_text_is_indexed_under_the_forwarder() {
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
