//! The match rules on Postgres agree with the memory reference model.
//!
//! Every scenario runs twice, from the same seed: over `crosstalk-memory`'s
//! reference index and [`MemoryProvenanceStore`](crate::store::MemoryProvenanceStore),
//! then over [`PgFingerprintIndex`](crate::index::PgFingerprintIndex) and
//! [`PgProvenanceStore`](crate::store::PgProvenanceStore) on a freshly
//! emptied database. Each run checks the rule's outcome (as the unit
//! evidence does), and the two runs must leave identical transcripts: every
//! turn's outcome with its envelopes, every exchange's record, status,
//! spans (with their final states and index sequences) and matches, the
//! `SpanIndex` answer for every span, and the index watermark.
//!
//! The rules read the stores in ways a single engine script does not: the
//! spread rule counts copies (`ProvenanceStore::relays`) and token
//! observations (`FingerprintIndex::frequency`), inherited fragments read
//! the origin's request list, the rarity bound reads `Propagated` hit
//! counts, the nearer-source rules read bodies and forwards, and shadowed
//! fragments compare counted coverage across agents.

use std::fmt::Debug;

use crosstalk_spec::batch::IdBatch;
use crosstalk_spec::derived::provenance::matching::{Carrier, MatchKind};
use crosstalk_spec::derived::provenance::span::{Origin, RelaySource, SpanState};
use crosstalk_spec::ids::{AgentId, ExchangeId, SpanId};
use crosstalk_spec::interfaces::l4_provenance::{FingerprintIndex, SpanIndex};
use crosstalk_spec::observed::message::{AssistantPart, Text};
use crosstalk_testkit::build::message::{
    assistant, assistant_text, system_text, tool_call, tool_result, user_text,
};
use serde_json::json;

use super::{database, pg_world_on, truncate};
use crate::config::{IndexSettings, ProvenanceConfig};
use crate::semantic::DisabledSemanticMatcher;
use crate::store::{ProvenanceStore, StoredMatch};
use crate::tests::bench_boilerplate::{
    BODY, PAGES, TEMPLATE_SENTENCE, VOCABULARY, read_task, system, write_task,
};
use crate::tests::bench_channel_template::{CACHE_V1, CACHE_V2, CACHE_V3};
use crate::tests::bench_verbatim_template::{CACHE_10, earlier_fills};
use crate::tests::fixtures::{RETENTION, Turn, World, at, brief_matches, config};
use crate::tests::match_quality::COMMON;
use crate::tests::nearer_source::{ALICE_SQL, BILL, BOB_SQL, ECHO, HEAD, ROWS, SCHEMA, get_log};
use crate::tests::scenarios::originate;

type W<I, S> = World<I, DisabledSemanticMatcher, S>;

/// The default shingles (k = 32, w = 16) with the test retention.
fn real() -> ProvenanceConfig {
    ProvenanceConfig::default()
        .with_index(IndexSettings::single_node(50, RETENTION).expect("valid index settings"))
}

/// Everything a world left behind, one line per fact, in turn order.
async fn transcript<I, S>(world: &W<I, S>) -> Vec<String>
where
    I: FingerprintIndex + Send + Sync,
    S: ProvenanceStore + SpanIndex + Clone + Send + Sync,
{
    let mut lines = Vec::new();
    let mut spans: Vec<SpanId> = Vec::new();
    for ran in &world.ran {
        let (record, status) = world
            .store
            .exchange(ran.exchange)
            .await
            .expect("exchange read")
            .expect("exchange recorded");
        let started = world
            .store
            .started_at(ran.exchange)
            .await
            .expect("start read");
        lines.push(format!(
            "exchange {:?} started {:?} ({started:?}) request {:?} output {:?} status {status:?}",
            ran.exchange, record.started_at, record.request, record.output
        ));
        lines.push(format!("  processed {:?}", ran.processed));
        for span in world
            .store
            .exchange_spans(ran.exchange)
            .await
            .expect("spans read")
        {
            spans.push(span.span.id);
            lines.push(format!("  span {span:?}"));
        }
        let ids: Vec<SpanId> = spans[spans.len()
            - world
                .store
                .exchange_spans(ran.exchange)
                .await
                .expect("spans read")
                .len()..]
            .to_vec();
        for coincidence in world
            .store
            .coincident_sources(&ids)
            .await
            .expect("coincidences read")
        {
            lines.push(format!("  coincidence {coincidence:?}"));
        }
        for stored in world
            .store
            .exchange_matches(ran.exchange)
            .await
            .expect("matches read")
        {
            lines.push(format!("  match {stored:?}"));
        }
    }
    for chunk in spans.chunks(IdBatch::<SpanId>::MAX) {
        let batch = IdBatch::new(chunk.iter().copied()).expect("a batch");
        let indexed = SpanIndex::spans(&world.store, &batch)
            .await
            .expect("span index read");
        for (id, span) in indexed {
            lines.push(format!("indexed {id:?} {span:?}"));
        }
    }
    lines.push(format!(
        "watermark {}",
        world.store.index_watermark().await.expect("watermark read")
    ));
    lines
}

/// Fail on the first line where `got` leaves `expected`.
fn assert_agrees<T: Debug + PartialEq>(script: &str, expected: &[T], got: &[T]) {
    for (n, (want, have)) in expected.iter().zip(got).enumerate() {
        assert_eq!(
            have, want,
            "{script}: Postgres leaves the memory model at line {n}"
        );
    }
    assert_eq!(
        got.len(),
        expected.len(),
        "{script}: transcripts of different lengths"
    );
}

/// Run `$script` over memory, then over Postgres (emptied first), with
/// `$config`, and require equal transcripts.
macro_rules! agree {
    ($db:expr, $config:expr, $script:ident) => {{
        let mut memory = World::new($config);
        $script(&mut memory).await;
        let expected = transcript(&memory).await;
        truncate($db.pool()).await;
        let pool = super::lazy_pool($db);
        let mut pg = pg_world_on(pool.clone(), $config);
        $script(&mut pg).await;
        let got = transcript(&pg).await;
        drop(pg);
        pool.close().await;
        assert_agrees(stringify!($script), &expected, &got);
    }};
}

async fn matches_of<I, S>(world: &W<I, S>, exchange: ExchangeId) -> Vec<StoredMatch>
where
    I: FingerprintIndex + Send + Sync,
    S: ProvenanceStore + Clone + Send + Sync,
{
    world.stored_matches(exchange).await
}

fn origin_agents(matches: &[StoredMatch]) -> Vec<AgentId> {
    matches
        .iter()
        .map(|stored| stored.content.origin_agent())
        .collect()
}

/// `texts` outputs by three agents using every word of `vocabulary` in
/// shifting order (as `tests::match_quality`'s chatter).
async fn chatter<I, S>(world: &mut W<I, S>, vocabulary: &[&str], texts: usize, at_seconds: u64)
where
    I: FingerprintIndex + Send + Sync,
    S: ProvenanceStore + Clone + Send + Sync,
{
    let talkers: Vec<_> = (0..3).map(|_| world.agent()).collect();
    for n in 0..texts {
        let mut words: Vec<&str> = vocabulary.to_vec();
        let shift = n % words.len().max(1);
        words.rotate_left(shift);
        if n % 2 == 1 {
            words.reverse();
        }
        let text = format!("log {n}: {}", words.join(" / "));
        world
            .run(Turn::new(talkers[n % 3], at(at_seconds)).output(assistant_text(&text)))
            .await;
    }
}

/// Every word of `pages`, as `tests::bench_channel_template`'s chatter.
async fn page_chatter<I, S>(world: &mut W<I, S>, pages: &[&str])
where
    I: FingerprintIndex + Send + Sync,
    S: ProvenanceStore + Clone + Send + Sync,
{
    let mut words: Vec<String> = pages
        .iter()
        .flat_map(|page| page.split(|c: char| !c.is_alphanumeric() && c != '-'))
        .filter(|word| word.len() >= 4 && !word.chars().any(|c| c.is_ascii_digit()))
        .map(str::to_lowercase)
        .collect();
    words.sort();
    words.dedup();
    let talkers: Vec<_> = (0..3).map(|_| world.agent()).collect();
    for n in 0..30usize {
        let mut shifted = words.clone();
        let shift = n % shifted.len().max(1);
        shifted.rotate_left(shift);
        if n % 2 == 1 {
            shifted.reverse();
        }
        let text = format!("log {n}: {}", shifted.join(" / "));
        world
            .run(Turn::new(talkers[n % 3], at(1)).output(assistant_text(&text)))
            .await;
    }
}

// ---------------------------------------------------------------------
// INV-1094 provenance.match.cross-agent-spread and INV-1150
// provenance.match.skeleton-dropped.

/// A short template five agents wrote, minutes apart, is boilerplate.
async fn spread_template<I, S>(world: &mut W<I, S>)
where
    I: FingerprintIndex + Send + Sync,
    S: ProvenanceStore + Clone + Send + Sync,
{
    chatter(world, &COMMON, 30, 1).await;
    let template = "Please review the plan now!";
    for n in 0..5u64 {
        let agent = world.agent();
        world
            .run(Turn::new(agent, at(1 + n * 200)).output(assistant_text(template)))
            .await;
    }
    let reader = world.agent();
    let read = world
        .run(Turn::new(reader, at(2000)).input(user_text(template)))
        .await;
    let matches = matches_of(world, read.exchange).await;
    assert!(matches.is_empty(), "{}", brief_matches(&matches));
}

/// A template skeleton filled with other slot words is dropped whole.
async fn skeleton<I, S>(world: &mut W<I, S>)
where
    I: FingerprintIndex + Send + Sync,
    S: ProvenanceStore + Clone + Send + Sync,
{
    let page = |topic: &str, a: &str, b: &str, c: &str| {
        format!(
            "Open question: does {a} interact with stampede under the second experiment? \
             Our notes on {topic} still say {b} is fine; that is no longer true. \
             For {topic}, {c} matters more than {a} at our current scale."
        )
    };
    chatter(world, &COMMON, 30, 1).await;
    let slots = [
        (
            "cache invalidation",
            "write-through",
            "stale reads",
            "stale reads",
        ),
        ("rate limiting", "token buckets", "burst credit", "fairness"),
        (
            "schema migration",
            "dual writes",
            "backfill lag",
            "lock time",
        ),
        // Mirrors `match_quality::template_skeleton_with_other_slot_words_is_not_matched`:
        // the fourth fill is "versioned keys" so the read shares only the
        // skeleton (`provenance.span.coincident-template-originated`).
        (
            "cache invalidation",
            "ttl jitter",
            "purge queue",
            "versioned keys",
        ),
        ("queue sharding", "rebalancing", "hot keys", "ordering"),
    ];
    for (n, (topic, a, b, c)) in slots.iter().enumerate() {
        let agent = world.agent();
        originate(world, agent, &page(topic, a, b, c), 1 + 120 * n as u64).await;
    }
    let reader = world.agent();
    let read_text = "Our notes on cache invalidation still say write-through is fine; that is no \
         longer true. Nobody owns write-through yet, so I propose we track it with the second \
         experiment. For cache invalidation, purge queue matters more than write-through at our \
         current scale.";
    let read = world
        .run(Turn::new(reader, at(1000)).input(tool_result("call_1", read_text)))
        .await;
    let matches = matches_of(world, read.exchange).await;
    assert!(matches.is_empty(), "{}", brief_matches(&matches));
}

/// A short secret five agents copied keeps matching its first writer.
async fn distinctive_broadcast<I, S>(world: &mut W<I, S>)
where
    I: FingerprintIndex + Send + Sync,
    S: ProvenanceStore + Clone + Send + Sync,
{
    chatter(world, &COMMON, 30, 1).await;
    let secret = "rendezvous key 7f3a, node 12, 03:00";
    let first = world.agent();
    let origin = originate(world, first, secret, 10).await;
    for n in 0..5u64 {
        let copier = world.agent();
        world
            .run(
                Turn::new(copier, at(100 + 60 * n))
                    .input(user_text("carry on with your task"))
                    .output(assistant_text(secret)),
            )
            .await;
    }
    let reader = world.agent();
    let read = world
        .run(Turn::new(reader, at(1000)).input(tool_result("call_1", &format!("inbox: {secret}"))))
        .await;
    let matches = matches_of(world, read.exchange).await;
    assert!(
        matches
            .iter()
            .any(|stored| stored.content.origin() == origin.span.id
                && stored.content.origin_agent() == first),
        "{}",
        brief_matches(&matches)
    );
}

/// A long distinctive run inside a widespread template still matches.
async fn long_run_in_template<I, S>(world: &mut W<I, S>)
where
    I: FingerprintIndex + Send + Sync,
    S: ProvenanceStore + Clone + Send + Sync,
{
    let template = "Our notes on the rollout still say it is fine; that is no longer true.";
    for n in 0..4u64 {
        let agent = world.agent();
        world
            .run(Turn::new(agent, at(1 + n)).output(assistant_text(template)))
            .await;
    }
    let writer = world.agent();
    let secret =
        "The vault combination moved to the blue binder behind the third shelf of the archive.";
    let page = format!("{template} {secret}");
    world
        .run(Turn::new(writer, at(10)).output(assistant_text(&page)))
        .await;
    let reader = world.agent();
    let read = world
        .run(Turn::new(reader, at(20)).input(tool_result("call_1", &page)))
        .await;
    let matches = matches_of(world, read.exchange).await;
    assert!(
        origin_agents(&matches).contains(&writer),
        "{}",
        brief_matches(&matches)
    );
}

/// `provenance.match.cross-agent-spread` and `provenance.match.skeleton-dropped`
/// on Postgres agree with the memory model.
#[tokio::test(flavor = "multi_thread")]
async fn pg_spread_and_skeleton_agree_with_memory() {
    let Some(db) = database("pg_spread_and_skeleton_agree_with_memory").await else {
        return;
    };
    agree!(&db, real(), spread_template);
    agree!(&db, real(), skeleton);
    agree!(&db, real(), distinctive_broadcast);
    agree!(&db, real(), long_run_in_template);
    db.close().await.expect("close");
}

// ---------------------------------------------------------------------
// INV-1151 provenance.match.inherited-fragment-dropped.

/// The writer's narration of the page its orchestrator named.
async fn write_page<I, S>(
    world: &mut W<I, S>,
    writer: AgentId,
    n: usize,
    (page, label): (&str, &str),
    task: &str,
    seconds: u64,
) where
    I: FingerprintIndex + Send + Sync,
    S: ProvenanceStore + Clone + Send + Sync,
{
    let narration = format!("I'll update the wiki page `{page}` with my notes on {label}.");
    let call = tool_call(
        &format!("toolu_w{n}"),
        "http_request",
        &json!({
            "body": BODY,
            "method": "PUT",
            "url": format!("http://wiki:8090/pages/{page}"),
        }),
    );
    world
        .run(
            Turn::new(writer, at(seconds))
                .system(system_text(&system(n, "rate limiting")))
                .input(user_text(task))
                .output(assistant(vec![AssistantPart::Text(Text(narration)), call])),
        )
        .await;
}

/// The page names the orchestrator gave the writers match no reader.
async fn inherited_page_names<I, S>(world: &mut W<I, S>)
where
    I: FingerprintIndex + Send + Sync,
    S: ProvenanceStore + Clone + Send + Sync,
{
    for (n, page) in PAGES.iter().enumerate() {
        let writer = world.agent();
        write_page(
            world,
            writer,
            n,
            *page,
            &write_task(page.0, page.1),
            1 + n as u64,
        )
        .await;
    }
    for (n, (page, _)) in PAGES.iter().enumerate() {
        for m in 0..3u64 {
            let reader = world.agent();
            let read = world
                .run(
                    Turn::new(reader, at(100 + 10 * n as u64 + m))
                        .system(system_text(&system(10 + n, "search ranking")))
                        .input(user_text(&read_task(page))),
                )
                .await;
            let matches = matches_of(world, read.exchange).await;
            assert!(matches.is_empty(), "{page}: {}", brief_matches(&matches));
        }
    }
}

/// The control: a page name the writer chose itself matches its reader.
async fn chosen_page_name<I, S>(world: &mut W<I, S>)
where
    I: FingerprintIndex + Send + Sync,
    S: ProvenanceStore + Clone + Send + Sync,
{
    let chooser = world.agent();
    let (page, label) = PAGES[0];
    write_page(
        world,
        chooser,
        0,
        (page, label),
        "Please write up your current findings in the team wiki.",
        1,
    )
    .await;
    let reader = world.agent();
    let read = world
        .run(Turn::new(reader, at(100)).input(user_text(&read_task(page))))
        .await;
    let matches = matches_of(world, read.exchange).await;
    assert!(
        matches
            .iter()
            .any(|stored| stored.content.origin_agent() == chooser
                && stored.content.carrier() == &Carrier::UserTurn),
        "{}",
        brief_matches(&matches)
    );
}

const VAULT: &str = "vault> rendezvous-key=7f3a; node=12; at=03:00";
const COPY: &str = "rendezvous key 7f3a, node 12, at 03:00";

/// A short re-punctuated copy of a given secret is not matched.
async fn inherited_copy<I, S>(world: &mut W<I, S>)
where
    I: FingerprintIndex + Send + Sync,
    S: ProvenanceStore + Clone + Send + Sync,
{
    let (vault, copy) = (VAULT, COPY);
    let partial = world.agent();
    world
        .run(
            Turn::new(partial, at(1))
                .input(tool_result("call_1", vault))
                .output(assistant_text(&format!(
                    "{copy}. Passing this on as asked; nothing else to report from the vault \
                     today."
                ))),
        )
        .await;
    let reader = world.agent();
    let read = world
        .run(Turn::new(reader, at(10)).input(user_text(copy)))
        .await;
    let matches = matches_of(world, read.exchange).await;
    assert!(
        !origin_agents(&matches).contains(&partial),
        "{}",
        brief_matches(&matches)
    );
}

/// A whole message copied from the writer's input is matched where it is
/// delivered.
async fn delivered_whole_message<I, S>(world: &mut W<I, S>)
where
    I: FingerprintIndex + Send + Sync,
    S: ProvenanceStore + Clone + Send + Sync,
{
    let (vault, copy) = (VAULT, COPY);
    let whole = world.agent();
    world
        .run(
            Turn::new(whole, at(20))
                .input(tool_result("call_2", vault))
                .output(assistant_text(copy)),
        )
        .await;
    let reader = world.agent();
    let read = world
        .run(Turn::new(reader, at(30)).input(user_text(copy)))
        .await;
    let matches = matches_of(world, read.exchange).await;
    assert!(
        origin_agents(&matches).contains(&whole),
        "{}",
        brief_matches(&matches)
    );
}

/// A reply composed from words seen apart is the writer's own.
async fn reply_from_words_seen_apart<I, S>(world: &mut W<I, S>)
where
    I: FingerprintIndex + Send + Sync,
    S: ProvenanceStore + Clone + Send + Sync,
{
    let composer = world.agent();
    let reply = "Budget review pending with finance for 2025.";
    world
        .run(
            Turn::new(composer, at(40))
                .history(user_text("Can you check the vendor budget for 2025?"))
                .history(tool_result("call_3", "status: pending finance review"))
                .input(user_text("Please confirm when the review is done."))
                .output(assistant_text(reply)),
        )
        .await;
    let reader = world.agent();
    let read = world
        .run(Turn::new(reader, at(50)).input(user_text(reply)))
        .await;
    let matches = matches_of(world, read.exchange).await;
    assert!(
        origin_agents(&matches).contains(&composer),
        "{}",
        brief_matches(&matches)
    );
}

/// `provenance.match.inherited-fragment-dropped` on Postgres agrees with
/// the memory model, including after the origin's request list is pruned.
#[tokio::test(flavor = "multi_thread")]
async fn pg_inherited_fragments_agree_with_memory() {
    let Some(db) = database("pg_inherited_fragments_agree_with_memory").await else {
        return;
    };
    agree!(&db, real(), inherited_page_names);
    agree!(&db, real(), chosen_page_name);
    agree!(&db, real(), inherited_copy);
    agree!(&db, real(), delivered_whole_message);
    agree!(&db, real(), reply_from_words_seen_apart);
    db.close().await.expect("close");
}

// ---------------------------------------------------------------------
// INV-1093 provenance.match.reader-output-strict and INV-1152
// provenance.match.reader-output-rare-token.

/// Two agents filling one template alike make no `ReaderOutput` match; a
/// long broadcast with a rare word keeps matching its first writer, and a
/// message many agents read still matches its unseen copy.
async fn reader_output<I, S>(world: &mut W<I, S>)
where
    I: FingerprintIndex + Send + Sync,
    S: ProvenanceStore + Clone + Send + Sync,
{
    chatter(world, &VOCABULARY, 30, 1).await;
    let writer = world.agent();
    originate(
        world,
        writer,
        &format!("Reducing query rewrite by 41% should be enough. {TEMPLATE_SENTENCE}"),
        10,
    )
    .await;
    let other = world.agent();
    let wrote = world
        .run(
            Turn::new(other, at(20))
                .input(user_text("carry on with your task"))
                .output(assistant_text(&format!(
                    "We tried drift alerts last month. {TEMPLATE_SENTENCE} I would keep online \
                     index build as is."
                ))),
        )
        .await;
    let matches = matches_of(world, wrote.exchange).await;
    assert!(
        matches
            .iter()
            .all(|stored| stored.content.carrier() != &Carrier::ReaderOutput),
        "{}",
        brief_matches(&matches)
    );

    let message = "The team would meet after the next release at the old boathouse to revisit \
                   the search ranking.";
    let first = world.agent();
    let origin = originate(world, first, message, 30).await;
    for n in 0..3u64 {
        let reader = world.agent();
        let read = world
            .run(Turn::new(reader, at(40 + n)).input(tool_result("call_1", message)))
            .await;
        assert!(
            !matches_of(world, read.exchange).await.is_empty(),
            "read {n}"
        );
    }
    for n in 0..3u64 {
        let copier = world.agent();
        let wrote = world
            .run(
                Turn::new(copier, at(120 + 60 * n))
                    .input(user_text("carry on with your task"))
                    .output(assistant_text(message)),
            )
            .await;
        let matches = matches_of(world, wrote.exchange).await;
        assert!(
            matches
                .iter()
                .any(|stored| stored.content.origin() == origin.span.id
                    && stored.content.carrier() == &Carrier::ReaderOutput),
            "copy {n}: {}",
            brief_matches(&matches)
        );
    }
}

/// A short stretch of another agent's text in an output is relayed from
/// its span, not matched; the same text read in a tool result is matched
/// (the test shingles, as `tests::match_quality`).
async fn reader_output_floor<I, S>(world: &mut W<I, S>)
where
    I: FingerprintIndex + Send + Sync,
    S: ProvenanceStore + Clone + Send + Sync,
{
    let (a, b, c) = (world.agent(), world.agent(), world.agent());
    let text = "t_date BETWEEN '2025-01-01' AND '2025-06-30' AND x";
    assert!(text.len() < 64);
    let origin = originate(world, a, text, 1).await;
    let wrote = world
        .run(Turn::new(b, at(2)).output(assistant_text(&format!("SELECT * WHERE {text}"))))
        .await;
    assert!(matches_of(world, wrote.exchange).await.is_empty());
    let spans = world
        .store
        .exchange_spans(wrote.exchange)
        .await
        .expect("spans");
    assert!(spans.iter().any(|record| record.span.state
        == SpanState::Relayed {
            source: RelaySource::Span(origin.span.id)
        }));
    let read = world
        .run(Turn::new(c, at(3)).input(tool_result("call_1", text)))
        .await;
    let matches = matches_of(world, read.exchange).await;
    assert_eq!(matches.len(), 1, "{}", brief_matches(&matches));
    assert_eq!(matches[0].content.origin(), origin.span.id);
}

/// The reader-output rules on Postgres agree with the memory model.
#[tokio::test(flavor = "multi_thread")]
async fn pg_reader_output_rules_agree_with_memory() {
    let Some(db) = database("pg_reader_output_rules_agree_with_memory").await else {
        return;
    };
    agree!(&db, real(), reader_output);
    agree!(&db, config(), reader_output_floor);
    db.close().await.expect("close");
}

// ---------------------------------------------------------------------
// INV-1153 provenance.match.own-output-replay.

/// SALT `get_log` and AgentDojo `send_money` replays match no peer; the
/// same texts read by a third agent do, and the reader's own phrase is
/// left out of a peer's match run by run.
async fn own_output<I, S>(world: &mut W<I, S>)
where
    I: FingerprintIndex + Send + Sync,
    S: ProvenanceStore + Clone + Send + Sync,
{
    let (alice, bob, carol) = (world.agent(), world.agent(), world.agent());
    world
        .run(Turn::new(alice, at(1)).output(assistant(vec![tool_call(
            "call_a",
            "run_query",
            &json!({ "query": ALICE_SQL }),
        )])))
        .await;
    let call = assistant(vec![tool_call(
        "call_q",
        "run_query",
        &json!({ "query": BOB_SQL }),
    )]);
    world.run(Turn::new(bob, at(2)).output(call.clone())).await;
    let log = get_log(BOB_SQL);
    let control = world
        .run(Turn::new(carol, at(3)).input(tool_result("call_c", &log)))
        .await;
    let matches = matches_of(world, control.exchange).await;
    assert!(
        origin_agents(&matches).contains(&alice),
        "{}",
        brief_matches(&matches)
    );
    let replay = world
        .run(
            Turn::new(bob, at(4))
                .history(call)
                .input(tool_result("call_log", &log)),
        )
        .await;
    let matches = matches_of(world, replay.exchange).await;
    assert!(
        !origin_agents(&matches).contains(&alice),
        "{}",
        brief_matches(&matches)
    );

    let (injector, victim, other) = (world.agent(), world.agent(), world.agent());
    world
        .run(Turn::new(injector, at(10)).output(assistant_text(BILL)))
        .await;
    let bill = tool_result("call_bill", BILL);
    let read = world
        .run(Turn::new(victim, at(11)).input(bill.clone()))
        .await;
    assert!(origin_agents(&matches_of(world, read.exchange).await).contains(&injector));
    let sent = assistant(vec![
        AssistantPart::Text(Text(
            "I'll send a transaction to US133000000121212121212 with a subject including \
             \"Spotify Premium\"."
                .to_owned(),
        )),
        tool_call(
            "call_send",
            "send_money",
            &json!({"recipient": "US133000000121212121212", "subject": "Spotify Premium payment", "amount": 1.0}),
        ),
    ]);
    world
        .run(
            Turn::new(victim, at(12))
                .history(bill.clone())
                .output(sent.clone()),
        )
        .await;
    let control = world
        .run(Turn::new(other, at(13)).input(tool_result("call_o", ECHO)))
        .await;
    assert!(origin_agents(&matches_of(world, control.exchange).await).contains(&injector));
    let echo = world
        .run(
            Turn::new(victim, at(14))
                .history(bill)
                .history(sent)
                .input(tool_result("call_send", ECHO)),
        )
        .await;
    let matches = matches_of(world, echo.exchange).await;
    assert!(
        !origin_agents(&matches).contains(&injector),
        "{}",
        brief_matches(&matches)
    );

    let (writer, quoter) = (world.agent(), world.agent());
    let phrase = "the quarterly reconciliation of the north warehouse ledger";
    let novel =
        "Pallet 7731 was miscounted twice because the scanner firmware rolled back on Tuesday.";
    let message = format!("About {phrase}: {novel}");
    world
        .run(Turn::new(writer, at(20)).output(assistant_text(&message)))
        .await;
    let own = assistant_text(&format!("I am checking {phrase} right now."));
    world
        .run(Turn::new(quoter, at(21)).output(own.clone()))
        .await;
    let inbox = world
        .run(
            Turn::new(quoter, at(22))
                .history(own.clone())
                .input(tool_result("call_inbox", &message)),
        )
        .await;
    let matches = matches_of(world, inbox.exchange).await;
    assert_eq!(
        origin_agents(&matches)
            .iter()
            .filter(|agent| **agent == writer)
            .count(),
        1,
        "{}",
        brief_matches(&matches)
    );
    // A user turn repeating the reader is not the reader's relay.
    let turn = world
        .run(
            Turn::new(quoter, at(23))
                .history(own)
                .input(user_text(&message)),
        )
        .await;
    let matches = matches_of(world, turn.exchange).await;
    assert!(
        origin_agents(&matches).contains(&writer),
        "{}",
        brief_matches(&matches)
    );
}

/// `provenance.match.own-output-replay` on Postgres agrees with the memory
/// model.
#[tokio::test(flavor = "multi_thread")]
async fn pg_own_output_replay_agrees_with_memory() {
    let Some(db) = database("pg_own_output_replay_agrees_with_memory").await else {
        return;
    };
    agree!(&db, real(), own_output);
    db.close().await.expect("close");
}

// ---------------------------------------------------------------------
// INV-1090 provenance.index.forwarded-indexed and INV-1154
// provenance.match.forward-direct-read (forwarding on).

/// A direct read of a forwarded source, now or earlier, does not match the
/// forwarder; a delivery of the forward does; a forward of the reader's own
/// text matches nobody; and forwards expire after retention.
async fn forward_direct_read<I, S>(world: &mut W<I, S>)
where
    I: FingerprintIndex + Send + Sync,
    S: ProvenanceStore + Clone + Send + Sync,
{
    let (alice, bob, carol, dave) = (world.agent(), world.agent(), world.agent(), world.agent());
    let source = format!("{HEAD}{SCHEMA}");
    let rows = format!("run_query returned 3 rows (0.4 ms):\n{ROWS}");
    let pasted = format!("Schema: {SCHEMA}\n\nRows: {ROWS}");
    let forwarded = world
        .run(
            Turn::new(alice, at(1))
                .input(tool_result("call_i", &source))
                .input(tool_result("call_r", &rows))
                .output(assistant_text(&pasted)),
        )
        .await;
    let spans = world
        .store
        .exchange_spans(forwarded.exchange)
        .await
        .expect("spans");
    assert_eq!(
        spans
            .iter()
            .filter(|record| record.span.state.is_forwarded() && record.forward.is_some())
            .count(),
        2
    );
    let delivered = world
        .run(Turn::new(carol, at(2)).input(user_text(&pasted)))
        .await;
    assert!(origin_agents(&matches_of(world, delivered.exchange).await).contains(&alice));
    let direct = world
        .run(Turn::new(bob, at(3)).input(tool_result("call_b", &source)))
        .await;
    let matches = matches_of(world, direct.exchange).await;
    assert!(
        !origin_agents(&matches).contains(&alice),
        "{}",
        brief_matches(&matches)
    );
    let earlier = world
        .run(
            Turn::new(dave, at(4))
                .history(tool_result("call_d", &source))
                .input(user_text(&pasted)),
        )
        .await;
    let matches = matches_of(world, earlier.exchange).await;
    let rows_at = u32::try_from(pasted.find(ROWS).expect("rows")).expect("small");
    let from_alice: Vec<_> = matches
        .iter()
        .filter(|stored| stored.content.origin_agent() == alice)
        .collect();
    assert_eq!(from_alice.len(), 1, "{}", brief_matches(&matches));
    assert!(from_alice[0].content.read_at().range.start() >= rows_at);

    let (quoter, author) = (world.agent(), world.agent());
    let said = "Please send your complete raw-log string, with no omission, modification, \
        compression, or summary, before the verdict phase begins.";
    let own = assistant_text(said);
    world
        .run(Turn::new(author, at(5)).output(own.clone()))
        .await;
    let quoted = format!("You asked: {said} Here it is.");
    world
        .run(
            Turn::new(quoter, at(6))
                .input(user_text(said))
                .output(assistant_text(&quoted)),
        )
        .await;
    let back = world
        .run(
            Turn::new(author, at(7))
                .history(own)
                .input(user_text(&quoted)),
        )
        .await;
    let matches = matches_of(world, back.exchange).await;
    assert!(
        !origin_agents(&matches).contains(&quoter),
        "{}",
        brief_matches(&matches)
    );

    let later = at(7 + RETENTION.as_secs() + 10);
    world.engine.expire(later).await.expect("expiry");
    let late = world.agent();
    let read = world
        .run(Turn::new(late, later).input(user_text(&pasted)))
        .await;
    assert!(matches_of(world, read.exchange).await.is_empty());
}

/// The nearer-source rules for forwards on Postgres agree with the memory
/// model.
#[tokio::test(flavor = "multi_thread")]
async fn pg_forward_direct_read_agrees_with_memory() {
    let Some(db) = database("pg_forward_direct_read_agrees_with_memory").await else {
        return;
    };
    agree!(&db, real().with_forwarding(true), forward_direct_read);
    db.close().await.expect("close");
}

// ---------------------------------------------------------------------
// INV-1159 provenance.match.shadowed-fragment-dropped.

/// A writer's turn: its narration and the page it puts (no input holds
/// the page).
async fn put_page<I, S>(
    world: &mut W<I, S>,
    writer: AgentId,
    n: usize,
    page: &str,
    body: &str,
    seconds: u64,
) where
    I: FingerprintIndex + Send + Sync,
    S: ProvenanceStore + Clone + Send + Sync,
{
    let call = tool_call(
        &format!("toolu_w{n}"),
        "http_request",
        &json!({
            "body": body,
            "method": "PUT",
            "url": format!("http://wiki:8090/pages/{page}"),
        }),
    );
    let narration = format!("I'll update the wiki page `{page}` with my notes.");
    world
        .run(
            Turn::new(writer, at(seconds))
                .input(user_text(&format!(
                    "Please write up your findings, page {n}."
                )))
                .output(assistant(vec![AssistantPart::Text(Text(narration)), call])),
        )
        .await;
}

async fn read_page<I, S>(
    world: &mut W<I, S>,
    reader: AgentId,
    page: &str,
    seconds: u64,
) -> Vec<AgentId>
where
    I: FingerprintIndex + Send + Sync,
    S: ProvenanceStore + Clone + Send + Sync,
{
    let read = world
        .run(Turn::new(reader, at(seconds)).input(tool_result("toolu_r", page)))
        .await;
    origin_agents(&matches_of(world, read.exchange).await)
}

/// Template runs of an older and a later writer inside the page a reader
/// read match only the page's writer.
async fn shadowed<I, S>(world: &mut W<I, S>)
where
    I: FingerprintIndex + Send + Sync,
    S: ProvenanceStore + Clone + Send + Sync,
{
    page_chatter(world, &[CACHE_V1, CACHE_V2, CACHE_V3]).await;
    let (older, labelled, later) = (world.agent(), world.agent(), world.agent());
    put_page(world, older, 8, "cache-invalidation-2", CACHE_V1, 10).await;
    put_page(world, labelled, 16, "cache-invalidation-2", CACHE_V2, 20).await;
    put_page(world, later, 1, "cache-invalidation-2", CACHE_V3, 25).await;
    for n in 0..2u64 {
        let reader = world.agent();
        let origins = read_page(world, reader, CACHE_V2, 30 + n).await;
        assert!(origins.contains(&labelled), "read {n}: {origins:?}");
        assert!(!origins.contains(&older), "read {n}: {origins:?}");
        assert!(!origins.contains(&later), "read {n}: {origins:?}");
    }
}

/// A short secret (a rare token) read inside another writer's long page
/// still matches its own writer.
async fn shadowed_secret<I, S>(world: &mut W<I, S>)
where
    I: FingerprintIndex + Send + Sync,
    S: ProvenanceStore + Clone + Send + Sync,
{
    page_chatter(world, &[CACHE_V1, CACHE_V2, CACHE_V3]).await;
    let (author, quoter, reader) = (world.agent(), world.agent(), world.agent());
    let secret = "rendezvous key 7f3a, node 12, 03:00";
    world
        .run(
            Turn::new(author, at(40))
                .input(user_text("Tell the team where and when we meet tonight."))
                .output(assistant_text(secret)),
        )
        .await;
    let quoted = format!("{CACHE_V2} The meeting note says: {secret}. Bring the ledger.");
    world
        .run(
            Turn::new(quoter, at(50))
                .input(tool_result("toolu_inbox", &format!("inbox: {secret}")))
                .output(assistant_text(&quoted)),
        )
        .await;
    let origins = read_page(world, reader, &quoted, 60).await;
    assert!(origins.contains(&quoter), "{origins:?}");
    assert!(origins.contains(&author), "{origins:?}");
}

/// The control: with no present writer around it, the older writer's run
/// still matches.
async fn shadowed_control<I, S>(world: &mut W<I, S>)
where
    I: FingerprintIndex + Send + Sync,
    S: ProvenanceStore + Clone + Send + Sync,
{
    page_chatter(world, &[CACHE_V1, CACHE_V2, CACHE_V3]).await;
    let (older, reader) = (world.agent(), world.agent());
    put_page(world, older, 8, "cache-invalidation-2", CACHE_V1, 10).await;
    let origins = read_page(world, reader, CACHE_V2, 30).await;
    assert!(origins.contains(&older), "{origins:?}");
}

/// `provenance.match.shadowed-fragment-dropped` on Postgres agrees with
/// the memory model.
#[tokio::test(flavor = "multi_thread")]
async fn pg_shadowed_fragments_agree_with_memory() {
    let Some(db) = database("pg_shadowed_fragments_agree_with_memory").await else {
        return;
    };
    agree!(&db, real(), shadowed);
    agree!(&db, real(), shadowed_secret);
    agree!(&db, real(), shadowed_control);
    db.close().await.expect("close");
}

// ---------------------------------------------------------------------
// provenance.span.coincident-template-originated, with the one-hop
// holder count of provenance.match.cross-agent-spread.

/// A whole page of template sentences, each coinciding with earlier
/// agents' fills, read verbatim: only its writer is matched, and the
/// coincidences are recorded alike.
async fn verbatim_template<I, S>(world: &mut W<I, S>)
where
    I: FingerprintIndex + Send + Sync,
    S: ProvenanceStore + Clone + Send + Sync,
{
    page_chatter(world, &[CACHE_10]).await;
    let earlier: Vec<AgentId> = (0..4).map(|_| world.agent()).collect();
    for (n, fill) in earlier_fills(CACHE_10).iter().enumerate() {
        world
            .run(
                Turn::new(earlier[n % 4], at(10 + n as u64))
                    .input(user_text("carry on with your task"))
                    .output(assistant_text(fill)),
            )
            .await;
    }
    let (writer, reader) = (world.agent(), world.agent());
    put_page(world, writer, 1, "cache-invalidation-10", CACHE_10, 40).await;
    let origins = read_page(world, reader, CACHE_10, 60).await;
    assert!(origins.contains(&writer), "{origins:?}");
    assert!(origins.iter().all(|agent| *agent == writer), "{origins:?}");
}

/// Coincident template stretches on Postgres agree with the memory model:
/// the same spans, coincidences, holder counts and matches.
#[tokio::test(flavor = "multi_thread")]
async fn pg_coincident_templates_agree_with_memory() {
    let Some(db) = database("pg_coincident_templates_agree_with_memory").await else {
        return;
    };
    agree!(&db, real(), verbatim_template);
    db.close().await.expect("close");
}

// ---------------------------------------------------------------------
// INV-1091 provenance.index.remainder-around-relay-matchable and INV-1092
// provenance.match.short-span-exact.

/// An originated remainder next to a forward is matched through its
/// context k-grams, and a short whole value by its exact hash, verbatim and
/// normalized.
async fn short_and_context<I, S>(world: &mut W<I, S>)
where
    I: FingerprintIndex + Send + Sync,
    S: ProvenanceStore + Clone + Send + Sync,
{
    let (alice, bob) = (world.agent(), world.agent());
    let asked = "Bob, I have received your raw_log for task 3-14 and will check it tonight.";
    world
        .run(Turn::new(alice, at(1)).output(assistant_text(asked)))
        .await;
    let reply =
        "Thanks, Alice. I have received your raw_log for task 3-15 and will review it as well.";
    let replied = world
        .run(
            Turn::new(bob, at(2))
                .input(user_text(asked))
                .output(assistant_text(reply)),
        )
        .await;
    let spans = world
        .store
        .exchange_spans(replied.exchange)
        .await
        .expect("spans");
    let forwarded_end = spans
        .iter()
        .find(|record| record.span.state.is_forwarded())
        .expect("a forwarded quote")
        .span
        .location
        .range
        .end();
    let remainder = spans
        .iter()
        .find(|record| {
            record.span.state.origin() == Some(Origin::Originated)
                && record.span.location.range.start() >= forwarded_end
        })
        .expect("an originated remainder")
        .span
        .id;
    let read = world
        .run(Turn::new(alice, at(3)).input(user_text(reply)))
        .await;
    let matches = matches_of(world, read.exchange).await;
    assert!(
        matches
            .iter()
            .any(|stored| stored.content.origin() == remainder),
        "{}",
        brief_matches(&matches)
    );

    let (a, b, c) = (world.agent(), world.agent(), world.agent());
    let note = "Note for my partner: IRxSBdcNMr";
    world
        .run(Turn::new(a, at(10)).output(assistant_text(note)))
        .await;
    let verbatim = world
        .run(
            Turn::new(b, at(11))
                .input(tool_result("call_1", &format!("inbox: \"{note}\" (1 new)"))),
        )
        .await;
    let matches = matches_of(world, verbatim.exchange).await;
    assert_eq!(matches.len(), 1, "{}", brief_matches(&matches));
    assert_eq!(matches[0].content.kind(), &MatchKind::Exact);
    let folded = world
        .run(Turn::new(c, at(12)).input(user_text("NOTE FOR MY   partner: irxsbdcnmr")))
        .await;
    let matches = matches_of(world, folded.exchange).await;
    assert_eq!(matches.len(), 1, "{}", brief_matches(&matches));
    assert_eq!(matches[0].content.kind(), &MatchKind::Normalized);
    let state = world
        .store
        .span(matches[0].content.origin())
        .await
        .expect("read")
        .expect("stored")
        .span
        .state;
    assert!(matches!(state, SpanState::Propagated { hits, .. } if hits.get() == 2));
}

/// Context k-grams and short-span hashes on Postgres agree with the memory
/// model.
#[tokio::test(flavor = "multi_thread")]
async fn pg_short_spans_and_context_kgrams_agree_with_memory() {
    let Some(db) = database("pg_short_spans_and_context_kgrams_agree_with_memory").await else {
        return;
    };
    agree!(&db, real(), short_and_context);
    db.close().await.expect("close");
}
