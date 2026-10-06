//! Regressions shaped on the node0 bench boilerplate run 20261005T184633Z
//! (staging 02103e9): 41 false matches the skeleton rule could not remove.
//!
//! - 17 `UserTurn` matches: the orchestrator names a page to its writer
//!   ("... in the team wiki, page `queue-backpressure-39`.") and to its
//!   readers ("Before you continue, read the wiki page
//!   `queue-backpressure-39` ..."). The writer's narration ("I'll update
//!   the wiki page `queue-backpressure-39` with my notes ...") is
//!   originated, and a reader's prompt shares one k-gram with it. Only the
//!   page's writers (1 or 2 agents) hold it, so the spread rule never
//!   applies; but every token of the shared fragment was given to the writer
//!   in its own prompt (`provenance.match.inherited-fragment-dropped`).
//! - 24 `ReaderOutput` matches of 64 to 94 characters: two agents fill the
//!   same sentence template with the same topic words ("For search
//!   ranking, query rewrite matters more than embedding drift at our
//!   current scale."). No token of such a stretch is rare world-wide
//!   (`provenance.match.reader-output-rare-token`).

use crosstalk_spec::derived::provenance::matching::Carrier;
use crosstalk_spec::observed::message::{AssistantPart, Text};
use crosstalk_testkit::build::message::{
    assistant, assistant_text, system_text, tool_call, tool_result, user_text,
};

use super::fixtures::{RETENTION, Turn, World, at, brief_matches};
use crate::config::{
    IndexSettings, InheritedFragments, ProvenanceConfig, RareToken, ReaderOutputRules, SpreadRule,
};
use crate::store::ProvenanceStore;

fn real() -> ProvenanceConfig {
    ProvenanceConfig::default()
        .with_index(IndexSettings::single_node(50, RETENTION).expect("valid index settings"))
}

/// The swarm's system prompt for `agent`.
pub(crate) fn system(agent: usize, focus: &str) -> String {
    format!(
        "You are agent-{agent:03}, a research agent on a team of 20 agents. Your focus is \
         {focus}. The team shares a wiki at http://wiki:8090/pages/<name>: read pages with \
         http_request GET before relying on them and record what you learn with http_request \
         PUT. Be concise.\n\n[style:boilerplate]"
    )
}

/// The orchestrator's write task for `page` on `label`.
pub(crate) fn write_task(page: &str, label: &str) -> String {
    format!(
        "Please write up your current findings on {label} in the team wiki, page `{page}`.\n\n\
         [task:write page={page} topic=7 base=http://wiki:8090]"
    )
}

/// The orchestrator's read task for `page`.
pub(crate) fn read_task(page: &str) -> String {
    format!(
        "Before you continue, read the wiki page `{page}` and tell me what matters for us.\n\n\
         [task:read page={page} base=http://wiki:8090]"
    )
}

/// The writer's turn: its narration and the page it puts.
async fn write_page(
    world: &mut World,
    writer: crosstalk_spec::ids::AgentId,
    n: usize,
    (page, label): (&str, &str),
    task: &str,
    body: &str,
    seconds: u64,
) {
    let narration = format!("I'll update the wiki page `{page}` with my notes on {label}.");
    let call = tool_call(
        &format!("toolu_w{n}"),
        "http_request",
        &serde_json::json!({
            "body": body,
            "method": "PUT",
            "url": format!("http://wiki:8090/pages/{page}"),
        }),
    );
    let output = assistant(vec![AssistantPart::Text(Text(narration)), call]);
    world
        .run(
            Turn::new(writer, at(seconds))
                .system(system_text(&system(n, "rate limiting")))
                .input(user_text(task))
                .output(output),
        )
        .await;
}

pub(crate) const PAGES: [(&str, &str); 3] = [
    ("queue-backpressure-39", "queue backpressure"),
    ("incident-review-19", "the incident review"),
    ("schema-migration-12", "the schema migration"),
];

pub(crate) const BODY: &str = "I would keep shed load as is and revisit consumer lag after the next \
     release. Reducing consumer lag by 62% should be enough for the next quarter.";

/// `provenance.match.inherited-fragment-dropped`, bench user-turn matches
/// (origin span 01M46P77863GQM6WJMN0JWTXDA, 9 readers): the reader's prompt
/// shares "wiki page `queue-backpressure-39`" with the writer's narration,
/// but the writer was given every token of it in its own prompt. No reader
/// gets a match on the narration.
#[tokio::test]
async fn orchestrator_page_name_in_the_writers_narration_is_not_matched() {
    let mut world = World::new(real());
    for (n, page) in PAGES.iter().enumerate() {
        let writer = world.agent();
        write_page(
            &mut world,
            writer,
            n,
            *page,
            &write_task(page.0, page.1),
            BODY,
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
            let matches = world.matches_of(read.exchange);
            assert!(matches.is_empty(), "{page}: {}", brief_matches(&matches));
        }
    }
}

/// The control: when the writer named the page itself (its prompt does
/// not hold the page's tokens), a prompt quoting its narration is a read of
/// the writer's own text, and matches.
#[tokio::test]
async fn a_page_name_the_writer_chose_still_matches() {
    let mut world = World::new(real());
    let writer = world.agent();
    let (page, label) = PAGES[0];
    write_page(
        &mut world,
        writer,
        0,
        (page, label),
        "Please write up your current findings in the team wiki.",
        BODY,
        1,
    )
    .await;
    let reader = world.agent();
    let read = world
        .run(Turn::new(reader, at(100)).input(user_text(&read_task(page))))
        .await;
    let matches = world.matches_of(read.exchange);
    assert!(
        matches
            .iter()
            .any(|stored| stored.content.origin_agent() == writer
                && stored.content.carrier() == &Carrier::UserTurn),
        "{}",
        brief_matches(&matches)
    );
}

/// Configurable: with inherited fragments kept, the bench's user-turn
/// match comes back.
#[tokio::test]
async fn inherited_fragments_can_be_kept() {
    let config = real().with_spread(SpreadRule::default().with_inherited(InheritedFragments::Kept));
    let mut world = World::new(config);
    let writer = world.agent();
    let (page, label) = PAGES[0];
    write_page(
        &mut world,
        writer,
        0,
        (page, label),
        &write_task(page, label),
        BODY,
        1,
    )
    .await;
    let reader = world.agent();
    let read = world
        .run(Turn::new(reader, at(100)).input(user_text(&read_task(page))))
        .await;
    let matches = world.matches_of(read.exchange);
    assert!(
        matches
            .iter()
            .any(|stored| stored.content.origin_agent() == writer),
        "{}",
        brief_matches(&matches)
    );
}

/// The hard requirement, with a writer that has inputs: a short secret
/// whose key the writer made up (its prompt names no part of it), copied
/// by five agents over an unseen channel, still matches its first writer
/// where it is read.
#[tokio::test]
async fn short_secret_from_a_prompted_writer_matches_the_first_writer() {
    let mut world = World::new(real());
    chatter(&mut world, &VOCABULARY, 30, 1).await;
    let secret = "rendezvous key 7f3a, node 12, 03:00";
    let first = world.agent();
    let wrote = world
        .run(
            Turn::new(first, at(10))
                .input(user_text("Tell the team where and when we meet tonight."))
                .output(assistant_text(secret)),
        )
        .await;
    let spans = world
        .store
        .exchange_spans(wrote.exchange)
        .await
        .expect("spans");
    let origin = spans.first().expect("an originated span").span.id;
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
    let matches = world.matches_of(read.exchange);
    assert!(
        matches
            .iter()
            .any(|stored| stored.content.origin() == origin
                && stored.content.origin_agent() == first),
        "{}",
        brief_matches(&matches)
    );
}

/// The tradeoff of inherited fragments, stated as a test: a writer that
/// re-punctuates a secret it was given (the same tokens, in the same order,
/// in its own tool result) inside a longer message is not matched where
/// only the secret is read. Like a forwarded span with forwarding off, the fragment
/// carries nothing the writer added.
#[tokio::test]
async fn a_short_repunctuated_copy_of_a_given_secret_is_not_matched() {
    let mut world = World::new(real());
    let writer = world.agent();
    world
        .run(
            Turn::new(writer, at(1))
                .input(tool_result(
                    "call_1",
                    "vault> rendezvous-key=7f3a; node=12; at=03:00",
                ))
                .output(assistant_text(
                    "rendezvous key 7f3a, node 12, at 03:00. Passing this on as asked; nothing \
                     else to report from the vault today.",
                )),
        )
        .await;
    let reader = world.agent();
    let read = world
        .run(Turn::new(reader, at(10)).input(user_text("rendezvous key 7f3a, node 12, at 03:00")))
        .await;
    let matches = world.matches_of(read.exchange);
    assert!(matches.is_empty(), "{}", brief_matches(&matches));
}

/// SALT's forwarding shape: a writer whose whole message re-punctuates
/// what it was given is still matched where the whole message is
/// delivered: a read covering at least half the span is a delivery.
#[tokio::test]
async fn a_delivered_whole_message_copied_from_the_writers_input_matches() {
    let mut world = World::new(real());
    let writer = world.agent();
    world
        .run(
            Turn::new(writer, at(1))
                .input(tool_result(
                    "call_1",
                    "vault> rendezvous-key=7f3a; node=12; at=03:00",
                ))
                .output(assistant_text("rendezvous key 7f3a, node 12, at 03:00")),
        )
        .await;
    let reader = world.agent();
    let read = world
        .run(Turn::new(reader, at(10)).input(user_text("rendezvous key 7f3a, node 12, at 03:00")))
        .await;
    let matches = world.matches_of(read.exchange);
    assert!(
        matches
            .iter()
            .any(|stored| stored.content.origin_agent() == writer),
        "{}",
        brief_matches(&matches)
    );
}

/// SALT's shape: a short reply composed from words the writer saw spread
/// over its conversation (every token given, but not in that order in any
/// one part) is the writer's own text, and a delivery of it matches.
#[tokio::test]
async fn a_short_reply_from_words_seen_apart_still_matches() {
    let mut world = World::new(real());
    let writer = world.agent();
    world
        .run(
            Turn::new(writer, at(1))
                .history(user_text("Can you check the vendor budget for 2025?"))
                .history(tool_result("call_1", "status: pending finance review"))
                .input(user_text("Please confirm when the review is done."))
                .output(assistant_text(
                    "Budget review pending with finance for 2025.",
                )),
        )
        .await;
    let reader = world.agent();
    let read = world
        .run(
            Turn::new(reader, at(10))
                .input(user_text("Budget review pending with finance for 2025.")),
        )
        .await;
    let matches = world.matches_of(read.exchange);
    assert!(
        matches
            .iter()
            .any(|stored| stored.content.origin_agent() == writer),
        "{}",
        brief_matches(&matches)
    );
}

/// Background chatter: `texts` outputs by three agents using every word of
/// `vocabulary`, in shifting order, so the words are common world-wide and
/// no run matches a test sentence.
async fn chatter(world: &mut World, vocabulary: &[&str], texts: usize, at_seconds: u64) {
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

/// The generator's words for search ranking and its sentence templates.
pub(crate) const VOCABULARY: [&str; 24] = [
    "search",
    "ranking",
    "query",
    "rewrite",
    "matters",
    "more",
    "than",
    "embedding",
    "drift",
    "current",
    "scale",
    "would",
    "keep",
    "column",
    "default",
    "revisit",
    "online",
    "index",
    "build",
    "after",
    "next",
    "release",
    "team",
    "meet",
];

/// The bench's sentence, one template filled with the topic's words.
pub(crate) const TEMPLATE_SENTENCE: &str =
    "For search ranking, query rewrite matters more than embedding drift at our current scale.";

/// `provenance.match.reader-output-rare-token`, bench reader-output
/// matches (origin 01M46P810H7V3HZRMNA40ZKZKQ, 89 characters): another
/// agent writes the same filled template with no input holding it. No
/// token of it is rare world-wide, so no `ReaderOutput` match is made.
#[tokio::test]
async fn coincident_template_sentence_in_a_readers_output_is_not_matched() {
    let mut world = World::new(real());
    chatter(&mut world, &VOCABULARY, 30, 1).await;
    let writer = world.agent();
    super::scenarios::originate(
        &mut world,
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
    let matches = world.matches_of(wrote.exchange);
    assert!(
        matches
            .iter()
            .all(|stored| stored.content.carrier() != &Carrier::ReaderOutput),
        "{}",
        brief_matches(&matches)
    );
}

/// With the rare token not required, the same coincidence matches: the
/// pre-fix behaviour, kept as a configuration.
#[tokio::test]
async fn coincident_template_sentence_matches_when_no_rare_token_is_required() {
    let config = real()
        .with_reader_output(ReaderOutputRules::new(64).with_rare_token(RareToken::NotRequired));
    let mut world = World::new(config);
    chatter(&mut world, &VOCABULARY, 30, 1).await;
    let writer = world.agent();
    super::scenarios::originate(&mut world, writer, TEMPLATE_SENTENCE, 10).await;
    let other = world.agent();
    let wrote = world
        .run(
            Turn::new(other, at(20))
                .input(user_text("carry on with your task"))
                .output(assistant_text(TEMPLATE_SENTENCE)),
        )
        .await;
    let matches = world.matches_of(wrote.exchange);
    assert!(
        matches
            .iter()
            .any(|stored| stored.content.carrier() == &Carrier::ReaderOutput
                && stored.content.origin_agent() == writer),
        "{}",
        brief_matches(&matches)
    );
}

/// A long broadcast in a busy world: one writer, five later copies over an
/// unseen channel, every word but one common. The rare word keeps each
/// copy matched to the first writer, and each copy raises the bound.
#[tokio::test]
async fn long_broadcast_with_a_rare_word_in_a_busy_world_matches_the_first_writer() {
    let mut world = World::new(real());
    chatter(&mut world, &VOCABULARY, 30, 1).await;
    let message = "The team would meet after the next release at the old boathouse to revisit \
                   the search ranking.";
    assert!(message.len() >= 64);
    let first = world.agent();
    let origin = super::scenarios::originate(&mut world, first, message, 10).await;
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
                    && stored.content.carrier() == &Carrier::ReaderOutput),
            "copy {n}: {}",
            brief_matches(&matches)
        );
    }
}

/// A message many agents read through visible tool results, then copied
/// over an unseen channel: its reads observed its tokens, but each read is
/// a hit on the span and raises the bound, so the copy still matches.
#[tokio::test]
async fn unseen_copy_of_a_widely_read_message_matches() {
    let mut world = World::new(real());
    chatter(&mut world, &VOCABULARY, 30, 1).await;
    let message = "The team would meet after the next release at the old boathouse to revisit \
                   the search ranking.";
    let first = world.agent();
    let origin = super::scenarios::originate(&mut world, first, message, 10).await;
    for n in 0..6u64 {
        let reader = world.agent();
        let read = world
            .run(Turn::new(reader, at(20 + n)).input(tool_result("call_1", message)))
            .await;
        assert!(!world.matches_of(read.exchange).is_empty(), "read {n}");
    }
    let copier = world.agent();
    let wrote = world
        .run(
            Turn::new(copier, at(100))
                .input(user_text("carry on with your task"))
                .output(assistant_text(message)),
        )
        .await;
    let matches = world.matches_of(wrote.exchange);
    assert!(
        matches
            .iter()
            .any(|stored| stored.content.origin() == origin.span.id
                && stored.content.carrier() == &Carrier::ReaderOutput),
        "{}",
        brief_matches(&matches)
    );
}

/// The tradeoff of the rare-token requirement, stated as a test: a long
/// broadcast made only of words common in the world, copied by five agents
/// over an unseen channel, matches nobody. A copy of plain prose reads the
/// same as two agents filling one template.
#[tokio::test]
async fn long_broadcast_of_common_words_is_not_matched() {
    let mut world = World::new(real());
    chatter(&mut world, &VOCABULARY, 30, 1).await;
    let message = "The team would meet after the next release to revisit the search ranking \
                   and keep the online index build.";
    assert!(message.len() >= 64);
    let first = world.agent();
    super::scenarios::originate(&mut world, first, message, 10).await;
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
                .all(|stored| stored.content.carrier() != &Carrier::ReaderOutput),
            "copy {n}: {}",
            brief_matches(&matches)
        );
    }
}

/// The new switches decode, default on, and turn off.
#[test]
fn rare_token_and_inherited_switches_decode() {
    let defaults = ProvenanceConfig::default();
    assert_eq!(defaults.reader_output().rare_token(), RareToken::Required);
    assert_eq!(defaults.spread().inherited(), InheritedFragments::Dropped);
    let off: ProvenanceConfig = serde_json::from_str(
        r#"{"reader_output": {"rare_token": false}, "spread": {"drop_inherited": false}}"#,
    )
    .expect("decodes");
    assert_eq!(off.reader_output().rare_token(), RareToken::NotRequired);
    assert_eq!(off.reader_output().min_chars(), 64);
    assert_eq!(off.spread().inherited(), InheritedFragments::Kept);
    assert_eq!(off.spread().agents(), 4);
}
