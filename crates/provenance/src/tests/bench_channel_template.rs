//! Regressions shaped on the node0 bench boilerplate run 20261006T021639Z
//! (staging 8090af0): 7 `Channel` / `ToolResult` / `Exact` false matches
//! of 32 to 46 bytes, each on a wiki page read that also matched the
//! page's labelled writer over most of its text.
//!
//! The pages' writers fill one topic's sentence templates with the topic's
//! slot words, so two writers of one page share short runs ("notes on
//! cache invalidation stil", " interact with purge queue under",
//! "is to pair freeze age with freeze age, which t"). The later writer's
//! output relays such a run to the earlier writer's span (too short and
//! too common for a `ReaderOutput` match, but relayed all the same), so
//! the page's own spans leave a hole there, and a reader of the page
//! matches whoever else holds the run: the earlier writer through the
//! relay, or a still later writer that wrote it itself. Two or three
//! agents hold each run, so the spread rule never applies
//! (`provenance.match.skeleton-dropped`); the run lies inside the extent
//! of the page writer's text (`provenance.match.shadowed-fragment-dropped`).

use crosstalk_spec::ids::AgentId;
use crosstalk_spec::observed::message::{AssistantPart, Text};
use crosstalk_testkit::build::message::{
    assistant, assistant_text, tool_call, tool_result, user_text,
};

use super::fixtures::{RETENTION, Turn, World, at, brief_matches};
use crate::config::{
    IndexSettings, InheritedFragments, ProvenanceConfig, ShadowedFragments, SpreadRule,
};

pub(super) fn real() -> ProvenanceConfig {
    ProvenanceConfig::default()
        .with_index(IndexSettings::single_node(50, RETENTION).expect("valid index settings"))
}

/// The bench's cache-invalidation-2, version 1 (agent-008, 02:17:01).
pub(crate) const CACHE_V1: &str = "The data from the team contradicts the earlier claim about stampede. \
     Nobody owns stale reads yet, so I propose we track it with the open question. For cache \
     invalidation, write-through matters more than TTL at our current scale. Our notes on cache \
     invalidation still say stampede is fine; that is no longer true. Compared with last month, \
     write-through improved while write-through regressed by 9%. We tried write-through in the \
     staging cluster and rolled it back after 494 minutes.";

/// Version 2 (agent-016, 02:17:12), the version every reader read.
pub(crate) const CACHE_V2: &str = "Open question: does TTL interact with TTL under our current design? If we \
     change versioned keys, expect 214 follow-up tickets around write-through. Open question: \
     does versioned keys interact with purge queue under the staging cluster? Our notes on \
     cache invalidation still say write-through is fine; that is no longer true. The main risk \
     with stale reads is that the staging cluster hides it until load peaks. If we change TTL, \
     expect 987 follow-up tickets around TTL. Reducing TTL by 31% should be enough for the next \
     quarter.";

/// Version 3 (agent-001, 02:17:56), put after version 2 and before three
/// reads of version 2 (the client puts once the response ends).
pub(crate) const CACHE_V3: &str = "I recommend a short spike on stale reads before committing to cache \
     invalidation. Compared with last month, purge queue improved while TTL regressed by 5%. \
     Nobody owns stale reads yet, so I propose we track it with our current design. Open \
     question: does stale reads interact with purge queue under our current design? Nobody owns \
     stale reads yet, so I propose we track it with our current design. The team suggests that \
     versioned keys accounts for about 36% of the problem.";

/// vacuum-tuning-17, version 1 (agent-012) and version 2 (agent-017).
const VACUUM_V1: &str = "Compared with last month, table bloat improved while dead tuples \
     regressed by 36%. One option is to pair cost delay with visibility map, which the staging \
     cluster already supports. One option is to pair freeze age with freeze age, which the \
     on-call notes already supports. One option is to pair freeze age with freeze age, which \
     the staging cluster already supports. Nobody owns autovacuum yet, so I propose we track it \
     with last week's numbers.";

const VACUUM_V2: &str = "Open question: does dead tuples interact with autovacuum under the \
     second experiment? I recommend a short spike on visibility map before committing to \
     Postgres vacuum tuning. If we change dead tuples, expect 946 follow-up tickets around \
     freeze age. One option is to pair freeze age with freeze age, which the dashboard already \
     supports. If we change freeze age, expect 573 follow-up tickets around autovacuum.";

/// Every word of the pages, written in shifting order by three other
/// agents: on the bench, the topic's words are in dozens of texts.
pub(super) async fn chatter(world: &mut World, pages: &[&str]) {
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

/// A writer's turn: its narration and the page it puts, with no input
/// holding the page (the bench's writers never read the page they put).
pub(super) async fn put_page(
    world: &mut World,
    writer: AgentId,
    n: usize,
    page: &str,
    body: &str,
    seconds: u64,
) -> super::fixtures::Ran {
    let call = tool_call(
        &format!("toolu_w{n}"),
        "http_request",
        &serde_json::json!({
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
        .await
}

/// The origin agents of `reader`'s matches on reading `page` at `seconds`.
pub(super) async fn read_page(
    world: &mut World,
    reader: AgentId,
    page: &str,
    seconds: u64,
) -> Vec<AgentId> {
    let read = world
        .run(Turn::new(reader, at(seconds)).input(tool_result("toolu_r", page)))
        .await;
    let matches = world.matches_of(read.exchange);
    eprintln!("{}", brief_matches(&matches));
    matches
        .iter()
        .map(|stored| stored.content.origin_agent())
        .collect()
}

/// Bench transmission 01M47G0MTQ5F4BYG3F65ZWKF5Q: agent-019 reads version 2
/// of cache-invalidation-2; version 1's writer shares only "notes on cache
/// invalidation stil" with it. Only version 2's writer is matched.
#[tokio::test]
async fn an_older_writers_template_run_inside_the_page_read_is_not_matched() {
    let mut world = World::new(real());
    chatter(&mut world, &[CACHE_V1, CACHE_V2, CACHE_V3]).await;
    let (older, labelled, reader) = (world.agent(), world.agent(), world.agent());
    put_page(&mut world, older, 8, "cache-invalidation-2", CACHE_V1, 10).await;
    put_page(
        &mut world,
        labelled,
        16,
        "cache-invalidation-2",
        CACHE_V2,
        20,
    )
    .await;
    let origins = read_page(&mut world, reader, CACHE_V2, 30).await;
    assert!(
        origins.contains(&labelled),
        "the page's writer: {origins:?}"
    );
    assert!(!origins.contains(&older), "the older writer: {origins:?}");
}

/// Bench transmissions 01M47G1RGW38HTFVZBFVNC3QC5, 01M47G1RK4XB51QE5F3YV9TK68
/// and 01M47G1TK06FNG944YHSN4R104: version 2's writer relays "Open
/// question: does versioned keys interact with" to an earlier writer's
/// template sentence, so the k-gram " interact with purge queue under"
/// straddling the relay's end is posted under nobody. Version 3's writer,
/// who never read version 2, writes the same k-gram after it and before
/// three agents read version 2: it is originated and posted under version
/// 3's writer. Only version 2's writer is matched, whoever wrote the run.
#[tokio::test]
async fn a_later_writers_template_run_inside_the_page_read_is_not_matched() {
    later_writer_reads(real(), false).await;
}

/// The same scenario with shadowed fragments kept: the later writer is
/// matched (the scenario reproduces the bench's false matches).
#[tokio::test]
async fn a_later_writers_template_run_matches_when_shadowed_fragments_are_kept() {
    let config = real().with_spread(SpreadRule::default().with_shadowed(ShadowedFragments::Kept));
    later_writer_reads(config, true).await;
}

async fn later_writer_reads(config: ProvenanceConfig, later_matched: bool) {
    let mut world = World::new(config);
    chatter(&mut world, &[CACHE_V1, CACHE_V2, CACHE_V3]).await;
    let (first, labelled, later) = (world.agent(), world.agent(), world.agent());
    put_page(
        &mut world,
        first,
        2,
        "cache-invalidation-26",
        "Reducing stampede by 12% should be enough for the next quarter. Open question: does \
         versioned keys interact with TTL under the second experiment? We tried TTL in the team \
         and rolled it back after 70 minutes.",
        15,
    )
    .await;
    put_page(
        &mut world,
        labelled,
        16,
        "cache-invalidation-2",
        CACHE_V2,
        20,
    )
    .await;
    put_page(&mut world, later, 1, "cache-invalidation-2", CACHE_V3, 25).await;
    for n in 0..3u64 {
        let reader = world.agent();
        let origins = read_page(&mut world, reader, CACHE_V2, 30 + n).await;
        assert!(
            origins.contains(&labelled),
            "read {n}, the page's writer: {origins:?}"
        );
        assert_eq!(
            origins.contains(&later),
            later_matched,
            "read {n}, the later writer: {origins:?}"
        );
    }
}

/// Bench transmission 01M47G1Z4P2S7NWYWZHPDJ4FVA: version 1's writer
/// wrote "One option is to pair freeze age with freeze age, which" twice,
/// as did version 2's writer once; two short runs (32 and 46 bytes) on
/// version 1's spans. Neither is matched.
#[tokio::test]
async fn an_older_writers_repeated_template_sentence_is_not_matched() {
    let mut world = World::new(real());
    chatter(&mut world, &[VACUUM_V1, VACUUM_V2]).await;
    let (older, labelled, reader) = (world.agent(), world.agent(), world.agent());
    put_page(&mut world, older, 12, "vacuum-tuning-17", VACUUM_V1, 10).await;
    put_page(&mut world, labelled, 17, "vacuum-tuning-17", VACUUM_V2, 60).await;
    let origins = read_page(&mut world, reader, VACUUM_V2, 90).await;
    assert!(
        origins.contains(&labelled),
        "the page's writer: {origins:?}"
    );
    assert!(!origins.contains(&older), "the older writer: {origins:?}");
}

/// The rule is configurable: with shadowed fragments kept, the bench's
/// false match comes back (the scenario reproduces it).
#[tokio::test]
async fn shadowed_fragments_can_be_kept() {
    let config = real().with_spread(SpreadRule::default().with_shadowed(ShadowedFragments::Kept));
    let mut world = World::new(config);
    chatter(&mut world, &[CACHE_V1, CACHE_V2, CACHE_V3]).await;
    let (older, labelled, reader) = (world.agent(), world.agent(), world.agent());
    put_page(&mut world, older, 8, "cache-invalidation-2", CACHE_V1, 10).await;
    put_page(
        &mut world,
        labelled,
        16,
        "cache-invalidation-2",
        CACHE_V2,
        20,
    )
    .await;
    let origins = read_page(&mut world, reader, CACHE_V2, 30).await;
    assert!(
        origins.contains(&labelled),
        "the page's writer: {origins:?}"
    );
    assert!(origins.contains(&older), "the older writer: {origins:?}");
}

/// The control: with no present writer around it (the page's writer is
/// not indexed), the same run still matches the older writer. Only a
/// fragment inside another agent's text is dropped.
#[tokio::test]
async fn a_template_run_no_present_writer_explains_still_matches() {
    let mut world = World::new(real());
    chatter(&mut world, &[CACHE_V1, CACHE_V2, CACHE_V3]).await;
    let (older, reader) = (world.agent(), world.agent());
    put_page(&mut world, older, 8, "cache-invalidation-2", CACHE_V1, 10).await;
    let origins = read_page(&mut world, reader, CACHE_V2, 30).await;
    assert!(origins.contains(&older), "the older writer: {origins:?}");
}

/// The control for quotes: a short secret (a rare token) read inside
/// another writer's long page still matches its writer, whether the page's
/// writer pasted it from an input (relayed) or holds it unobserved.
#[tokio::test]
async fn a_secret_inside_a_long_page_still_matches_its_writer() {
    let mut world = World::new(real());
    chatter(&mut world, &[CACHE_V1, CACHE_V2, CACHE_V3]).await;
    let (author, quoter, copier, reader) =
        (world.agent(), world.agent(), world.agent(), world.agent());
    let secret = "rendezvous key 7f3a, node 12, 03:00";
    let wrote = world
        .run(
            Turn::new(author, at(10))
                .input(user_text("Tell the team where and when we meet tonight."))
                .output(assistant_text(secret)),
        )
        .await;
    assert!(world.matches_of(wrote.exchange).is_empty());
    let quoted = format!("{CACHE_V2} The meeting note says: {secret}. Bring the ledger.");
    world
        .run(
            Turn::new(quoter, at(20))
                .input(tool_result("toolu_inbox", &format!("inbox: {secret}")))
                .output(assistant_text(&quoted)),
        )
        .await;
    let origins = read_page(&mut world, reader, &quoted, 30).await;
    assert!(origins.contains(&quoter), "the page's writer: {origins:?}");
    assert!(
        origins.contains(&author),
        "the secret's writer: {origins:?}"
    );

    let copied = format!("{CACHE_V1} Also: {secret}. Bring the ledger.");
    put_page(&mut world, copier, 4, "cache-invalidation-9", &copied, 40).await;
    let second = world.agent();
    let origins = read_page(&mut world, second, &copied, 50).await;
    assert!(
        origins.contains(&author),
        "the secret's writer, unobserved copy: {origins:?}"
    );
}

/// The switch decodes, default on.
#[test]
fn shadowed_switch_decodes() {
    assert_eq!(
        ProvenanceConfig::default().spread().shadowed(),
        ShadowedFragments::Dropped
    );
    let off: ProvenanceConfig =
        serde_json::from_str(r#"{"spread": {"drop_shadowed": false}}"#).expect("decodes");
    assert_eq!(off.spread().shadowed(), ShadowedFragments::Kept);
    assert_eq!(off.spread().inherited(), InheritedFragments::Dropped);
}
