//! Regressions shaped on the node0 bench boilerplate run 20261006T062146Z
//! (staging 690d124; 8090af0 replays it alike): 3 labelled wiki reads
//! missed, each a whole page of template sentences (333 to 650 bytes) read
//! verbatim through a tool result.
//!
//! Each sentence of the page shares a run of 32 characters or more with
//! some earlier agent's text, because every writer fills the same topic's
//! templates with the same slot words. Output resolution relayed each such
//! run to that earlier span; no `ReaderOutput` match was made (the runs
//! hold no rare token), yet the runs stayed `Relayed(Span)`, so the page's
//! writer posted almost nothing (24 of 333 bytes originated on
//! cache-invalidation-2). The reader then matched only short runs of the
//! earlier holders, which the skeleton and related rules drop, and nobody
//! matched the writer. A stretch holding no rare token is a coincidence,
//! not a copy, and stays the writer's own
//! (`provenance.span.coincident-template-originated`).

use crosstalk_spec::derived::provenance::span::Origin;
use crosstalk_spec::ids::AgentId;
use crosstalk_testkit::build::message::{assistant_text, user_text};

use super::bench_channel_template::{chatter, put_page, read_page, real};
use super::fixtures::{Turn, World, at};
use crate::config::{RareToken, ReaderOutputRules};
use crate::store::ProvenanceStore;

/// agent-001's cache-invalidation-2 (sender exchange
/// 01M47Y2ZWYGDBG1S7JCJB12H5P), read by agent-014.
pub(crate) const CACHE_2: &str = "If we change stale reads, expect 589 follow-up tickets around purge \
     queue. One option is to pair versioned keys with TTL, which the on-call notes already \
     supports. The data from the second experiment contradicts the earlier claim about stale \
     reads. The on-call notes suggests that write-through accounts for about 32% of the problem.";

/// agent-013's rate-limiting-0 (01M47Y2KJ4A9NKYHH99WAVCJGG), read by
/// agent-003.
pub(crate) const RATE_0: &str = "The data from the staging cluster contradicts the earlier claim about \
     per-tenant quota. We measured per-tenant quota on the staging cluster and saw roughly 45 \
     events per minute. I would keep per-tenant quota as is and revisit 429 responses after the \
     next release. One option is to pair burst budget with leaky bucket, which the open \
     question already supports. The cheapest fix is to document 429 responses and alert on 429 \
     responses. Compared with last month, per-tenant quota improved while retry-after header \
     regressed by 44%. Nobody owns token bucket yet, so I propose we track it with the second \
     experiment.";

/// agent-009's cache-invalidation-10 (01M47Y30CHHHWE4DF1PBMKNQN1), read by
/// agent-001.
pub(crate) const CACHE_10: &str = "Our notes on cache invalidation still say purge queue is fine; that is \
     no longer true. For cache invalidation, TTL matters more than versioned keys at our current \
     scale. For cache invalidation, stale reads matters more than write-through at our current \
     scale. For cache invalidation, TTL matters more than purge queue at our current scale. Our \
     notes on cache invalidation still say stale reads is fine; that is no longer true. Compared \
     with last month, write-through improved while write-through regressed by 54%. Compared with \
     last month, stampede improved while write-through regressed by 64%. Our notes on cache \
     invalidation still say TTL is fine; that is no longer true.";

/// The page's sentences, each filled differently by two earlier agents:
/// one keeps its first two thirds, the other its last two thirds, so every
/// byte of the page lies in a run of 47 characters or more that an
/// earlier span holds, as the bench's template fills do.
pub(crate) fn earlier_fills(page: &str) -> Vec<String> {
    page.split_inclusive(". ")
        .map(str::trim)
        .flat_map(|sentence| {
            let chars: Vec<char> = sentence.chars().collect();
            let third = chars.len() / 3;
            let head: String = chars[..chars.len() - third].iter().collect();
            let tail: String = chars[third..].iter().collect();
            [
                format!("{head} something else entirely, as far as we know."),
                format!("Elsewhere, and only once, {tail}"),
            ]
        })
        .collect()
}

/// The bench's shape: chatter makes the topic's words common, four
/// earlier agents write the template fills, the writer puts the page
/// (no input holds it), and a reader reads it verbatim. Returns the
/// writer, the reader's matched origin agents, and the bytes of the
/// writer's page that are originated.
async fn verbatim_read(world: &mut World, page: &str) -> (AgentId, Vec<AgentId>, u32) {
    chatter(world, &[page]).await;
    let earlier: Vec<AgentId> = (0..4).map(|_| world.agent()).collect();
    for (n, fill) in earlier_fills(page).iter().enumerate() {
        world
            .run(
                Turn::new(earlier[n % 4], at(10 + n as u64))
                    .input(user_text("carry on with your task"))
                    .output(assistant_text(fill)),
            )
            .await;
    }
    let (writer, reader) = (world.agent(), world.agent());
    let exchange = put_page(world, writer, 1, "cache-invalidation-2", page, 40)
        .await
        .exchange;
    let originated: u32 = world
        .store
        .exchange_spans(exchange)
        .await
        .expect("spans")
        .iter()
        .filter(|record| record.span.state.origin() == Some(Origin::Originated))
        .map(|record| record.span.location.range.len().get())
        .sum();
    let origins = read_page(world, reader, page, 60).await;
    (writer, origins, originated)
}

/// Bench miss agent-001 → agent-014 via /pages/cache-invalidation-2.
#[tokio::test]
async fn a_verbatim_template_page_matches_its_writer_cache_2() {
    let mut world = World::new(real());
    let (writer, origins, originated) = verbatim_read(&mut world, CACHE_2).await;
    assert!(
        originated as usize >= CACHE_2.len() / 2,
        "originated {originated} bytes"
    );
    assert!(origins.contains(&writer), "the page's writer: {origins:?}");
}

/// Bench miss agent-013 → agent-003 via /pages/rate-limiting-0.
#[tokio::test]
async fn a_verbatim_template_page_matches_its_writer_rate_0() {
    let mut world = World::new(real());
    let (writer, origins, _) = verbatim_read(&mut world, RATE_0).await;
    assert!(origins.contains(&writer), "the page's writer: {origins:?}");
}

/// Bench miss agent-009 → agent-001 via /pages/cache-invalidation-10.
#[tokio::test]
async fn a_verbatim_template_page_matches_its_writer_cache_10() {
    let mut world = World::new(real());
    let (writer, origins, _) = verbatim_read(&mut world, CACHE_10).await;
    assert!(origins.contains(&writer), "the page's writer: {origins:?}");
}

/// The earlier template holders are not matched by the page's reader: the
/// read is the writer's page (`provenance.match.shadowed-fragment-dropped`
/// once the writer's own text is posted).
#[tokio::test]
async fn earlier_template_holders_are_not_matched_by_the_pages_reader() {
    let mut world = World::new(real());
    let (writer, origins, _) = verbatim_read(&mut world, CACHE_10).await;
    assert!(
        origins.iter().all(|agent| *agent == writer),
        "only the page's writer: {origins:?}"
    );
}

/// With the rare token not required, the pre-fix relaying comes back:
/// every stretch matching an earlier span is relayed to it.
#[tokio::test]
async fn coincident_stretches_are_relayed_when_no_rare_token_is_required() {
    let config = real()
        .with_reader_output(ReaderOutputRules::new(64).with_rare_token(RareToken::NotRequired));
    let mut world = World::new(config);
    let (_, _, originated) = verbatim_read(&mut world, CACHE_2).await;
    assert!(
        (originated as usize) < CACHE_2.len() / 2,
        "originated {originated} bytes"
    );
}
