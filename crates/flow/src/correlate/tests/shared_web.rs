//! Shared public web content: agents that fetch one public page all read
//! the same text, so the page's content matches between them, yet none of
//! them wrote the page. Shaped like AI Village's bash web reads (`curl` of
//! a public quiz page by several agents), with synthetic pages and text.
//!
//! The matches are evidence of a shared upstream source, not of a
//! transmission (`flow.route.shared-upstream-stays-suspected`): they
//! confirm nothing. With no writer the readers' co-reads open nothing; with
//! a writer (a third agent submitting a form to the same URL) its
//! co-access opens a transmission that stays suspected.

use proptest::prelude::*;
use serde_json::json;

use crosstalk_spec::derived::flow::access::Access;
use crosstalk_spec::derived::provenance::matching::ContentMatch;
use crosstalk_spec::ids::{AgentId, SpanId};
use crosstalk_spec::interfaces::l5_flow::ExtractedOp;

use super::fixtures::{Scene, secs, timing};
use super::lifecycle::{Stage, UpdateKind};
use super::{Decided, fold, kinds};
use crate::correlate::WindowedCorrelator;
use crate::extract::tests::support::{call, context_in, extract, ok};
use crate::extract::{Classified, ExtractConfig};

/// Two agents' bash fetches of one public page, spelled differently, as
/// AI Village agents make them.
fn fetches() -> (Vec<Classified>, Vec<Classified>) {
    let page = "<h1>Daily quiz</h1><p>Which planet has the most moons?</p>";
    let config = ExtractConfig::default();
    let alice = extract(
        &config,
        &context_in("/home/alice"),
        &call(
            "bash",
            json!({ "command": "curl -s https://quiz.example.org/daily | head -50" }),
        ),
        Some(&ok(page)),
    )
    .expect("extracts");
    let bob = extract(
        &config,
        &context_in("/home/bob"),
        &call(
            "bash",
            json!({ "command": "curl -sL 'https://Quiz.Example.org:443/daily#top' -H 'User-Agent: x'" }),
        ),
        Some(&ok(page)),
    )
    .expect("extracts");
    (alice, bob)
}

#[test]
fn two_fetches_of_one_page_are_two_reads_of_one_resource() {
    let (alice, bob) = fetches();
    assert_eq!(alice.len(), 1);
    assert_eq!(bob.len(), 1);
    assert_eq!(alice[0].op, ExtractedOp::Read);
    assert_eq!(bob[0].op, ExtractedOp::Read);
    assert_eq!(alice[0].locator, bob[0].locator);
}

fn confirms(out: &[Decided]) -> bool {
    kinds(out).iter().any(|kind| {
        matches!(
            kind,
            UpdateKind::Confirm | UpdateKind::OpenConfirmed | UpdateKind::Extend
        )
    })
}

/// Alice and Bob both fetch the page; Bob's result matches text Alice wrote
/// in her own output earlier (she quoted the page). No one wrote the page:
/// nothing opens, nothing is confirmed.
#[test]
fn readers_alone_open_nothing() {
    let mut scene = Scene::new(40);
    let (alice, bob) = (scene.agent(), scene.agent());
    let page = scene.resource();
    let quoted = scene.span();
    let mut correlator = WindowedCorrelator::new(timing());
    let first = scene.read(alice, page, secs(0));
    let second = scene.read(bob, page, secs(40));
    let mut out = correlator.access(&first, None);
    out.extend(correlator.access(&second, None));
    let content = scene.carried(&second, alice, quoted);
    out.extend(correlator.content(&content));
    out.extend(correlator.tick(timing().window_closes_at(second.at)));
    out.extend(correlator.tick(secs(100_000)));
    assert!(out.is_empty(), "{out:?}");
}

/// Carol submits an answer to the page's URL (a write by a third agent);
/// Alice and Bob fetch it. Bob's result matches Alice's quote of the page,
/// but Alice never wrote it: the transmission Carol's co-access opened to
/// Bob stays suspected, and nothing names Alice as a sender.
#[test]
fn a_match_between_readers_keeps_the_transmission_suspected() {
    let mut scene = Scene::new(41);
    let (alice, bob, carol) = (scene.agent(), scene.agent(), scene.agent());
    let page = scene.resource();
    let (answer, quoted) = (scene.span(), scene.span());
    let mut correlator = WindowedCorrelator::new(timing());
    let submit = scene.write(carol, page, secs(0), vec![answer]);
    let first = scene.read(alice, page, secs(10));
    let second = scene.read(bob, page, secs(40));
    let mut out = correlator.access(&submit, None);
    out.extend(correlator.access(&first, None));
    out.extend(correlator.access(&second, None));
    let content = scene.carried(&second, alice, quoted);
    out.extend(correlator.content(&content));
    out.extend(correlator.tick(timing().window_closes_at(second.at)));
    assert!(!confirms(&out), "{out:?}");
    let finals = fold(&out).expect("a lawful lifecycle");
    let to_bob: Vec<Stage> = finals
        .values()
        .filter(|state| state.to == bob)
        .map(|state| state.stage)
        .collect();
    assert_eq!(to_bob, vec![Stage::Suspected]);
}

/// Who fetched, who quoted and who wrote, for the property.
#[derive(Debug, Clone)]
struct World {
    readers: usize,
    writer: bool,
    /// (reader, origin reader) pairs: the reader's result matches a span of
    /// the origin reader.
    matches: Vec<(usize, usize)>,
}

fn world() -> impl Strategy<Value = World> {
    (2usize..5, any::<bool>()).prop_flat_map(|(readers, writer)| {
        prop::collection::vec((0..readers, 0..readers), 1..8).prop_map(move |pairs| World {
            readers,
            writer,
            matches: pairs.into_iter().filter(|(to, from)| to != from).collect(),
        })
    })
}

proptest! {
    /// `flow.route.shared-public-content-stays-suspected`: any number of
    /// agents fetching one page they never wrote, any matches among them,
    /// with or without a third agent writing the page: no update confirms
    /// a transmission, and every transmission ends suspected or discarded.
    #[test]
    fn shared_public_content_confirms_nothing(world in world()) {
        let mut scene = Scene::new(42);
        let page = scene.resource();
        let readers: Vec<AgentId> = (0..world.readers).map(|_| scene.agent()).collect();
        let quotes: Vec<SpanId> = (0..world.readers).map(|_| scene.span()).collect();
        let mut correlator = WindowedCorrelator::new(timing());
        let mut out = Vec::new();
        if world.writer {
            let writer = scene.agent();
            let own = scene.span();
            out.extend(correlator.access(&scene.write(writer, page, secs(0), vec![own]), None));
        }
        let reads: Vec<Access> = readers
            .iter()
            .enumerate()
            .map(|(at, reader)| scene.read(*reader, page, secs(10 + 20 * at as u64)))
            .collect();
        for read in &reads {
            out.extend(correlator.access(read, None));
        }
        let contents: Vec<ContentMatch> = world
            .matches
            .iter()
            .map(|(to, from)| scene.carried(&reads[*to], readers[*from], quotes[*from]))
            .collect();
        for content in &contents {
            out.extend(correlator.content(content));
        }
        out.extend(correlator.tick(secs(100_000)));
        prop_assert!(!confirms(&out), "{:?}", out);
        let finals = fold(&out).map_err(TestCaseError::fail)?;
        prop_assert!(finals.values().all(|state| matches!(state.stage, Stage::Suspected | Stage::Discarded)));
        if !world.writer {
            prop_assert!(out.is_empty(), "{:?}", out);
        }
    }
}
