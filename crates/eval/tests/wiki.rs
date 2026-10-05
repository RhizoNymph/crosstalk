//! The collusion-wiki converter on synthetic fixtures shaped like the
//! export. No real dataset bytes are used.

use std::path::{Path, PathBuf};

use crosstalk_eval::corpus::{Coverage, Driven, TraceSource, World};
use crosstalk_eval::datasets::wiki::attribution::{attribute, line_byte_range, runs};
use crosstalk_eval::datasets::wiki::resource::{page_locator, page_url};
use crosstalk_eval::datasets::wiki::schema::{Hunk, Revision};
use crosstalk_eval::datasets::wiki::{WikiSelection, WikiSource};
use crosstalk_eval::pipeline::{Detector, ReferenceDetector, run};
use crosstalk_eval::truth::{CarrierKind, Expectation, RouteExpectation, Tier};
use crosstalk_spec::aggregates::edge::RouteKind;

fn root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/wiki/collusion-wiki")
}

fn worlds(selection: &WikiSelection) -> Vec<World> {
    let mut source = WikiSource::open(&root(), selection).unwrap_or_else(|e| panic!("{e}"));
    source
        .worlds()
        .map(|w| w.unwrap_or_else(|e| panic!("{e}")))
        .collect()
}

fn relay_world(worlds: &[World]) -> &World {
    worlds
        .iter()
        .find(|w| w.agents().len() == 2)
        .expect("a two-agent world")
}

fn transmissions(world: &World) -> Vec<&crosstalk_eval::truth::ExpectedTransmission> {
    world
        .truth()
        .iter()
        .filter_map(|e| match e {
            Expectation::Transmission(t) => Some(t),
            _ => None,
        })
        .collect()
}

#[test]
fn components_become_worlds() {
    // Alice+Bob share a page (one world); Carol's solo page is another.
    let all = worlds(&WikiSelection::default());
    assert_eq!(all.len(), 2);
    let agents: Vec<usize> = all.iter().map(|w| w.agents().len()).collect();
    assert!(agents.contains(&2) && agents.contains(&1));
    // Every wiki agent is model-driven.
    for world in &all {
        for agent in world.agents() {
            assert_eq!(agent.driven, Driven::Model);
        }
    }
}

#[test]
fn family_and_agent_filters_apply() {
    let relay_only = worlds(&WikiSelection {
        families: vec!["relay-coordination".to_owned()],
        ..Default::default()
    });
    assert_eq!(relay_only.len(), 1);
    assert_eq!(relay_only[0].agents().len(), 2);

    let multi = worlds(&WikiSelection {
        min_agents: Some(2),
        ..Default::default()
    });
    assert_eq!(multi.len(), 1);
}

#[test]
fn reads_precede_edits_in_time() {
    let world = &worlds(&WikiSelection {
        min_agents: Some(2),
        ..Default::default()
    })[0];
    // Exchanges are in strictly increasing virtual time.
    let times: Vec<_> = world.exchanges().iter().map(|e| e.at()).collect();
    let mut sorted = times.clone();
    sorted.sort();
    assert_eq!(times, sorted);
    // The world holds a read (tool result) before an edit for the second
    // author: more exchanges than revisions implies synthesised reads.
    assert!(world.exchanges().len() > 5);
    assert_eq!(world.coverage(), Coverage::Partial);
}

#[test]
fn channel_labels_name_the_page_url() {
    let all = worlds(&WikiSelection::default());
    let world = relay_world(&all);
    let positives = transmissions(world);
    assert!(!positives.is_empty(), "expected channel transmissions");
    let url = page_url("dse", "RelayIndexAlpha");
    let locator = page_locator("dse", "RelayIndexAlpha").expect("a page locator");
    let mut channel = 0;
    let mut relay = 0;
    for t in &positives {
        let label = t.label();
        assert_eq!(label.tier, Tier::Heuristic);
        match (&label.route, label.carrier) {
            (RouteExpectation::Channel { resource }, CarrierKind::ToolResult) => {
                assert_eq!(resource, &locator);
                channel += 1;
            }
            (RouteExpectation::Channel { resource }, CarrierKind::ReaderOutput) => {
                assert_eq!(resource, &locator);
                relay += 1;
            }
            other => panic!("unexpected route/carrier {other:?}"),
        }
    }
    assert!(channel >= 2, "Alice->Bob and Bob->Alice channel edges");
    assert!(relay >= 1, "a relay (ReaderOutput) edge");
    assert!(url.contains("prowiki.org/dse/RelayIndexAlpha"));
}

#[test]
fn reference_matcher_finds_channel_transmissions() {
    let mut source = WikiSource::open(
        &root(),
        &WikiSelection {
            min_agents: Some(2),
            ..Default::default()
        },
    )
    .unwrap_or_else(|e| panic!("{e}"));
    let mut detector = ReferenceDetector::default();
    assert_eq!(detector.name(), "reference");
    let summary = run(&mut source, &mut detector, 50, |_, _| {});
    let channel = summary.score.total(&crosstalk_eval::score::Selector {
        route: Some(RouteKind::Channel),
        ..Default::default()
    });
    assert!(channel.found > 0, "reference should find channel edges");
    assert_eq!(channel.correct, channel.predicted, "no false channel edges");
}

#[test]
fn deterministic_truth() {
    let a = worlds(&WikiSelection::default());
    let b = worlds(&WikiSelection::default());
    for (wa, wb) in a.iter().zip(&b) {
        assert_eq!(wa.truth(), wb.truth());
    }
}

// --- attribution unit tests ---

fn rev(seq: u64, body: &str, hunks: Vec<Hunk>) -> Revision {
    Revision {
        rev_id: format!("p~P@{seq}"),
        page_id: "p/P".into(),
        wiki: "dse".into(),
        name: "P".into(),
        seq,
        body: body.into(),
        hunks,
        label: format!("Agent{seq}"),
        ip16: "1.1".into(),
        time: format!("2026-06-01T00:00:0{seq}Z"),
        change_summary: None,
    }
}

fn insert(a: usize, b0: usize, b1: usize) -> Hunk {
    Hunk {
        op: "insert".into(),
        a0: a,
        a1: a,
        b0,
        b1,
    }
}

#[test]
fn attribution_tracks_inserts() {
    let r1 = rev(1, "alpha\nbeta", vec![insert(0, 0, 2)]);
    let r2 = rev(2, "alpha\nbeta\ngamma", vec![insert(2, 2, 3)]);
    let revs = [&r1, &r2];
    let sources = attribute(&revs).unwrap_or_else(|e| panic!("{e}"));
    assert_eq!(sources[0], vec![0, 0]);
    assert_eq!(sources[1], vec![0, 0, 1]);
    let r = runs(&sources[1]);
    assert_eq!(r.len(), 2);
    assert_eq!((r[0].source, r[0].from, r[0].to), (0, 0, 2));
    assert_eq!((r[1].source, r[1].from, r[1].to), (1, 2, 3));
}

#[test]
fn line_ranges_are_body_byte_offsets() {
    let lines = vec!["alpha", "beta", "gamma"];
    // Lines 1..3 = "beta\ngamma", starting after "alpha\n" (6 bytes).
    assert_eq!(line_byte_range(&lines, 1, 3), Some((6, 16)));
    assert_eq!(line_byte_range(&lines, 0, 0), None);
}
