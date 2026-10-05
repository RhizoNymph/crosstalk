//! The collusion-wiki converter on synthetic fixtures shaped like the
//! export. No real dataset bytes are used.

use std::path::{Path, PathBuf};

use crosstalk_eval::corpus::{Coverage, Driven, TraceSource, World};
use crosstalk_eval::datasets::wiki::attribution::{attribute, line_byte_range, runs};
use crosstalk_eval::datasets::wiki::resource::{page_locator, page_url};
use crosstalk_eval::datasets::wiki::schema::{Hunk, Revision};
use crosstalk_eval::datasets::wiki::tools;
use crosstalk_eval::datasets::wiki::{WikiSelection, WikiSource};
use crosstalk_eval::location::SpanLocationExt;
use crosstalk_eval::pipeline::{Detector, ReferenceDetector, run};
use crosstalk_eval::reference::{ReferenceConfig, run as reference_run};
use crosstalk_eval::truth::{CarrierKind, Expectation, RouteExpectation, Tier};
use crosstalk_spec::aggregates::edge::RouteKind;
use crosstalk_spec::observed::message::{AssistantPart, Message, MessageBody, ToolArguments};

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

// --- the agreed L5 HttpTool shape ---

/// The first `http_request` call in `message`, as its parsed arguments.
fn http_call(message: &Message) -> Option<serde_json::Value> {
    let MessageBody::Assistant(parts) = &message.body else {
        return None;
    };
    parts.iter().find_map(|part| match part {
        AssistantPart::ToolCall(call) if call.name.0 == tools::TOOL => match &call.arguments {
            ToolArguments::Json(json) => serde_json::from_str(&json.0).ok(),
            ToolArguments::Invalid(_) => None,
        },
        _ => None,
    })
}

#[test]
fn reads_and_writes_take_the_http_tool_shape() {
    let all = worlds(&WikiSelection::default());
    let world = relay_world(&all);
    let url = page_url("dse", "RelayIndexAlpha");
    let mut gets = 0;
    let mut posts = 0;
    for exchange in world.exchanges() {
        for message in exchange.request().chain(exchange.response()) {
            let Some(args) = http_call(message) else {
                continue;
            };
            assert_eq!(args["url"], serde_json::Value::from(url.clone()));
            match args["method"].as_str() {
                Some("GET") => {
                    assert!(args.get("body").is_none(), "a read carries no body");
                    gets += 1;
                }
                Some("POST") => {
                    assert!(args["body"].is_string(), "a write carries its text");
                    posts += 1;
                }
                other => panic!("unexpected method {other:?}"),
            }
        }
    }
    assert!(gets >= 2, "reads before each change of author");
    assert_eq!(posts, 4, "one write per revision of the shared page");
}

#[test]
fn channel_labels_sit_in_the_read_tool_result() {
    // INV-269: the expected text is inside the read call's tool result, and
    // that result holds the page body as of the previous revision.
    let all = worlds(&WikiSelection::default());
    let world = relay_world(&all);
    for t in transmissions(world) {
        let label = t.label();
        if label.carrier != CarrierKind::ToolResult {
            continue;
        }
        let exchange = world
            .exchanges()
            .iter()
            .find(|e| e.id() == label.reader_exchange)
            .expect("the reader exchange");
        let message = exchange
            .message(label.content.at.message())
            .expect("the labelled message is in the reader exchange");
        assert!(matches!(message.body, MessageBody::Tool(_)));
        let text = label
            .content
            .at
            .text(message)
            .unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(text, label.content.text);
        let call = exchange
            .request()
            .find_map(http_call)
            .expect("the read call precedes its result");
        assert_eq!(call["method"], "GET");
    }
}

// --- regression: large bodies of shared text ---

const TEMPLATE: &str = "Describe the new page here and add your notes below";

fn jsonl<T: serde::Serialize>(rows: &[T]) -> String {
    rows.iter()
        .map(|row| serde_json::to_string(row).unwrap_or_else(|e| panic!("{e}")))
        .collect::<Vec<_>>()
        .join("\n")
}

/// A synthetic export: `writers` agents each create a page whose body is
/// `copies` lines of the wiki's new-page template and one line of their own;
/// then one reviewer appends a line to every page, reading it first. The
/// reviewer links every page into one world.
fn large_body_export(writers: usize, copies: usize) -> PathBuf {
    let dir = Path::new(env!("CARGO_TARGET_TMPDIR")).join(format!("wiki-large-{writers}-{copies}"));
    std::fs::create_dir_all(&dir).unwrap_or_else(|e| panic!("{e}"));
    let mut revisions = Vec::new();
    let mut pages = Vec::new();
    for writer in 0..writers {
        let name = format!("Page{writer:03}");
        let page_id = format!("dse/{name}");
        let own = format!(
            "{} owns this page and keeps its findings here",
            unique_tag(writer)
        );
        let mut body = vec![TEMPLATE; copies].join("\n");
        body.push('\n');
        body.push_str(&own);
        let created_lines = copies + 1;
        let row = |seq: u64, body: &str, label: &str, hunk: serde_json::Value, minute: usize| {
            serde_json::json!({
                "rev_id": format!("dse~{name}@{seq}"),
                "page_id": page_id,
                "wiki": "dse",
                "name": name,
                "seq": seq,
                "body": body,
                "hunks": [hunk],
                "label": label,
                "ip16": "10.0",
                "time": format!("2026-06-01T{:02}:{:02}:00Z", minute / 60, minute % 60),
            })
        };
        revisions.push(row(
            1,
            &body,
            &format!("Writer{writer:03}"),
            serde_json::json!({"op":"insert","a0":0,"a1":0,"b0":0,"b1":created_lines}),
            writer,
        ));
        let reviewed = format!("{body}\nreviewer checked {} and agrees", unique_tag(writer));
        revisions.push(row(
            2,
            &reviewed,
            "Reviewer",
            serde_json::json!({"op":"insert","a0":created_lines,"a1":created_lines,"b0":created_lines,"b1":created_lines + 1}),
            writers + writer,
        ));
        pages.push(serde_json::json!({
            "page_id": page_id, "wiki": "dse", "name": name, "page_family": "synthetic",
        }));
    }
    std::fs::write(dir.join("revisions.jsonl"), jsonl(&revisions))
        .unwrap_or_else(|e| panic!("{e}"));
    std::fs::write(dir.join("pages.jsonl"), jsonl(&pages)).unwrap_or_else(|e| panic!("{e}"));
    dir
}

/// A word no other writer's text shares a 24-byte window with.
fn unique_tag(writer: usize) -> String {
    let tag: String = [writer / 26 % 26, writer % 26]
        .iter()
        .map(|&d| char::from(b'a' + u8::try_from(d).unwrap_or(0)))
        .collect();
    (0..4)
        .map(|word| format!("{tag}{tag}x{word}"))
        .collect::<Vec<_>>()
        .join(" ")
}

#[test]
fn large_template_bodies_stay_linear() {
    // 74 pages x 74 originators x 300 template lines would be 1,642,800
    // matches if every copy matched every page creator's span. The
    // originators exceed the boilerplate cutoff.
    let writers = ReferenceConfig::default().max_postings + 24;
    let copies = 300;
    let root = large_body_export(writers, copies);
    let mut source =
        WikiSource::open(&root, &WikiSelection::default()).unwrap_or_else(|e| panic!("{e}"));
    let worlds: Vec<World> = source
        .worlds()
        .map(|w| w.unwrap_or_else(|e| panic!("{e}")))
        .collect();
    assert_eq!(worlds.len(), 1);
    let world = &worlds[0];
    assert_eq!(world.agents().len(), writers + 1);
    let output = reference_run(world, ReferenceConfig::default()).unwrap_or_else(|e| panic!("{e}"));
    assert!(
        output.matches <= 2 * writers,
        "matches must not scale with template copies x originators: {}",
        output.matches
    );
    // Every writer's own line still reaches the reviewer.
    let mut source =
        WikiSource::open(&root, &WikiSelection::default()).unwrap_or_else(|e| panic!("{e}"));
    let summary = run(&mut source, &mut ReferenceDetector::default(), 0, |_, _| {});
    let channel = summary.score.total(&crosstalk_eval::score::Selector {
        route: Some(RouteKind::Channel),
        ..Default::default()
    });
    let writers = u64::try_from(writers).unwrap_or(u64::MAX);
    assert_eq!(channel.expected, writers);
    assert_eq!(channel.found, writers);
}

#[test]
fn demo_selects_small_relay_coordination_worlds() {
    let demo = WikiSelection::demo();
    assert_eq!(demo.families, vec!["relay-coordination".to_owned()]);
    let selected = worlds(&demo);
    // The fixture's relay page is one two-agent world; Carol's solo page is
    // another family and below the agent floor.
    assert_eq!(selected.len(), 1);
    assert_eq!(selected[0].agents().len(), 2);
    let max = demo.max_agents.expect("the demo bounds world size");
    let limit = demo.limit.expect("the demo bounds world count");
    assert!(max <= 16 && limit <= 10);
}

#[test]
fn family_tally_counts_multi_author_pages() {
    let source =
        WikiSource::open(&root(), &WikiSelection::default()).unwrap_or_else(|e| panic!("{e}"));
    let tally = source.families();
    let relay = &tally.families["relay-coordination"];
    assert_eq!(
        (relay.pages, relay.multi_author_pages, relay.revisions),
        (1, 1, 4)
    );
    let solo = &tally.families["source-cache-url-list"];
    assert_eq!(
        (solo.pages, solo.multi_author_pages, solo.revisions),
        (1, 0, 1)
    );
    let shown = tally.to_string();
    assert!(shown.contains("relay-coordination"));
}
