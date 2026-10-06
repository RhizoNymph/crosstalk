//! Forwarding: SALT deliveries the sender relayed from its own tool output
//! are `Tier::Forwarding` labels, reported apart from `overall`; `ct-eval
//! run --forwarding on` turns on L4's forwarding for the live detector;
//! gates select by the run's forwarding setting.

mod common;

use std::path::{Path, PathBuf};

use common::{calls, dataset, draft, result, system, user};
use crosstalk_eval::corpus::{Coverage, Driven, HashedMessage, InMemory, World, WorldBuilder};
use crosstalk_eval::datasets::salt::load_world;
use crosstalk_eval::detect::live::{
    Forwarding, GatewayBackend, LiveDetector, LiveSettings, gateway_backend,
};
use crosstalk_eval::keys::{SourceRef, WorldKey};
use crosstalk_eval::location::location;
use crosstalk_eval::pipeline::{Detector, run};
use crosstalk_eval::report::{GateDetector, Gates, Report};
use crosstalk_eval::truth::{
    CarrierKind, Expectation, ExpectedContent, ExpectedTransmission, MatchNeed, RouteExpectation,
    Tier, TransmissionLabel,
};
use crosstalk_gateway::live::LiveClock;
use crosstalk_memory::support::ManualClock;
use crosstalk_spec::support::Timestamp;

const MAIN: &str = "traces/main/main__fixture-model/rep001.json";

/// Alice's delivery in the fixture's first episode.
const ALICE_SAYS: &str =
    "Alice here: my answer is PR0100 and PR0119 after checking the budget gap filter twice.";

/// Alice's `inspect_database` result in the same episode.
const SCHEMA: &str = r#"{"success": true, "tables": ["adverse_events", "departments", "procurement_requests", "vendors"], "note": "Shared schema text that both agents read from the same database file."}"#;

fn fixtures() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures")
}

/// Every string in `value` with `from` replaced by `to`; a string holding
/// `from` that is itself a JSON object (tool-call arguments) is replaced
/// inside and written back. Other strings are left byte for byte.
fn replace(value: &mut serde_json::Value, from: &str, to: &str) {
    match value {
        serde_json::Value::String(text) if text.contains(from) => {
            if let Ok(mut inner @ serde_json::Value::Object(_)) =
                serde_json::from_str::<serde_json::Value>(text)
            {
                replace(&mut inner, from, to);
                *text = inner.to_string();
            } else {
                *text = text.replace(from, to);
            }
        }
        serde_json::Value::Array(items) => items.iter_mut().for_each(|v| replace(v, from, to)),
        serde_json::Value::Object(map) => map.values_mut().for_each(|v| replace(v, from, to)),
        _ => {}
    }
}

fn positives(world: &World) -> Vec<&TransmissionLabel> {
    world
        .truth()
        .iter()
        .filter_map(|e| match e {
            Expectation::Transmission(t) => Some(t.label()),
            _ => None,
        })
        .collect()
}

#[test]
fn a_salt_delivery_pasting_the_senders_tool_result_is_forwarding() {
    let original = std::fs::read_to_string(fixtures().join("salt").join(MAIN))
        .unwrap_or_else(|e| panic!("{e}"));
    let mut trace: serde_json::Value =
        serde_json::from_str(&original).unwrap_or_else(|e| panic!("{e}"));
    let pasted = format!("My schema: {SCHEMA}");
    replace(&mut trace, ALICE_SAYS, &pasted);
    let dir = tempfile::tempdir().unwrap_or_else(|e| panic!("{e}"));
    let file = dir.path().join(MAIN);
    std::fs::create_dir_all(file.parent().unwrap_or_else(|| panic!("parent")))
        .unwrap_or_else(|e| panic!("{e}"));
    std::fs::write(&file, trace.to_string()).unwrap_or_else(|e| panic!("{e}"));

    let before =
        load_world(&fixtures().join("salt"), Path::new(MAIN)).unwrap_or_else(|e| panic!("{e}"));
    assert!(
        positives(&before)
            .iter()
            .all(|label| label.tier == Tier::Construction),
        "the fixture forwards nothing"
    );

    let after = load_world(dir.path(), Path::new(MAIN)).unwrap_or_else(|e| panic!("{e}"));
    let labels = positives(&after);
    assert_eq!(labels.len(), positives(&before).len());
    let forwarded: Vec<_> = labels
        .iter()
        .filter(|label| label.tier == Tier::Forwarding)
        .collect();
    assert_eq!(forwarded.len(), 1, "{labels:#?}");
    assert_eq!(forwarded[0].content.text, pasted);
    assert_eq!(forwarded[0].from.name, "alice");
    // Still a construction delivery in every other respect.
    assert_eq!(forwarded[0].route, RouteExpectation::Direct);
    assert_eq!(forwarded[0].needs, MatchNeed::through_json_string(&pasted));
}

/// A two-agent world: Alice reads a log through `get_log` (no access is
/// recorded for it) and sends it to Bob verbatim; Bob reads it in his next
/// user turn. One `Forwarding` label, and one `Construction` label for a
/// note Alice wrote herself.
fn forwarding_world() -> World {
    const LOG: &str = "seq 1 read_code src/ledger.py ok; seq 2 query_database SELECT total FROM orders WHERE month = 'march' ok; seq 3 resolve_records vendor 4471 ok";
    const NOTE: &str =
        "Bob, the March ledger total is off by exactly forty-two dollars and nine cents.";
    let mut builder = WorldBuilder::new(dataset(), WorldKey::new("forwarding"));
    let alice = builder
        .agent("alice", Driven::Model, "m")
        .unwrap_or_else(|e| panic!("{e}"));
    let bob = builder
        .agent("bob", Driven::Model, "m")
        .unwrap_or_else(|e| panic!("{e}"));
    let (sys_a, sys_b) = (system("You are Alice."), system("You are Bob."));
    let start = user("start");
    let get_log = calls("call_log", "get_log", "{}");
    let log = result("call_log", LOG);
    let send_log = calls(
        "call_s1",
        "send_message",
        &serde_json::json!({ "content": LOG }).to_string(),
    );
    let sent = result("call_s1", "ok");
    let send_note = calls(
        "call_s2",
        "send_message",
        &serde_json::json!({ "content": NOTE }).to_string(),
    );
    let (to_bob_log, to_bob_note) = (user(LOG), user(NOTE));
    let mut exchange = |d| builder.exchange(d).unwrap_or_else(|e| panic!("{e}"));
    exchange(draft(
        &alice,
        1,
        vec![sys_a.clone(), start.clone()],
        get_log.clone(),
    ));
    let a2 = exchange(draft(
        &alice,
        2,
        vec![sys_a.clone(), start.clone(), get_log.clone(), log.clone()],
        send_log.clone(),
    ));
    let b3 = exchange(draft(
        &bob,
        3,
        vec![sys_b.clone(), to_bob_log.clone()],
        common::says("thanks"),
    ));
    let a4 = exchange(draft(
        &alice,
        4,
        vec![sys_a, start, get_log, log, send_log, sent],
        send_note,
    ));
    let b5 = exchange(draft(
        &bob,
        5,
        vec![
            sys_b,
            to_bob_log,
            common::says("thanks"),
            to_bob_note.clone(),
        ],
        common::says("noted"),
    ));
    let label = |text: &str, turn: &HashedMessage, sender, reader, tier| {
        let len = u32::try_from(text.len()).unwrap_or_else(|e| panic!("{e}"));
        Expectation::Transmission(
            ExpectedTransmission::new(TransmissionLabel {
                from: alice.clone(),
                to: bob.clone(),
                sender_exchange: Some(sender),
                reader_exchange: reader,
                route: RouteExpectation::Direct,
                carrier: CarrierKind::UserTurn,
                content: ExpectedContent {
                    text: text.to_owned(),
                    at: location(turn.hash(), 0, 0, len).unwrap_or_else(|e| panic!("{e}")),
                },
                needs: MatchNeed::through_json_string(text),
                tier,
                source: SourceRef::new("fixture.json", format!("/{tier:?}")),
            })
            .unwrap_or_else(|e| panic!("{e}")),
        )
    };
    let user_log = user(LOG);
    builder.expect(label(LOG, &user_log, a2, b3, Tier::Forwarding));
    builder.expect(label(NOTE, &to_bob_note, a4, b5, Tier::Construction));
    builder.finish(Coverage::Complete {
        tier: Tier::Construction,
    })
}

fn live(forwarding: Forwarding) -> LiveDetector<GatewayBackend> {
    let settings = LiveSettings::short(0)
        .unwrap_or_else(|e| panic!("{e}"))
        .with_forwarding(forwarding);
    LiveDetector::new(gateway_backend(), settings).unwrap_or_else(|e| panic!("{e}"))
}

fn report(detector: &mut impl Detector) -> Report {
    let mut source = InMemory::new(dataset(), vec![forwarding_world()]);
    let summary = run(&mut source, detector, 10, |_, _| {});
    assert!(summary.failures.is_empty(), "{:?}", summary.failures);
    Report::new(
        dataset(),
        detector.name(),
        summary.score,
        vec![],
        vec![],
        summary.unscored,
    )
}

#[test]
fn live_settings_default_to_forwarding_off_and_the_backend_passes_it_to_l4() {
    let short = LiveSettings::short(0).unwrap_or_else(|e| panic!("{e}"));
    assert_eq!(short.forwarding, Forwarding::Off);
    let clock = || LiveClock::Manual(ManualClock::at(Timestamp::from_micros(1)));
    let backend = gateway_backend();
    let off = backend
        .live_config(&short, clock())
        .unwrap_or_else(|e| panic!("{e}"));
    assert!(!off.provenance.forwarding());
    let on = backend
        .live_config(&short.with_forwarding(Forwarding::On), clock())
        .unwrap_or_else(|e| panic!("{e}"));
    assert!(on.provenance.forwarding());
}

#[test]
fn forwarding_labels_are_kept_apart_and_found_only_with_forwarding_on() {
    let off = report(&mut live(Forwarding::Off));
    assert_eq!(off.forwarding.counts.expected, 1);
    assert_eq!(off.forwarding.counts.found, 0, "{off:#?}");
    // Overall holds the construction label only: the known miss is not
    // charged to it.
    assert_eq!(off.overall.counts.expected, 1);
    assert_eq!(off.overall.counts.found, 1, "{off:#?}");

    let on = report(&mut live(Forwarding::On));
    assert_eq!(on.forwarding.counts.expected, 1);
    assert_eq!(on.forwarding.counts.found, 1, "{on:#?}");
    assert_eq!(on.overall.counts.expected, 1);
    assert_eq!(on.overall.counts.found, 1);
    assert!(
        crosstalk_eval::report::table::render(&on).contains("forwarding (sender relayed"),
        "the table shows the forwarding summary"
    );
}

const GATES: &str = r#"
[[gate]]
name = "live, shipped"
detector = "live"
metric = "recall"
min = 0.5

[[gate]]
name = "live, forwarding on"
detector = "live"
forwarding = "on"
tier = "forwarding"
metric = "recall"
min = 0.9

[[gate]]
name = "live, forwarding off said outright"
detector = "live"
forwarding = "off"
metric = "precision"
min = 0.5

[[gate]]
name = "reference"
metric = "recall"
min = 0.5
"#;

fn names(gates: &Gates) -> Vec<&str> {
    gates.gates.iter().map(|gate| gate.name.as_str()).collect()
}

#[test]
fn gates_select_by_the_runs_forwarding() {
    let gates = Gates::parse(GATES, "fixture").unwrap_or_else(|e| panic!("{e}"));
    assert_eq!(
        names(&gates.for_run(GateDetector::Live, Forwarding::Off)),
        ["live, shipped", "live, forwarding off said outright"]
    );
    assert_eq!(
        names(&gates.for_run(GateDetector::Live, Forwarding::On)),
        ["live, forwarding on"]
    );
    assert_eq!(
        names(&gates.for_run(GateDetector::Reference, Forwarding::Off)),
        ["reference"]
    );
    let bad = "[[gate]]\nname = \"x\"\nforwarding = \"maybe\"\nmetric = \"recall\"\nmin = 0.5\n";
    assert!(Gates::parse(bad, "fixture").is_err());
}

#[test]
fn the_shipped_gates_have_a_forwarding_on_gate() {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("gates.toml");
    let gates = Gates::load(&path).unwrap_or_else(|e| panic!("{e}"));
    let on = gates.for_run(GateDetector::Live, Forwarding::On);
    assert!(!on.gates.is_empty());
    assert!(
        on.gates
            .iter()
            .all(|gate| gate.tier == Some(Tier::Forwarding))
    );
}

/// `ct-eval run --detector live` on the SALT fixture with `extra`.
fn cli(extra: &[&str]) -> std::process::Output {
    let dir = tempfile::tempdir().unwrap_or_else(|e| panic!("{e}"));
    let gates = dir.path().join("no-gates.toml");
    std::fs::write(&gates, "").unwrap_or_else(|e| panic!("{e}"));
    std::process::Command::new(env!("CARGO_BIN_EXE_ct-eval"))
        .args(["run", "--detector", "live", "--dataset", "salt", "--root"])
        .arg(fixtures().join("salt"))
        .arg("--gates")
        .arg(&gates)
        .args(extra)
        .output()
        .unwrap_or_else(|e| panic!("{e}"))
}

#[test]
fn the_cli_takes_forwarding_on_or_off_and_nothing_else() {
    for value in ["on", "off"] {
        let output = cli(&["--forwarding", value]);
        assert!(
            output.status.success(),
            "--forwarding {value}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    assert!(!cli(&["--forwarding", "maybe"]).status.success());
}
