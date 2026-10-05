//! `session` rows (the primary agent ↔ session map), the exchange count a
//! swarm report carries, and the gates a swarm run checks.

use crosstalk_eval::datasets::swarm_truth::truth_file::{Row, TruthFileError};
use crosstalk_eval::datasets::swarm_truth::{DETECTOR, Effect, RowKind, Side, run};
use crosstalk_eval::report::table::render;
use crosstalk_eval::report::{GateDetector, GateStatus, Gates};
use serde_json::json;

use super::fixture;
use super::{inputs, judged, read_rows};

/// A `session` row in the swarm's pinned key order.
fn session(agent: &str, session: &str) -> serde_json::Value {
    json!({"kind": "session", "world": fixture::WORLD, "agent": agent, "key_group": 2,
        "session": session, "started_at_unix_ms": 1_790_812_800_100_u64})
}

/// Rows that name only a001 and a003 (a003's read of p2 and a001's
/// self-read): no row names a002's session, where two of the gateway's
/// three detections are read.
fn without_a002() -> Vec<serde_json::Value> {
    let all = fixture::truth_rows();
    vec![fixture::header(), all[2].clone(), all[3].clone()]
}

fn unknown_agents(outcome: &crosstalk_eval::datasets::swarm_truth::SwarmOutcome) -> usize {
    outcome.diagnostics.named("unknown_detected_agent").count()
}

// ---- decoding ----

#[test]
fn a_session_row_decodes_with_exactly_the_pinned_keys() {
    let row = session("a002", "session-a002");
    let mut keys: Vec<&str> = row
        .as_object()
        .expect("an object")
        .keys()
        .map(String::as_str)
        .collect();
    keys.sort_unstable();
    let mut pinned = [
        "kind",
        "world",
        "agent",
        "key_group",
        "session",
        "started_at_unix_ms",
    ];
    pinned.sort_unstable();
    assert_eq!(keys, pinned);
    let truth = read_rows(&[fixture::header(), row.clone()]).expect("decodes");
    let Row::Session(start) = &truth.rows[0].row else {
        panic!("a session row");
    };
    assert_eq!(
        (
            start.agent.as_str(),
            start.session.as_str(),
            start.key_group
        ),
        ("a002", "session-a002", 2)
    );
    assert_eq!(start.started_at_unix_ms, 1_790_812_800_100);

    let mut extra = row.clone();
    extra["reader"] = json!("a002");
    assert!(matches!(
        read_rows(&[fixture::header(), extra]),
        Err(TruthFileError::Decode { line: 2, .. })
    ));
    for key in [
        "world",
        "agent",
        "key_group",
        "session",
        "started_at_unix_ms",
    ] {
        let mut missing = row.clone();
        missing
            .as_object_mut()
            .expect("an object")
            .remove(key)
            .expect("the key is there");
        assert!(
            matches!(
                read_rows(&[fixture::header(), missing]),
                Err(TruthFileError::Decode { line: 2, .. })
            ),
            "a session row without {key} is refused"
        );
    }
    let mut other_world = row;
    other_world["world"] = json!("swarm-other");
    assert!(matches!(
        read_rows(&[fixture::header(), other_world]),
        Err(TruthFileError::OtherWorld { line: 2, .. })
    ));
}

// ---- the agent ↔ session map ----

#[test]
fn a_session_only_conversations_detections_are_scored_not_dropped() {
    let dir = fixture::dir("session-none");
    let written = fixture::write(&dir, &without_a002());
    let before = run(&inputs(&written), 50, &Gates::default()).expect("the run scores");
    // The found and reread detections are read in a002's session, which no
    // row names: both are dropped.
    assert_eq!(unknown_agents(&before), 2);
    assert!(
        before
            .diagnostics
            .named("unknown_detected_agent")
            .all(|entry| entry.effect == Effect::PredictionsDropped)
    );
    assert_eq!(before.detected.predictions, 1);

    let dir = fixture::dir("session-row");
    let mut rows = without_a002();
    rows.push(session("a002", "session-a002"));
    let written = fixture::write(&dir, &rows);
    let after = run(&inputs(&written), 50, &Gates::default()).expect("the run scores");
    assert_eq!(unknown_agents(&after), 0);
    assert_eq!(after.resolved.sessions, 1);
    assert_eq!(after.detected.predictions, 3);
    // No label covers a002's reads, so both are false positives that count
    // against precision; the self-read detection still violates its control.
    let (_, before_false, _) = judged(&before);
    let (correct, after_false, unjudged) = judged(&after);
    assert_eq!((correct, after_false, unjudged), (0, before_false + 2, 0));
    let a002_reads = [written.a002[1].id, written.a002[2].id];
    let on_a002: Vec<_> = after
        .report
        .false_positives
        .iter()
        .filter(|fp| a002_reads.contains(&fp.prediction.reader_exchange))
        .collect();
    assert_eq!(on_a002.len(), 2);
    assert!(on_a002.iter().all(|fp| fp.violated.is_none()));
    assert!(after.report.overall.precision.is_some_and(|p| p < 1.0));
}

#[test]
fn a_session_row_names_a_sessions_agent_over_the_other_rows() {
    let dir = fixture::dir("session-conflict");
    let mut rows = fixture::truth_rows();
    // a002's session is a005's by its session row; the delivery rows still
    // name a002 for it.
    rows.push(session("a005", "session-a002"));
    let written = fixture::write(&dir, &rows);
    let outcome = run(&inputs(&written), 50, &Gates::default()).expect("the run scores");
    let conflicts: Vec<_> = outcome.diagnostics.named("session_conflict").collect();
    assert_eq!(conflicts.len(), 1);
    assert_eq!(conflicts[0].effect, Effect::Noted);
    let found = outcome
        .predictions
        .iter()
        .find(|prediction| prediction.reader_exchange == written.a002[1].id)
        .expect("the found detection");
    assert_eq!(found.to, super::key("a005"));
}

#[test]
fn a_session_row_the_log_lacks_is_noted() {
    let dir = fixture::dir("session-unknown");
    let mut rows = fixture::truth_rows();
    rows.push(session("a004", "session-a004"));
    let written = fixture::write(&dir, &rows);
    let outcome = run(&inputs(&written), 50, &Gates::default()).expect("the run scores");
    let unknown: Vec<_> = outcome
        .diagnostics
        .entries
        .iter()
        .filter(|entry| entry.row == Some(RowKind::Session))
        .map(|entry| (entry.line, entry.side, entry.failure.name(), entry.effect))
        .collect();
    assert_eq!(
        unknown,
        [(Some(10), Side::Row, "unknown_session", Effect::Noted)]
    );
    assert_eq!(outcome.resolved.sessions, 1);
}

#[test]
fn a_truth_file_without_session_rows_scores_as_before() {
    let dir = fixture::dir("session-agree-none");
    let written = fixture::write(&dir, &fixture::truth_rows());
    let plain = run(&inputs(&written), 50, &Gates::default()).expect("the run scores");
    assert_eq!(plain.resolved.sessions, 0);

    // Session rows that agree with the other rows change nothing but
    // their own count. They interleave with the other rows in event
    // order, as the swarm writes them: a002's and a003's come after
    // transmission rows that already named their sessions.
    let dir = fixture::dir("session-agree-rows");
    let mut rows = fixture::truth_rows();
    rows.insert(7, session("a003", "session-a003"));
    rows.insert(3, session("a002", "session-a002"));
    rows.insert(1, session("a001", "session-a001"));
    let kinds: Vec<&str> = rows
        .iter()
        .map(|row| row["kind"].as_str().expect("a kind"))
        .collect();
    assert_eq!(
        kinds,
        [
            "header",
            "session",
            "transmission",
            "transmission",
            "session",
            "self_read",
            "reread",
            "miss",
            "transmission",
            "session",
            "transmission",
            "agent_cluster"
        ]
    );
    let written = fixture::write(&dir, &rows);
    let started = run(&inputs(&written), 50, &Gates::default()).expect("the run scores");
    assert_eq!(started.resolved.sessions, 3);
    assert_eq!(started.resolved.rows, plain.resolved.rows + 3);
    assert_eq!(started.predictions, plain.predictions);
    assert_eq!(started.report.overall, plain.report.overall);
    assert_eq!(started.report.rows, plain.report.rows);
    assert_eq!(started.report.totals, plain.report.totals);
    let names = |outcome: &crosstalk_eval::datasets::swarm_truth::SwarmOutcome| {
        outcome
            .diagnostics
            .entries
            .iter()
            .map(|entry| (entry.failure.name(), entry.effect))
            .collect::<Vec<_>>()
    };
    assert_eq!(names(&started), names(&plain));
}

// ---- the exchange count ----

#[test]
fn the_report_counts_the_exchanges_of_the_truths_sessions() {
    let dir = fixture::dir("exchanges-all");
    let written = fixture::write(&dir, &fixture::truth_rows());
    let outcome = run(&inputs(&written), 50, &Gates::default()).expect("the run scores");
    let all = (written.a001.len() + written.a002.len() + written.a003.len()) as u64;
    assert_eq!(all, 11);
    assert_eq!(outcome.resolved.exchanges, all);
    assert_eq!(outcome.report.totals.exchanges, all);
    assert!(render(&outcome.report).contains("11 exchanges"));

    // a002's session is in the log but in no row: its exchanges are not
    // the truth's.
    let dir = fixture::dir("exchanges-some");
    let written = fixture::write(&dir, &without_a002());
    let outcome = run(&inputs(&written), 50, &Gates::default()).expect("the run scores");
    let named = (written.a001.len() + written.a003.len()) as u64;
    assert_eq!(outcome.report.totals.exchanges, named);

    // A session row brings them in.
    let dir = fixture::dir("exchanges-session");
    let mut rows = without_a002();
    rows.push(session("a002", "session-a002"));
    let written = fixture::write(&dir, &rows);
    let outcome = run(&inputs(&written), 50, &Gates::default()).expect("the run scores");
    assert_eq!(outcome.report.totals.exchanges, all);
}

// ---- gates ----

fn reread_gate(detector: &str, max: u64) -> String {
    format!(
        "[[gate]]\nname = \"{detector} rereads\"\ndetector = \"{detector}\"\ndataset = \"demo-swarm/headline\"\nmetric = \"violations\"\nreason = \"reread\"\nmax = {max}\n"
    )
}

#[test]
fn gates_name_the_swarm_detector_as_its_reports_do() {
    let gates = Gates::parse(&reread_gate(DETECTOR, 0), "fixture").expect("parses");
    assert_eq!(gates.gates[0].detector, GateDetector::GatewayExport);
}

#[test]
fn a_swarm_run_checks_only_its_own_detectors_gates() {
    let dir = fixture::dir("gates");
    let written = fixture::write(&dir, &fixture::truth_rows());
    // The fixture's reread detection violates one reread control.
    let text = [
        reread_gate(DETECTOR, 1),
        reread_gate(DETECTOR, 0),
        reread_gate("live", 0),
        "[[gate]]\nname = \"reference rereads\"\nmetric = \"violations\"\nreason = \"reread\"\nmax = 0\n".to_owned(),
    ]
    .concat();
    let gates = Gates::parse(&text, "fixture").expect("parses");
    let outcome = run(&inputs(&written), 50, &gates).expect("the run scores");
    let statuses: Vec<&GateStatus> = outcome
        .report
        .gates
        .iter()
        .map(|outcome| &outcome.status)
        .collect();
    assert_eq!(
        statuses,
        [
            &GateStatus::Pass { value: 1.0 },
            &GateStatus::Fail {
                value: 1.0,
                bound: 0.0
            },
        ]
    );
    assert!(outcome.report.gates_failed());
}

#[test]
fn the_shipped_gates_hold_the_demo_swarm_gates() {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("gates.toml");
    let gates = Gates::load(&path).expect("the shipped gates load");
    let swarm = gates.for_detector(GateDetector::GatewayExport);
    let datasets: Vec<&str> = swarm
        .gates
        .iter()
        .map(|gate| gate.dataset.as_ref().map_or("", |d| d.as_str()))
        .collect();
    assert_eq!(
        datasets,
        [
            "demo-swarm/headline",
            "demo-swarm/headline",
            "demo-swarm/headline",
            "demo-swarm/boilerplate"
        ]
    );
    // A swarm run checks all four, and only them.
    let dir = fixture::dir("gates-shipped");
    let written = fixture::write(&dir, &fixture::truth_rows());
    let outcome = run(&inputs(&written), 50, &gates).expect("the run scores");
    assert_eq!(outcome.report.gates.len(), 4);
}
