//! The run window: a reused session id's exchanges from an earlier run in
//! the same exchange log are left out of the traffic count, the session
//! ordinals and the agent map, and a detection read in them is not scored.

use std::io::Cursor;

use crosstalk_eval::datasets::swarm_truth::bodies::{BlobBodies, Cached};
use crosstalk_eval::datasets::swarm_truth::exchange_log::{self, Sessions};
use crosstalk_eval::datasets::swarm_truth::window::{self, DEFAULT_SLACK_MS, RunWindow};
use crosstalk_eval::datasets::swarm_truth::{
    Effect, JoinFailure, Options, Side, SwarmOutcome, resolve, run, run_with, truth_file,
};
use crosstalk_eval::report::Gates;
use crosstalk_eval::report::table::render;
use crosstalk_eval::truth::{Expectation, NegativeReason};
use crosstalk_spec::support::Timestamp;
use serde_json::json;

use super::fixture::{self, P1, Written};
use super::{inputs, judged, key, read_rows};

/// The fixture header's start, in Unix milliseconds.
const START_MS: u64 = 1_790_812_800_000;
/// The latest time the fixture's rows name (the miss, at `START_MS` + 2 s).
const LATEST_MS: u64 = 1_790_812_802_000;

fn scored(name: &str, prior: bool) -> (Written, SwarmOutcome) {
    let dir = fixture::dir(name);
    let written = if prior {
        fixture::write_with_prior_run(&dir, &fixture::truth_rows())
    } else {
        fixture::write(&dir, &fixture::truth_rows())
    };
    let outcome = run(&inputs(&written), 50, &Gates::default()).expect("the run scores");
    (written, outcome)
}

/// The diagnostics table without the run window's own rows.
fn other_diagnostics(
    outcome: &SwarmOutcome,
) -> Vec<crosstalk_eval::datasets::swarm_truth::diagnostics::DiagnosticCount> {
    outcome
        .diagnostics
        .table()
        .into_iter()
        .filter(|count| {
            count.failure != "session_reused_outside_run" && count.failure != "outside_run_window"
        })
        .collect()
}

/// The world's labels, resolved from the fixture's files through the run
/// window, as `score` does.
fn windowed_truth(written: &Written) -> Vec<Expectation> {
    let text = std::fs::read_to_string(&written.truth).expect("the truth file");
    let truth = truth_file::read(Cursor::new(text)).expect("a valid truth file");
    let log = exchange_log::read(&written.exchanges).expect("the exchange log");
    let run_window = RunWindow::of(&truth, DEFAULT_SLACK_MS);
    let split = window::split(log.exchanges, run_window, &window::truth_sessions(&truth));
    let sessions = Sessions::index(split.inside);
    let mut bodies = Cached::new(BlobBodies::open(&written.blobs).expect("the blobs"));
    resolve(&truth, "truth.jsonl", &sessions, &mut bodies)
        .expect("resolves")
        .world
        .truth()
        .to_vec()
}

// ---- the window ----

#[test]
fn the_window_runs_from_the_header_to_the_latest_row_plus_the_slack() {
    let truth = read_rows(&fixture::truth_rows()).expect("decodes");
    let found = RunWindow::of(&truth, DEFAULT_SLACK_MS);
    assert_eq!(DEFAULT_SLACK_MS, 60_000);
    assert_eq!(
        found,
        RunWindow {
            start_unix_ms: START_MS,
            end_unix_ms: LATEST_MS + 60_000,
        }
    );
    assert_eq!(
        RunWindow::of(&truth, 5).end_unix_ms,
        LATEST_MS + 5,
        "the slack is configurable"
    );
}

#[test]
fn a_rows_read_and_write_times_extend_the_window() {
    let mut late = fixture::delivery(
        "transmission",
        ("a001", "session-a001", 0, "toolu_w1"),
        ("a002", "session-a002", 1, "toolu_r1"),
        "p1",
        P1,
    );
    late["read_at_unix_ms"] = json!(START_MS + 9_000);
    late["written_at_unix_ms"] = json!(START_MS + 7_000);
    let truth = read_rows(&[fixture::header(), late.clone()]).expect("decodes");
    assert_eq!(RunWindow::of(&truth, 0).end_unix_ms, START_MS + 9_000);

    late["read_at_unix_ms"] = json!(START_MS + 1_000);
    let truth = read_rows(&[fixture::header(), late]).expect("decodes");
    assert_eq!(RunWindow::of(&truth, 0).end_unix_ms, START_MS + 7_000);
}

#[test]
fn a_truth_with_no_timed_row_ends_at_its_start_plus_the_slack() {
    let cluster = json!({"kind": "agent_cluster", "world": fixture::WORLD, "key_group": 0,
        "agents": ["a001"]});
    let truth = read_rows(&[fixture::header(), cluster]).expect("decodes");
    assert_eq!(
        RunWindow::of(&truth, 1_000),
        RunWindow {
            start_unix_ms: START_MS,
            end_unix_ms: START_MS + 1_000,
        }
    );
}

#[test]
fn both_ends_of_the_window_are_inclusive() {
    let window = RunWindow {
        start_unix_ms: START_MS,
        end_unix_ms: LATEST_MS,
    };
    let at = |micros: u64| Timestamp::from_micros(micros);
    assert!(!window.contains(at(START_MS * 1000 - 1)));
    assert!(window.contains(at(START_MS * 1000)));
    assert!(window.contains(at(LATEST_MS * 1000 + 999)));
    assert!(!window.contains(at((LATEST_MS + 1) * 1000)));
}

// ---- a run without reuse ----

#[test]
fn a_run_without_reuse_excludes_nothing() {
    let (written, outcome) = scored("window-no-reuse", false);
    assert!(written.prior_a002.is_empty());
    assert_eq!(outcome.resolved.excluded_outside_window, 0);
    assert_eq!(outcome.resolved.exchanges, 11);
    assert_eq!(outcome.report.totals.exchanges, 11);
    assert_eq!(
        outcome
            .diagnostics
            .named("session_reused_outside_run")
            .count(),
        0
    );
    assert_eq!(outcome.diagnostics.named("outside_run_window").count(), 0);
    assert_eq!(outcome.detected.predictions, 3);
    assert_eq!(
        outcome.window,
        RunWindow {
            start_unix_ms: START_MS,
            end_unix_ms: LATEST_MS + DEFAULT_SLACK_MS,
        }
    );
}

#[test]
fn a_smaller_slack_leaves_out_the_later_exchanges() {
    let dir = fixture::dir("window-no-slack");
    let written = fixture::write(&dir, &fixture::truth_rows());
    let options = Options {
        run_slack_ms: 0,
        ..Options::new(50)
    };
    let outcome = run_with(&inputs(&written), options, &Gates::default()).expect("scores");
    // The run's exchanges start at T0 + 1 s, + 2 s, …, + 11 s; the rows end
    // at T0 + 2 s.
    assert_eq!(outcome.window.end_unix_ms, LATEST_MS);
    assert_eq!(outcome.resolved.exchanges, 2);
    assert_eq!(outcome.resolved.excluded_outside_window, 9);
    assert_eq!(outcome.report.totals.exchanges, 2);
}

// ---- a reused session ----

#[test]
fn a_reused_sessions_earlier_exchanges_are_excluded_and_reported() {
    let (written, outcome) = scored("window-reuse-excluded", true);
    assert_eq!(written.prior_a002.len(), 2);
    let reused: Vec<_> = outcome
        .diagnostics
        .named("session_reused_outside_run")
        .collect();
    assert_eq!(reused.len(), 2);
    for (diagnostic, turn) in reused.iter().zip(&written.prior_a002) {
        assert_eq!(diagnostic.effect, Effect::Excluded);
        assert_eq!(diagnostic.side, Side::Row);
        assert_eq!(diagnostic.line, None);
        assert_eq!(
            diagnostic.failure,
            JoinFailure::SessionReusedOutsideRun {
                session: "session-a002".to_owned(),
                exchange: turn.id,
            }
        );
    }
    assert_eq!(outcome.resolved.excluded_outside_window, 2);
    let excluded = outcome
        .diagnostics
        .table()
        .into_iter()
        .find(|count| count.failure == "session_reused_outside_run")
        .expect("a table row");
    assert_eq!((excluded.effect, excluded.count), (Effect::Excluded, 2));
    assert!(
        outcome
            .diagnostics
            .render()
            .contains("| - | row | session_reused_outside_run | excluded | 2 |")
    );
}

#[test]
fn the_traffic_and_the_fp_denominator_count_in_window_exchanges_only() {
    let (_, plain) = scored("window-denominator-plain", false);
    let (_, reused) = scored("window-denominator-reused", true);
    assert_eq!(reused.resolved.exchanges, 11);
    assert_eq!(reused.report.totals.exchanges, 11);
    assert!(render(&reused.report).contains("11 exchanges"));
    let background = |outcome: &SwarmOutcome| {
        outcome
            .report
            .background
            .as_ref()
            .map(|background| (background.false_positives, background.exchanges))
    };
    assert_eq!(background(&reused), background(&plain));
    assert_eq!(
        background(&reused).map(|(_, exchanges)| exchanges),
        Some(11)
    );
}

#[test]
fn ordinals_count_only_the_in_window_exchanges() {
    let (written, outcome) = scored("window-ordinals", true);
    let truth = windowed_truth(&written);
    let label = truth
        .iter()
        .find_map(|expectation| match expectation {
            Expectation::Transmission(expected) if expected.label().to == key("a002") => {
                Some(expected.label().clone())
            }
            _ => None,
        })
        .expect("line 2 is labelled");
    // The earlier run's second turn holds the same tool result; the join
    // lands on this run's.
    assert_eq!(label.reader_exchange, written.a002[1].id);
    assert_ne!(label.reader_exchange, written.prior_a002[1].id);
    let reread = truth
        .iter()
        .find_map(|expectation| match expectation {
            Expectation::NoTransmission(control)
                if control.label().reason == NegativeReason::Reread =>
            {
                Some(control.label().clone())
            }
            _ => None,
        })
        .expect("a reread control");
    assert_eq!(reread.reader_exchange, Some(written.a002[2].id));

    // Every other join is as in a run without reuse: one turn mismatch
    // (line 7), no new one.
    let (_, plain) = scored("window-ordinals-plain", false);
    assert_eq!(other_diagnostics(&outcome), other_diagnostics(&plain));
    assert_eq!(outcome.diagnostics.named("turn_mismatch").count(), 1);
}

#[test]
fn a_detection_read_before_the_run_is_excluded_not_false() {
    let (written, outcome) = scored("window-detection", true);
    let (_, plain) = scored("window-detection-plain", false);
    assert_eq!(written.transmissions.len(), 4);
    let earlier = &written.transmissions[3];
    let excluded: Vec<_> = outcome.diagnostics.named("outside_run_window").collect();
    assert_eq!(excluded.len(), 1);
    assert_eq!(excluded[0].effect, Effect::Excluded);
    assert_eq!(
        excluded[0].failure,
        JoinFailure::OutsideRunWindow {
            transmission: earlier.id,
            exchange: written.prior_a002[1].id,
        }
    );
    assert_eq!(outcome.detected.exported, 4);
    assert_eq!(outcome.detected.evidence, 4);
    assert_eq!(outcome.detected.predictions, plain.detected.predictions);
    assert_eq!(judged(&outcome), judged(&plain));
    assert_eq!(
        outcome.report.totals.predictions,
        plain.report.totals.predictions
    );
    assert_eq!(
        outcome.diagnostics.named("unknown_detected_agent").count(),
        0
    );
}
