//! The run window: a reused session id's exchanges from an earlier run in
//! the same exchange log are left out of the capture and of the session
//! ordinals, and a transmission read only in them is not written.

use std::io::Cursor;

use crosstalk_bench_adapter::swarm::exchange_log::{self, Sessions};
use crosstalk_bench_adapter::swarm::truth_file;
use crosstalk_bench_adapter::swarm::window::{
    self, DEFAULT_LEAD_MS, DEFAULT_SLACK_MS, Margins, Reused, RunWindow, Split,
};
use crosstalk_spec::support::Timestamp;
use serde_json::json;

use super::fixture::{self, P1, Written};
use super::read_rows;

/// The fixture header's start, in Unix milliseconds.
const START_MS: u64 = 1_790_812_800_000;
/// The latest time the fixture's rows name (the miss, at `START_MS` + 101 s).
const LATEST_MS: u64 = 1_790_812_901_000;

fn margins(lead_ms: u64, slack_ms: u64) -> Margins {
    Margins { lead_ms, slack_ms }
}

/// The fixture written into `name` (with an earlier run in a002's session
/// when `prior` is `Some(0)`, its clock `behind` seconds behind the
/// header's when `prior` is `Some(behind)` with `behind > 0`), and its
/// exchange log split by the run window under `margins`.
fn split_of(name: &str, prior: Option<u64>, margins: Margins) -> (Written, Split) {
    let dir = fixture::dir(name);
    let written = match prior {
        None => fixture::write(&dir, &fixture::truth_rows()),
        Some(0) => fixture::write_with_prior_run(&dir, &fixture::truth_rows()),
        Some(behind) => fixture::write_skewed(&dir, &fixture::truth_rows(), behind),
    };
    let text = std::fs::read_to_string(&written.truth).expect("the truth file");
    let truth = truth_file::read(Cursor::new(text)).expect("a valid truth file");
    let log = exchange_log::read(&written.exchanges).expect("the exchange log");
    let run_window = RunWindow::of(&truth, margins);
    let split = window::split(log.exchanges, run_window, &window::truth_sessions(&truth));
    (written, split)
}

// ---- the window ----

#[test]
fn the_window_runs_from_the_header_less_the_lead_to_the_latest_row_plus_the_slack() {
    let truth = read_rows(&fixture::truth_rows()).expect("decodes");
    let found = RunWindow::of(&truth, Margins::default());
    assert_eq!(DEFAULT_SLACK_MS, 60_000);
    assert_eq!(DEFAULT_LEAD_MS, 5_000);
    assert_eq!(
        found,
        RunWindow {
            start_unix_ms: START_MS - 5_000,
            end_unix_ms: LATEST_MS + 60_000,
        }
    );
    assert_eq!(
        RunWindow::of(&truth, margins(7, 5)),
        RunWindow {
            start_unix_ms: START_MS - 7,
            end_unix_ms: LATEST_MS + 5,
        },
        "the lead and the slack are configurable"
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
    late["at_unix_ms"] = json!(START_MS + 1_000);
    late["read_at_unix_ms"] = json!(START_MS + 9_000);
    late["written_at_unix_ms"] = json!(START_MS + 7_000);
    let truth = read_rows(&[fixture::header(), late.clone()]).expect("decodes");
    assert_eq!(
        RunWindow::of(&truth, margins(0, 0)).end_unix_ms,
        START_MS + 9_000
    );

    late["read_at_unix_ms"] = json!(START_MS + 1_000);
    let truth = read_rows(&[fixture::header(), late]).expect("decodes");
    assert_eq!(
        RunWindow::of(&truth, margins(0, 0)).end_unix_ms,
        START_MS + 7_000
    );
}

#[test]
fn a_truth_with_no_timed_row_ends_at_its_start_plus_the_slack() {
    let cluster = json!({"kind": "agent_cluster", "world": fixture::WORLD, "key_group": 0,
        "agents": ["a001"]});
    let truth = read_rows(&[fixture::header(), cluster]).expect("decodes");
    assert_eq!(
        RunWindow::of(&truth, margins(0, 1_000)),
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
    let (written, split) = split_of("window-no-reuse", None, Margins::default());
    assert!(written.prior_a002.is_empty());
    assert!(split.outside.is_empty());
    assert!(split.reused.is_empty());
    let all = written.a001.len() + written.a002.len() + written.a003.len();
    assert!(
        split.inside.len() >= all,
        "every agent's exchange is inside"
    );
}

// ---- the lead ----

#[test]
fn an_exchange_just_before_the_headers_start_is_in_the_run() {
    // The run's clock is 12 s behind the header's: its first exchange
    // started 2 s before `started_at_unix_ms`.
    let (_, with_lead) = split_of("window-lead", Some(12), Margins::default());
    assert!(with_lead.outside.is_empty());
    // Without the lead, the exchange before the start (a001's turn 0, at
    // -2 s) is left out.
    let (written, without) = split_of("window-no-lead", Some(12), margins(0, DEFAULT_SLACK_MS));
    let expected: Vec<Reused> = written.a001[..1]
        .iter()
        .map(|turn| Reused {
            session: "session-a001".to_owned(),
            exchange: turn.id,
        })
        .collect();
    assert_eq!(without.reused, expected);
    assert_eq!(without.inside.len() + 1, with_lead.inside.len());
}

#[test]
fn the_lead_does_not_reach_an_earlier_run_an_hour_before() {
    let truth = read_rows(&fixture::truth_rows()).expect("decodes");
    let window = RunWindow::of(&truth, Margins::default());
    let at = |ms: u64| Timestamp::from_micros(ms * 1000);
    assert!(window.contains(at(START_MS - 2_000)));
    assert!(!window.contains(at(START_MS - 3_600_000)));
    let (written, split) = split_of("window-lead-prior", Some(0), Margins::default());
    assert_eq!(
        split.reused.len(),
        2,
        "the earlier run's two a002 exchanges"
    );
    assert_eq!(
        split.inside.len(),
        split_of("window-lead-plain", None, Margins::default())
            .1
            .inside
            .len()
    );
    assert!(
        written
            .prior_a002
            .iter()
            .all(|turn| split.outside.contains(&turn.id))
    );
}

// ---- a reused session ----

#[test]
fn ordinals_count_only_the_in_window_exchanges() {
    let (written, split) = split_of("window-ordinals", Some(0), Margins::default());
    let sessions = Sessions::index(split.inside);
    let a002 = sessions.get("session-a002").expect("a002's session");
    // The earlier run's second turn holds the same tool result; the
    // ordinal lands on this run's.
    assert_eq!(
        a002.at(1).map(|exchange| exchange.meta.id),
        Some(written.a002[1].id)
    );
    assert_eq!(a002.ordinal(written.a002[2].id), Some(2));
    assert!(
        written
            .prior_a002
            .iter()
            .all(|turn| a002.ordinal(turn.id).is_none())
    );
}
