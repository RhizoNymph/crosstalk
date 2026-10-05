//! `ct-eval replay` over the synthetic run: the fixture's exchange log and
//! blobs through the live composition, scored against its truth.
//!
//! The fixture holds the minimal reread shape of the node0 bench's
//! violations: a002 reads a001's p1 (turn 1), then reads the same version
//! again (turn 2, a `reread` row). L5 opens a co-access on the reread and
//! discards it (INV-1122); the scorer dismisses it.

use std::time::Duration;

use crosstalk_eval::datasets::swarm_truth::replay::{ReplayError, demo_flow};
use crosstalk_eval::datasets::swarm_truth::{
    ReplayInputs, ReplayOptions, ReplayOutcome, SwarmTruthError, run_replay,
};
use crosstalk_eval::predict::EvidenceClass;
use crosstalk_eval::report::Gates;
use crosstalk_eval::score::Selector;
use crosstalk_eval::truth::NegativeReason;
use crosstalk_spec::support::Timestamp;
use crosstalk_testkit::time::{T0, after};

use super::fixture::{self, Written};
use super::key;

fn inputs(written: &Written) -> ReplayInputs {
    ReplayInputs {
        truth: written.truth.clone(),
        exchanges: written.exchanges.clone(),
        blobs: written.blobs.clone(),
    }
}

fn options(since: Option<Timestamp>) -> ReplayOptions {
    ReplayOptions {
        flow: demo_flow(10_000, 60_000),
        seed: 0,
        since,
        until: None,
    }
}

fn replayed(
    name: &str,
    since: Option<Timestamp>,
) -> (Written, Result<ReplayOutcome, SwarmTruthError>) {
    let dir = fixture::dir(name);
    let written = fixture::write(&dir, &fixture::truth_rows());
    let outcome = run_replay(&inputs(&written), &options(since), 50, &Gates::default());
    (written, outcome)
}

#[test]
fn a_replay_scores_the_runs_log_through_the_live_composition() {
    let (written, outcome) = replayed("replay", None);
    let outcome = outcome.expect("the replay scores");
    assert_eq!(
        (outcome.replayed.ingested, outcome.replayed.skipped),
        (11, 0)
    );
    assert_eq!(
        outcome.replayed.exported.transmissions.len(),
        outcome.replayed.evidence.len(),
        "every exported row has its evidence"
    );
    let report = &outcome.scored.report;
    assert_eq!(
        (report.overall.counts.found, report.overall.counts.expected),
        (3, 3)
    );
    assert_eq!(report.overall.counts.false_positive, 0);
    // The reread: one discarded co-access at a002's second read of p1,
    // dismissed, never a violation.
    let discarded: Vec<_> = outcome
        .scored
        .predictions
        .iter()
        .filter(|p| p.class == EvidenceClass::Discarded)
        .collect();
    assert_eq!(discarded.len(), 1, "{discarded:?}");
    assert_eq!(discarded[0].from, key("a001"));
    assert_eq!(discarded[0].to, key("a002"));
    assert_eq!(discarded[0].reader_exchange, written.a002[2].id);
    let score_rows = Selector {
        class: Some(EvidenceClass::Discarded),
        ..Selector::default()
    };
    let rows: Vec<_> = report
        .rows
        .iter()
        .filter(|row| score_rows.matches(&row.key))
        .collect();
    assert_eq!(rows.len(), 1);
    assert_eq!(
        (rows[0].counts.dismissed, rows[0].counts.false_positive),
        (1, 0)
    );
    assert!(
        report
            .violations
            .iter()
            .all(|row| row.reason != NegativeReason::Reread),
        "{:?}",
        report.violations
    );
    // No content prediction at the reread exchange.
    assert!(
        outcome
            .scored
            .predictions
            .iter()
            .filter(|p| p.class.is_content())
            .all(|p| p.reader_exchange != written.a002[2].id)
    );
}

#[test]
fn a_replay_is_deterministic() {
    let (_, first) = replayed("replay-first", None);
    let (_, second) = replayed("replay-second", None);
    let (first, second) = (
        first.expect("the first replay"),
        second.expect("the second replay"),
    );
    assert_eq!(first.scored.predictions, second.scored.predictions);
    assert_eq!(first.scored.report, second.scored.report);
    assert_eq!(first.replayed.export_bytes, second.replayed.export_bytes);
}

#[test]
fn entries_before_since_are_skipped() {
    // The fixture's exchanges are 10 s apart from T0 + 10 s.
    let (_, outcome) = replayed("replay-since", Some(after(T0, Duration::from_secs(35))));
    let outcome = outcome.expect("the replay scores");
    assert_eq!(
        (outcome.replayed.ingested, outcome.replayed.skipped),
        (8, 3)
    );
}

#[test]
fn a_replay_with_nothing_to_replay_is_refused() {
    let (_, outcome) = replayed("replay-empty", Some(after(T0, Duration::from_secs(3600))));
    assert!(
        matches!(
            outcome,
            Err(SwarmTruthError::Replay(ReplayError::Empty { .. }))
        ),
        "{outcome:?}"
    );
}
