//! Access-only predictions under negative controls. One rule: a discarded
//! prediction is dismissed (no false positive, no violation), with the
//! control it fell under recorded; a suspected one stays a false positive
//! in its own access-class row but is never charged to the control. Both
//! show on the access-only line; neither is gated.

use crosstalk_eval::datasets::swarm_truth::{DETECTOR, SwarmOutcome, run};
use crosstalk_eval::predict::EvidenceClass;
use crosstalk_eval::report::table::render;
use crosstalk_eval::report::{GateStatus, Gates};
use crosstalk_eval::score::{AccessOnlyControlRow, Counts, ViolationRow};
use crosstalk_eval::truth::NegativeReason;

use super::fixture;
use super::inputs;

const HEADING: &str =
    "access-only predictions under negative controls (not violations, not gated):";

fn count(rows: &[ViolationRow], reason: NegativeReason) -> u64 {
    rows.iter()
        .filter(|row| row.reason == reason)
        .map(|row| row.count)
        .sum()
}

fn under(rows: &[AccessOnlyControlRow], class: EvidenceClass, reason: NegativeReason) -> u64 {
    rows.iter()
        .filter(|row| row.class == class && row.reason == reason)
        .map(|row| row.count)
        .sum()
}

fn class_counts(outcome: &SwarmOutcome, class: EvidenceClass) -> Counts {
    let mut sum = Counts::default();
    for row in outcome
        .report
        .rows
        .iter()
        .filter(|row| row.key.class == class)
    {
        sum.add(&row.counts);
    }
    sum
}

/// The fixture with one access-only transmission on line 5's reread,
/// scored with a reread gate allowing the confirmed one.
fn scored(name: &str, discarded: bool) -> SwarmOutcome {
    let dir = fixture::dir(name);
    let written = fixture::write(&dir, &fixture::truth_rows());
    fixture::append_access_only_reread(&written, discarded);
    let gates = Gates::parse(
        &format!(
            "[[gate]]\nname = \"rereads\"\ndetector = \"{DETECTOR}\"\ndataset = \"demo-swarm/headline\"\nmetric = \"violations\"\nreason = \"reread\"\nmax = 1\n"
        ),
        "fixture",
    )
    .expect("parses");
    run(&inputs(&written), 50, &gates).expect("the run scores")
}

#[test]
fn a_discarded_prediction_on_a_reread_is_dismissed_with_its_control_recorded() {
    let outcome = scored("access-only-discarded-reread", true);
    assert_eq!(outcome.detected.predictions, 4);
    let discarded = class_counts(&outcome, EvidenceClass::Discarded);
    assert_eq!(
        (
            discarded.predicted,
            discarded.dismissed,
            discarded.false_positive
        ),
        (1, 1, 0)
    );
    // The gateway's confirmed reread detection is still the one violation.
    assert_eq!(count(&outcome.report.violations, NegativeReason::Reread), 1);
    let rows = &outcome.report.access_only_under_controls;
    assert_eq!(
        under(rows, EvidenceClass::Discarded, NegativeReason::Reread),
        1
    );
    assert_eq!(rows.len(), 1, "one breakdown row, no parallel counter");
    let text = render(&outcome.report);
    assert!(text.contains(&format!("{HEADING}\n  dismissed on reread controls: 1")));
    assert_eq!(
        outcome.report.gates[0].status,
        GateStatus::Pass { value: 1.0 }
    );
}

#[test]
fn a_suspected_prediction_on_a_reread_is_reported_apart_not_a_violation() {
    let outcome = scored("access-only-suspected-reread", false);
    assert_eq!(outcome.detected.predictions, 4);
    let suspected = class_counts(&outcome, EvidenceClass::Suspected);
    assert_eq!(
        (
            suspected.predicted,
            suspected.false_positive,
            suspected.dismissed
        ),
        (1, 1, 0)
    );
    assert_eq!(count(&outcome.report.violations, NegativeReason::Reread), 1);
    assert_eq!(
        under(
            &outcome.report.access_only_under_controls,
            EvidenceClass::Suspected,
            NegativeReason::Reread
        ),
        1
    );
    assert!(
        render(&outcome.report)
            .contains(&format!("{HEADING}\n  suspected under reread controls: 1"))
    );
    assert_eq!(
        outcome.report.gates[0].status,
        GateStatus::Pass { value: 1.0 }
    );
}

#[test]
fn a_run_with_no_access_only_overlap_reports_none() {
    let dir = fixture::dir("access-only-none");
    let written = fixture::write(&dir, &fixture::truth_rows());
    let outcome = run(&inputs(&written), 50, &Gates::default()).expect("the run scores");
    assert!(outcome.report.access_only_under_controls.is_empty());
    assert_eq!(count(&outcome.report.violations, NegativeReason::Reread), 1);
    assert!(!render(&outcome.report).contains(HEADING));
}
