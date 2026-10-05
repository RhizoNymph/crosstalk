//! Access-only predictions under negative controls: reported apart from
//! the violations, never gated, as access-only predictions finding a label
//! never count as found.

use crosstalk_eval::datasets::swarm_truth::{DETECTOR, run};
use crosstalk_eval::predict::EvidenceClass;
use crosstalk_eval::report::table::render;
use crosstalk_eval::report::{GateStatus, Gates};
use crosstalk_eval::score::ViolationRow;
use crosstalk_eval::truth::NegativeReason;

use super::fixture;
use super::inputs;

fn count(rows: &[ViolationRow], reason: NegativeReason) -> u64 {
    rows.iter()
        .filter(|row| row.reason == reason)
        .map(|row| row.count)
        .sum()
}

#[test]
fn a_discarded_prediction_on_a_reread_is_not_a_violation() {
    let dir = fixture::dir("access-only-reread");
    let written = fixture::write(&dir, &fixture::truth_rows());
    fixture::append_discarded_reread(&written);
    let gates = Gates::parse(
        &format!(
            "[[gate]]\nname = \"rereads\"\ndetector = \"{DETECTOR}\"\ndataset = \"demo-swarm/headline\"\nmetric = \"violations\"\nreason = \"reread\"\nmax = 1\n"
        ),
        "fixture",
    )
    .expect("parses");
    let outcome = run(&inputs(&written), 50, &gates).expect("the run scores");

    // The discarded transmission predicts, and falls under line 5's reread
    // control.
    assert_eq!(outcome.detected.predictions, 4);
    let discarded = outcome
        .report
        .rows
        .iter()
        .find(|row| row.key.class == EvidenceClass::Discarded)
        .expect("a discarded row");
    assert_eq!(
        (discarded.counts.predicted, discarded.counts.false_positive),
        (1, 1)
    );

    // The gateway's confirmed reread detection is still a violation; the
    // discarded one is reported apart.
    assert_eq!(count(&outcome.report.violations, NegativeReason::Reread), 1);
    assert_eq!(
        count(
            &outcome.report.access_only_violations,
            NegativeReason::Reread
        ),
        1
    );
    assert_eq!(
        count(
            &outcome.report.access_only_violations,
            NegativeReason::SelfRead
        ),
        0
    );
    let text = render(&outcome.report);
    assert!(text.contains(
        "access-only predictions under negative controls (not violations, not gated):\n  reread               1"
    ));

    // The gate counts the confirmed violation only.
    assert_eq!(outcome.report.gates.len(), 1);
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
    assert!(outcome.report.access_only_violations.is_empty());
    assert_eq!(count(&outcome.report.violations, NegativeReason::Reread), 1);
    assert!(!render(&outcome.report).contains("access-only predictions under"));
}
