//! Gates check only the runs of the detector they are tuned on.

use crosstalk_eval::report::{GateDetector, Gates};

const GATES: &str = r#"
[[gate]]
name = "reference recall"
metric = "recall"
min = 0.9

[[gate]]
name = "live recall"
detector = "live"
metric = "recall"
min = 0.5

[[gate]]
name = "reference precision"
detector = "reference"
metric = "precision"
min = 0.9
"#;

fn names(gates: &Gates) -> Vec<&str> {
    gates.gates.iter().map(|gate| gate.name.as_str()).collect()
}

#[test]
fn a_gate_without_a_detector_is_the_reference_matchers() {
    let gates = Gates::parse(GATES, "fixture").unwrap_or_else(|e| panic!("{e}"));
    assert_eq!(gates.gates[0].detector, GateDetector::Reference);
    assert_eq!(
        names(&gates.for_detector(GateDetector::Reference)),
        ["reference recall", "reference precision"]
    );
    assert_eq!(
        names(&gates.for_detector(GateDetector::Live)),
        ["live recall"]
    );
    assert!(gates.for_detector(GateDetector::Pipeline).gates.is_empty());
}

#[test]
fn an_unknown_detector_is_refused() {
    let text = "[[gate]]\nname = \"x\"\ndetector = \"oracle\"\nmetric = \"recall\"\nmin = 0.5\n";
    assert!(Gates::parse(text, "fixture").is_err());
}

#[test]
fn the_shipped_gates_parse_and_gate_both_detectors() {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("gates.toml");
    let gates = Gates::load(&path).unwrap_or_else(|e| panic!("{e}"));
    assert!(!gates.for_detector(GateDetector::Reference).gates.is_empty());
    assert!(!gates.for_detector(GateDetector::Live).gates.is_empty());
}
