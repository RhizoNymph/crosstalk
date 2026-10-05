//! The whole run on the fixtures: source, reference detector, scorer,
//! report, gates, config, and the `ct-eval` binary.

use std::path::{Path, PathBuf};
use std::process::Command;

use crosstalk_eval::config::{EvalConfig, expand};
use crosstalk_eval::corpus::World;
use crosstalk_eval::datasets::salt::{SaltSource, Selection};
use crosstalk_eval::pipeline::{DetectError, Detection, Detector, ReferenceDetector, run};
use crosstalk_eval::predict::EvidenceClass;
use crosstalk_eval::report::table::render;
use crosstalk_eval::report::{GateStatus, Gates, Report};
use crosstalk_eval::score::Selector;
use crosstalk_eval::truth::{NegativeReason, jsonl};

fn root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/salt")
}

fn temp(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("ct-eval-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap_or_else(|e| panic!("{e}"));
    dir
}

fn report() -> Report {
    let mut source =
        SaltSource::open(&root(), &Selection::default()).unwrap_or_else(|e| panic!("{e}"));
    let summary = run(
        &mut source,
        &mut ReferenceDetector::default(),
        50,
        |_, _| {},
    );
    assert!(summary.failures.is_empty(), "{:?}", summary.failures);
    // The shipped gates are tuned on the real dataset; they must parse.
    let shipped = Gates::load(&Path::new(env!("CARGO_MANIFEST_DIR")).join("gates.toml"))
        .unwrap_or_else(|e| panic!("{e}"));
    assert!(!shipped.gates.is_empty());
    let gates = Gates::parse(
        "[[gate]]\nname = \"labels found\"\nmetric = \"recall\"\nmin = 0.8\n",
        "fixture",
    )
    .unwrap_or_else(|e| panic!("{e}"));
    let outcomes = gates.evaluate(&summary.score);
    Report::new(
        source_id(),
        "reference",
        summary.score,
        outcomes,
        vec![],
        summary.unscored,
    )
}

fn source_id() -> crosstalk_eval::keys::DatasetId {
    crosstalk_eval::keys::DatasetId::new("salt")
}

#[test]
fn the_reference_finds_every_delivery_long_enough_to_match() {
    let report = report();
    let all = &report.overall.counts;
    assert_eq!(all.expected, 12);
    // "OK." twice (once per main-shaped world) is below the minimum span.
    assert_eq!((all.found, all.missed), (10, 2));
    assert_eq!(all.false_positive, 0);
    assert_eq!(report.misses.len(), 2);
    assert!(
        report
            .misses
            .iter()
            .all(|m| m.expectation.label().content.text == "OK.")
    );
    let decoded = report
        .rows
        .iter()
        .filter(|r| r.key.class == EvidenceClass::Decoded)
        .map(|r| r.counts.found)
        .sum::<u64>();
    assert_eq!(
        decoded, 2,
        "the escaped delivery, a JSON string, in both worlds"
    );
    assert_eq!(report.totals.worlds, 3);
    assert!(!report.gates_failed(), "{:?}", report.gates);
}

#[test]
fn reports_are_byte_identical_across_runs() {
    let a = serde_json::to_string(&report()).unwrap_or_default();
    let b = serde_json::to_string(&report()).unwrap_or_default();
    assert_eq!(a, b);
    assert!(render(&report()).contains("user_turn"));
}

#[test]
fn gates_pass_fail_and_skip() {
    let mut source =
        SaltSource::open(&root(), &Selection::default()).unwrap_or_else(|e| panic!("{e}"));
    let summary = run(&mut source, &mut ReferenceDetector::default(), 0, |_, _| {});
    let gates = Gates::parse(
        r#"
        [[gate]]
        name = "recall high"
        metric = "recall"
        min = 0.8

        [[gate]]
        name = "recall perfect"
        metric = "recall"
        min = 1.0

        [[gate]]
        name = "no channel labels here"
        route = "channel"
        metric = "recall"
        min = 0.5

        [[gate]]
        name = "no rejected text leaks"
        metric = "violations"
        reason = "rejected_send"
        max = 0
        "#,
        "inline",
    )
    .unwrap_or_else(|e| panic!("{e}"));
    let outcomes = gates.evaluate(&summary.score);
    assert!(matches!(outcomes[0].status, GateStatus::Pass { .. }));
    assert!(matches!(outcomes[1].status, GateStatus::Fail { .. }));
    assert_eq!(outcomes[2].status, GateStatus::Skipped);
    assert!(matches!(outcomes[3].status, GateStatus::Pass { .. }));
    assert!(Gates::parse("[[gate]]\nname = \"x\"\nmetric = \"bogus\"\n", "bad").is_err());
    let _ = NegativeReason::RejectedSend;
}

/// A detector that finds nothing: everything is missed, nothing is false.
struct Silent;

impl Detector for Silent {
    fn name(&self) -> &str {
        "silent"
    }

    fn detect(&mut self, _: &World) -> Result<Detection, DetectError> {
        Ok(Detection::default())
    }
}

#[test]
fn any_detector_plugs_into_the_run() {
    let mut source =
        SaltSource::open(&root(), &Selection::default()).unwrap_or_else(|e| panic!("{e}"));
    let summary = run(&mut source, &mut Silent, 10, |_, _| {});
    let all = summary.score.total(&Selector::default());
    assert_eq!((all.expected, all.found, all.predicted), (12, 0, 0));
}

#[test]
fn config_resolves_dataset_directories() {
    let config = EvalConfig::parse(
        "root = \"~/data\"\n[datasets.salt]\npath = \"salt-nlp\"\n[datasets.abs]\npath = \"/srv/abs\"\n",
        "inline",
    )
    .unwrap_or_else(|e| panic!("{e}"));
    let home = Path::new("/home/someone");
    assert_eq!(
        config.dataset_root("salt", Some(home)).ok(),
        Some(PathBuf::from("/home/someone/data/salt-nlp"))
    );
    assert_eq!(
        config.dataset_root("abs", Some(home)).ok(),
        Some(PathBuf::from("/srv/abs"))
    );
    assert!(config.dataset_root("missing", Some(home)).is_err());
    assert_eq!(expand("plain", Some(home)), PathBuf::from("plain"));
    let shipped = EvalConfig::load(&Path::new(env!("CARGO_MANIFEST_DIR")).join("datasets.toml"))
        .unwrap_or_else(|e| panic!("{e}"));
    assert_eq!(shipped, EvalConfig::default());
}

fn ct_eval(args: &[&str]) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_ct-eval"))
        .args(args)
        .output()
        .unwrap_or_else(|e| panic!("{e}"))
}

#[test]
fn the_binary_runs_reports_and_gates() {
    let out = temp("run");
    let root = root();
    let lenient = out.join("lenient.toml");
    std::fs::write(
        &lenient,
        "[[gate]]\nname = \"some\"\nmetric = \"recall\"\nmin = 0.5\n",
    )
    .unwrap_or_else(|e| panic!("{e}"));
    let result = ct_eval(&[
        "run",
        "--dataset",
        "salt",
        "--root",
        root.to_str().unwrap_or(""),
        "--gates",
        lenient.to_str().unwrap_or(""),
        "--out",
        out.to_str().unwrap_or(""),
    ]);
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    let report: Report =
        serde_json::from_slice(&std::fs::read(out.join("report.json")).unwrap_or_default())
            .unwrap_or_else(|e| panic!("{e}"));
    assert_eq!(report.overall.counts.expected, 12);
    assert!(out.join("report.txt").exists());

    let gates = out.join("strict.toml");
    std::fs::write(
        &gates,
        "[[gate]]\nname = \"all\"\nmetric = \"recall\"\nmin = 1.0\n",
    )
    .unwrap_or_else(|e| panic!("{e}"));
    let failed = ct_eval(&[
        "run",
        "--dataset",
        "salt",
        "--root",
        root.to_str().unwrap_or(""),
        "--gates",
        gates.to_str().unwrap_or(""),
    ]);
    assert_eq!(failed.status.code(), Some(2));
    assert!(String::from_utf8_lossy(&failed.stdout).contains("FAIL"));
}

#[test]
fn the_binary_dumps_truth() {
    let out = temp("truth");
    let file = out.join("truth.jsonl");
    let root = root();
    let result = ct_eval(&[
        "truth",
        "--dataset",
        "salt",
        "--root",
        root.to_str().unwrap_or(""),
        "--limit",
        "1",
        "--out",
        file.to_str().unwrap_or(""),
    ]);
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    let text = std::fs::read(&file).unwrap_or_default();
    let labels: Result<Vec<_>, _> = jsonl::read(text.as_slice()).collect();
    let labels = labels.unwrap_or_else(|e| panic!("{e}"));
    assert!(!labels.is_empty());
}
