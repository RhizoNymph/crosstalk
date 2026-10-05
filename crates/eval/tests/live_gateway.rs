//! `--detector live` over the real gateway composition
//! (`crosstalk_gateway::live::Live`, through `detect::live::gateway`) on
//! the synthetic SALT, wiki and splice fixtures: real confirmed
//! transmissions come back and are scored, and two runs give byte-identical
//! reports.

use std::path::{Path, PathBuf};

use crosstalk_eval::corpus::{TraceSource, World};
use crosstalk_eval::datasets::salt::{SaltSource, Selection};
use crosstalk_eval::datasets::swe_splice::SpliceSource;
use crosstalk_eval::datasets::wiki::{WikiSelection, WikiSource};
use crosstalk_eval::detect::live::{GatewayBackend, LiveDetector, LiveSettings, gateway_backend};
use crosstalk_eval::pipeline::{Detector, predictions, run};
use crosstalk_eval::score::Selector;

fn fixtures() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures")
}

fn salt() -> SaltSource {
    SaltSource::open(&fixtures().join("salt"), &Selection::default())
        .unwrap_or_else(|e| panic!("{e}"))
}

fn wiki() -> WikiSource {
    WikiSource::open(
        &fixtures().join("wiki/collusion-wiki"),
        &WikiSelection::default(),
    )
    .unwrap_or_else(|e| panic!("{e}"))
}

fn splice() -> SpliceSource {
    SpliceSource::open(&fixtures().join("open_swe"), &Selection::default(), 4, 0)
        .unwrap_or_else(|e| panic!("{e}"))
}

fn detector() -> LiveDetector<GatewayBackend> {
    let settings = LiveSettings::short(0).unwrap_or_else(|e| panic!("{e}"));
    LiveDetector::new(gateway_backend(), settings).unwrap_or_else(|e| panic!("{e}"))
}

fn worlds(source: &mut impl TraceSource) -> Vec<World> {
    source
        .worlds()
        .map(|world| world.unwrap_or_else(|e| panic!("{e}")))
        .collect()
}

/// How many confirmed transmissions and predictions the live detector
/// gives over `worlds`; every world is detected and predicted from.
fn confirmed_and_predicted(worlds: &[World]) -> (usize, usize) {
    let mut detector = detector();
    let (mut confirmed, mut predicted) = (0, 0);
    for world in worlds {
        let detection = detector
            .detect(world)
            .unwrap_or_else(|e| panic!("{}: {e}", world.key()));
        confirmed += detection
            .transmissions
            .iter()
            .filter(|t| t.state.confirmed().is_some())
            .count();
        predicted += predictions(world, &detection)
            .unwrap_or_else(|e| panic!("{}: {e}", world.key()))
            .len();
    }
    (confirmed, predicted)
}

#[test]
fn salt_deliveries_are_confirmed_and_found() {
    let worlds = worlds(&mut salt());
    let (confirmed, predicted) = confirmed_and_predicted(&worlds);
    assert!(confirmed > 0, "no confirmed transmission");
    assert!(predicted > 0, "no prediction");
    let summary = run(&mut salt(), &mut detector(), 10, |_, _| {});
    assert!(summary.failures.is_empty(), "{:?}", summary.failures);
    let total = summary.score.total(&Selector::default());
    assert!(total.found > 0, "no label found: {total:?}");
    assert!(total.correct > 0, "no correct prediction: {total:?}");
}

#[test]
fn wiki_reads_are_confirmed_and_scored() {
    let worlds = worlds(&mut wiki());
    let (confirmed, predicted) = confirmed_and_predicted(&worlds);
    assert!(confirmed > 0, "no confirmed transmission");
    assert!(predicted > 0, "no prediction");
    let summary = run(&mut wiki(), &mut detector(), 10, |_, _| {});
    assert!(summary.failures.is_empty(), "{:?}", summary.failures);
    let total = summary.score.total(&Selector::default());
    assert_eq!(total.predicted, u64::try_from(predicted).unwrap_or(u64::MAX));
}

#[test]
fn splice_worlds_are_confirmed_and_scored() {
    let worlds = worlds(&mut splice());
    let (confirmed, predicted) = confirmed_and_predicted(&worlds);
    assert!(confirmed > 0, "no confirmed transmission");
    assert!(predicted > 0, "no prediction");
    let summary = run(&mut splice(), &mut detector(), 10, |_, _| {});
    assert!(summary.failures.is_empty(), "{:?}", summary.failures);
    let total = summary.score.total(&Selector::default());
    assert_eq!(total.predicted, u64::try_from(predicted).unwrap_or(u64::MAX));
}

/// `ct-eval run --detector live` with `args`, its report.json and
/// predictions.jsonl.
fn cli_run(args: &[&str], out: &Path) -> (Vec<u8>, Vec<u8>) {
    let predictions = out.join("predictions.jsonl");
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_ct-eval"))
        .args(["run", "--detector", "live", "--gates"])
        .arg(out.join("no-gates.toml"))
        .args(args)
        .arg("--out")
        .arg(out)
        .arg("--predictions")
        .arg(&predictions)
        .output()
        .unwrap_or_else(|e| panic!("{e}"));
    assert!(
        output.status.success(),
        "{args:?}: {}: {}",
        output.status,
        String::from_utf8_lossy(&output.stderr)
    );
    let read = |path: &Path| std::fs::read(path).unwrap_or_else(|e| panic!("{e}"));
    (read(&out.join("report.json")), read(&predictions))
}

#[test]
fn two_live_runs_give_byte_identical_reports() {
    let salt = fixtures().join("salt");
    let wiki = fixtures().join("wiki/collusion-wiki");
    let splice = fixtures().join("open_swe");
    let runs: [Vec<&str>; 3] = [
        vec!["--dataset", "salt", "--root", path(&salt)],
        vec!["--dataset", "wiki", "--root", path(&wiki)],
        vec![
            "--dataset",
            "swe-splice",
            "--count",
            "4",
            "--root",
            path(&splice),
        ],
    ];
    for args in runs {
        let dir = tempfile::tempdir().unwrap_or_else(|e| panic!("{e}"));
        let (one, two) = (dir.path().join("one"), dir.path().join("two"));
        for out in [&one, &two] {
            std::fs::create_dir_all(out).unwrap_or_else(|e| panic!("{e}"));
            std::fs::write(out.join("no-gates.toml"), "").unwrap_or_else(|e| panic!("{e}"));
        }
        let first = cli_run(&args, &one);
        let second = cli_run(&args, &two);
        assert!(!first.1.is_empty(), "{args:?}: no prediction");
        assert!(first.0 == second.0, "{args:?}: reports differ");
        assert!(first.1 == second.1, "{args:?}: predictions differ");
    }
}

fn path(path: &Path) -> &str {
    path.to_str().unwrap_or_else(|| panic!("non-UTF-8 fixture path"))
}
