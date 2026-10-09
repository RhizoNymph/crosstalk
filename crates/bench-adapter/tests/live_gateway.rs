//! The live mode over the real gateway composition
//! (`crosstalk_gateway::live::Live`, through `detect::live::gateway`) on
//! the checked-in SALT, wiki and splice inputs: real confirmed
//! transmissions come back and become transmission rows, and two runs of
//! `ct-bench-detect` write byte-identical predictions.

use std::path::{Path, PathBuf};

use a2a_bench_format::predictions::Prediction;
use crosstalk_bench_adapter::convert;
use crosstalk_bench_adapter::detect::live::{
    GatewayBackend, LiveDetector, LiveSettings, gateway_backend,
};
use crosstalk_bench_adapter::input::{InputDir, WorldRead};
use crosstalk_bench_adapter::run::live_world;
use crosstalk_bench_adapter::to_bench::Lossy;

fn input(name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/bench")
        .join(name)
}

fn detector() -> LiveDetector<GatewayBackend> {
    let settings = LiveSettings::short(0).unwrap_or_else(|e| panic!("{e}"));
    LiveDetector::new(gateway_backend(), settings).unwrap_or_else(|e| panic!("{e}"))
}

/// How many confirmed transmissions the live detector gives over input
/// `name`, and how many transmission rows it writes; every world is
/// detected and written.
fn confirmed_and_written(name: &str) -> (usize, usize) {
    let mut dir = InputDir::open(&input(name)).unwrap_or_else(|e| panic!("{name}: {e}"));
    let dataset = dir.manifest().dataset.clone();
    let mut detector = detector();
    let (mut confirmed, mut written) = (0, 0);
    while let Some(read) = dir.next_world().unwrap_or_else(|e| panic!("{name}: {e}")) {
        let WorldRead::Ready(inputs) = read else {
            panic!("{name}: a fixture world does not check");
        };
        let world = convert::world(&dataset, &inputs).unwrap_or_else(|e| panic!("{name}: {e}"));
        let raw = detector
            .detect_exchanges(&world.timed())
            .unwrap_or_else(|e| panic!("{name}: {e}"))
            .unwrap_or_else(|| panic!("{name}: a world without exchanges"));
        confirmed += raw
            .transmissions
            .iter()
            .filter(|t| t.state.confirmed().is_some())
            .count();
        let rows = live_world(&mut detector, &dataset, &inputs, &mut Lossy::default())
            .unwrap_or_else(|e| panic!("{name}: {e}"));
        written += rows
            .iter()
            .filter(|row| matches!(row, Prediction::Transmission(row) if row.fields().quality.is_some()))
            .count();
    }
    (confirmed, written)
}

#[test]
fn salt_deliveries_are_confirmed_and_written() {
    let (confirmed, written) = confirmed_and_written("salt");
    assert!(confirmed > 0, "no confirmed transmission");
    assert!(
        written >= confirmed,
        "every confirmed transmission is a row"
    );
}

#[test]
fn wiki_reads_are_confirmed_and_written() {
    let (confirmed, written) = confirmed_and_written("wiki");
    assert!(confirmed > 0, "no confirmed transmission");
    assert!(
        written >= confirmed,
        "every confirmed transmission is a row"
    );
}

#[test]
fn splice_worlds_are_confirmed_and_written() {
    let (confirmed, written) = confirmed_and_written("splice");
    assert!(confirmed > 0, "no confirmed transmission");
    assert!(
        written >= confirmed,
        "every confirmed transmission is a row"
    );
}

/// `ct-bench-detect` (live) on input `name`, its predictions file.
fn cli_run(name: &str, out: &Path) -> Vec<u8> {
    let predictions = out.join("predictions.jsonl");
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_ct-bench-detect"))
        .arg("--input")
        .arg(input(name))
        .arg("--output")
        .arg(&predictions)
        .output()
        .unwrap_or_else(|e| panic!("{e}"));
    assert!(
        output.status.success(),
        "{name}: {}: {}",
        output.status,
        String::from_utf8_lossy(&output.stderr)
    );
    std::fs::read(&predictions).unwrap_or_else(|e| panic!("{e}"))
}

#[test]
fn two_live_runs_give_byte_identical_predictions() {
    for name in ["salt", "wiki", "splice"] {
        let dir = Path::new(env!("CARGO_TARGET_TMPDIR"))
            .join("live_gateway")
            .join(name);
        let (one, two) = (dir.join("one"), dir.join("two"));
        for out in [&one, &two] {
            std::fs::create_dir_all(out).unwrap_or_else(|e| panic!("{e}"));
        }
        let first = cli_run(name, &one);
        let second = cli_run(name, &two);
        assert!(!first.is_empty(), "{name}: no predictions");
        assert!(first == second, "{name}: predictions differ");
    }
}
