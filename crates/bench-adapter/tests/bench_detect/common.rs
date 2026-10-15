//! Running `ct-bench-detect`, the checked-in bench inputs, and reading
//! what it wrote.

use std::path::{Path, PathBuf};
use std::process::{Command, Output};

use a2a_bench_format::files::{Predictions, PredictionsHeader};
use a2a_bench_format::jsonl::{FileReader, WorldSection};

/// A fresh, empty directory under the test target's scratch space.
pub fn dir(name: &str) -> PathBuf {
    let dir = Path::new(env!("CARGO_TARGET_TMPDIR"))
        .join("bench_detect")
        .join(name);
    if dir.exists() {
        std::fs::remove_dir_all(&dir).unwrap_or_else(|e| panic!("{}: {e}", dir.display()));
    }
    std::fs::create_dir_all(&dir).unwrap_or_else(|e| panic!("{}: {e}", dir.display()));
    dir
}

/// The checked-in bench inputs: input views (manifest.json without labels,
/// messages.jsonl, exchanges.jsonl) that ct-eval's golden export wrote at
/// crosstalk ff3dc44 from the converters' synthetic fixtures, and under
/// `expected/` the predictions ct-eval's `run --predictions-out` wrote on
/// them (parity stage P5's reference).
pub fn bench_fixtures() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/bench")
}

/// One checked-in input view.
pub fn input(name: &str) -> PathBuf {
    bench_fixtures().join(name)
}

/// ct-eval's predictions on input `name` under `variant`
/// (`live-off`, `live-on`, `pipeline`).
pub fn expected(name: &str, variant: &str) -> PathBuf {
    bench_fixtures()
        .join("expected")
        .join(format!("{name}.{variant}.jsonl"))
}

/// Every checked-in input view.
pub const INPUTS: [&str; 6] = [
    "salt",
    "wiki",
    "swarm-traces",
    "agentdojo",
    "tau2",
    "splice",
];

pub fn path(path: &Path) -> &str {
    path.to_str().unwrap_or_else(|| panic!("non-UTF-8 path"))
}

fn output(binary: &str, args: &[&str]) -> Output {
    Command::new(binary)
        .args(args)
        .output()
        .unwrap_or_else(|e| panic!("{e}"))
}

pub fn bench_detect(args: &[&str]) {
    let output = bench_detect_output(args);
    assert!(
        output.status.success(),
        "{args:?}: {}: {}",
        output.status,
        String::from_utf8_lossy(&output.stderr)
    );
}

/// Runs `ct-bench-detect` and returns its output, whatever its status.
pub fn bench_detect_output(args: &[&str]) -> Output {
    output(env!("CARGO_BIN_EXE_ct-bench-detect"), args)
}

/// A predictions file's header and world sections.
pub fn predictions(file: &Path) -> (PredictionsHeader, Vec<WorldSection<Predictions>>) {
    let handle = std::fs::File::open(file).unwrap_or_else(|e| panic!("{}: {e}", file.display()));
    let mut reader = FileReader::<Predictions, _>::open(std::io::BufReader::new(handle))
        .unwrap_or_else(|e| panic!("{e}"));
    let header = reader.header().clone();
    let mut worlds = Vec::new();
    while let Some(section) = reader.next_world().unwrap_or_else(|e| panic!("{e}")) {
        worlds.push(section);
    }
    (header, worlds)
}

pub fn read(file: &Path) -> Vec<u8> {
    std::fs::read(file).unwrap_or_else(|e| panic!("{}: {e}", file.display()))
}

/// The header line as JSON without `detector.version` (the commit the
/// binary was built from).
fn header_without_version(line: &[u8]) -> serde_json::Value {
    let mut header: serde_json::Value =
        serde_json::from_slice(line).unwrap_or_else(|e| panic!("{e}"));
    header
        .get_mut("detector")
        .and_then(serde_json::Value::as_object_mut)
        .and_then(|detector| detector.remove("version"))
        .unwrap_or_else(|| panic!("a header without detector.version"));
    header
}

/// Asserts that `actual` holds `expected`'s bytes, but for the header's
/// `detector.version` and the trailer's digest (which covers the header):
/// every world and row line byte-identical, the same header otherwise, the
/// same world and row counts. `actual` reads back through its framing, so
/// its own digest is checked.
pub fn assert_same_predictions(expected: &Path, actual: &Path) {
    predictions(actual);
    let (expected_bytes, actual_bytes) = (read(expected), read(actual));
    let lines = |bytes: &[u8]| -> Vec<Vec<u8>> {
        bytes
            .split(|byte| *byte == b'\n')
            .filter(|line| !line.is_empty())
            .map(<[u8]>::to_vec)
            .collect()
    };
    let (want, got) = (lines(&expected_bytes), lines(&actual_bytes));
    assert_eq!(want.len(), got.len(), "{}: line count", expected.display());
    assert!(
        want.len() >= 2,
        "{}: a header and a trailer",
        expected.display()
    );
    assert_eq!(
        header_without_version(&want[0]),
        header_without_version(&got[0]),
        "{}: header",
        expected.display()
    );
    let last = want.len() - 1;
    for at in 1..last {
        assert!(
            want[at] == got[at],
            "{}: line {} differs",
            expected.display(),
            at + 1
        );
    }
    let counts = |line: &[u8]| {
        let trailer: serde_json::Value =
            serde_json::from_slice(line).unwrap_or_else(|e| panic!("{e}"));
        (
            trailer["kind"].clone(),
            trailer["worlds"].clone(),
            trailer["rows"].clone(),
        )
    };
    assert_eq!(
        counts(&want[last]),
        counts(&got[last]),
        "{}: trailer",
        expected.display()
    );
}
