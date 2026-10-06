//! Running the two binaries and reading what they wrote.

use std::path::{Path, PathBuf};
use std::process::{Command, Output};

use a2a_bench_format::files::{Predictions, PredictionsHeader};
use a2a_bench_format::jsonl::{FileReader, WorldSection};
use crosstalk_eval::golden::verify::read_manifest;
use crosstalk_eval::golden::writer::{EXCHANGES_FILE, MANIFEST_FILE, MESSAGES_FILE};

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

pub fn fixtures() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures")
}

pub fn path(path: &Path) -> &str {
    path.to_str().unwrap_or_else(|| panic!("non-UTF-8 path"))
}

fn output(binary: &str, args: &[&str]) -> Output {
    Command::new(binary)
        .args(args)
        .output()
        .unwrap_or_else(|e| panic!("{e}"))
}

/// Runs a binary and requires success.
fn succeed(binary: &str, args: &[&str]) {
    let output = output(binary, args);
    assert!(
        output.status.success(),
        "{args:?}: {}: {}",
        output.status,
        String::from_utf8_lossy(&output.stderr)
    );
}

pub fn ct_eval(args: &[&str]) {
    succeed(env!("CARGO_BIN_EXE_ct-eval"), args);
}

pub fn bench_detect(args: &[&str]) {
    succeed(env!("CARGO_BIN_EXE_ct-bench-detect"), args);
}

/// Runs `ct-bench-detect` and returns its output, whatever its status.
pub fn bench_detect_output(args: &[&str]) -> Output {
    output(env!("CARGO_BIN_EXE_ct-bench-detect"), args)
}

/// `ct-eval export --format a2a-bench/1` of `source` into `out`.
pub fn export(source: &[&str], out: &Path) {
    let mut args = vec!["export", "--format", "a2a-bench/1"];
    args.extend_from_slice(source);
    args.extend_from_slice(&["--out", path(out)]);
    ct_eval(&args);
}

/// The input view of the export in `export` copied to `input`: its
/// manifest's input view, its messages and exchanges, never its labels.
pub fn input_view(export: &Path, input: &Path) {
    std::fs::create_dir_all(input).unwrap_or_else(|e| panic!("{e}"));
    let manifest = read_manifest(export).unwrap_or_else(|e| panic!("{e}"));
    let text =
        serde_json::to_string_pretty(&manifest.input_view()).unwrap_or_else(|e| panic!("{e}"));
    std::fs::write(input.join(MANIFEST_FILE), text + "\n").unwrap_or_else(|e| panic!("{e}"));
    for file in [MESSAGES_FILE, EXCHANGES_FILE] {
        std::fs::copy(export.join(file), input.join(file)).unwrap_or_else(|e| panic!("{e}"));
    }
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
