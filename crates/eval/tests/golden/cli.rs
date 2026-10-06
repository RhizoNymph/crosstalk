//! `ct-eval export`, `run --predictions-out`, `verify` and `swarm
//! --export-out --predictions-out` end to end: an export is byte-identical
//! on rerun, and a run's predictions name its export's manifest.

use std::path::Path;
use std::process::Command;

use crosstalk_eval::golden::verify;

use super::common::{dir, fixtures};
use super::swarm_fixture as fixture;

const FILES: [&str; 4] = [
    "manifest.json",
    "messages.jsonl",
    "exchanges.jsonl",
    "labels.jsonl",
];

fn path(path: &Path) -> &str {
    path.to_str().unwrap_or_else(|| panic!("non-UTF-8 path"))
}

fn ct_eval(args: &[&str]) {
    let output = Command::new(env!("CARGO_BIN_EXE_ct-eval"))
        .args(args)
        .output()
        .unwrap_or_else(|e| panic!("{e}"));
    assert!(
        output.status.success(),
        "{args:?}: {}: {}",
        output.status,
        String::from_utf8_lossy(&output.stderr)
    );
}

fn export(source: &[&str], out: &Path) {
    let mut args = vec!["export", "--format", "a2a-bench/1"];
    args.extend_from_slice(source);
    args.extend_from_slice(&["--out", path(out)]);
    ct_eval(&args);
}

#[test]
fn an_export_is_byte_identical_on_rerun_and_matches_its_runs_predictions() {
    let salt = fixtures().join("salt");
    let wiki = fixtures().join("wiki/collusion-wiki");
    let selections: [(&str, Vec<&str>); 2] = [
        ("salt", vec!["--dataset", "salt", "--root", path(&salt)]),
        (
            "wiki",
            vec![
                "--dataset",
                "wiki",
                "--root",
                path(&wiki),
                "--corpus-seed",
                "3",
            ],
        ),
    ];
    for (name, source) in selections {
        let base = dir(&format!("cli-{name}"));
        let (one, two) = (base.join("one"), base.join("two"));
        export(&source, &one);
        export(&source, &two);
        for file in FILES {
            let read = |dir: &Path| std::fs::read(dir.join(file)).unwrap_or_else(|e| panic!("{e}"));
            assert!(read(&one) == read(&two), "{name}: {file} differs on rerun");
        }
        let gates = base.join("no-gates.toml");
        std::fs::write(&gates, "").unwrap_or_else(|e| panic!("{e}"));
        let predictions = base.join("predictions.jsonl");
        let mut args = vec!["run", "--gates", path(&gates)];
        args.extend_from_slice(&source);
        args.extend_from_slice(&["--predictions-out", path(&predictions)]);
        ct_eval(&args);
        let verified = verify(&one, Some(&predictions)).unwrap_or_else(|e| panic!("{e}"));
        assert!(verified.predictions > 0, "{name}: no prediction rows");
        ct_eval(&[
            "verify",
            "--export",
            path(&one),
            "--predictions",
            path(&predictions),
        ]);
    }
}

#[test]
fn the_swarm_command_exports_a_bench_run() {
    let base = fixture::dir("golden-cli");
    let written = fixture::write(&base, &fixture::truth_rows());
    let out = base.join("a2a");
    let predictions = base.join("predictions.jsonl");
    let gates = base.join("no-gates.toml");
    std::fs::write(&gates, "").unwrap_or_else(|e| panic!("{e}"));
    ct_eval(&[
        "swarm",
        "--truth",
        path(&written.truth),
        "--exchanges",
        path(&written.exchanges),
        "--blobs",
        path(&written.blobs),
        "--export",
        path(&written.export),
        "--evidence",
        path(&written.evidence),
        "--gates",
        path(&gates),
        "--export-out",
        path(&out),
        "--predictions-out",
        path(&predictions),
    ]);
    verify(&out, Some(&predictions)).unwrap_or_else(|e| panic!("{e}"));
}
