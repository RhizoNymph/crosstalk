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
        let manifest =
            crosstalk_eval::golden::verify::read_manifest(&one).unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(
            manifest.converter.git,
            crosstalk_eval::golden::manifest::CROSSTALK_COMMIT,
            "{name}: the binary's build-time commit"
        );
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

/// `converter.git` is the commit the binary was built from: an export and
/// a run made in one process name the same one, and so the same manifest,
/// whatever the checkout's HEAD is when they run.
#[test]
fn export_and_run_in_one_process_name_the_same_converter() {
    use crosstalk_eval::datasets::salt::{SaltSource, Selection};
    use crosstalk_eval::golden::manifest::{self, CROSSTALK_COMMIT};
    use crosstalk_eval::golden::run::write_manifest;
    use crosstalk_eval::golden::{ExportWriter, GoldenRun, PredictionsWriter};
    use crosstalk_eval::pipeline::{ReferenceDetector, run_with};

    let out = dir("converter");
    let salt = || {
        SaltSource::open(&fixtures().join("salt"), &Selection::default())
            .unwrap_or_else(|e| panic!("{e}"))
    };
    let spec = || {
        let mut spec = super::common::spec(&crosstalk_eval::keys::DatasetId::new("salt"));
        spec.converter = manifest::converter();
        spec
    };
    let mut exported = GoldenRun::new(
        ExportWriter::create(&out.join("export"), &spec().dataset)
            .unwrap_or_else(|e| panic!("{e}")),
        None,
    );
    for world in super::common::worlds(&mut salt()) {
        exported.world(&world).unwrap_or_else(|e| panic!("{e}"));
    }
    let exported = exported
        .finish(&spec(), None)
        .unwrap_or_else(|e| panic!("{e}"));
    write_manifest(&out.join("export"), &exported.manifest).unwrap_or_else(|e| panic!("{e}"));

    let predictions = out.join("predictions.jsonl");
    let mut run = GoldenRun::new(
        ExportWriter::sink(&spec().dataset).unwrap_or_else(|e| panic!("{e}")),
        Some(PredictionsWriter::create(&predictions).unwrap_or_else(|e| panic!("{e}"))),
    );
    run_with(
        &mut salt(),
        &mut ReferenceDetector::default(),
        0,
        |outcome| run.observe(outcome),
    );
    let ran = run
        .finish(&spec(), Some(super::common::detector_info("reference")))
        .unwrap_or_else(|e| panic!("{e}"));

    assert_eq!(exported.manifest.converter.git, CROSSTALK_COMMIT);
    assert_eq!(ran.manifest.converter.git, CROSSTALK_COMMIT);
    assert_eq!(exported.manifest, ran.manifest);
    verify(&out.join("export"), Some(&predictions)).unwrap_or_else(|e| panic!("{e}"));
    let sha = CROSSTALK_COMMIT.trim_end_matches("-dirty");
    assert!(
        CROSSTALK_COMMIT == manifest::UNKNOWN
            || (sha.len() == 40 && sha.bytes().all(|b| b.is_ascii_hexdigit())),
        "{CROSSTALK_COMMIT:?}"
    );
}
