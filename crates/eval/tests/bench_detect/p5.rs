//! Parity stage P5: `ct-bench-detect` on an export's input view writes the
//! same bytes as `ct-eval run --predictions-out` on the same selection,
//! for the live composition and the bare pipeline.

use a2a_bench_format::predictions::{Prediction, WorldStatus};
use crosstalk_eval::bench_detect::config::{LIVE, PIPELINE};
use crosstalk_eval::golden::manifest::CROSSTALK_COMMIT;

use super::common::{
    bench_detect, ct_eval, dir, export, fixtures, input_view, path, predictions, read,
};

/// The fixtures' selections, as `ct-eval` flags.
fn selections() -> Vec<(&'static str, Vec<String>)> {
    let root = |relative: &str| fixtures().join(relative).display().to_string();
    vec![
        (
            "salt",
            vec![
                "--dataset".into(),
                "salt".into(),
                "--root".into(),
                root("salt"),
            ],
        ),
        (
            "wiki",
            vec![
                "--dataset".into(),
                "wiki".into(),
                "--root".into(),
                root("wiki/collusion-wiki"),
                "--corpus-seed".into(),
                "3".into(),
            ],
        ),
        (
            "swarm-traces",
            vec![
                "--dataset".into(),
                "swarm".into(),
                "--root".into(),
                root("swarm/swarm-traces"),
            ],
        ),
        (
            "agentdojo",
            vec![
                "--dataset".into(),
                "agentdojo".into(),
                "--root".into(),
                root("agentdojo"),
            ],
        ),
        (
            "tau2",
            vec![
                "--dataset".into(),
                "tau2".into(),
                "--root".into(),
                root("tau2"),
            ],
        ),
    ]
}

/// Exports `source`, runs `ct-eval run` with `run_flags` and
/// `ct-bench-detect` with `detect_flags` on the input view; returns both
/// predictions files.
fn both(
    name: &str,
    source: &[String],
    run_flags: &[&str],
    detect_flags: &[&str],
) -> (std::path::PathBuf, std::path::PathBuf) {
    let base = dir(name);
    let source: Vec<&str> = source.iter().map(String::as_str).collect();
    let (full, input) = (base.join("export"), base.join("input"));
    export(&source, &full);
    input_view(&full, &input);
    let gates = base.join("no-gates.toml");
    std::fs::write(&gates, "").unwrap_or_else(|e| panic!("{e}"));
    let golden = base.join("ct-eval.jsonl");
    let mut args = vec!["run", "--gates", path(&gates)];
    args.extend_from_slice(&source);
    args.extend_from_slice(run_flags);
    args.extend_from_slice(&["--predictions-out", path(&golden)]);
    ct_eval(&args);
    let detected = base.join("ct-bench-detect.jsonl");
    let mut args = vec!["--input", path(&input), "--output", path(&detected)];
    args.extend_from_slice(detect_flags);
    bench_detect(&args);
    (golden, detected)
}

#[test]
fn live_predictions_are_byte_identical_to_ct_evals() {
    for (name, source) in selections() {
        let (golden, detected) = both(
            &format!("p5-live-{name}"),
            &source,
            &["--detector", "live"],
            &[],
        );
        assert!(
            read(&golden) == read(&detected),
            "{name}: the predictions differ"
        );
        let (header, worlds) = predictions(&detected);
        assert_eq!(header.detector.name, LIVE);
        assert_eq!(header.detector.variant, "forwarding-off");
        assert_eq!(header.detector.version, CROSSTALK_COMMIT);
        assert!(header.detector.config_digest.is_some());
        assert!(
            worlds
                .iter()
                .all(|world| world.world.status == WorldStatus::Scored),
            "{name}: every fixture world is scored"
        );
        let transmissions = worlds
            .iter()
            .flat_map(|world| &world.rows)
            .filter(|row| matches!(row, Prediction::Transmission(_)))
            .count();
        assert!(transmissions > 0, "{name}: no transmission");
    }
}

#[test]
fn forwarding_on_is_its_own_variant_and_still_identical() {
    let (name, source) = &selections()[0];
    let (golden, detected) = both(
        &format!("p5-forwarding-{name}"),
        source,
        &["--detector", "live", "--forwarding", "on"],
        &["--forwarding", "on"],
    );
    assert!(read(&golden) == read(&detected));
    let (header, _) = predictions(&detected);
    assert_eq!(header.detector.variant, "forwarding-on");
    let (off, _) = both(
        &format!("p5-forwarding-off-{name}"),
        source,
        &["--detector", "live"],
        &[],
    );
    let (off, _) = predictions(&off);
    assert_ne!(
        off.detector.config_digest, header.detector.config_digest,
        "the setting is in the digest"
    );
}

#[test]
fn pipeline_predictions_are_byte_identical_to_ct_evals() {
    let (name, source) = &selections()[0];
    let (golden, detected) = both(
        &format!("p5-pipeline-{name}"),
        source,
        &["--detector", "pipeline"],
        &["--mode", "pipeline"],
    );
    assert!(read(&golden) == read(&detected));
    let (header, worlds) = predictions(&detected);
    assert_eq!(header.detector.name, PIPELINE);
    assert_eq!(header.detector.variant, "default");
    for world in &worlds {
        assert!(
            matches!(world.world.status, WorldStatus::NoConsumers { ingested } if ingested > 0),
            "{:?}",
            world.world.status
        );
        assert!(world.rows.is_empty());
    }
}

#[test]
fn a_rerun_writes_the_same_bytes() {
    let (name, source) = &selections()[1];
    let base = dir(&format!("rerun-{name}"));
    let source: Vec<&str> = source.iter().map(String::as_str).collect();
    let (full, input) = (base.join("export"), base.join("input"));
    export(&source, &full);
    input_view(&full, &input);
    let (one, two) = (base.join("one.jsonl"), base.join("two.jsonl"));
    for out in [&one, &two] {
        bench_detect(&["--input", path(&input), "--output", path(out)]);
    }
    assert!(read(&one) == read(&two));
}
