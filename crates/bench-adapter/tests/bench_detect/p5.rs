//! Parity stage P5, frozen: `ct-bench-detect` on an export's input view
//! writes the bytes `ct-eval run --predictions-out` wrote on the same
//! selection (crosstalk ff3dc44, before ct-eval's scoring moved to the
//! bench), for the live composition and the bare pipeline. Only the
//! header's `detector.version` (the build's commit) and so the trailer's
//! digest may differ. A detector change that moves these rows is a change
//! to review, and its expected files are regenerated with it.

use a2a_bench_format::predictions::{Prediction, WorldStatus};
use crosstalk_bench_adapter::config::{LIVE, PIPELINE};
use crosstalk_bench_adapter::to_bench::manifest::CROSSTALK_COMMIT;

use super::common::{
    INPUTS, assert_same_predictions, bench_detect, dir, expected, input, path, predictions, read,
};

/// Runs `ct-bench-detect` with `flags` on input `name`; returns its file.
fn detect(name: &str, run: &str, flags: &[&str]) -> std::path::PathBuf {
    let out = dir(&format!("p5-{run}-{name}")).join("ct-bench-detect.jsonl");
    let input = input(name);
    let mut args = vec!["--input", path(&input), "--output", path(&out)];
    args.extend_from_slice(flags);
    bench_detect(&args);
    out
}

#[test]
fn live_predictions_are_byte_identical_to_ct_evals() {
    for name in INPUTS {
        let detected = detect(name, "live", &[]);
        assert_same_predictions(&expected(name, "live-off"), &detected);
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
    let on = detect("salt", "forwarding-on", &["--forwarding", "on"]);
    assert_same_predictions(&expected("salt", "live-on"), &on);
    let (on, _) = predictions(&on);
    assert_eq!(on.detector.variant, "forwarding-on");
    let off = detect("salt", "forwarding-off", &[]);
    let (off, _) = predictions(&off);
    assert_ne!(
        off.detector.config_digest, on.detector.config_digest,
        "the setting is in the digest"
    );
}

#[test]
fn pipeline_predictions_are_byte_identical_to_ct_evals() {
    let detected = detect("salt", "pipeline", &["--mode", "pipeline"]);
    assert_same_predictions(&expected("salt", "pipeline"), &detected);
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
    let base = dir("rerun-wiki");
    let input = input("wiki");
    let (one, two) = (base.join("one.jsonl"), base.join("two.jsonl"));
    for out in [&one, &two] {
        bench_detect(&["--input", path(&input), "--output", path(out)]);
    }
    assert!(read(&one) == read(&two));
}
