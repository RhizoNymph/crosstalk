//! Forwarding: `ct-bench-detect --forwarding on` turns on L4's forwarding
//! for the live composition (`ProvenanceConfig::forwarding`), and off is
//! the default. Which labels need it, and the gates that select on the
//! `forwarding-on` variant, are the bench's.

use std::path::{Path, PathBuf};

use crosstalk_bench_adapter::detect::live::{Forwarding, LiveSettings, gateway_backend};
use crosstalk_gateway::live::LiveClock;
use crosstalk_memory::support::ManualClock;
use crosstalk_spec::support::Timestamp;

fn salt() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/bench/salt")
}

#[test]
fn live_settings_default_to_forwarding_off_and_the_backend_passes_it_to_l4() {
    let short = LiveSettings::short(0).unwrap_or_else(|e| panic!("{e}"));
    assert_eq!(short.forwarding, Forwarding::Off);
    let clock = || LiveClock::Manual(ManualClock::at(Timestamp::from_micros(1)));
    let backend = gateway_backend();
    let off = backend
        .live_config(&short, clock())
        .unwrap_or_else(|e| panic!("{e}"));
    assert!(!off.provenance.forwarding());
    let on = backend
        .live_config(&short.with_forwarding(Forwarding::On), clock())
        .unwrap_or_else(|e| panic!("{e}"));
    assert!(on.provenance.forwarding());
}

/// `ct-bench-detect` on the SALT input with `extra`, writing under `name`.
fn cli(name: &str, extra: &[&str]) -> std::process::Output {
    let dir = Path::new(env!("CARGO_TARGET_TMPDIR"))
        .join("forwarding")
        .join(name);
    std::fs::create_dir_all(&dir).unwrap_or_else(|e| panic!("{e}"));
    std::process::Command::new(env!("CARGO_BIN_EXE_ct-bench-detect"))
        .arg("--input")
        .arg(salt())
        .arg("--output")
        .arg(dir.join("predictions.jsonl"))
        .args(extra)
        .output()
        .unwrap_or_else(|e| panic!("{e}"))
}

#[test]
fn the_cli_takes_forwarding_on_or_off_and_nothing_else() {
    for value in ["on", "off"] {
        let output = cli(value, &["--forwarding", value]);
        assert!(
            output.status.success(),
            "--forwarding {value}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    assert!(!cli("maybe", &["--forwarding", "maybe"]).status.success());
}
