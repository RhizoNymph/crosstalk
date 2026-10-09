//! collusion-wiki rereads through the live composition
//! (`flow.correlator.reread-refreshes-delivery`, INV-1122; INV-1155): a
//! reader that reads a page again is not sent the lines it already read
//! as another transmission, while text new to it in that read is.
//!
//! The input is the shape of `dse/BridgeLAProd1782007689` (wiki demo, seed
//! 0, its first five revisions, real texts): OpenAIJun24Research reads
//! ResearchHelper7690's two-line header at its first edit and again at its
//! second. `tests/fixtures/bench/wiki-rereads/` is its input view, written
//! by ct-eval's golden export at ff3dc44; `expected/wiki-rereads.labels.jsonl`
//! is that export's labels, read here as the test's oracle only: its
//! transmission rows and its one `reread` control.

use std::path::{Path, PathBuf};

use a2a_bench_format::predictions::Prediction;
use serde_json::Value;

fn fixtures() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/bench")
}

/// A location as `(exchange, message, part, start, end)`.
type At = (String, String, u64, u64, u64);

fn at(value: &Value) -> At {
    let text = |key: &str| value[key].as_str().unwrap_or_default().to_owned();
    let int = |value: &Value| value.as_u64().unwrap_or_else(|| panic!("{value}"));
    (
        text("exchange"),
        text("message"),
        int(&value["part"]),
        int(&value["range"]["start"]),
        int(&value["range"]["end"]),
    )
}

fn overlaps(a: &At, b: &At) -> bool {
    a.0 == b.0 && a.1 == b.1 && a.2 == b.2 && a.3 < b.4 && b.3 < a.4
}

/// The labels' transmission locations and reread control locations.
fn oracle() -> (Vec<At>, Vec<At>) {
    let text = std::fs::read_to_string(fixtures().join("expected/wiki-rereads.labels.jsonl"))
        .unwrap_or_else(|e| panic!("{e}"));
    let (mut transmissions, mut rereads) = (Vec::new(), Vec::new());
    for line in text.lines() {
        let row: Value = serde_json::from_str(line).unwrap_or_else(|e| panic!("{e}"));
        match row["kind"].as_str() {
            Some("transmission") => transmissions.push(at(&row["content"]["at"])),
            Some("negative_control") => {
                assert_eq!(row["reason"], "reread");
                rereads.push(at(&row["at"]));
            }
            _ => {}
        }
    }
    (transmissions, rereads)
}

#[test]
fn the_live_detector_finds_every_first_read_and_no_reread() {
    let out = Path::new(env!("CARGO_TARGET_TMPDIR")).join("rereads");
    std::fs::create_dir_all(&out).unwrap_or_else(|e| panic!("{e}"));
    let predictions = out.join("predictions.jsonl");
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_ct-bench-detect"))
        .arg("--input")
        .arg(fixtures().join("wiki-rereads"))
        .arg("--output")
        .arg(&predictions)
        .output()
        .unwrap_or_else(|e| panic!("{e}"));
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let file = std::fs::File::open(&predictions).unwrap_or_else(|e| panic!("{e}"));
    let mut reader =
        a2a_bench_format::jsonl::FileReader::<a2a_bench_format::files::Predictions, _>::open(
            std::io::BufReader::new(file),
        )
        .unwrap_or_else(|e| panic!("{e}"));
    let mut matches: Vec<At> = Vec::new();
    while let Some(section) = reader.next_world().unwrap_or_else(|e| panic!("{e}")) {
        for row in &section.rows {
            if let Prediction::Transmission(row) = row {
                for evidence in &row.fields().matches {
                    let value =
                        serde_json::to_value(evidence.read_at).unwrap_or_else(|e| panic!("{e}"));
                    matches.push(at(&value));
                }
            }
        }
    }
    let (transmissions, rereads) = oracle();
    assert_eq!(transmissions.len(), 8);
    assert_eq!(rereads.len(), 1);
    // Every first read, and the text new to Jun24 at its second read, is
    // confirmed where it was read.
    for label in &transmissions {
        assert!(
            matches.iter().any(|found| overlaps(found, label)),
            "no match at {label:?}"
        );
    }
    // Nothing falls at the reread, and every match is a labelled read.
    for found in &matches {
        assert!(
            !rereads.iter().any(|reread| overlaps(found, reread)),
            "a match at the reread {found:?}"
        );
        assert!(
            transmissions.iter().any(|label| overlaps(found, label)),
            "a match no label holds {found:?}"
        );
    }
}
