//! A demo-swarm bench run (`tests/swarm_truth/fixture.rs`) exported: the
//! gateway's minted ids with the session and its turn in `client`, labels
//! from the truth file, key groups as `key_group` clusters, and the saved
//! export and evidence as predictions, all passing the format's checks.

use a2a_bench_format::files::{ExchangeRow, Exchanges, Predictions};
use a2a_bench_format::jsonl::FileReader;
use a2a_bench_format::labels::Label;
use a2a_bench_format::predictions::Prediction;
use crosstalk_eval::datasets::swarm_truth::Inputs;
use crosstalk_eval::datasets::swarm_truth::window::Margins;
use crosstalk_eval::golden::swarm::{DETECTOR, Outputs, export};
use crosstalk_eval::golden::{Gap, GoldenError, ids, verify};
use serde_json::json;

use super::common::labels;
use super::swarm_fixture::{self as fixture, Written};

fn inputs(written: &Written) -> Inputs {
    Inputs {
        truth: written.truth.clone(),
        exchanges: written.exchanges.clone(),
        blobs: written.blobs.clone(),
        export: written.export.clone(),
        evidence: written.evidence.clone(),
    }
}

fn written(name: &str) -> Written {
    fixture::write(&fixture::dir(name), &fixture::truth_rows())
}

#[test]
fn a_bench_run_exports_with_its_predictions() {
    let written = written("golden-export");
    let out = written.truth.with_file_name("a2a");
    let predictions = written.truth.with_file_name("predictions.jsonl");
    let finished = export(
        &inputs(&written),
        Margins::default(),
        &Outputs {
            export: Some(out.clone()),
            predictions: Some(predictions.clone()),
            detector_version: "test".to_owned(),
        },
    )
    .unwrap_or_else(|e| panic!("{e}"));
    let verified = verify(&out, Some(&predictions)).unwrap_or_else(|e| panic!("{e}"));
    assert_eq!(verified.worlds, 1);
    assert_eq!(
        verified.exchanges,
        (written.a001.len() + written.a002.len() + written.a003.len()) as u64
    );
    assert_eq!(finished.lossy.single_agent_key_groups, 1);

    let file = std::fs::File::open(out.join("exchanges.jsonl")).unwrap_or_else(|e| panic!("{e}"));
    let mut reader = FileReader::<Exchanges, _>::open(std::io::BufReader::new(file))
        .unwrap_or_else(|e| panic!("{e}"));
    let section = reader
        .next_world()
        .unwrap_or_else(|e| panic!("{e}"))
        .unwrap_or_else(|| panic!("one world"));
    for (turn, expected) in written.a002.iter().enumerate() {
        let found = section
            .rows
            .iter()
            .map(|ExchangeRow::Exchange(exchange)| exchange)
            .find(|exchange| exchange.id == ids::exchange(expected.id))
            .unwrap_or_else(|| panic!("a002's turn {turn} keeps the gateway's id"));
        assert_eq!(found.client.session.as_deref(), Some("session-a002"));
        assert_eq!(found.client.turn, Some(turn as u32));
    }

    let truth = labels(&out)
        .into_iter()
        .flat_map(|(_, rows)| rows)
        .filter(|row| !matches!(row, Label::ExchangeAgent(_)))
        .count();
    assert!(truth > 0, "the truth file's rows are labels");

    let file = std::fs::File::open(&predictions).unwrap_or_else(|e| panic!("{e}"));
    let mut reader = FileReader::<Predictions, _>::open(std::io::BufReader::new(file))
        .unwrap_or_else(|e| panic!("{e}"));
    assert_eq!(reader.header().detector.name, DETECTOR);
    let section = reader
        .next_world()
        .unwrap_or_else(|e| panic!("{e}"))
        .unwrap_or_else(|| panic!("one world"));
    let transmissions = section
        .rows
        .iter()
        .filter(|row| matches!(row, Prediction::Transmission(_)))
        .count();
    assert!(transmissions > 0, "the gateway's transmissions are written");
}

#[test]
fn the_manifest_is_the_same_without_writing_the_export() {
    let written = written("golden-sink");
    let out = written.truth.with_file_name("a2a");
    let with = export(
        &inputs(&written),
        Margins::default(),
        &Outputs {
            export: Some(out),
            predictions: None,
            detector_version: "test".to_owned(),
        },
    )
    .unwrap_or_else(|e| panic!("{e}"));
    let without = export(
        &inputs(&written),
        Margins::default(),
        &Outputs {
            export: None,
            predictions: None,
            detector_version: "test".to_owned(),
        },
    )
    .unwrap_or_else(|e| panic!("{e}"));
    assert_eq!(with.manifest, without.manifest);
}

/// A key group of two agents is a `key_group` cluster, which the format
/// cannot write today (its row holds `kind` twice): the export stops on it.
#[test]
fn a_shared_key_group_is_refused_as_a_gap() {
    let dir = fixture::dir("golden-key-group");
    let mut truth = fixture::truth_rows();
    truth.push(json!({"kind": "agent_cluster", "world": fixture::WORLD, "key_group": 7, "agents": ["a002", "a003"]}));
    let written = fixture::write(&dir, &truth);
    let result = export(
        &inputs(&written),
        Margins::default(),
        &Outputs {
            export: None,
            predictions: None,
            detector_version: "test".to_owned(),
        },
    );
    match result {
        Err(GoldenError::Unexpressible(Gap::ClusterRow { label })) => assert_eq!(label, "k7"),
        other => panic!("expected the cluster gap, got {:?}", other.map(|_| ())),
    }
}
