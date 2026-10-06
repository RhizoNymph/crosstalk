//! `from-export` and `replay` over the synthetic swarm run
//! (`tests/swarm_truth/fixture.rs`): the capture as a labelless input view
//! (the gateway's ids, the session and its turn in `client`), and the
//! gateway's detections as predictions attributed from the evidence, or
//! from saved conversation reads when they are there.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use a2a_bench_format::files::{ExchangeRow, Exchanges};
use a2a_bench_format::jsonl::FileReader;
use a2a_bench_format::predictions::Prediction;
use crosstalk_eval::bench_detect::from_export::{
    self, AttributionSource, GATEWAY_EXPORT, PREDICTIONS_FILE, RunFiles, replayed_detections,
    saved_detections,
};
use crosstalk_eval::datasets::swarm_truth::detected::read_evidence;
use crosstalk_eval::datasets::swarm_truth::queried::{Queried, origin_spans};
use crosstalk_eval::datasets::swarm_truth::replay::{ReplaySettings, demo_flow};
use crosstalk_eval::datasets::swarm_truth::window::Margins;
use crosstalk_eval::golden::verify::read_manifest;
use crosstalk_spec::derived::provenance::span::SpanLocation;
use crosstalk_spec::interfaces::l3_reconstruction::conversations::{ExchangePlacement, TurnIndex};
use crosstalk_spec::interfaces::l8_surface::conversation::SpanPoint;
use crosstalk_spec::observed::message::PartRef;
use crosstalk_spec::support::{ByteRange, Timestamp};
use crosstalk_testkit::ids::Ids;
use crosstalk_testkit::time::T0;

use super::common::{predictions, read};
use super::swarm_fixture::{self as fixture, Written};

fn files(written: &Written) -> RunFiles {
    RunFiles {
        dir: written
            .truth
            .parent()
            .unwrap_or_else(|| panic!("a fixture directory"))
            .to_owned(),
        truth: written.truth.clone(),
        exchanges: written.exchanges.clone(),
        blobs: written.blobs.clone(),
        export: written.export.clone(),
        evidence: written.evidence.clone(),
    }
}

fn exchanges(out: &Path) -> Vec<a2a_bench_format::exchange::Exchange> {
    let file = std::fs::File::open(out.join("exchanges.jsonl")).unwrap_or_else(|e| panic!("{e}"));
    let mut reader = FileReader::<Exchanges, _>::open(std::io::BufReader::new(file))
        .unwrap_or_else(|e| panic!("{e}"));
    let mut rows = Vec::new();
    while let Some(section) = reader.next_world().unwrap_or_else(|e| panic!("{e}")) {
        rows.extend(
            section
                .rows
                .into_iter()
                .map(|ExchangeRow::Exchange(exchange)| exchange),
        );
    }
    rows
}

fn attributed(out: &Path) -> (BTreeSet<String>, usize) {
    let (_, worlds) = predictions(&out.join(PREDICTIONS_FILE));
    let mut exchanges = BTreeSet::new();
    let mut origins = 0;
    for row in worlds.iter().flat_map(|world| &world.rows) {
        match row {
            Prediction::Attribution(row) => {
                exchanges.extend(row.exchanges.iter().map(ToString::to_string));
            }
            Prediction::Transmission(row) => {
                origins += row
                    .fields()
                    .matches
                    .iter()
                    .filter(|evidence| evidence.origin_at.is_some())
                    .count();
            }
            Prediction::Unattributed(_) => {}
        }
    }
    (exchanges, origins)
}

#[test]
fn a_saved_run_becomes_an_input_view_and_the_gateways_predictions() {
    let written = fixture::write(&fixture::dir("bench-from-export"), &fixture::truth_rows());
    let run = files(&written);
    let out = run.dir.join("bench");
    let detections =
        saved_detections(&run, "test-gateway".to_owned()).unwrap_or_else(|e| panic!("{e}"));
    let outcome = from_export::write(&run, &detections, Margins::default(), &out)
        .unwrap_or_else(|e| panic!("{e}"));
    assert_eq!(outcome.attribution, AttributionSource::Evidence);
    assert_eq!(outcome.exchanges, 11);
    assert_eq!(outcome.dataset, "demo-swarm/headline");
    assert_eq!(outcome.world, fixture::WORLD);
    assert!(outcome.same_micros.is_empty());
    assert!(
        !out.join("labels.jsonl").exists(),
        "an input view holds no labels"
    );
    let manifest = read_manifest(&out).unwrap_or_else(|e| panic!("{e}"));
    assert_eq!(manifest, manifest.input_view());
    assert!(manifest.converter.version.starts_with("ct-bench-detect "));
    assert_eq!(manifest.selection.len(), 2, "the run window's margins");
    let rows = exchanges(&out);
    let a001: Vec<String> = written
        .a001
        .iter()
        .map(|turn| turn.id.ulid_text())
        .collect();
    for (turn, id) in a001.iter().enumerate() {
        let row = rows
            .iter()
            .find(|row| row.id.to_string() == *id)
            .unwrap_or_else(|| panic!("a001's exchange {id}"));
        assert_eq!(row.client.session.as_deref(), Some("session-a001"));
        assert_eq!(row.client.turn, Some(turn as u32));
    }
    let (header, _) = predictions(&out.join(PREDICTIONS_FILE));
    assert_eq!(header.detector.name, GATEWAY_EXPORT);
    assert_eq!(header.detector.version, "test-gateway");
    assert_eq!(
        header.manifest_digest,
        manifest.digest().unwrap_or_else(|e| panic!("{e}"))
    );
    let (attributed, origins) = attributed(&out);
    assert!(!attributed.is_empty(), "readers and accessors are placed");
    assert_eq!(origins, 0, "the evidence alone gives no origin");

    let again = run.dir.join("bench-again");
    from_export::write(&run, &detections, Margins::default(), &again)
        .unwrap_or_else(|e| panic!("{e}"));
    for file in [
        "manifest.json",
        "messages.jsonl",
        "exchanges.jsonl",
        PREDICTIONS_FILE,
    ] {
        assert!(
            read(&out.join(file)) == read(&again.join(file)),
            "{file} differs on rerun"
        );
    }
}

#[test]
fn saved_conversation_reads_place_every_exchange_and_give_origins() {
    let written = fixture::write(
        &fixture::dir("bench-from-export-queried"),
        &fixture::truth_rows(),
    );
    let run = files(&written);
    let mut ids = Ids::seeded(11);
    let mut queried = Queried::default();
    for turns in [&written.a001, &written.a002, &written.a003] {
        let (agent, conversation) = (ids.agent(), ids.conversation());
        for (at, turn) in turns.iter().enumerate() {
            queried.turns.insert(
                turn.id,
                ExchangePlacement {
                    agent,
                    conversation,
                    turn: TurnIndex(at as u32),
                },
            );
        }
    }
    let file = std::fs::File::open(&written.evidence).unwrap_or_else(|e| panic!("{e}"));
    let evidence = read_evidence(std::io::BufReader::new(file)).unwrap_or_else(|e| panic!("{e}"));
    let writer = written.a001[0];
    let location = SpanLocation {
        part: PartRef {
            message: writer.response,
            index: 0,
        },
        range: ByteRange::new(0, 1).unwrap_or_else(|e| panic!("{e:?}")),
    };
    for span in origin_spans(&evidence) {
        queried.spans.insert(
            span,
            SpanPoint {
                span,
                agent: queried.turns[&writer.id].agent,
                exchange: writer.id,
                turn: None,
                location,
            },
        );
    }
    assert!(
        !queried.spans.is_empty(),
        "the fixture's evidence has content matches"
    );
    queried.write(&run.dir).unwrap_or_else(|e| panic!("{e}"));
    let out = run.dir.join("bench");
    let detections =
        saved_detections(&run, "test-gateway".to_owned()).unwrap_or_else(|e| panic!("{e}"));
    let outcome = from_export::write(&run, &detections, Margins::default(), &out)
        .unwrap_or_else(|e| panic!("{e}"));
    assert_eq!(outcome.attribution, AttributionSource::Query);
    assert_eq!(outcome.attributed_exchanges, 11);
    let (attributed, origins) = attributed(&out);
    assert_eq!(attributed.len(), 11, "every exchange is placed");
    assert!(origins > 0, "span points give origins");
}

#[test]
fn a_reused_sessions_earlier_run_is_left_out() {
    let written = fixture::write_with_prior_run(
        &fixture::dir("bench-from-export-prior"),
        &fixture::truth_rows(),
    );
    let run = files(&written);
    let detections =
        saved_detections(&run, "test-gateway".to_owned()).unwrap_or_else(|e| panic!("{e}"));
    let outcome = from_export::write(
        &run,
        &detections,
        Margins::default(),
        &run.dir.join("bench"),
    )
    .unwrap_or_else(|e| panic!("{e}"));
    assert_eq!(outcome.exchanges, 11);
    assert_eq!(outcome.outside_window, written.prior_a002.len() as u64);
}

#[test]
fn a_replay_writes_live_predictions_with_its_own_conversation_reads() {
    let written = fixture::write(&fixture::dir("bench-replay"), &fixture::truth_rows());
    let run = files(&written);
    let settings = ReplaySettings {
        flow: demo_flow(10_000, 60_000),
        seed: 0,
        since: T0,
        until: None::<Timestamp>,
    };
    let (detections, replayed) =
        replayed_detections(&run, &settings).unwrap_or_else(|e| panic!("{e}"));
    assert_eq!(replayed.ingested, 11);
    assert_eq!(detections.detector.name, "crosstalk-live");
    assert!(detections.detector.config_digest.is_some());
    let out = run.dir.join("bench");
    let outcome = from_export::write(&run, &detections, Margins::default(), &out)
        .unwrap_or_else(|e| panic!("{e}"));
    assert_eq!(outcome.attribution, AttributionSource::Query);
    assert_eq!(outcome.attributed_exchanges, 11, "L3 places every exchange");
    let (_, origins) = attributed(&out);
    assert!(origins > 0, "the composition's span points give origins");
    let mut turns: BTreeMap<String, usize> = BTreeMap::new();
    for row in exchanges(&out) {
        *turns
            .entry(row.client.session.unwrap_or_default())
            .or_default() += 1;
    }
    assert_eq!(turns.len(), 3);
}
