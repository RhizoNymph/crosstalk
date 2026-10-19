//! A saved node0 bench run as a bench input directory and the gateway's
//! predictions on it (`ct-bench-detect from-export`, `replay`).
//!
//! ```text
//! <run>/truth.jsonl          header only: dataset demo-swarm/<scenario>, world, run ULID,
//!                            the run window, the agents' names (public), session owners (report only)
//! <run>/exchange-log.jsonl   capture::build: the exchanges that started in the run window
//! <run>/blobs/                 (lead 5 s, slack 60 s), in (started_at, id) order, with the gateway's
//!                              ids, client.session and client.turn (ordinal in its session)
//! <run>/export.jsonl         predict::rows: the exported transmissions plus every suspected or
//! <run>/evidence.jsonl         discarded one (detected::choose), less those read outside the window
//! <run>/exchange-turns.json  attribution: each exchange's canonical agent (the gateway's L3)
//! <run>/span-points.json     content matches' origin_at
//!                            absent: attribution from the evidence (readers and accessors), no origin_at
//! ──▶ <out>/manifest.json, messages.jsonl, exchanges.jsonl   the input view: no labels
//!     <out>/predictions.jsonl                                crosstalk-gateway-export (or crosstalk-live)
//! ```
//!
//! Nothing here reads truth beyond the header and the agents' names: the
//! bench's demo-swarm converter labels the capture.

pub mod capture;
pub mod predict;

use std::collections::{BTreeMap, BTreeSet};
use std::fs::File;
use std::io::{BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};

use a2a_bench_format as bench;
use bench::check::WorldInputs;
use bench::files::{
    DetectorInfo, ExchangeRow, Exchanges, MessageRow, Messages, PredictionsHeader, WorldOnly,
};
use bench::jsonl::{BasicHeader, FileWriter};
use bench::manifest::{Converter, FileDigests, Manifest, Source, Split, WorldEntry};
use bench::predictions::WorldStatus;
use bench::version::FORMAT;
use crosstalk_spec::interfaces::l8_surface::evidence::TransmissionEvidence;
use serde::Serialize;

use crate::swarm::bodies::{BlobBodies, Cached};
use crate::swarm::detected::{Exported, read_evidence, read_export};
use crate::swarm::queried::{Queried, QueriedError, origin_spans};
use crate::swarm::replay::{ReplaySettings, Replayed, replay};
use crate::swarm::truth_file::{self, TruthFile};
use crate::swarm::window::{Margins, RunWindow};
use crate::swarm::{SwarmError, exchange_log};
use crate::to_bench::manifest::{self, CROSSTALK_COMMIT, DATASET_VERSION, digest_files};
use crate::to_bench::writer::{
    EXCHANGES_FILE, MESSAGES_FILE, PredictionsWriter, manifest_digest, write_manifest,
};
use crate::to_bench::{ToBenchError, ids};
use crosstalk_spec::ids::{ExchangeId, SpanId};

pub use capture::{Capture, SameMicros};
pub use predict::AttributionSource;

/// The predictions file written beside the input view.
pub const PREDICTIONS_FILE: &str = "predictions.jsonl";
/// The gateway's own detections, as its export and evidence hold them.
pub const GATEWAY_EXPORT: &str = "crosstalk-gateway-export";

#[derive(Debug, thiserror::Error)]
pub enum FromExportError {
    #[error("{path}: {source}")]
    Io {
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error(transparent)]
    Swarm(Box<SwarmError>),
    #[error(transparent)]
    Queried(#[from] QueriedError),
    #[error(transparent)]
    Golden(Box<ToBenchError>),
    #[error("the run's world does not check: {0}")]
    Inputs(#[source] bench::check::InputError),
    #[error("the detector's settings: {0}")]
    Config(String),
    #[error("the capture is empty: no exchange started in the run window")]
    Empty,
}

impl From<SwarmError> for FromExportError {
    fn from(error: SwarmError) -> Self {
        Self::Swarm(Box::new(error))
    }
}

impl From<ToBenchError> for FromExportError {
    fn from(error: ToBenchError) -> Self {
        Self::Golden(Box::new(error))
    }
}

impl FromExportError {
    fn io(path: &Path, source: std::io::Error) -> Self {
        Self::Io {
            path: path.display().to_string(),
            source,
        }
    }
}

/// A run directory's files.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunFiles {
    pub dir: PathBuf,
    pub truth: PathBuf,
    pub exchanges: PathBuf,
    pub blobs: PathBuf,
    pub export: PathBuf,
    pub evidence: PathBuf,
}

impl RunFiles {
    /// The files `run.sh bench` saves in `dir`.
    pub fn in_dir(dir: &Path) -> Self {
        Self {
            dir: dir.to_owned(),
            truth: dir.join("truth.jsonl"),
            exchanges: dir.join("exchange-log.jsonl"),
            blobs: dir.join("blobs"),
            export: dir.join("export.jsonl"),
            evidence: dir.join("evidence.jsonl"),
        }
    }
}

/// The gateway's side of a run: what it detected and, when saved, its
/// conversation reads.
#[derive(Debug, Clone)]
pub struct Detections {
    pub exported: Exported,
    pub evidence: Vec<TransmissionEvidence>,
    pub queried: Option<Queried>,
    pub detector: DetectorInfo,
    /// The files read for them, by name, for the source digest.
    pub files: Vec<(String, PathBuf)>,
}

/// What a from-export run wrote, without any text.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Outcome {
    pub dataset: String,
    pub world: String,
    pub exchanges: u64,
    pub messages: u64,
    /// Log exchanges that started outside the run window, left out.
    pub outside_window: u64,
    pub transmissions: u64,
    pub attributed_exchanges: u64,
    pub unattributed_agents: u64,
    pub attribution: AttributionSource,
    /// Transmissions whose evidence lies outside the world, dropped.
    pub dropped_transmissions: u64,
    /// Two exchanges of one agent (by the truth's session owners) at the
    /// same microsecond: the format wants an agent's exchanges strictly
    /// increasing. Reported, never nudged.
    pub same_micros: Vec<SameMicros>,
    pub manifest_digest: String,
}

fn file_name(path: &Path) -> String {
    path.file_name().map_or_else(
        || path.display().to_string(),
        |name| name.to_string_lossy().into_owned(),
    )
}

/// The truth file of `files`.
pub fn read_truth(files: &RunFiles) -> Result<TruthFile, FromExportError> {
    let file =
        File::open(&files.truth).map_err(|source| FromExportError::io(&files.truth, source))?;
    truth_file::read(BufReader::new(file)).map_err(|source| {
        FromExportError::from(SwarmError::Truth {
            path: files.truth.display().to_string(),
            source,
        })
    })
}

/// The saved export, evidence and (if fetched) conversation reads of
/// `files`, as `crosstalk-gateway-export` predictions under `version`.
pub fn saved_detections(files: &RunFiles, version: String) -> Result<Detections, FromExportError> {
    let detected = |path: &Path| {
        let path = path.display().to_string();
        move |source| FromExportError::from(SwarmError::Detected { path, source })
    };
    let bytes = std::fs::read(&files.export)
        .map_err(|source| FromExportError::io(&files.export, source))?;
    let exported = read_export(&bytes).map_err(detected(&files.export))?;
    let evidence_file = File::open(&files.evidence)
        .map_err(|source| FromExportError::io(&files.evidence, source))?;
    let evidence =
        read_evidence(BufReader::new(evidence_file)).map_err(detected(&files.evidence))?;
    let queried = Queried::read(&files.dir)?;
    let mut read = vec![
        (file_name(&files.export), files.export.clone()),
        (file_name(&files.evidence), files.evidence.clone()),
    ];
    if queried.is_some() {
        for name in [
            crate::swarm::queried::EXCHANGE_TURNS_FILE,
            crate::swarm::queried::SPAN_POINTS_FILE,
        ] {
            read.push((name.to_owned(), files.dir.join(name)));
        }
    }
    Ok(Detections {
        exported,
        evidence,
        queried,
        detector: DetectorInfo {
            name: GATEWAY_EXPORT.to_owned(),
            version,
            variant: crate::config::DEFAULT_VARIANT.to_owned(),
            config_digest: None,
        },
        files: read,
    })
}

/// The gateway build a run's `bench.env` names (`crosstalk_image`), if any.
pub fn gateway_version(dir: &Path) -> Option<String> {
    let text = std::fs::read_to_string(dir.join("bench.env")).ok()?;
    text.lines().find_map(|line| {
        let (key, value) = line.split_once('=')?;
        (key.trim() == "crosstalk_image").then(|| value.trim().to_owned())
    })
}

/// Writes `files`' run as an input view and `detections` as predictions
/// in `out` (module docs).
pub fn write(
    files: &RunFiles,
    detections: &Detections,
    margins: Margins,
    out: &Path,
) -> Result<Outcome, FromExportError> {
    let truth = read_truth(files)?;
    let log = exchange_log::read(&files.exchanges).map_err(SwarmError::from)?;
    let mut bodies = Cached::new(BlobBodies::open(&files.blobs).map_err(SwarmError::from)?);
    let capture = capture::build(
        &truth,
        log,
        &mut bodies,
        margins,
        &file_name(&files.exchanges),
    )?;
    if capture.world.exchanges.is_empty() {
        return Err(FromExportError::Empty);
    }
    let mut lossy = capture.world.lossy;
    let predicted = predict::rows(&capture, detections, &mut bodies, &mut lossy)?;
    std::fs::create_dir_all(out).map_err(|source| FromExportError::io(out, source))?;
    let dataset =
        ids::dataset(&truth.header.scenario().dataset()).map_err(FromExportError::from)?;
    let world = &capture.world;
    let inputs = WorldInputs::new(
        &world.key,
        world.messages.clone(),
        world.decl.clone(),
        world.exchanges.clone(),
    )
    .map_err(FromExportError::Inputs)?;
    let digests = write_inputs(out, &dataset, world)?;
    let mut read = vec![
        (file_name(&files.truth), files.truth.clone()),
        (file_name(&files.exchanges), files.exchanges.clone()),
    ];
    read.extend(detections.files.iter().cloned());
    let manifest = Manifest {
        format: FORMAT,
        dataset: dataset.clone(),
        dataset_version: DATASET_VERSION,
        split: Split::Dev,
        source: Source {
            path: file_name(&files.dir),
            revision: truth.header.run.clone(),
            digest: digest_files(&read)?,
        },
        converter: Converter {
            version: crate::converter_version(),
            git: CROSSTALK_COMMIT.to_owned(),
        },
        selection: selection(margins),
        pace: BTreeMap::new(),
        worlds: vec![WorldEntry {
            key: world.key.clone(),
            exchanges: world.exchanges.len() as u64,
            labels: None,
            notes: BTreeMap::new(),
        }],
        files: digests,
    };
    write_manifest(out, &manifest)?;
    let path = out.join(PREDICTIONS_FILE);
    let mut writer = PredictionsWriter::create(&path)?;
    writer.world(&inputs, WorldStatus::Scored, &predicted.rows)?;
    let digest = manifest_digest(&manifest)?;
    writer.finish(&PredictionsHeader::new(
        dataset.clone(),
        detections.detector.clone(),
        digest,
    ))?;
    crate::to_bench::verify::predictions_file(&path, &manifest)?;
    Ok(Outcome {
        dataset: dataset.to_string(),
        world: world.key.to_string(),
        exchanges: world.exchanges.len() as u64,
        messages: world.messages.len() as u64,
        outside_window: capture.outside.len() as u64,
        transmissions: predicted.transmissions,
        attributed_exchanges: predicted.attributed_exchanges,
        unattributed_agents: lossy.unattributed_agents,
        attribution: predicted.source,
        dropped_transmissions: lossy.dropped_transmissions,
        same_micros: capture.same_micros.clone(),
        manifest_digest: digest.to_string(),
    })
}

/// The selection a capture pins: the run window's margins.
pub fn selection(margins: Margins) -> BTreeMap<String, bench::manifest::Setting> {
    BTreeMap::from([
        ("run_lead_ms".to_owned(), manifest::int(margins.lead_ms)),
        ("run_slack_ms".to_owned(), manifest::int(margins.slack_ms)),
    ])
}

/// Writes the world's messages and exchanges files; returns their digests.
fn write_inputs(
    out: &Path,
    dataset: &bench::ids::DatasetId,
    world: &crate::to_bench::WorldExport,
) -> Result<FileDigests, FromExportError> {
    let create = |name: &str| {
        let path = out.join(name);
        File::create(&path)
            .map(BufWriter::new)
            .map_err(|source| FromExportError::io(&path, source))
    };
    let mut messages = FileWriter::<Messages, _>::new(
        create(MESSAGES_FILE)?,
        &BasicHeader::new::<Messages>(dataset.clone()),
    )
    .map_err(ToBenchError::from)?;
    messages
        .world(&WorldOnly {
            key: world.key.clone(),
        })
        .map_err(ToBenchError::from)?;
    for message in &world.messages {
        messages
            .row(&MessageRow::Message(message.clone()))
            .map_err(ToBenchError::from)?;
    }
    let mut exchanges = FileWriter::<Exchanges, _>::new(
        create(EXCHANGES_FILE)?,
        &BasicHeader::new::<Exchanges>(dataset.clone()),
    )
    .map_err(ToBenchError::from)?;
    exchanges.world(&world.decl).map_err(ToBenchError::from)?;
    for exchange in &world.exchanges {
        exchanges
            .row(&ExchangeRow::Exchange(exchange.clone()))
            .map_err(ToBenchError::from)?;
    }
    let (mut messages_out, messages_trailer) = messages.finish().map_err(ToBenchError::from)?;
    let (mut exchanges_out, exchanges_trailer) = exchanges.finish().map_err(ToBenchError::from)?;
    for (name, out) in [
        (MESSAGES_FILE, &mut messages_out),
        (EXCHANGES_FILE, &mut exchanges_out),
    ] {
        out.flush()
            .map_err(|source| FromExportError::io(Path::new(name), source))?;
    }
    Ok(FileDigests {
        messages: messages_trailer.digest,
        exchanges: exchanges_trailer.digest,
        labels: None,
    })
}

/// The flow settings a replay ran with, as its config digest reads them.
#[derive(Serialize)]
struct ReplayDigested {
    correlation_window_ms: u64,
    evidence_window_ms: u64,
    suspected_ttl_ms: u64,
    content_retention_ms: u64,
    shards: u64,
    tick_ms: u64,
    seed: u64,
    since_us: u64,
    until_us: Option<u64>,
}

/// `files`' log replayed through the live composition in memory
/// (`swarm::replay`), its export, evidence and conversation reads as
/// `crosstalk-live` predictions.
pub fn replayed_detections(
    files: &RunFiles,
    settings: &ReplaySettings,
) -> Result<(Detections, Replayed), FromExportError> {
    let log = exchange_log::read(&files.exchanges).map_err(SwarmError::from)?;
    let mut bodies = Cached::new(BlobBodies::open(&files.blobs).map_err(SwarmError::from)?);
    let replayed = replay(&log, &mut bodies, settings).map_err(SwarmError::from)?;
    let flow = settings.flow;
    let config_digest = crate::config::digest(&ReplayDigested {
        correlation_window_ms: flow.correlation_window_ms,
        evidence_window_ms: flow.evidence_window_ms,
        suspected_ttl_ms: flow.suspected_ttl_ms,
        content_retention_ms: flow.content_retention_ms,
        shards: u64::try_from(flow.shards).unwrap_or(u64::MAX),
        tick_ms: flow.tick_ms,
        seed: settings.seed,
        since_us: settings.since.as_micros(),
        until_us: settings.until.map(|until| until.as_micros()),
    })
    .map_err(|error| FromExportError::Config(error.to_string()))?;
    let detections = Detections {
        exported: replayed.exported.clone(),
        evidence: replayed.evidence.clone(),
        queried: Some(replayed.queried.clone()),
        detector: DetectorInfo {
            name: crate::config::LIVE.to_owned(),
            version: CROSSTALK_COMMIT.to_owned(),
            variant: crate::config::live_variant(crate::detect::live::Forwarding::Off).to_owned(),
            config_digest: Some(config_digest),
        },
        files: Vec::new(),
    };
    Ok((detections, replayed))
}

/// What `ct-bench-detect fetch` asks the gateway's conversation reads
/// for: every log exchange that started in the run window, and every span
/// a content match of the saved evidence names as its origin.
pub fn query_ids(
    files: &RunFiles,
    truth: &TruthFile,
    margins: Margins,
) -> Result<(BTreeSet<ExchangeId>, BTreeSet<SpanId>), FromExportError> {
    let log = exchange_log::read(&files.exchanges).map_err(SwarmError::from)?;
    let run_window = RunWindow::of(truth, margins);
    let exchanges = log
        .exchanges
        .iter()
        .filter(|exchange| run_window.contains(exchange.meta.started_at))
        .map(|exchange| exchange.meta.id)
        .collect();
    let file = File::open(&files.evidence)
        .map_err(|source| FromExportError::io(&files.evidence, source))?;
    let evidence = read_evidence(BufReader::new(file)).map_err(|source| {
        FromExportError::from(SwarmError::Detected {
            path: files.evidence.display().to_string(),
            source,
        })
    })?;
    Ok((exchanges, origin_spans(&evidence)))
}
