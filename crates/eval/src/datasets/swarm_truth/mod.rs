//! The demo swarm benchmark: the live gateway scored on traffic from the
//! demo swarm (`crates/demo`), against the ground truth the swarm wrote.
//!
//! ```text
//! truth.jsonl (swarm --ground-truth) ─▶ truth_file::read ─┐
//! exchange-log.jsonl + blobs/ ─▶ exchange_log, bodies ────┴▶ resolve ─▶ World (labels over real exchange ids)
//!          (window::split: only exchanges inside the run window)          + AgentIndex + Diagnostics
//! export.jsonl + evidence.jsonl (fetch, or saved) ─▶ detected::predictions(AgentIndex) ─▶ Vec<Prediction>
//!                                                    ─▶ Scorer::add_world ─▶ Report (+ gates) + diagnostics
//! ```
//!
//! The exchange log accumulates across runs and a reused seed reuses
//! session ids, so the log is first cut to the run window ([`window`]):
//! the exchanges of the truth's sessions outside it are reported
//! (`session_reused_outside_run`) and left out of the traffic count, the
//! session ordinals and the agent map, and a detection read outside it is
//! reported (`outside_run_window`) and not scored.
//!
//! Everything runs from saved files, so a run is offline and
//! deterministic; [`fetch`] saves the gateway's side over HTTP.

pub mod bodies;
pub mod detected;
pub mod diagnostics;
pub mod exchange_log;
pub mod fetch;
pub mod locate;
pub mod replay;
pub mod resolve;
pub mod schema;
pub mod truth_file;
pub mod window;

use std::fs::File;
use std::io::BufReader;
use std::path::{Path, PathBuf};

use crosstalk_spec::interfaces::l8_surface::evidence::TransmissionEvidence;
use serde::Serialize;

pub use diagnostics::{Diagnostic, Diagnostics, Effect, JoinFailure, RowKind, Side};
pub use resolve::{AgentIndex, ResolveCounts, Resolved, resolve};

use crate::pipeline::Unscored;
use crate::predict::Prediction;
use crate::report::{GateDetector, Gates, Report};
use crate::score::{Score, Scorer};
use bodies::{BlobBodies, Bodies, Cached};
use detected::{Exported, read_evidence, read_export};
use exchange_log::{ExchangeLog, Sessions};
use truth_file::TruthFile;
use window::{Margins, RunWindow};

/// The prefix of every swarm-benchmark dataset id: a run scores under
/// `demo-swarm/<scenario>` ([`schema::Scenario::dataset`]).
pub const DATASET_PREFIX: &str = "demo-swarm";

/// The model name the world's agents are declared with (the swarm's
/// agents talk to an Anthropic-shaped upstream).
pub const MODEL: &str = "anthropic/claude";

/// The detector name reports carry; gates name it the same way
/// ([`GateDetector::GatewayExport`]).
pub const DETECTOR: &str = "gateway-export";

#[derive(Debug, thiserror::Error)]
pub enum SwarmTruthError {
    #[error("opening {path}: {source}")]
    Open {
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error("truth file {path}: {source}")]
    Truth {
        path: String,
        #[source]
        source: truth_file::TruthFileError,
    },
    #[error(transparent)]
    ExchangeLog(#[from] exchange_log::ExchangeLogError),
    #[error(transparent)]
    Bodies(#[from] bodies::OpenBodiesError),
    #[error(transparent)]
    Resolve(#[from] resolve::ResolveError),
    #[error(transparent)]
    Replay(#[from] replay::ReplayError),
    #[error("export {path}: {source}")]
    Detected {
        path: String,
        #[source]
        source: detected::DetectedError,
    },
}

/// The files a run reads.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Inputs {
    pub truth: PathBuf,
    pub exchanges: PathBuf,
    pub blobs: PathBuf,
    pub export: PathBuf,
    pub evidence: PathBuf,
}

/// The blob directory beside an exchange log at
/// `<data dir>/exchanges/exchange-log.jsonl`: `<data dir>/blobs`.
pub fn default_blobs(exchanges: &Path) -> PathBuf {
    exchanges
        .parent()
        .and_then(Path::parent)
        .unwrap_or_else(|| Path::new("."))
        .join("blobs")
}

/// The evidence file beside an export: `evidence.jsonl` in its directory.
pub fn default_evidence(export: &Path) -> PathBuf {
    export
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .join("evidence.jsonl")
}

/// What the gateway's side of a run held.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize)]
pub struct DetectedCounts {
    pub exported: u64,
    pub evidence: u64,
    pub predictions: u64,
}

/// A finished swarm run.
#[derive(Debug)]
pub struct SwarmOutcome {
    pub report: Report,
    /// The run window the exchange log was cut to.
    pub window: RunWindow,
    pub resolved: ResolveCounts,
    pub detected: DetectedCounts,
    pub diagnostics: Diagnostics,
    pub predictions: Vec<Prediction>,
    pub key_groups: usize,
}

/// The gateway's side of a run: a verified export and the evidence of its
/// transmissions.
#[derive(Debug, Clone, Copy)]
pub struct Detections<'a> {
    pub exported: &'a Exported,
    pub evidence: &'a [TransmissionEvidence],
}

/// How a run is scored, beyond its files.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Options {
    /// How many misses and false positives to keep as examples.
    pub examples: usize,
    /// How far the run window reaches before the header's start and past
    /// the truth's latest row time ([`window::RunWindow::of`]).
    pub margins: Margins,
}

impl Options {
    /// `examples` examples and the default margins ([`Margins::default`]).
    pub fn new(examples: usize) -> Self {
        Self {
            examples,
            margins: Margins::default(),
        }
    }
}

/// Scores the gateway's detections against the resolved truth, checking
/// the gates tuned on [`DETECTOR`] only. `truth_name` names the truth file in labels' source references.
/// Only the log's exchanges inside the run window count ([`window`]).
pub fn score<B: Bodies>(
    truth: &TruthFile,
    truth_name: &str,
    log: ExchangeLog,
    bodies: B,
    detections: Detections<'_>,
    options: Options,
    gates: &Gates,
) -> Result<SwarmOutcome, SwarmTruthError> {
    let Detections { exported, evidence } = detections;
    let run_window = RunWindow::of(truth, options.margins);
    let split = window::split(log.exchanges, run_window, &window::truth_sessions(truth));
    let sessions = Sessions::index(split.inside);
    let mut bodies = Cached::new(bodies);
    let resolved = resolve(truth, truth_name, &sessions, &mut bodies)?;
    let mut counts = resolved.counts;
    counts.excluded_outside_window = split.reused.len() as u64;
    let mut diagnostics = resolved.diagnostics;
    for reused in split.reused {
        diagnostics.push(Diagnostic {
            line: None,
            row: None,
            side: Side::Row,
            failure: JoinFailure::SessionReusedOutsideRun {
                session: reused.session,
                exchange: reused.exchange,
            },
            effect: Effect::Excluded,
        });
    }
    let predictions = detected::predictions(
        exported,
        evidence,
        &resolved.agents,
        &split.outside,
        &mut bodies,
        &mut diagnostics,
    );
    let mut scorer = Scorer::new(options.examples);
    scorer.add_world(&resolved.world, &predictions);
    let mut score: Score = scorer.finish();
    // The world holds labels over the log's exchange ids, not the
    // exchanges themselves, so the scorer counts none: its traffic is the
    // exchanges of the truth's sessions.
    score.totals.exchanges = resolved.counts.exchanges;
    let outcomes = gates
        .for_detector(GateDetector::GatewayExport)
        .evaluate(&score);
    let report = Report::new(
        truth.header.scenario().dataset(),
        DETECTOR,
        score,
        outcomes,
        Vec::new(),
        Unscored::default(),
    );
    Ok(SwarmOutcome {
        report,
        window: run_window,
        resolved: counts,
        detected: DetectedCounts {
            exported: exported.transmissions.len() as u64,
            evidence: evidence.len() as u64,
            predictions: predictions.len() as u64,
        },
        diagnostics,
        predictions,
        key_groups: resolved.key_groups.len(),
    })
}

fn open(path: &Path) -> Result<BufReader<File>, SwarmTruthError> {
    File::open(path)
        .map(BufReader::new)
        .map_err(|source| SwarmTruthError::Open {
            path: path.display().to_string(),
            source,
        })
}

/// Reads every input file and scores with the default run-window margins,
/// reading bodies from the gateway's blob directory.
pub fn run(
    inputs: &Inputs,
    examples: usize,
    gates: &Gates,
) -> Result<SwarmOutcome, SwarmTruthError> {
    run_with(inputs, Options::new(examples), gates)
}

/// [`run`] with explicit [`Options`].
pub fn run_with(
    inputs: &Inputs,
    options: Options,
    gates: &Gates,
) -> Result<SwarmOutcome, SwarmTruthError> {
    let shown = inputs.truth.display().to_string();
    let truth =
        truth_file::read(open(&inputs.truth)?).map_err(|source| SwarmTruthError::Truth {
            path: shown.clone(),
            source,
        })?;
    let log = exchange_log::read(&inputs.exchanges)?;
    let bodies = BlobBodies::open(&inputs.blobs)?;
    let detected_error = |source| SwarmTruthError::Detected {
        path: inputs.export.display().to_string(),
        source,
    };
    let export_bytes = std::fs::read(&inputs.export).map_err(|source| SwarmTruthError::Open {
        path: inputs.export.display().to_string(),
        source,
    })?;
    let exported = read_export(&export_bytes).map_err(detected_error)?;
    let evidence = read_evidence(open(&inputs.evidence)?).map_err(detected_error)?;
    let name = inputs
        .truth
        .file_name()
        .map_or_else(|| shown.clone(), |name| name.to_string_lossy().into_owned());
    let detections = Detections {
        exported: &exported,
        evidence: &evidence,
    };
    score(&truth, &name, log, bodies, detections, options, gates)
}

/// The files `ct-eval replay` reads: a run's truth, and the exchange log
/// and blobs its gateway wrote.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplayInputs {
    pub truth: PathBuf,
    pub exchanges: PathBuf,
    pub blobs: PathBuf,
}

/// How to replay a run; `since` defaults to the truth header's
/// `started_at_unix_ms`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReplayOptions {
    pub flow: crosstalk_flow::consumer::FlowConfig,
    pub seed: u64,
    pub since: Option<crosstalk_spec::support::Timestamp>,
    pub until: Option<crosstalk_spec::support::Timestamp>,
}

/// A replayed and scored run.
#[derive(Debug)]
pub struct ReplayOutcome {
    pub replayed: replay::Replayed,
    pub settings: replay::ReplaySettings,
    pub scored: SwarmOutcome,
}

/// Replays the run's exchanges through the live composition
/// ([`replay::replay`]) and scores the export and evidence it serves
/// exactly as [`run`] scores a fetched one, against the log's run window
/// (default margins).
pub fn run_replay(
    inputs: &ReplayInputs,
    options: &ReplayOptions,
    examples: usize,
    gates: &Gates,
) -> Result<ReplayOutcome, SwarmTruthError> {
    let shown = inputs.truth.display().to_string();
    let truth =
        truth_file::read(open(&inputs.truth)?).map_err(|source| SwarmTruthError::Truth {
            path: shown.clone(),
            source,
        })?;
    let log = exchange_log::read(&inputs.exchanges)?;
    let settings = replay::ReplaySettings {
        flow: options.flow,
        seed: options.seed,
        since: options.since.unwrap_or_else(|| {
            crosstalk_spec::support::Timestamp::from_micros(
                truth.header.started_at_unix_ms.saturating_mul(1000),
            )
        }),
        until: options.until,
    };
    let mut bodies = Cached::new(BlobBodies::open(&inputs.blobs)?);
    let replayed = replay::replay(&log, &mut bodies, &settings)?;
    let name = inputs
        .truth
        .file_name()
        .map_or_else(|| shown.clone(), |name| name.to_string_lossy().into_owned());
    let detections = Detections {
        exported: &replayed.exported,
        evidence: &replayed.evidence,
    };
    let scored = score(
        &truth,
        &name,
        log,
        BlobBodies::open(&inputs.blobs)?,
        detections,
        Options::new(examples),
        gates,
    )?;
    Ok(ReplayOutcome {
        replayed,
        settings,
        scored,
    })
}
