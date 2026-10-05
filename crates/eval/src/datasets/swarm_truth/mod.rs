//! The demo swarm benchmark: the live gateway scored on traffic from the
//! demo swarm (`crates/demo`), against the ground truth the swarm wrote.
//!
//! ```text
//! truth.jsonl (swarm --ground-truth) ─▶ truth_file::read ─┐
//! exchange-log.jsonl + blobs/ ─▶ exchange_log, bodies ────┴▶ resolve ─▶ World (labels over real exchange ids)
//!                                                                         + AgentIndex + Diagnostics
//! export.jsonl + evidence.jsonl (fetch, or saved) ─▶ detected::predictions(AgentIndex) ─▶ Vec<Prediction>
//!                                                    ─▶ Scorer::add_world ─▶ Report (+ gates) + diagnostics
//! ```
//!
//! Everything runs from saved files, so a run is offline and
//! deterministic; [`fetch`] saves the gateway's side over HTTP.

pub mod bodies;
pub mod detected;
pub mod diagnostics;
pub mod exchange_log;
pub mod fetch;
pub mod locate;
pub mod resolve;
pub mod schema;
pub mod truth_file;

use std::fs::File;
use std::io::BufReader;
use std::path::{Path, PathBuf};

use crosstalk_spec::interfaces::l8_surface::evidence::TransmissionEvidence;
use serde::Serialize;

pub use diagnostics::{Diagnostic, Diagnostics, Effect, JoinFailure, RowKind, Side};
pub use resolve::{AgentIndex, ResolveCounts, Resolved, resolve};

use crate::keys::DatasetId;
use crate::pipeline::Unscored;
use crate::predict::Prediction;
use crate::report::{Gates, Report};
use crate::score::{Score, Scorer};
use bodies::{BlobBodies, Bodies, Cached};
use detected::{Exported, read_evidence, read_export};
use exchange_log::{ExchangeLog, Sessions};
use truth_file::TruthFile;

/// The dataset id of every swarm-benchmark row.
pub const DATASET: &str = "demo-swarm";

/// The model name the world's agents are declared with (the swarm's
/// agents talk to an Anthropic-shaped upstream).
pub const MODEL: &str = "anthropic/claude";

/// The detector name reports carry.
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

/// Scores the gateway's detections against the resolved truth.
/// `truth_name` names the truth file in labels' source references.
pub fn score<B: Bodies>(
    truth: &TruthFile,
    truth_name: &str,
    log: ExchangeLog,
    bodies: B,
    detections: Detections<'_>,
    examples: usize,
    gates: &Gates,
) -> Result<SwarmOutcome, SwarmTruthError> {
    let Detections { exported, evidence } = detections;
    let sessions = Sessions::index(log.exchanges);
    let mut bodies = Cached::new(bodies);
    let resolved = resolve(truth, truth_name, &sessions, &mut bodies)?;
    let mut diagnostics = resolved.diagnostics;
    let predictions = detected::predictions(
        exported,
        evidence,
        &resolved.agents,
        &mut bodies,
        &mut diagnostics,
    );
    let mut scorer = Scorer::new(examples);
    scorer.add_world(&resolved.world, &predictions);
    let score: Score = scorer.finish();
    let outcomes = gates.evaluate(&score);
    let report = Report::new(
        DatasetId::new(DATASET),
        DETECTOR,
        score,
        outcomes,
        Vec::new(),
        Unscored::default(),
    );
    Ok(SwarmOutcome {
        report,
        resolved: resolved.counts,
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

/// Reads every input file and scores, reading bodies from the gateway's
/// blob directory.
pub fn run(
    inputs: &Inputs,
    examples: usize,
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
    score(&truth, &name, log, bodies, detections, examples, gates)
}
