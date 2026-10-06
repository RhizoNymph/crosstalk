//! `ct-bench-detect --input DIR --output FILE`: one predictions file from
//! one input directory (module docs of [`super`]).
//!
//! | world | predictions world |
//! | --- | --- |
//! | live, detected | `scored`, with `golden::predictions::rows` |
//! | pipeline | `no_consumers { ingested }` |
//! | cannot be processed | `failed { "<code>: <detail>" }` |
//!
//! An agent merge (one detector agent over exchanges of two true agents)
//! is not a failure here: the attribution is written as the detector made
//! it, and the scorer fails the world.

use std::collections::BTreeMap;
use std::path::Path;

use a2a_bench_format as bench;
use bench::check::WorldInputs;
use bench::files::{DetectorInfo, PredictionsHeader};
use bench::predictions::{Prediction, WorldStatus};
use crosstalk_flow::extract::ExtractConfig;

use super::config::{self, ConfigError};
use super::convert;
use super::directory::BenchDirectory;
use super::input::{InputDir, InputError, WorldRead};
use super::{FailureCode, WorldFailure};
use crate::detect::live::{BackendError, GatewayBackend, LiveDetector, LiveError, LiveSettings};
use crate::gateway::{PipelineDetector, PipelineError};
use crate::golden::predictions::{self, Unlocated};
use crate::golden::writer::{PredictionsWriter, manifest_digest};
use crate::golden::{GoldenError, Lossy};

/// Which detector runs.
#[derive(Debug, Clone)]
pub enum Mode {
    /// The live composition (`crosstalk-live`).
    Live {
        settings: LiveSettings,
        extract: ExtractConfig,
    },
    /// The bare gateway pipeline (`crosstalk-pipeline`).
    Pipeline { seed: u64 },
}

impl Mode {
    pub fn info(&self) -> Result<DetectorInfo, ConfigError> {
        match self {
            Self::Live { settings, extract } => config::live_info(settings, extract),
            Self::Pipeline { seed } => config::pipeline_info(*seed),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum RunError {
    #[error("the input: {0}")]
    Input(#[from] InputError),
    #[error("the detector's settings: {0}")]
    Config(#[from] ConfigError),
    #[error("starting the live detector: {0}")]
    Live(#[source] LiveError),
    #[error("starting the pipeline: {0}")]
    Pipeline(#[source] PipelineError),
    #[error("writing the predictions: {0}")]
    Write(#[from] GoldenError),
}

/// What a run wrote.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Summary {
    pub worlds: u64,
    pub scored: u64,
    pub unscored: u64,
    pub failed: u64,
    pub rows: u64,
    pub lossy: Lossy,
}

enum Engine {
    Live(Box<LiveDetector<GatewayBackend>>),
    Pipeline(PipelineDetector),
}

/// Runs `mode` over every world of `input`, writing `output`.
pub fn run(input: &Path, output: &Path, mode: &Mode) -> Result<Summary, RunError> {
    let mut dir = InputDir::open(input)?;
    let info = mode.info()?;
    let mut engine = match mode {
        Mode::Live { settings, extract } => Engine::Live(Box::new(
            LiveDetector::new(
                GatewayBackend::default().with_extract(extract.clone()),
                *settings,
            )
            .map_err(RunError::Live)?,
        )),
        Mode::Pipeline { seed } => {
            Engine::Pipeline(PipelineDetector::new(*seed).map_err(RunError::Pipeline)?)
        }
    };
    let dataset = dir.manifest().dataset.clone();
    let mut writer = PredictionsWriter::create(output)?;
    let mut summary = Summary::default();
    while let Some(read) = dir.next_world()? {
        summary.worlds += 1;
        let inputs = match read {
            WorldRead::Ready(inputs) => inputs,
            WorldRead::Unreadable { key, failure } => {
                tracing::warn!(world = %key, reason = %failure, "world unreadable");
                summary.failed += 1;
                writer.failed(key, failure.reason())?;
                continue;
            }
        };
        let outcome = match &mut engine {
            Engine::Live(detector) => {
                live_world(detector, &dataset, &inputs, &mut summary.lossy).map(|rows| {
                    (WorldStatus::Scored, rows)
                })
            }
            Engine::Pipeline(detector) => pipeline_world(detector, &dataset, &inputs)
                .map(|ingested| (WorldStatus::NoConsumers { ingested }, Vec::new())),
        };
        write_world(&mut writer, &inputs, outcome, &mut summary)?;
    }
    let manifest = dir.manifest().clone();
    writer.finish(&PredictionsHeader::new(
        dataset,
        info,
        manifest_digest(&manifest)?,
    ))?;
    crate::golden::verify::predictions_file(output, &manifest)?;
    Ok(summary)
}

/// Writes one world's outcome; rows `check_predictions` refuses fail the
/// world instead.
fn write_world(
    writer: &mut PredictionsWriter,
    inputs: &WorldInputs,
    outcome: Result<(WorldStatus, Vec<Prediction>), WorldFailure>,
    summary: &mut Summary,
) -> Result<(), GoldenError> {
    let key = inputs.decl().key.clone();
    let failure = match outcome {
        Ok((status, rows)) => {
            let scored = matches!(status, WorldStatus::Scored);
            match writer.world(inputs, status, &rows) {
                Ok(()) => {
                    if scored {
                        summary.scored += 1;
                    } else {
                        summary.unscored += 1;
                    }
                    summary.rows += rows.len() as u64;
                    return Ok(());
                }
                Err(GoldenError::Predictions { source, .. }) => {
                    WorldFailure::new(FailureCode::Conversion, source)
                }
                Err(other) => return Err(other),
            }
        }
        Err(failure) => failure,
    };
    tracing::warn!(world = %key, reason = %failure, "world failed");
    summary.failed += 1;
    writer.world(
        inputs,
        WorldStatus::Failed {
            reason: failure.reason(),
        },
        &[],
    )
}

/// The failure a live error is.
pub fn live_failure(error: LiveError) -> WorldFailure {
    let code = match &error {
        LiveError::Backend(BackendError::Ingest { .. } | BackendError::Build { .. }) => {
            FailureCode::Ingest
        }
        LiveError::Backend(BackendError::Settle { .. }) => FailureCode::Settle,
        LiveError::Backend(BackendError::Read { .. })
        | LiveError::Reads(_)
        | LiveError::Agents(_) => FailureCode::Read,
        LiveError::Runtime(_) | LiveError::Timing(_) => FailureCode::Ingest,
    };
    WorldFailure::new(code, error)
}

/// The failure a row conversion error is.
pub fn rows_failure(error: GoldenError) -> WorldFailure {
    let code = match &error {
        GoldenError::UnknownAccess(_)
        | GoldenError::WrongAccess { .. }
        | GoldenError::UnlocatedAccess { .. } => FailureCode::UnlocatedAccess,
        _ => FailureCode::Conversion,
    };
    WorldFailure::new(code, error)
}

/// One world through a fresh live composition, as prediction rows.
pub fn live_world(
    detector: &mut LiveDetector<GatewayBackend>,
    dataset: &bench::ids::DatasetId,
    inputs: &WorldInputs,
    lossy: &mut Lossy,
) -> Result<Vec<Prediction>, WorldFailure> {
    let converted = convert::world(dataset, inputs)?;
    let Some(raw) = detector
        .detect_exchanges(&converted.timed())
        .map_err(live_failure)?
    else {
        return Ok(Vec::new());
    };
    let directory = BenchDirectory::new(&converted, &raw.resolved);
    predictions::rows(
        &raw.transmissions,
        &directory,
        &predictions::held(&raw.attribution),
        &BTreeMap::new(),
        Unlocated::Fail,
        &converted.index,
        lossy,
    )
    .map_err(rows_failure)
}

/// One world through the bare pipeline: how many exchanges it took in.
pub fn pipeline_world(
    detector: &mut PipelineDetector,
    dataset: &bench::ids::DatasetId,
    inputs: &WorldInputs,
) -> Result<u64, WorldFailure> {
    let converted = convert::world(dataset, inputs)?;
    detector
        .ingest(&converted.timed())
        .map_err(|error| WorldFailure::new(FailureCode::Ingest, error))
}
