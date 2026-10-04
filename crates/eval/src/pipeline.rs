//! The run loop: source → detector → predictions → scorer.
//!
//! A [`Detector`] turns one world's exchanges into spec `Transmission`s.
//! [`ReferenceDetector`] is the naive matcher.
//! [`crate::gateway::PipelineDetector`] feeds each exchange's
//! `NormalizedExchange` to the gateway pipeline's `ingest(normalized, at)`
//! in the world's time order; until the detection layers consume the bus it
//! reports [`DetectionStatus::NoConsumers`], and the run leaves the world
//! unscored rather than scoring it zero. Everything after `detect`
//! (predictions, scoring, reports, gates) is detector-agnostic.

use std::collections::BTreeMap;

use crosstalk_spec::derived::flow::resource::Locator;
use crosstalk_spec::derived::flow::transmission::Transmission;
use crosstalk_spec::derived::provenance::span::SpanLocation;
use crosstalk_spec::ids::{ChannelId, SpanId};

use crate::corpus::{SourceError, TraceSource, World};
use crate::predict::{PredictError, Prediction, WorldDirectory, from_transmission};
use crate::reference::{ReferenceConfig, ReferenceError, run as reference_run};
use crate::score::{Score, Scorer};

/// Whether a detection can be scored.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum DetectionStatus {
    /// The detector ran its detection: score it.
    #[default]
    Detected,
    /// The exchanges went in, but nothing consumes them to detect yet (the
    /// gateway pipeline before L3–L5 exist): do not score.
    NoConsumers { ingested: u64 },
}

/// What a detector reports for one world.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct Detection {
    pub status: DetectionStatus,
    pub transmissions: Vec<Transmission>,
    /// The resources of each channel its transmissions were routed through.
    pub channels: BTreeMap<ChannelId, Vec<Locator>>,
    /// Where each originated span sits, when the detector can say.
    pub spans: BTreeMap<SpanId, SpanLocation>,
}

#[derive(Debug, thiserror::Error)]
pub enum DetectError {
    #[error(transparent)]
    Reference(#[from] ReferenceError),
    #[error(transparent)]
    Pipeline(#[from] crate::gateway::PipelineError),
}

/// Something that finds transmissions in a world's exchanges.
pub trait Detector {
    fn name(&self) -> &str;

    /// Detects over one world. Called once per world, with the world's
    /// exchanges in time order.
    fn detect(&mut self, world: &World) -> Result<Detection, DetectError>;
}

/// The reference matcher as a detector.
#[derive(Debug, Clone, Copy, Default)]
pub struct ReferenceDetector {
    pub config: ReferenceConfig,
}

impl Detector for ReferenceDetector {
    fn name(&self) -> &str {
        "reference"
    }

    fn detect(&mut self, world: &World) -> Result<Detection, DetectError> {
        let output = reference_run(world, self.config)?;
        Ok(Detection {
            status: DetectionStatus::Detected,
            spans: output
                .spans
                .iter()
                .map(|span| (span.id, span.location))
                .collect(),
            transmissions: output.transmissions,
            channels: output.channels,
        })
    }
}

/// Why one world was skipped.
#[derive(Debug, thiserror::Error)]
pub enum WorldError {
    #[error("source: {0}")]
    Source(#[from] SourceError),
    #[error("world {world}: {source}")]
    Detect {
        world: String,
        #[source]
        source: DetectError,
    },
    #[error("world {world}: {source}")]
    Predict {
        world: String,
        #[source]
        source: PredictError,
    },
}

/// The predictions of one world's detection, sorted.
pub fn predictions(world: &World, detection: &Detection) -> Result<Vec<Prediction>, PredictError> {
    let directory =
        WorldDirectory::new(world, detection.channels.clone()).with_spans(detection.spans.clone());
    let mut out = Vec::new();
    for transmission in &detection.transmissions {
        out.extend(from_transmission(transmission, &directory)?);
    }
    out.sort_by(|a, b| a.sort_key().cmp(&b.sort_key()));
    Ok(out)
}

/// Worlds the detector ran over but could not detect in yet.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Unscored {
    pub worlds: u64,
    /// Exchanges the detector took in.
    pub ingested: u64,
}

/// A finished run: the score, the worlds that could not be scored, and
/// those the detector could not detect in yet.
#[derive(Debug)]
pub struct RunSummary {
    pub score: Score,
    pub failures: Vec<WorldError>,
    pub unscored: Unscored,
}

/// Runs `detector` over every world of `source` and scores it. A world that
/// fails to load or detect is recorded and skipped; the run goes on.
/// `observe` sees each world after it is scored (progress, truth dumps).
pub fn run<S: TraceSource, D: Detector>(
    source: &mut S,
    detector: &mut D,
    example_cap: usize,
    mut observe: impl FnMut(&World, &[Prediction]),
) -> RunSummary {
    let mut scorer = Scorer::new(example_cap);
    let mut failures = Vec::new();
    let mut unscored = Unscored::default();
    for world in source.worlds() {
        let world = match world {
            Ok(world) => world,
            Err(error) => {
                tracing::warn!(error = %error, "world skipped");
                failures.push(WorldError::Source(error));
                continue;
            }
        };
        let name = world.key().to_string();
        let detection = match detector.detect(&world) {
            Ok(detection) => detection,
            Err(source) => {
                tracing::warn!(world = %name, error = %source, "detection failed");
                failures.push(WorldError::Detect {
                    world: name,
                    source,
                });
                continue;
            }
        };
        if let DetectionStatus::NoConsumers { ingested } = detection.status {
            tracing::info!(
                world = %name,
                ingested,
                detector = detector.name(),
                "no detector consumers yet; world not scored"
            );
            unscored.worlds += 1;
            unscored.ingested += ingested;
            continue;
        }
        let predicted = match predictions(&world, &detection) {
            Ok(predicted) => predicted,
            Err(source) => {
                tracing::warn!(world = %name, error = %source, "predictions failed");
                failures.push(WorldError::Predict {
                    world: name,
                    source,
                });
                continue;
            }
        };
        scorer.add_world(&world, &predicted);
        tracing::info!(
            world = %name,
            exchanges = world.exchanges().len(),
            labels = world.truth().len(),
            transmissions = detection.transmissions.len(),
            predictions = predicted.len(),
            "scored world"
        );
        observe(&world, &predicted);
    }
    RunSummary {
        score: scorer.finish(),
        failures,
        unscored,
    }
}
