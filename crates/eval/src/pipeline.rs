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

use crosstalk_spec::derived::flow::transmission::Transmission;
use crosstalk_spec::interfaces::l4_provenance::IndexedSpan;

use crate::corpus::{SourceError, TraceSource, World};
use crate::predict::memory::{AccessTable, ChannelTable, SpanTable};
use crate::predict::reads::{ReadError, Reads, Resolved, ready};
use crate::predict::{AgentMap, PredictError, Prediction, WorldDirectory, from_transmission};
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

/// What a detector reports for one world: its transmissions, the corpus
/// agents behind its agent ids, and what its transmissions name (spans,
/// accesses, channel resources) read through the spec's read traits
/// ([`Resolved::gather`]).
#[derive(Debug, Clone, PartialEq, Default)]
pub struct Detection {
    pub status: DetectionStatus,
    pub transmissions: Vec<Transmission>,
    pub agents: AgentMap,
    pub resolved: Resolved,
}

#[derive(Debug, thiserror::Error)]
pub enum DetectError {
    #[error(transparent)]
    Reference(#[from] ReferenceError),
    #[error(transparent)]
    Pipeline(#[from] crate::gateway::PipelineError),
    #[error(transparent)]
    Live(#[from] crate::detect::live::LiveError),
    #[error("reading the detection's evidence: {0}")]
    Read(#[from] ReadError),
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

    /// Runs the matcher, then reads its evidence back through the spec's
    /// read traits over eval-owned tables: the same path a gateway's stores
    /// are read through.
    fn detect(&mut self, world: &World) -> Result<Detection, DetectError> {
        let output = reference_run(world, self.config)?;
        let mut spans = SpanTable::default();
        for span in &output.spans {
            spans.insert(
                span.id,
                IndexedSpan {
                    exchange: span.exchange,
                    author: span.author,
                    location: span.location,
                },
            );
        }
        let channels = ChannelTable::new(output.channels);
        let reads = Reads {
            spans: &spans,
            accesses: &AccessTable::default(),
            channels: &channels,
        };
        let resolved = ready(Resolved::gather(&output.transmissions, reads))??;
        Ok(Detection {
            status: DetectionStatus::Detected,
            transmissions: output.transmissions,
            agents: AgentMap::of_world(world),
            resolved,
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
    let directory = WorldDirectory::new(world, &detection.agents, &detection.resolved);
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

/// What became of one loaded world, for [`run_with`]'s observer.
#[derive(Debug, Clone, Copy)]
pub enum WorldOutcome<'a> {
    /// Detected, predicted and scored.
    Scored {
        world: &'a World,
        detection: &'a Detection,
        predictions: &'a [Prediction],
    },
    /// The detector took the world in but has nothing that detects yet.
    Unscored { world: &'a World, ingested: u64 },
    /// Detection or predictions failed; the run recorded `error` and went on.
    Failed {
        world: &'a World,
        error: &'a WorldError,
    },
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
    run_with(source, detector, example_cap, |outcome| {
        if let WorldOutcome::Scored {
            world, predictions, ..
        } = outcome
        {
            observe(world, predictions);
        }
    })
}

/// [`run`], with `observe` seeing every loaded world's outcome in source
/// order: scored (with its detection), unscored or failed. A world that
/// fails to load has no world to observe and is only recorded.
pub fn run_with<S: TraceSource, D: Detector>(
    source: &mut S,
    detector: &mut D,
    example_cap: usize,
    mut observe: impl FnMut(WorldOutcome<'_>),
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
                let error = WorldError::Detect {
                    world: name,
                    source,
                };
                observe(WorldOutcome::Failed {
                    world: &world,
                    error: &error,
                });
                failures.push(error);
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
            observe(WorldOutcome::Unscored {
                world: &world,
                ingested,
            });
            continue;
        }
        let predicted = match predictions(&world, &detection) {
            Ok(predicted) => predicted,
            Err(source) => {
                tracing::warn!(world = %name, error = %source, "predictions failed");
                let error = WorldError::Predict {
                    world: name,
                    source,
                };
                observe(WorldOutcome::Failed {
                    world: &world,
                    error: &error,
                });
                failures.push(error);
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
        observe(WorldOutcome::Scored {
            world: &world,
            detection: &detection,
            predictions: &predicted,
        });
    }
    RunSummary {
        score: scorer.finish(),
        failures,
        unscored,
    }
}
