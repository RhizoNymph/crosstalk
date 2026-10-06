//! Driving an export (and a run's predictions) world by world.
//!
//! [`GoldenRun`] is what `ct-eval export` and `ct-eval run
//! --predictions-out` share: each loaded world is converted and written (to
//! files, or to sinks when only the manifest is wanted), and with a
//! [`PredictionsWriter`] its detection becomes a predictions world:
//!
//! | [`WorldOutcome`] | predictions world status | rows |
//! | --- | --- | --- |
//! | `Scored` | `scored` | `predictions::rows` |
//! | `Unscored` | `no_consumers { ingested }` | none |
//! | `Failed` | `failed { reason }` (the run's error text) | none |
//!
//! A world that fails to load is in neither file, as it is in no ct-eval
//! report, so an export and a run over the same selection hold the same
//! worlds and derive the same manifest.

use std::io::Write;

use a2a_bench_format as bench;
use bench::files::{DetectorInfo, PredictionsHeader};
use bench::jsonl::Trailer;
use bench::manifest::Manifest;
use bench::predictions::WorldStatus;

use super::writer::{ExportWriter, PredictionsWriter, Written, manifest_digest};
use super::{GoldenError, Lossy, ManifestSpec, predictions, world};
use crate::corpus::World;
use crate::pipeline::WorldOutcome;
use crate::predict::WorldDirectory;

/// What a finished run wrote.
#[derive(Debug, Clone)]
pub struct Finished {
    pub manifest: Manifest,
    pub written: Written,
    pub predictions: Option<Trailer>,
    pub lossy: Lossy,
}

/// An export in progress, with or without predictions (module docs).
pub struct GoldenRun<W: Write> {
    writer: ExportWriter<W>,
    predictions: Option<PredictionsWriter>,
    lossy: Lossy,
    error: Option<GoldenError>,
}

impl<W: Write> GoldenRun<W> {
    pub fn new(writer: ExportWriter<W>, predictions: Option<PredictionsWriter>) -> Self {
        Self {
            writer,
            predictions,
            lossy: Lossy::default(),
            error: None,
        }
    }

    /// Exports `world` (no predictions).
    pub fn world(&mut self, world: &World) -> Result<(), GoldenError> {
        let export = world::export(world)?;
        self.writer.world(&export)?;
        self.lossy.add(export.lossy);
        Ok(())
    }

    /// Exports an outcome's world and writes its predictions world. The
    /// first error is kept and stops all later work; [`GoldenRun::finish`]
    /// returns it.
    pub fn observe(&mut self, outcome: WorldOutcome<'_>) {
        if self.error.is_some() {
            return;
        }
        if let Err(error) = self.outcome(outcome) {
            self.error = Some(error);
        }
    }

    fn outcome(&mut self, outcome: WorldOutcome<'_>) -> Result<(), GoldenError> {
        let world = match outcome {
            WorldOutcome::Scored { world, .. }
            | WorldOutcome::Unscored { world, .. }
            | WorldOutcome::Failed { world, .. } => world,
        };
        let export = world::export(world)?;
        let inputs = self.writer.world(&export)?;
        self.lossy.add(export.lossy);
        let Some(predictions) = self.predictions.as_mut() else {
            return Ok(());
        };
        match outcome {
            WorldOutcome::Scored { detection, .. } => {
                let directory = WorldDirectory::new(world, &detection.agents, &detection.resolved);
                let rows = predictions::rows(
                    &detection.transmissions,
                    &directory,
                    &predictions::held(detection.agents.attribution()),
                    &std::collections::BTreeMap::new(),
                    predictions::Unlocated::Fail,
                    &export.index,
                    &mut self.lossy,
                )?;
                predictions.world(&inputs, WorldStatus::Scored, &rows)
            }
            WorldOutcome::Unscored { ingested, .. } => {
                predictions.world(&inputs, WorldStatus::NoConsumers { ingested }, &[])
            }
            WorldOutcome::Failed { error, .. } => predictions.world(
                &inputs,
                WorldStatus::Failed {
                    reason: error.to_string(),
                },
                &[],
            ),
        }
    }

    /// Finishes the files: the export's trailers, the manifest, and the
    /// predictions file under `detector` (needed exactly when predictions
    /// are written).
    pub fn finish(
        self,
        spec: &ManifestSpec,
        detector: Option<DetectorInfo>,
    ) -> Result<Finished, GoldenError> {
        if let Some(error) = self.error {
            return Err(error);
        }
        let written = self.writer.finish()?;
        let manifest = spec.manifest(&written);
        let predictions = match (self.predictions, detector) {
            (Some(writer), Some(detector)) => Some(writer.finish(&PredictionsHeader::new(
                spec.dataset.clone(),
                detector,
                manifest_digest(&manifest)?,
            ))?),
            (Some(_), None) => {
                return Err(GoldenError::Verify(super::verify::Mismatch::NoDetector));
            }
            (None, _) => None,
        };
        Ok(Finished {
            manifest,
            written,
            predictions,
            lossy: self.lossy,
        })
    }
}

/// Writes `manifest` as `manifest.json` in `dir`: pretty JSON (members in
/// declaration order, maps sorted), newline-terminated.
pub fn write_manifest(dir: &std::path::Path, manifest: &Manifest) -> Result<(), GoldenError> {
    let path = dir.join(super::writer::MANIFEST_FILE);
    let text = serde_json::to_string_pretty(manifest).map_err(GoldenError::Encode)? + "\n";
    std::fs::write(&path, text).map_err(|source| GoldenError::io(&path, source))
}
