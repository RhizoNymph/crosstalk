//! Writing a predictions file and a manifest.
//!
//! [`PredictionsWriter`] cannot write its header first: the header names
//! the manifest's digest, which is known only once every world is
//! converted. It spills each world's rows to `<file>.partial` as it goes,
//! then writes the framed file from the spill on [`PredictionsWriter::finish`]
//! and removes the spill, so memory stays bounded by one world.

use std::fs::{self, File};
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};

use a2a_bench_format as bench;
use bench::check::{WorldInputs, check_predictions};
use bench::files::{Predictions, PredictionsHeader, PredictionsWorld};
use bench::ids::Digest;
use bench::jsonl::{FileWriter, Trailer};
use bench::manifest::Manifest;
use bench::predictions::{Prediction, WorldStatus};
use serde::{Deserialize, Serialize};

use super::ToBenchError;

pub const MANIFEST_FILE: &str = "manifest.json";
pub const MESSAGES_FILE: &str = "messages.jsonl";
pub const EXCHANGES_FILE: &str = "exchanges.jsonl";

/// A spilled line: a world opener or one of its rows.
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Spilled {
    World(PredictionsWorld),
    Row(Prediction),
}

/// Writes a predictions file whose header is known only at the end
/// (module docs).
pub struct PredictionsWriter {
    path: PathBuf,
    spill_path: PathBuf,
    spill: BufWriter<File>,
    worlds: u64,
}

impl PredictionsWriter {
    pub fn create(path: &Path) -> Result<Self, ToBenchError> {
        if let Some(parent) = path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
        {
            fs::create_dir_all(parent).map_err(|source| ToBenchError::io(parent, source))?;
        }
        let mut spill_path = path.as_os_str().to_owned();
        spill_path.push(".partial");
        let spill_path = PathBuf::from(spill_path);
        let spill = File::create(&spill_path)
            .map(BufWriter::new)
            .map_err(|source| ToBenchError::io(&spill_path, source))?;
        Ok(Self {
            path: path.to_owned(),
            spill_path,
            spill,
            worlds: 0,
        })
    }

    /// Checks `rows` against the world's `inputs` (`check_predictions`;
    /// only a scored world has rows) and spills them.
    pub fn world(
        &mut self,
        inputs: &WorldInputs,
        status: WorldStatus,
        rows: &[Prediction],
    ) -> Result<(), ToBenchError> {
        let key = inputs.decl().key.clone();
        check_predictions(inputs, rows).map_err(|source| ToBenchError::Predictions {
            world: key.to_string(),
            source,
        })?;
        self.spill_line(&Spilled::World(PredictionsWorld { key, status }))?;
        for row in rows {
            self.spill_line(&Spilled::Row(row.clone()))?;
        }
        self.worlds += 1;
        Ok(())
    }

    /// A world whose inputs could not be read, written `failed { reason }`
    /// with no rows (nothing to check them against).
    pub fn failed(
        &mut self,
        key: bench::ids::WorldKey,
        reason: String,
    ) -> Result<(), ToBenchError> {
        self.spill_line(&Spilled::World(PredictionsWorld {
            key,
            status: WorldStatus::Failed { reason },
        }))?;
        self.worlds += 1;
        Ok(())
    }

    fn spill_line(&mut self, line: &Spilled) -> Result<(), ToBenchError> {
        serde_json::to_writer(&mut self.spill, line).map_err(ToBenchError::Encode)?;
        self.spill
            .write_all(b"\n")
            .map_err(|source| ToBenchError::io(&self.spill_path, source))
    }

    /// Writes the framed file under `header` from the spill, then removes
    /// the spill.
    pub fn finish(mut self, header: &PredictionsHeader) -> Result<Trailer, ToBenchError> {
        self.spill
            .flush()
            .map_err(|source| ToBenchError::io(&self.spill_path, source))?;
        drop(self.spill);
        let out = File::create(&self.path)
            .map(BufWriter::new)
            .map_err(|source| ToBenchError::io(&self.path, source))?;
        let mut writer = FileWriter::<Predictions, _>::new(out, header)?;
        let spill = File::open(&self.spill_path)
            .map(BufReader::new)
            .map_err(|source| ToBenchError::io(&self.spill_path, source))?;
        for line in spill.lines() {
            let line = line.map_err(|source| ToBenchError::io(&self.spill_path, source))?;
            match serde_json::from_str(&line).map_err(ToBenchError::Encode)? {
                Spilled::World(world) => writer.world(&world)?,
                Spilled::Row(row) => writer.row(&row)?,
            }
        }
        let (_, trailer) = writer.finish()?;
        fs::remove_file(&self.spill_path)
            .map_err(|source| ToBenchError::io(&self.spill_path, source))?;
        Ok(trailer)
    }

    pub fn worlds(&self) -> u64 {
        self.worlds
    }
}

/// The digest a predictions header names for `manifest`.
pub fn manifest_digest(manifest: &Manifest) -> Result<Digest, ToBenchError> {
    manifest.digest().map_err(ToBenchError::Encode)
}

/// Writes `manifest` as `manifest.json` in `dir`: pretty JSON (members in
/// declaration order, maps sorted), newline-terminated.
pub fn write_manifest(dir: &Path, manifest: &Manifest) -> Result<(), ToBenchError> {
    let path = dir.join(MANIFEST_FILE);
    let text = serde_json::to_string_pretty(manifest).map_err(ToBenchError::Encode)? + "\n";
    fs::write(&path, text).map_err(|source| ToBenchError::io(&path, source))
}
