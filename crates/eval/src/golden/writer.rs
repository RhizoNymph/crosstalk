//! Writing an export's three files and a run's predictions file.
//!
//! [`ExportWriter`] writes `messages.jsonl`, `exchanges.jsonl` and
//! `labels.jsonl` world by world, in the same world order, checking each
//! world first (`WorldExport::check`), and returns the trailers and the
//! manifest's world entries. Over [`std::io::Sink`]s it writes nothing and
//! still yields the same digests, which is how `ct-eval run
//! --predictions-out` derives the manifest digest of the export it
//! matches without writing it.
//!
//! [`PredictionsWriter`] cannot write its header first: the header names
//! the manifest's digest, which is known only once every world is
//! converted. It spills each world's rows to `<file>.partial` as it goes,
//! then writes the framed file from the spill on [`PredictionsWriter::finish`]
//! and removes the spill, so memory stays bounded by one world.

use std::fs::{self, File};
use std::io::{BufRead, BufReader, BufWriter, Sink, Write};
use std::path::{Path, PathBuf};

use a2a_bench_format as bench;
use bench::check::{WorldInputs, check_predictions};
use bench::files::{
    ExchangeRow, Exchanges, Labels, LabelsWorld, MessageRow, Messages, Predictions,
    PredictionsHeader, PredictionsWorld, WorldOnly,
};
use bench::ids::{DatasetId, Digest};
use bench::jsonl::{BasicHeader, FileWriter, Trailer};
use bench::manifest::{FileDigests, WorldEntry};
use bench::predictions::{Prediction, WorldStatus};
use serde::{Deserialize, Serialize};

use super::GoldenError;
use super::world::WorldExport;

pub const MANIFEST_FILE: &str = "manifest.json";
pub const MESSAGES_FILE: &str = "messages.jsonl";
pub const EXCHANGES_FILE: &str = "exchanges.jsonl";
pub const LABELS_FILE: &str = "labels.jsonl";

/// What an export's files hold, once written.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Written {
    pub files: FileDigests,
    pub worlds: Vec<WorldEntry>,
    pub messages: Trailer,
    pub exchanges: Trailer,
    pub labels: Trailer,
}

/// Writes an export's three JSONL files.
pub struct ExportWriter<W: Write> {
    messages: FileWriter<Messages, W>,
    exchanges: FileWriter<Exchanges, W>,
    labels: FileWriter<Labels, W>,
    worlds: Vec<WorldEntry>,
}

impl ExportWriter<BufWriter<File>> {
    /// The three files in `dir`, created (or truncated).
    pub fn create(dir: &Path, dataset: &DatasetId) -> Result<Self, GoldenError> {
        fs::create_dir_all(dir).map_err(|source| GoldenError::io(dir, source))?;
        let open = |name: &str| {
            let path = dir.join(name);
            File::create(&path)
                .map(BufWriter::new)
                .map_err(|source| GoldenError::io(&path, source))
        };
        Self::new(
            open(MESSAGES_FILE)?,
            open(EXCHANGES_FILE)?,
            open(LABELS_FILE)?,
            dataset,
        )
    }
}

impl ExportWriter<Sink> {
    /// Writes nothing; digests and entries are the same as a written export's.
    pub fn sink(dataset: &DatasetId) -> Result<Self, GoldenError> {
        Self::new(std::io::sink(), std::io::sink(), std::io::sink(), dataset)
    }
}

impl<W: Write> ExportWriter<W> {
    pub fn new(
        messages: W,
        exchanges: W,
        labels: W,
        dataset: &DatasetId,
    ) -> Result<Self, GoldenError> {
        Ok(Self {
            messages: FileWriter::new(messages, &BasicHeader::new::<Messages>(dataset.clone()))?,
            exchanges: FileWriter::new(exchanges, &BasicHeader::new::<Exchanges>(dataset.clone()))?,
            labels: FileWriter::new(labels, &BasicHeader::new::<Labels>(dataset.clone()))?,
            worlds: Vec::new(),
        })
    }

    /// Checks `world` and writes its three sections; returns its checked
    /// inputs, for checking predictions against.
    pub fn world(&mut self, world: &WorldExport) -> Result<WorldInputs, GoldenError> {
        let inputs = world.check()?;
        self.messages.world(&WorldOnly {
            key: world.key.clone(),
        })?;
        for message in &world.messages {
            self.messages.row(&MessageRow::Message(message.clone()))?;
        }
        self.exchanges.world(&world.decl)?;
        for exchange in &world.exchanges {
            self.exchanges
                .row(&ExchangeRow::Exchange(exchange.clone()))?;
        }
        self.labels.world(&LabelsWorld {
            key: world.key.clone(),
            coverage: world.coverage,
        })?;
        for label in &world.labels {
            self.labels.row(label)?;
        }
        self.worlds.push(WorldEntry {
            key: world.key.clone(),
            exchanges: u64::try_from(world.exchanges.len()).unwrap_or(u64::MAX),
            labels: Some(u64::try_from(world.labels.len()).unwrap_or(u64::MAX)),
            notes: world.notes.clone(),
        });
        Ok(inputs)
    }

    pub fn finish(self) -> Result<Written, GoldenError> {
        let (_, messages) = self.messages.finish()?;
        let (_, exchanges) = self.exchanges.finish()?;
        let (_, labels) = self.labels.finish()?;
        Ok(Written {
            files: FileDigests {
                messages: messages.digest,
                exchanges: exchanges.digest,
                labels: Some(labels.digest),
            },
            worlds: self.worlds,
            messages,
            exchanges,
            labels,
        })
    }
}

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
    pub fn create(path: &Path) -> Result<Self, GoldenError> {
        if let Some(parent) = path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
        {
            fs::create_dir_all(parent).map_err(|source| GoldenError::io(parent, source))?;
        }
        let mut spill_path = path.as_os_str().to_owned();
        spill_path.push(".partial");
        let spill_path = PathBuf::from(spill_path);
        let spill = File::create(&spill_path)
            .map(BufWriter::new)
            .map_err(|source| GoldenError::io(&spill_path, source))?;
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
    ) -> Result<(), GoldenError> {
        let key = inputs.decl().key.clone();
        check_predictions(inputs, rows).map_err(|source| GoldenError::Predictions {
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
    ) -> Result<(), GoldenError> {
        self.spill_line(&Spilled::World(PredictionsWorld {
            key,
            status: WorldStatus::Failed { reason },
        }))?;
        self.worlds += 1;
        Ok(())
    }

    fn spill_line(&mut self, line: &Spilled) -> Result<(), GoldenError> {
        serde_json::to_writer(&mut self.spill, line).map_err(GoldenError::Encode)?;
        self.spill
            .write_all(b"\n")
            .map_err(|source| GoldenError::io(&self.spill_path, source))
    }

    /// Writes the framed file under `header` from the spill, then removes
    /// the spill.
    pub fn finish(mut self, header: &PredictionsHeader) -> Result<Trailer, GoldenError> {
        self.spill
            .flush()
            .map_err(|source| GoldenError::io(&self.spill_path, source))?;
        drop(self.spill);
        let out = File::create(&self.path)
            .map(BufWriter::new)
            .map_err(|source| GoldenError::io(&self.path, source))?;
        let mut writer = FileWriter::<Predictions, _>::new(out, header)?;
        let spill = File::open(&self.spill_path)
            .map(BufReader::new)
            .map_err(|source| GoldenError::io(&self.spill_path, source))?;
        for line in spill.lines() {
            let line = line.map_err(|source| GoldenError::io(&self.spill_path, source))?;
            match serde_json::from_str(&line).map_err(GoldenError::Encode)? {
                Spilled::World(world) => writer.world(&world)?,
                Spilled::Row(row) => writer.row(&row)?,
            }
        }
        let (_, trailer) = writer.finish()?;
        fs::remove_file(&self.spill_path)
            .map_err(|source| GoldenError::io(&self.spill_path, source))?;
        Ok(trailer)
    }

    pub fn worlds(&self) -> u64 {
        self.worlds
    }
}

/// The digest a predictions header names for `manifest`.
pub fn manifest_digest(manifest: &bench::manifest::Manifest) -> Result<Digest, GoldenError> {
    manifest.digest().map_err(GoldenError::Encode)
}
