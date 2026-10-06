//! Reading an export back and checking it with the format's own checks:
//! every file's framing and trailer (`FileReader`), each world's inputs
//! (`WorldInputs::new`), labels (`check_labels`) and, when given,
//! predictions (`check_predictions`); the manifest's file digests against
//! the trailers, its worlds against the files' worlds, and a predictions
//! header's manifest digest against the manifest.

use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;

use a2a_bench_format as bench;
use bench::check::{WorldInputs, check_labels, check_predictions};
use bench::files::{ExchangeRow, Exchanges, Labels, MessageRow, Messages, Predictions};
use bench::ids::Digest;
use bench::jsonl::{FileKind, FileReader, Trailer};
use bench::manifest::Manifest;

use super::GoldenError;
use super::writer::{EXCHANGES_FILE, LABELS_FILE, MANIFEST_FILE, MESSAGES_FILE};

/// Where an export's files, its manifest or a predictions file disagree.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum Mismatch {
    #[error("{file} does not end with a trailer line")]
    NoTrailer { file: String },
    #[error("{file}: the manifest names digest {stated:?}, the file's trailer {actual}")]
    FileDigest {
        file: String,
        stated: Option<Digest>,
        actual: Digest,
    },
    #[error("{file} names manifest {named}, the export's is {actual}")]
    ManifestDigest {
        file: String,
        named: Digest,
        actual: Digest,
    },
    #[error("world {world}: {file} holds {found:?} in its place")]
    World {
        world: String,
        file: String,
        found: Option<String>,
    },
    #[error("{file} holds worlds past the export's last")]
    ExtraWorlds { file: String },
    #[error("a predictions file needs a detector for its header")]
    NoDetector,
}

/// What a verified export holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Verified {
    pub worlds: u64,
    pub messages: u64,
    pub exchanges: u64,
    pub labels: u64,
    /// Prediction rows, when a predictions file was checked.
    pub predictions: u64,
}

fn open<K: FileKind>(path: &Path) -> Result<FileReader<K, BufReader<File>>, GoldenError> {
    let file = File::open(path).map_err(|source| GoldenError::io(path, source))?;
    FileReader::open(BufReader::new(file)).map_err(|source| GoldenError::Read {
        file: path.display().to_string(),
        source,
    })
}

/// The trailer on a framed file's last line.
fn trailer(path: &Path) -> Result<Trailer, GoldenError> {
    let file = File::open(path).map_err(|source| GoldenError::io(path, source))?;
    let mut last = None;
    for line in BufReader::new(file).lines() {
        last = Some(line.map_err(|source| GoldenError::io(path, source))?);
    }
    let no_trailer = || {
        GoldenError::Verify(Mismatch::NoTrailer {
            file: path.display().to_string(),
        })
    };
    let last = last.ok_or_else(no_trailer)?;
    let mut value: serde_json::Value = serde_json::from_str(&last).map_err(GoldenError::Encode)?;
    let kind = value
        .as_object_mut()
        .and_then(|object| object.remove("kind"));
    if kind.as_ref().and_then(serde_json::Value::as_str) != Some("trailer") {
        return Err(no_trailer());
    }
    serde_json::from_value(value).map_err(GoldenError::Encode)
}

/// Reads `manifest.json` in `dir`.
pub fn read_manifest(dir: &Path) -> Result<Manifest, GoldenError> {
    let path = dir.join(MANIFEST_FILE);
    let text = std::fs::read_to_string(&path).map_err(|source| GoldenError::io(&path, source))?;
    serde_json::from_str(&text).map_err(GoldenError::Encode)
}

/// Verifies the export in `dir` and, when given, the predictions file
/// `predictions` made on it (module docs).
pub fn verify(dir: &Path, predictions: Option<&Path>) -> Result<Verified, GoldenError> {
    let manifest = read_manifest(dir)?;
    let paths = [
        dir.join(MESSAGES_FILE),
        dir.join(EXCHANGES_FILE),
        dir.join(LABELS_FILE),
    ];
    let [messages_path, exchanges_path, labels_path] = &paths;
    let digests = [
        trailer(messages_path)?.digest,
        trailer(exchanges_path)?.digest,
        trailer(labels_path)?.digest,
    ];
    let stated = [
        Some(manifest.files.messages),
        Some(manifest.files.exchanges),
        manifest.files.labels,
    ];
    for (path, (actual, stated)) in paths.iter().zip(digests.iter().zip(stated.iter())) {
        if Some(*actual) != *stated {
            return Err(GoldenError::Verify(Mismatch::FileDigest {
                file: path.display().to_string(),
                stated: *stated,
                actual: *actual,
            }));
        }
    }
    let mut messages = open::<Messages>(messages_path)?;
    let mut exchanges = open::<Exchanges>(exchanges_path)?;
    let mut labels = open::<Labels>(labels_path)?;
    let mut predicted = match predictions {
        Some(path) => {
            let reader = open::<Predictions>(path)?;
            let digest = manifest.digest().map_err(GoldenError::Encode)?;
            if reader.header().manifest_digest != digest {
                return Err(GoldenError::Verify(Mismatch::ManifestDigest {
                    file: path.display().to_string(),
                    named: reader.header().manifest_digest,
                    actual: digest,
                }));
            }
            Some((path, reader))
        }
        None => None,
    };
    let read = |file: &Path| {
        let file = file.display().to_string();
        move |source| GoldenError::Read { file, source }
    };
    let mut verified = Verified::default();
    let mut entries = manifest.worlds.iter();
    while let Some(message_section) = messages.next_world().map_err(read(messages_path))? {
        let world = message_section.world.key.clone();
        let mismatch = |file: &str, found: Option<&str>| {
            GoldenError::Verify(Mismatch::World {
                world: world.to_string(),
                file: file.to_owned(),
                found: found.map(str::to_owned),
            })
        };
        let exchange_section = exchanges
            .next_world()
            .map_err(read(exchanges_path))?
            .ok_or_else(|| mismatch(EXCHANGES_FILE, None))?;
        let label_section = labels
            .next_world()
            .map_err(read(labels_path))?
            .ok_or_else(|| mismatch(LABELS_FILE, None))?;
        if label_section.world.key != world {
            return Err(mismatch(
                LABELS_FILE,
                Some(label_section.world.key.as_str()),
            ));
        }
        let entry = entries
            .next()
            .ok_or_else(|| mismatch(MANIFEST_FILE, None))?;
        if entry.key != world
            || entry.exchanges != u64::try_from(exchange_section.rows.len()).unwrap_or(u64::MAX)
        {
            return Err(mismatch(MANIFEST_FILE, Some(entry.key.as_str())));
        }
        verified.worlds += 1;
        verified.messages += message_section.rows.len() as u64;
        verified.exchanges += exchange_section.rows.len() as u64;
        verified.labels += label_section.rows.len() as u64;
        let inputs = WorldInputs::new(
            &world,
            message_section
                .rows
                .into_iter()
                .map(|MessageRow::Message(message)| message)
                .collect(),
            exchange_section.world,
            exchange_section
                .rows
                .into_iter()
                .map(|ExchangeRow::Exchange(exchange)| exchange)
                .collect(),
        )
        .map_err(|source| GoldenError::Inputs {
            world: world.to_string(),
            source,
        })?;
        check_labels(&inputs, &label_section.rows).map_err(|source| GoldenError::Labels {
            world: world.to_string(),
            source,
        })?;
        if let Some((path, reader)) = predicted.as_mut() {
            let section = reader
                .next_world()
                .map_err(read(path))?
                .ok_or_else(|| mismatch("predictions", None))?;
            if section.world.key != world {
                return Err(mismatch("predictions", Some(section.world.key.as_str())));
            }
            check_predictions(&inputs, &section.rows).map_err(|source| {
                GoldenError::Predictions {
                    world: world.to_string(),
                    source,
                }
            })?;
            verified.predictions += section.rows.len() as u64;
        }
    }
    let extra = |file: &Path| {
        GoldenError::Verify(Mismatch::ExtraWorlds {
            file: file.display().to_string(),
        })
    };
    if exchanges
        .next_world()
        .map_err(read(exchanges_path))?
        .is_some()
    {
        return Err(extra(exchanges_path));
    }
    if labels.next_world().map_err(read(labels_path))?.is_some() {
        return Err(extra(labels_path));
    }
    if entries.next().is_some() {
        return Err(extra(&dir.join(MANIFEST_FILE)));
    }
    if let Some((path, reader)) = predicted.as_mut()
        && reader.next_world().map_err(read(path))?.is_some()
    {
        return Err(extra(path));
    }
    Ok(verified)
}

/// Reads a predictions file back through its framing (header, worlds,
/// trailer) and checks it names `manifest`; returns its row count. Its
/// rows were checked against their worlds when written.
pub fn predictions_file(path: &Path, manifest: &Manifest) -> Result<u64, GoldenError> {
    let mut reader = open::<Predictions>(path)?;
    let digest = manifest.digest().map_err(GoldenError::Encode)?;
    if reader.header().manifest_digest != digest {
        return Err(GoldenError::Verify(Mismatch::ManifestDigest {
            file: path.display().to_string(),
            named: reader.header().manifest_digest,
            actual: digest,
        }));
    }
    let mut rows = 0u64;
    let mut entries = manifest.worlds.iter();
    while let Some(section) = reader.next_world().map_err(|source| GoldenError::Read {
        file: path.display().to_string(),
        source,
    })? {
        let world = section.world.key.to_string();
        let entry = entries.next().ok_or_else(|| {
            GoldenError::Verify(Mismatch::World {
                world: world.clone(),
                file: MANIFEST_FILE.to_owned(),
                found: None,
            })
        })?;
        if entry.key != section.world.key {
            return Err(GoldenError::Verify(Mismatch::World {
                world,
                file: MANIFEST_FILE.to_owned(),
                found: Some(entry.key.to_string()),
            }));
        }
        rows += section.rows.len() as u64;
    }
    if entries.next().is_some() {
        return Err(GoldenError::Verify(Mismatch::ExtraWorlds {
            file: MANIFEST_FILE.to_owned(),
        }));
    }
    Ok(rows)
}
