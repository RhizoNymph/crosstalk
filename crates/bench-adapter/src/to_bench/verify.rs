//! Reading a written predictions file back through the format's framing
//! (header, worlds, trailer) and checking it against its manifest: the
//! header's manifest digest, and its worlds in the manifest's order.

use std::fs::File;
use std::io::BufReader;
use std::path::Path;

use a2a_bench_format as bench;
use bench::files::Predictions;
use bench::ids::Digest;
use bench::jsonl::{FileKind, FileReader};
use bench::manifest::Manifest;

use super::ToBenchError;
use super::writer::MANIFEST_FILE;

/// Where a predictions file and its manifest disagree.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum Mismatch {
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
}

fn open<K: FileKind>(path: &Path) -> Result<FileReader<K, BufReader<File>>, ToBenchError> {
    let file = File::open(path).map_err(|source| ToBenchError::io(path, source))?;
    FileReader::open(BufReader::new(file)).map_err(|source| ToBenchError::Read {
        file: path.display().to_string(),
        source,
    })
}

/// Reads `manifest.json` in `dir`.
pub fn read_manifest(dir: &Path) -> Result<Manifest, ToBenchError> {
    let path = dir.join(MANIFEST_FILE);
    let text = std::fs::read_to_string(&path).map_err(|source| ToBenchError::io(&path, source))?;
    serde_json::from_str(&text).map_err(ToBenchError::Encode)
}

/// Reads a predictions file back through its framing (header, worlds,
/// trailer) and checks it names `manifest`; returns its row count. Its
/// rows were checked against their worlds when written.
pub fn predictions_file(path: &Path, manifest: &Manifest) -> Result<u64, ToBenchError> {
    let mut reader = open::<Predictions>(path)?;
    let digest = manifest.digest().map_err(ToBenchError::Encode)?;
    if reader.header().manifest_digest != digest {
        return Err(ToBenchError::Verify(Mismatch::ManifestDigest {
            file: path.display().to_string(),
            named: reader.header().manifest_digest,
            actual: digest,
        }));
    }
    let mut rows = 0u64;
    let mut entries = manifest.worlds.iter();
    while let Some(section) = reader.next_world().map_err(|source| ToBenchError::Read {
        file: path.display().to_string(),
        source,
    })? {
        let world = section.world.key.to_string();
        let entry = entries.next().ok_or_else(|| {
            ToBenchError::Verify(Mismatch::World {
                world: world.clone(),
                file: MANIFEST_FILE.to_owned(),
                found: None,
            })
        })?;
        if entry.key != section.world.key {
            return Err(ToBenchError::Verify(Mismatch::World {
                world,
                file: MANIFEST_FILE.to_owned(),
                found: Some(entry.key.to_string()),
            }));
        }
        rows += section.rows.len() as u64;
    }
    if entries.next().is_some() {
        return Err(ToBenchError::Verify(Mismatch::ExtraWorlds {
            file: MANIFEST_FILE.to_owned(),
        }));
    }
    Ok(rows)
}
