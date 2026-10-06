//! A detector's input directory: `manifest.json` (the input view),
//! `messages.jsonl` and `exchanges.jsonl`, read one world at a time.
//!
//! Both files are read with the format's `FileReader` in lockstep. Their
//! headers must name the manifest's dataset, and the n-th world of each
//! must be the manifest's n-th world with as many exchanges as the manifest
//! says; anything else is a run failure ([`InputError`]), since the files
//! are not one export. A world whose rows do not pass `WorldInputs::new`
//! is a world failure (`read: …`) and the run goes on. Once the last world
//! is read, both trailers' digests must be the manifest's.

use std::fs::File;
use std::io::BufReader;
use std::path::{Path, PathBuf};

use a2a_bench_format as bench;
use bench::check::WorldInputs;
use bench::files::{ExchangeRow, Exchanges, MessageRow, Messages};
use bench::ids::{Digest, WorldKey};
use bench::jsonl::{FileReader, HeaderFields, ReadError};
use bench::manifest::Manifest;

use super::{FailureCode, WorldFailure};
use crate::golden::writer::{EXCHANGES_FILE, MANIFEST_FILE, MESSAGES_FILE};

#[derive(Debug, thiserror::Error)]
pub enum InputError {
    #[error("{path}: {source}")]
    Io {
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error("{path} is not a manifest: {source}")]
    Manifest {
        path: String,
        #[source]
        source: serde_json::Error,
    },
    #[error("reading {file}: {source}")]
    Read {
        file: &'static str,
        #[source]
        source: ReadError,
    },
    #[error("{file} is of dataset {found}, the manifest's is {expected}")]
    Dataset {
        file: &'static str,
        expected: String,
        found: String,
    },
    #[error(
        "world {position}: the manifest says {manifest}, messages.jsonl {messages}, exchanges.jsonl {exchanges}"
    )]
    WorldOrder {
        position: usize,
        manifest: String,
        messages: String,
        exchanges: String,
    },
    #[error("world {world}: the manifest counts {manifest} exchanges, the file holds {file}")]
    ExchangeCount {
        world: String,
        manifest: u64,
        file: u64,
    },
    #[error("{file} ends before the manifest's world {world}")]
    MissingWorld { file: &'static str, world: String },
    #[error("{file} holds a world the manifest does not list")]
    ExtraWorld { file: &'static str },
    #[error("{file}: the manifest names digest {manifest}, the trailer {trailer}")]
    FileDigest {
        file: &'static str,
        manifest: Digest,
        trailer: Digest,
    },
}

/// One world as read.
#[derive(Debug)]
pub enum WorldRead {
    /// Its inputs, checked.
    Ready(Box<WorldInputs>),
    /// Its rows do not check; written `failed`.
    Unreadable {
        key: WorldKey,
        failure: WorldFailure,
    },
}

/// An input directory, open.
pub struct InputDir {
    manifest: Manifest,
    messages: FileReader<Messages, BufReader<File>>,
    exchanges: FileReader<Exchanges, BufReader<File>>,
    position: usize,
}

fn open<K: bench::jsonl::FileKind>(
    path: &Path,
    file: &'static str,
) -> Result<FileReader<K, BufReader<File>>, InputError> {
    let handle = File::open(path).map_err(|source| InputError::Io {
        path: path.display().to_string(),
        source,
    })?;
    FileReader::open(BufReader::new(handle)).map_err(|source| InputError::Read { file, source })
}

/// The manifest in `dir`.
pub fn read_manifest(dir: &Path) -> Result<Manifest, InputError> {
    let path: PathBuf = dir.join(MANIFEST_FILE);
    let text = std::fs::read_to_string(&path).map_err(|source| InputError::Io {
        path: path.display().to_string(),
        source,
    })?;
    serde_json::from_str(&text).map_err(|source| InputError::Manifest {
        path: path.display().to_string(),
        source,
    })
}

impl InputDir {
    pub fn open(dir: &Path) -> Result<Self, InputError> {
        let manifest = read_manifest(dir)?;
        let messages = open::<Messages>(&dir.join(MESSAGES_FILE), MESSAGES_FILE)?;
        let exchanges = open::<Exchanges>(&dir.join(EXCHANGES_FILE), EXCHANGES_FILE)?;
        for (file, dataset) in [
            (MESSAGES_FILE, messages.header().dataset()),
            (EXCHANGES_FILE, exchanges.header().dataset()),
        ] {
            if *dataset != manifest.dataset {
                return Err(InputError::Dataset {
                    file,
                    expected: manifest.dataset.to_string(),
                    found: dataset.to_string(),
                });
            }
        }
        Ok(Self {
            manifest,
            messages,
            exchanges,
            position: 0,
        })
    }

    /// The manifest as read (its digest is what predictions name).
    pub fn manifest(&self) -> &Manifest {
        &self.manifest
    }

    /// The next world, `None` after the last (both trailers checked
    /// against the manifest).
    pub fn next_world(&mut self) -> Result<Option<WorldRead>, InputError> {
        let messages = self
            .messages
            .next_world()
            .map_err(|source| InputError::Read {
                file: MESSAGES_FILE,
                source,
            })?;
        let exchanges = self
            .exchanges
            .next_world()
            .map_err(|source| InputError::Read {
                file: EXCHANGES_FILE,
                source,
            })?;
        let entry = self.manifest.worlds.get(self.position);
        let (messages, exchanges, entry) = match (messages, exchanges, entry) {
            (None, None, None) => {
                self.check_trailers()?;
                return Ok(None);
            }
            (Some(messages), Some(exchanges), Some(entry)) => (messages, exchanges, entry),
            (None, _, Some(entry)) => {
                return Err(InputError::MissingWorld {
                    file: MESSAGES_FILE,
                    world: entry.key.to_string(),
                });
            }
            (_, None, Some(entry)) => {
                return Err(InputError::MissingWorld {
                    file: EXCHANGES_FILE,
                    world: entry.key.to_string(),
                });
            }
            (Some(_), _, None) => {
                return Err(InputError::ExtraWorld {
                    file: MESSAGES_FILE,
                });
            }
            (None, Some(_), None) => {
                return Err(InputError::ExtraWorld {
                    file: EXCHANGES_FILE,
                });
            }
        };
        if messages.world.key != entry.key || exchanges.world.key != entry.key {
            return Err(InputError::WorldOrder {
                position: self.position,
                manifest: entry.key.to_string(),
                messages: messages.world.key.to_string(),
                exchanges: exchanges.world.key.to_string(),
            });
        }
        let count = u64::try_from(exchanges.rows.len()).unwrap_or(u64::MAX);
        if count != entry.exchanges {
            return Err(InputError::ExchangeCount {
                world: entry.key.to_string(),
                manifest: entry.exchanges,
                file: count,
            });
        }
        self.position += 1;
        let key = entry.key.clone();
        let inputs = WorldInputs::new(
            &messages.world.key,
            messages
                .rows
                .into_iter()
                .map(|MessageRow::Message(message)| message)
                .collect(),
            exchanges.world,
            exchanges
                .rows
                .into_iter()
                .map(|ExchangeRow::Exchange(exchange)| exchange)
                .collect(),
        );
        Ok(Some(match inputs {
            Ok(inputs) => WorldRead::Ready(Box::new(inputs)),
            Err(error) => WorldRead::Unreadable {
                key,
                failure: WorldFailure::new(FailureCode::Read, error),
            },
        }))
    }

    fn check_trailers(&self) -> Result<(), InputError> {
        let trailers = [
            (
                MESSAGES_FILE,
                self.messages.trailer(),
                self.manifest.files.messages,
            ),
            (
                EXCHANGES_FILE,
                self.exchanges.trailer(),
                self.manifest.files.exchanges,
            ),
        ];
        for (file, trailer, manifest) in trailers {
            let Some(trailer) = trailer else {
                return Err(InputError::Read {
                    file,
                    source: ReadError::Truncated,
                });
            };
            if trailer.digest != manifest {
                return Err(InputError::FileDigest {
                    file,
                    manifest,
                    trailer: trailer.digest,
                });
            }
        }
        Ok(())
    }
}
