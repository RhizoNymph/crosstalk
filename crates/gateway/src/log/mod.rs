//! The exchange log: **a P3 stopgap** for persisting captured exchanges.
//!
//! The spec has no exchange store or exchange log trait (a spec gap,
//! recorded in `docs/features/gateway.md`), so single-node mode persists
//! each `ExchangeCaptured` envelope, as its wire JSON, to an append-only
//! file under the data directory: one envelope per line
//! (`<data_dir>/exchange-log.jsonl`). A bus consumer ([`consumer`]) writes
//! it; the message bodies the exchanges name are in the blob store. When
//! the spec grows an exchange store this module goes away.
//!
//! - Appends are durable before they return: the line is written and the
//!   file's data synced, so a consumer acks only what is on disk.
//! - Envelope ids already in the log are skipped, so a redelivery (the bus
//!   is at least once) never writes a second line.
//! - A crash mid-append can leave a last line without its newline. Opening
//!   the log for writing truncates it back to the last complete line;
//!   reading reports it as a torn tail and ignores it.
//!
//! The file is never rewritten otherwise: entries are only appended.

pub mod consumer;

use std::collections::HashSet;
use std::path::{Path, PathBuf};

use crosstalk_spec::events::Envelope;
use crosstalk_spec::ids::EventId;
use tokio::io::AsyncWriteExt;

/// Why the log could not be opened, read or appended to.
#[derive(Debug, thiserror::Error)]
pub enum LogError {
    #[error("{action} {path}: {source}")]
    Io {
        action: &'static str,
        path: PathBuf,
        source: std::io::Error,
    },
    /// A complete line that is not an encoded envelope. Carries the line
    /// number and the decoder's position, never the line.
    #[error("{path} line {line} is not an envelope: {reason}")]
    Corrupt {
        path: PathBuf,
        line: usize,
        reason: String,
    },
    /// An envelope that would not encode (never expected: every spec type
    /// encodes).
    #[error("encoding an envelope: {reason}")]
    Encode { reason: String },
}

/// What an append did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Appended {
    Written,
    /// The envelope's id was already in the log; nothing was written.
    Duplicate,
}

/// The log open for appending.
#[derive(Debug)]
pub struct ExchangeLog {
    path: PathBuf,
    file: tokio::fs::File,
    ids: HashSet<EventId>,
}

/// The log as read.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct LogContents {
    /// Every complete entry, in append order.
    pub entries: Vec<Envelope>,
    /// Bytes after the last newline (a crash mid-append), ignored.
    pub torn_tail: usize,
}

impl ExchangeLog {
    /// Open the log at `path` for appending, creating it (and its
    /// directory) when missing, and truncating a torn last line.
    pub async fn open(path: &Path) -> Result<Self, LogError> {
        let io = |action: &'static str| {
            let path = path.to_owned();
            move |source| LogError::Io {
                action,
                path,
                source,
            }
        };
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .map_err(io("creating the directory of"))?;
        }
        let file = tokio::fs::OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .open(path)
            .await
            .map_err(io("opening"))?;
        let bytes = tokio::fs::read(path).await.map_err(io("reading"))?;
        let contents = parse(path, &bytes)?;
        if contents.torn_tail > 0 {
            let keep = bytes.len() - contents.torn_tail;
            tracing::warn!(
                path = %path.display(),
                torn_bytes = contents.torn_tail,
                "exchange log ended mid-entry; truncating the torn tail"
            );
            file.set_len(u64::try_from(keep).unwrap_or(u64::MAX))
                .await
                .map_err(io("truncating"))?;
            file.sync_all().await.map_err(io("syncing"))?;
        }
        let ids = contents.entries.iter().map(|entry| entry.id).collect();
        tracing::info!(
            path = %path.display(),
            entries = contents.entries.len(),
            "exchange log opened"
        );
        Ok(Self {
            path: path.to_owned(),
            file,
            ids,
        })
    }

    /// The log file.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// How many distinct envelopes the log holds.
    pub fn len(&self) -> usize {
        self.ids.len()
    }

    pub fn is_empty(&self) -> bool {
        self.ids.is_empty()
    }

    /// Append `envelope` and sync it to disk, unless its id is already in
    /// the log.
    pub async fn append(&mut self, envelope: &Envelope) -> Result<Appended, LogError> {
        if self.ids.contains(&envelope.id) {
            return Ok(Appended::Duplicate);
        }
        let mut line = serde_json::to_vec(envelope).map_err(|error| LogError::Encode {
            reason: error.to_string(),
        })?;
        line.push(b'\n');
        let path = &self.path;
        let io = |action: &'static str| {
            move |source| LogError::Io {
                action,
                path: path.clone(),
                source,
            }
        };
        self.file
            .write_all(&line)
            .await
            .map_err(io("appending to"))?;
        self.file.flush().await.map_err(io("flushing"))?;
        self.file.sync_data().await.map_err(io("syncing"))?;
        self.ids.insert(envelope.id);
        Ok(Appended::Written)
    }

    /// Flush and sync everything written.
    pub async fn close(mut self) -> Result<(), LogError> {
        let path = &self.path;
        let io = |action: &'static str| {
            move |source| LogError::Io {
                action,
                path: path.clone(),
                source,
            }
        };
        self.file.flush().await.map_err(io("flushing"))?;
        self.file.sync_all().await.map_err(io("syncing"))?;
        tracing::info!(path = %self.path.display(), entries = self.ids.len(), "exchange log closed");
        Ok(())
    }
}

/// Read the log at `path`. A missing file is an empty log.
pub async fn read(path: &Path) -> Result<LogContents, LogError> {
    match tokio::fs::read(path).await {
        Ok(bytes) => parse(path, &bytes),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(LogContents::default()),
        Err(source) => Err(LogError::Io {
            action: "reading",
            path: path.to_owned(),
            source,
        }),
    }
}

fn parse(path: &Path, bytes: &[u8]) -> Result<LogContents, LogError> {
    let complete = bytes
        .iter()
        .rposition(|byte| *byte == b'\n')
        .map_or(0, |last| last + 1);
    let mut entries = Vec::new();
    for (index, line) in bytes[..complete].split(|byte| *byte == b'\n').enumerate() {
        if line.is_empty() {
            continue;
        }
        let envelope = serde_json::from_slice(line).map_err(|error| LogError::Corrupt {
            path: path.to_owned(),
            line: index + 1,
            reason: format!("{:?} at column {}", error.classify(), error.column()),
        })?;
        entries.push(envelope);
    }
    Ok(LogContents {
        entries,
        torn_tail: bytes.len() - complete,
    })
}

#[cfg(test)]
mod tests;
