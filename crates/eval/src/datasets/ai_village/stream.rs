//! Streaming the dataset's gzipped JSON Lines tables.
//!
//! The tables are sorted by UUID, not time, so any time window needs a full
//! pass. [`Table::scan`] reads one table line by line through flate2
//! (`MultiGzDecoder`, so concatenated members work) and hands each line to a
//! visitor; nothing is held but the line. [`created_at`] pulls a row's
//! `created_at` out of the raw line without parsing it, so a window can
//! drop most rows before any JSON is decoded. A table may also be stored
//! uncompressed as `<name>.jsonl` (tests use both).

use std::fs::File;
use std::io::{BufRead, BufReader, Read};
use std::path::{Path, PathBuf};

use crosstalk_spec::support::Timestamp;
use flate2::read::MultiGzDecoder;
use serde::de::DeserializeOwned;

use super::time::parse_timestamp;

/// The tables the converter reads (images are never read).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Table {
    Agents,
    ChatRooms,
    ChatMessages,
    Events,
    ComputerUseSessions,
    ComputerUseTurns,
    AgentMemories,
    VillageGoals,
    AgentGoals,
    ClaudeCodeMessages,
}

impl Table {
    /// The table's file stem.
    pub fn name(self) -> &'static str {
        match self {
            Self::Agents => "agents",
            Self::ChatRooms => "chat_rooms",
            Self::ChatMessages => "chat_messages",
            Self::Events => "events",
            Self::ComputerUseSessions => "computer_use_sessions",
            Self::ComputerUseTurns => "computer_use_turns",
            Self::AgentMemories => "agent_memories",
            Self::VillageGoals => "village_goals",
            Self::AgentGoals => "agent_goals",
            Self::ClaudeCodeMessages => "claude_code_messages",
        }
    }

    /// The file name source references cite.
    pub fn file_name(self) -> String {
        format!("{}.jsonl.gz", self.name())
    }

    /// The table's path under `root`: `<name>.jsonl.gz`, or `<name>.jsonl`
    /// when only that exists.
    pub fn path(self, root: &Path) -> PathBuf {
        let gz = root.join(self.file_name());
        if gz.exists() {
            return gz;
        }
        let plain = root.join(format!("{}.jsonl", self.name()));
        if plain.exists() { plain } else { gz }
    }

    /// Calls `visit` with every non-empty line of the table, in file order.
    /// The visitor's error stops the scan.
    pub fn scan<E: From<StreamError>>(
        self,
        root: &Path,
        mut visit: impl FnMut(&str) -> Result<(), E>,
    ) -> Result<u64, E> {
        let path = self.path(root);
        let shown = path.display().to_string();
        let file = File::open(&path).map_err(|source| StreamError::Io {
            path: shown.clone(),
            source,
        })?;
        let reader: Box<dyn Read> = if path.extension().is_some_and(|ext| ext == "gz") {
            Box::new(MultiGzDecoder::new(file))
        } else {
            Box::new(file)
        };
        let mut reader = BufReader::with_capacity(1 << 20, reader);
        let mut line = String::new();
        let mut rows = 0u64;
        loop {
            line.clear();
            let read = reader
                .read_line(&mut line)
                .map_err(|source| StreamError::Io {
                    path: shown.clone(),
                    source,
                })?;
            if read == 0 {
                break;
            }
            let text = line.trim_end();
            if text.is_empty() {
                continue;
            }
            rows += 1;
            visit(text)?;
        }
        Ok(rows)
    }

    /// Every row of the table, decoded. Only for the small tables.
    pub fn load<T: DeserializeOwned>(self, root: &Path) -> Result<Vec<T>, StreamError> {
        let mut out = Vec::new();
        self.scan::<StreamError>(root, |line| {
            out.push(decode(self, line)?);
            Ok(())
        })?;
        Ok(out)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum StreamError {
    #[error("reading {path}: {source}")]
    Io {
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error("a row of {table} is not the expected JSON: {source}")]
    Json {
        table: &'static str,
        #[source]
        source: serde_json::Error,
    },
}

/// One line decoded as `T`.
pub fn decode<T: DeserializeOwned>(table: Table, line: &str) -> Result<T, StreamError> {
    serde_json::from_str(line).map_err(|source| StreamError::Json {
        table: table.name(),
        source,
    })
}

/// The top-level `created_at` of a raw row, without decoding it.
///
/// Every table dumps `created_at` after its nested columns (and
/// `updated_at` after it), so the last `"created_at":"` in the line is the
/// row's own; a quoted key inside a string value is escaped and never
/// matches. `None` when the line has none or it does not parse: then the
/// caller decodes the row and reads it there.
pub fn created_at(line: &str) -> Option<Timestamp> {
    let key = "\"created_at\":\"";
    let at = line.rfind(key)? + key.len();
    let end = line[at..].find('"')? + at;
    parse_timestamp(&line[at..end]).ok()
}

/// The first string value of member `name` in a raw row (a quick look, for
/// filters; the decoded row is authoritative).
pub fn string_field<'a>(line: &'a str, name: &str) -> Option<&'a str> {
    let key = format!("\"{name}\":\"");
    let at = line.find(&key)? + key.len();
    let end = line[at..].find('"')? + at;
    Some(&line[at..end])
}
