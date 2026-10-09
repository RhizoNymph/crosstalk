//! Reading a truth file: the header first, then rows, each with its line
//! number so an error can cite it.

use std::io::BufRead;

use super::schema::{
    Delivery, Header, KeyGroup, Miss, SessionStart, TruthLine, UnattributedRead, VERSION,
};

/// What a delivery line says about the read.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DeliveryKind {
    Transmission,
    SelfRead,
    Reread,
}

/// One row after the header.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Row {
    Session(SessionStart),
    Delivery {
        kind: DeliveryKind,
        row: Box<Delivery>,
    },
    Miss(Miss),
    Unattributed(UnattributedRead),
    Cluster(KeyGroup),
}

/// A row and the line it was on (from 1).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Numbered {
    pub line: usize,
    pub row: Row,
}

/// A whole truth file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TruthFile {
    pub header: Header,
    pub rows: Vec<Numbered>,
}

#[derive(Debug, thiserror::Error)]
pub enum TruthFileError {
    #[error("reading truth line {line}: {source}")]
    Read {
        line: usize,
        #[source]
        source: std::io::Error,
    },
    #[error("truth line {line} is not a v2 truth row: {source}")]
    Decode {
        line: usize,
        #[source]
        source: serde_json::Error,
    },
    #[error("the truth file has no header line")]
    NoHeader,
    #[error("truth line {line} is a row before the header")]
    HeaderNotFirst { line: usize },
    #[error("truth line {line} is a second header")]
    SecondHeader { line: usize },
    #[error("truth version {found} is not supported (only {VERSION})")]
    UnsupportedVersion { found: u32 },
    #[error("truth line {line} is in world {found:?}, the header's is {header:?}")]
    OtherWorld {
        line: usize,
        found: String,
        header: String,
    },
}

/// Reads a truth file. Blank lines are skipped. The header must come first
/// and be version 2; every row must be in the header's world.
pub fn read<R: BufRead>(input: R) -> Result<TruthFile, TruthFileError> {
    let mut header: Option<Header> = None;
    let mut rows = Vec::new();
    for (at, line) in input.lines().enumerate() {
        let line_no = at + 1;
        let text = line.map_err(|source| TruthFileError::Read {
            line: line_no,
            source,
        })?;
        if text.trim().is_empty() {
            continue;
        }
        let parsed: TruthLine =
            serde_json::from_str(&text).map_err(|source| TruthFileError::Decode {
                line: line_no,
                source,
            })?;
        let row = match parsed {
            TruthLine::Header(found) => {
                if header.is_some() {
                    return Err(TruthFileError::SecondHeader { line: line_no });
                }
                if found.version != VERSION {
                    return Err(TruthFileError::UnsupportedVersion {
                        found: found.version,
                    });
                }
                header = Some(found);
                continue;
            }
            TruthLine::Session(row) => Row::Session(row),
            TruthLine::Transmission(row) => Row::Delivery {
                kind: DeliveryKind::Transmission,
                row: Box::new(row),
            },
            TruthLine::SelfRead(row) => Row::Delivery {
                kind: DeliveryKind::SelfRead,
                row: Box::new(row),
            },
            TruthLine::Reread(row) => Row::Delivery {
                kind: DeliveryKind::Reread,
                row: Box::new(row),
            },
            TruthLine::Miss(row) => Row::Miss(row),
            TruthLine::UnattributedRead(row) => Row::Unattributed(row),
            TruthLine::AgentCluster(row) => Row::Cluster(row),
        };
        let Some(head) = &header else {
            return Err(TruthFileError::HeaderNotFirst { line: line_no });
        };
        let world = match &row {
            Row::Session(row) => &row.world,
            Row::Delivery { row, .. } => &row.world,
            Row::Miss(row) => &row.world,
            Row::Unattributed(row) => &row.world,
            Row::Cluster(row) => &row.world,
        };
        if *world != head.world {
            return Err(TruthFileError::OtherWorld {
                line: line_no,
                found: world.clone(),
                header: head.world.clone(),
            });
        }
        rows.push(Numbered { line: line_no, row });
    }
    let header = header.ok_or(TruthFileError::NoHeader)?;
    Ok(TruthFile { header, rows })
}
