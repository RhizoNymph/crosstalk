//! The gateway's conversation reads a bench run needs beside its export:
//! where each exchange sits (`POST /query/exchange-turns`: its agent,
//! canonical at the read, its conversation and turn) and where each
//! matched span sits (`POST /query/span-points`: its author, exchange and
//! location). They place every exchange under the detector's agent and
//! give content matches their origin, which the export and the evidence
//! alone cannot.
//!
//! Saved beside the export as the answers came, merged over batches of
//! [`IdBatch::MAX`] ids: [`EXCHANGE_TURNS_FILE`] and [`SPAN_POINTS_FILE`],
//! each one JSON object keyed by id.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use crosstalk_spec::batch::IdBatch;
use crosstalk_spec::ids::{ExchangeId, SpanId};
use crosstalk_spec::interfaces::l8_surface::conversation::{ExchangePlacement, SpanPoint};
use crosstalk_spec::interfaces::l8_surface::evidence::TransmissionEvidence;
use serde::{Deserialize, Serialize};

/// `POST /query/exchange-turns`'s answers, merged.
pub const EXCHANGE_TURNS_FILE: &str = "exchange-turns.json";
/// `POST /query/span-points`'s answers, merged.
pub const SPAN_POINTS_FILE: &str = "span-points.json";

/// Both reads' answers.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct Queried {
    pub turns: BTreeMap<ExchangeId, ExchangePlacement>,
    pub spans: BTreeMap<SpanId, SpanPoint>,
}

#[derive(Debug, thiserror::Error)]
pub enum QueriedError {
    #[error("{path}: {source}")]
    Io {
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error("{path} is not the read's answer: {source}")]
    Decode {
        path: String,
        #[source]
        source: serde_json::Error,
    },
    #[error("encoding {path}: {source}")]
    Encode {
        path: String,
        #[source]
        source: serde_json::Error,
    },
    #[error("only one of {EXCHANGE_TURNS_FILE} and {SPAN_POINTS_FILE} is in {dir}")]
    Half { dir: String },
}

/// `ids` in batches the reads accept.
pub fn batches<T: Ord + Copy>(ids: &BTreeSet<T>) -> Vec<IdBatch<T>> {
    let all: Vec<T> = ids.iter().copied().collect();
    all.chunks(IdBatch::<T>::MAX)
        .filter_map(|chunk| IdBatch::new(chunk.iter().copied()).ok())
        .collect()
}

/// Every span a content match of `evidence` names as its origin.
pub fn origin_spans(evidence: &[TransmissionEvidence]) -> BTreeSet<SpanId> {
    let mut out = BTreeSet::new();
    for item in evidence {
        if let Some(confirmed) = item.transmission().state.confirmed() {
            for content in confirmed.content().iter() {
                out.insert(content.origin());
            }
        }
    }
    out
}

fn read_json<T: serde::de::DeserializeOwned>(path: &Path) -> Result<Option<T>, QueriedError> {
    let text = match std::fs::read_to_string(path) {
        Ok(text) => text,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(source) => {
            return Err(QueriedError::Io {
                path: path.display().to_string(),
                source,
            });
        }
    };
    serde_json::from_str(&text)
        .map(Some)
        .map_err(|source| QueriedError::Decode {
            path: path.display().to_string(),
            source,
        })
}

fn write_json(path: &Path, value: &impl Serialize) -> Result<(), QueriedError> {
    let text = serde_json::to_string_pretty(value).map_err(|source| QueriedError::Encode {
        path: path.display().to_string(),
        source,
    })? + "\n";
    std::fs::write(path, text).map_err(|source| QueriedError::Io {
        path: path.display().to_string(),
        source,
    })
}

impl Queried {
    /// The saved answers in `dir`; `None` when neither file is there.
    pub fn read(dir: &Path) -> Result<Option<Self>, QueriedError> {
        let turns = read_json(&dir.join(EXCHANGE_TURNS_FILE))?;
        let spans = read_json(&dir.join(SPAN_POINTS_FILE))?;
        match (turns, spans) {
            (Some(turns), Some(spans)) => Ok(Some(Self { turns, spans })),
            (None, None) => Ok(None),
            _ => Err(QueriedError::Half {
                dir: dir.display().to_string(),
            }),
        }
    }

    /// Saves both files in `dir`.
    pub fn write(&self, dir: &Path) -> Result<(), QueriedError> {
        write_json(&dir.join(EXCHANGE_TURNS_FILE), &self.turns)?;
        write_json(&dir.join(SPAN_POINTS_FILE), &self.spans)
    }
}
