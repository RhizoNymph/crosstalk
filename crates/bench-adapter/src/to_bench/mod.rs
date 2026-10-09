//! Spec values as bench rows (`a2a-bench/1`, `a2a-bench-format`): the
//! direction opposite to [`crate::convert`].
//!
//! ```text
//! spec Exchange + its Messages ──▶ world::WorldBuilder ──▶ WorldExport { messages, decl, exchanges, MessageIndex }
//!                                    message::convert (spec Message → bench Message, same parts in the same order)
//! detection (transmissions, attribution) ──▶ predictions::rows (attribution, unattributed, transmission rows)
//!                                    ──▶ writer::PredictionsWriter::world (check_predictions, spilled)
//! finish ──▶ header (detector, Manifest::digest) ──▶ verify::predictions_file (read back)
//! ```
//!
//! `ct-bench-detect` writes every predictions file through here; only
//! `from-export` builds worlds (the gateway's logged exchanges and blobs).
//! What the format cannot express is refused with a [`Gap`], never worked
//! around; what it holds only without some detail (a semantic match's
//! similarity) is counted in [`Lossy`].

pub mod ids;
pub mod kinds;
pub mod manifest;
pub mod message;
pub mod predictions;
pub mod resource;
pub mod verify;
pub mod world;
pub mod writer;

use std::path::Path;

use a2a_bench_format as bench;
use crosstalk_spec::derived::flow::access::AccessKind;
use crosstalk_spec::ids::{AccessId, ExchangeId, MessageHash};
use serde::Serialize;

pub use world::WorldExport;
pub use writer::PredictionsWriter;

/// A construct the format cannot express. The export stops on it.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum Gap {
    #[error(
        "exchange {exchange:?} carries only the input added since a previous response (Continuation::Increment); a bench request is the whole ordered history"
    )]
    IncrementalRequest { exchange: ExchangeId },
    #[error(
        "exchange {exchange:?} has no credential; a bench client needs a credential fingerprint"
    )]
    NoCredential { exchange: ExchangeId },
}

/// Details the format has no field for, counted per run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize)]
pub struct Lossy {
    /// Unknown provider blocks, written without their kind.
    pub unknown_blocks: u64,
    /// Semantic matches, written without their similarity.
    pub similarities: u64,
    /// Detector agents the evidence names that hold no exchange, written
    /// as `unattributed`.
    pub unattributed_agents: u64,
    /// `from-export` transmissions whose evidence names an exchange
    /// outside the captured world, dropped.
    pub dropped_transmissions: u64,
}

impl Lossy {
    pub fn add(&mut self, other: Self) {
        self.unknown_blocks += other.unknown_blocks;
        self.similarities += other.similarities;
        self.unattributed_agents += other.unattributed_agents;
        self.dropped_transmissions += other.dropped_transmissions;
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ToBenchError {
    #[error("the format cannot express this: {0}")]
    Unexpressible(Gap),
    #[error("a key: {0}")]
    Key(bench::ids::InvalidKey),
    #[error("message {hash:?}: {source}")]
    Message {
        hash: MessageHash,
        #[source]
        source: bench::message::InvalidMessage,
    },
    #[error(
        "message {hash:?} part {part}: the spec's canonical arguments are not the bench's canonical JSON: {source}"
    )]
    Arguments {
        hash: MessageHash,
        part: usize,
        #[source]
        source: bench::json::CanonicalJsonError,
    },
    #[error("exchange {exchange:?} names message {hash:?}, which its source does not hold")]
    MissingBody {
        exchange: ExchangeId,
        hash: MessageHash,
    },
    #[error("a location names exchange {0:?}, which the world does not export")]
    UnknownExchange(ExchangeId),
    #[error("a location names message {0:?}, which no exported exchange carries")]
    UnknownMessage(MessageHash),
    #[error("a location's range: {0}")]
    Range(bench::location::EmptyRange),
    #[error("transmission {transmission}: {source}")]
    Transmission {
        transmission: String,
        #[source]
        source: bench::predictions::InvalidTransmission,
    },
    #[error("access {0:?} is not in the detection's evidence")]
    UnknownAccess(AccessId),
    #[error("access {access:?} of a co-access record is not a {expected:?}")]
    WrongAccess {
        access: AccessId,
        expected: AccessKind,
    },
    #[error("access {access:?} names a part its exchange does not hold")]
    UnlocatedAccess { access: AccessId },
    #[error("world {world}: {source}")]
    Predictions {
        world: String,
        #[source]
        source: bench::check::PredictionError,
    },
    #[error("writing: {0}")]
    Write(#[from] bench::jsonl::WriteError),
    #[error("reading {file}: {source}")]
    Read {
        file: String,
        #[source]
        source: bench::jsonl::ReadError,
    },
    #[error("{path}: {source}")]
    Io {
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error("the source digest: {0}")]
    Source(bench::source::SourceDigestError),
    #[error("encoding: {0}")]
    Encode(#[source] serde_json::Error),
    #[error("verifying: {0}")]
    Verify(verify::Mismatch),
}

impl ToBenchError {
    pub fn io(path: &Path, source: std::io::Error) -> Self {
        Self::Io {
            path: path.display().to_string(),
            source,
        }
    }
}
