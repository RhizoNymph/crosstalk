//! The golden export: ct-eval's worlds, labels and predictions in the
//! detector-neutral bench format `a2a-bench/1` (`a2a-bench-format`), so the
//! standalone benchmark can prove parity with ct-eval by byte diffs
//! (separation design §7.1).
//!
//! It is a pure serialisation of existing types:
//!
//! ```text
//! World ──▶ world::export ──▶ WorldExport { messages, decl, exchanges, labels, coverage, MessageIndex }
//!             message::convert (spec Message → bench Message, same parts in the same order)
//!             labels::truth    (Expectation → Label, PartRef → (MessageId, part))
//!        ──▶ ExportWriter::world (WorldInputs::new + check_labels, then the three sections)
//! Detection ──▶ predictions::rows (attribution, unattributed, transmission rows)
//!        ──▶ PredictionsWriter::world (check_predictions, spilled)
//! finish ──▶ Manifest (ManifestSpec + written digests) ──▶ predictions header's manifest digest
//! ```
//!
//! What the format cannot express is refused with a [`Gap`], never worked
//! around; what it can express only without some detail (a semantic match's
//! similarity, a failed exchange's failure) is counted in [`Lossy`].

pub mod ids;
pub mod kinds;
pub mod labels;
pub mod manifest;
pub mod message;
pub mod predictions;
pub mod resource;
pub mod run;
pub mod swarm;
pub mod verify;
pub mod world;
pub mod writer;

use std::path::Path;

use a2a_bench_format as bench;
use crosstalk_spec::derived::flow::access::AccessKind;
use crosstalk_spec::ids::{AccessId, ChannelId, ExchangeId, MessageHash};
use serde::Serialize;

pub use manifest::ManifestSpec;
pub use run::{Finished, GoldenRun};
pub use verify::{Verified, verify};
pub use world::{WorldExport, export};
pub use writer::{ExportWriter, PredictionsWriter, Written};

/// The format this export writes.
pub const FORMAT: &str = "a2a-bench/1";

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
    #[error(
        "channel {channel:?} holds {resources} canonical resources; a bench route names exactly one"
    )]
    ChannelResources {
        channel: ChannelId,
        resources: usize,
    },
    #[error("{locator}: a file on a host that is not a repository; a bench `file` has no host")]
    FileOnHost { locator: String },
    #[error("{locator}: an MCP resource; a bench `opaque` has no server and needs a key")]
    McpLocator { locator: String },
    #[error(
        "label {label}: an agent_cluster row; the format writes its `kind` twice (the row's tag and the cluster's kind), so labels.jsonl could not be read back"
    )]
    ClusterRow { label: String },
    #[error(
        "label {label}: it names a place in message {message:?} and no exchange, and no exchange carries that message; a bench location is in an exchange that carries its message"
    )]
    UncarriedLocation { label: String, message: MessageHash },
    #[error(
        "exchange {exchange:?} is tied to detector agents {first:?} and {second:?}; a bench attribution gives an exchange one agent"
    )]
    ExchangeTiedTwice {
        exchange: ExchangeId,
        first: crosstalk_spec::ids::AgentId,
        second: crosstalk_spec::ids::AgentId,
    },
}

/// Details the format has no field for, counted per export.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize)]
pub struct Lossy {
    /// Media parts written with their kind (`image`, `audio`,
    /// `document`) as the media type: the spec records no MIME type.
    pub media_kinds: u64,
    /// Unknown provider blocks, written without their kind.
    pub unknown_blocks: u64,
    /// Failed exchanges, written with their partial response (if any) and
    /// no stop reason: the bench has no failure field.
    pub failed_exchanges: u64,
    /// Semantic matches, written without their similarity.
    pub similarities: u64,
    /// Detector agents the evidence names that hold no exchange, written
    /// as `unattributed`.
    pub unattributed_agents: u64,
    /// Demo-swarm key groups of one agent, which a bench cluster (two or
    /// more agents) cannot hold.
    pub single_agent_key_groups: u64,
    /// Demo-swarm transmissions whose evidence names an exchange outside
    /// the exported world; ct-eval drops their predictions too.
    pub dropped_transmissions: u64,
    /// Demo-swarm detector agents tied to two truth agents
    /// (`detected_agent_conflict`): ct-eval keeps the first, a bench scorer
    /// sees a merge.
    pub agent_conflicts: u64,
}

impl Lossy {
    pub fn add(&mut self, other: Self) {
        self.media_kinds += other.media_kinds;
        self.unknown_blocks += other.unknown_blocks;
        self.failed_exchanges += other.failed_exchanges;
        self.similarities += other.similarities;
        self.unattributed_agents += other.unattributed_agents;
        self.single_agent_key_groups += other.single_agent_key_groups;
        self.dropped_transmissions += other.dropped_transmissions;
        self.agent_conflicts += other.agent_conflicts;
    }
}

#[derive(Debug, thiserror::Error)]
pub enum GoldenError {
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
    #[error("agent {agent} is not of the world it is used in")]
    ForeignAgent { agent: String },
    #[error("label {label}: {source}")]
    Label {
        label: String,
        #[source]
        source: bench::labels::InvalidLabel,
    },
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
    Inputs {
        world: String,
        #[source]
        source: bench::check::InputError,
    },
    #[error("world {world}: {source}")]
    Labels {
        world: String,
        #[source]
        source: bench::check::LabelError,
    },
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
    #[error("encoding: {0}")]
    Encode(#[source] serde_json::Error),
    #[error("verifying: {0}")]
    Verify(verify::Mismatch),
    #[error("the demo swarm: {0}")]
    Swarm(Box<crate::datasets::swarm_truth::SwarmTruthError>),
}

impl From<crate::datasets::swarm_truth::SwarmTruthError> for GoldenError {
    fn from(error: crate::datasets::swarm_truth::SwarmTruthError) -> Self {
        Self::Swarm(Box::new(error))
    }
}

impl GoldenError {
    pub fn io(path: &Path, source: std::io::Error) -> Self {
        Self::Io {
            path: path.display().to_string(),
            source,
        }
    }
}
