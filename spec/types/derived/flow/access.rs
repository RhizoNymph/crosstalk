//! Accesses: one agent reading or writing one resource.

use serde::{Deserialize, Serialize};

use crate::ids::{AccessId, AgentId, ExchangeId, ResourceId, SpanId};
use crate::observed::message::PartRef;
use crate::support::Timestamp;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct Access {
    pub id: AccessId,
    pub agent: AgentId,
    pub exchange: ExchangeId,
    pub resource: ResourceId,
    pub at: Timestamp,
    pub via: Extraction,
    pub op: AccessOp,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "type",
    content = "data",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum AccessOp {
    /// `call` is the assistant's tool call part. `spans` are what was
    /// written: the originated spans inside its arguments, and, for every
    /// span inside them relayed from the writer's own earlier output
    /// (`Relayed(RelaySource::Span(s))` where `s` is a span of the same
    /// agent), that source span `s` (`flow.access.write-spans-include-self-relay`).
    /// So a retry of a rejected write carries the spans the rejected one
    /// did, and a reader's match on them links to both writes; the rejected
    /// one is then left out by its outcome.
    ///
    /// `outcome` is what became of the write ([`WriteOutcome`]). Every
    /// write is recorded whatever its outcome, a rejected one included, but
    /// only a write that [`WriteOutcome::pairs`] pairs into a `CoAccess`.
    Write {
        call: PartRef,
        spans: Vec<SpanId>,
        outcome: WriteOutcome,
    },
    /// `result` is the tool result part that returned the resource's
    /// content: what was read.
    Read { result: PartRef },
}

/// What became of a write: whether the call's arguments reached the
/// resource.
///
/// The flow consumer holds a write's tool call until its result arrives,
/// usually in the writer's next request (in the same response for a server
/// tool), or until the correlator's settle window closes
/// ([`CorrelationTiming::write_settles_at`]); the extractor then classifies
/// it, per known tool, from the result's `ToolOutcome` and, for a tool it
/// knows, the result's content. A write whose result never arrived is
/// `Unknown`. The access is recorded once, with its final outcome.
///
/// Like [`Extraction`], the outcome is the write's confidence: correlation
/// pairs `Delivered` and `Unknown` writes and weights `Unknown` ones down,
/// as it weights down lower-confidence extractions; it never pairs a
/// `Rejected` one. The spec fixes no numeric weight for either.
///
/// [`CorrelationTiming::write_settles_at`]: crate::derived::flow::timing::CorrelationTiming::write_settles_at
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WriteOutcome {
    /// The tool reported success: the arguments reached the resource.
    Delivered,
    /// The tool refused or failed the call (a flagged error, or a known
    /// tool's rejection in its result text): nothing reached the resource.
    /// Recorded, because an attempted write is itself a signal (attempted
    /// exfiltration, a message refused as too long), but never paired.
    Rejected,
    /// Nothing says whether it reached the resource: the protocol flags no
    /// failure and the tool is not one whose result text the extractor
    /// reads, or no result arrived before the settle window closed. Paired,
    /// at lower confidence than `Delivered`.
    Unknown,
}

impl WriteOutcome {
    /// Whether a write with this outcome can pair with a read into a
    /// `CoAccess`: every outcome but `Rejected`.
    pub fn pairs(self) -> bool {
        match self {
            Self::Delivered | Self::Unknown => true,
            Self::Rejected => false,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AccessKind {
    Write,
    Read,
}

impl AccessOp {
    pub fn kind(&self) -> AccessKind {
        match self {
            Self::Write { .. } => AccessKind::Write,
            Self::Read { .. } => AccessKind::Read,
        }
    }
}

/// How the locator was found. Lower-confidence extractions are kept but
/// weighted down in correlation (`flow.access.extraction-recorded`); a
/// write's [`WriteOutcome`] is the other confidence an access carries.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Extraction {
    /// Pulled from a URL found anywhere in the arguments.
    Scanned,
    /// Parsed out of code (a bash command, a Python snippet).
    Parsed,
    /// Read from a known argument of a known tool schema.
    Structured,
}
