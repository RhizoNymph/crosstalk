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
    /// `call` is the assistant's tool call part. `spans` are the originated
    /// spans inside its arguments: what was written.
    Write { call: PartRef, spans: Vec<SpanId> },
    /// `result` is the tool result part that returned the resource's
    /// content: what was read.
    Read { result: PartRef },
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
/// weighted down in correlation.
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
