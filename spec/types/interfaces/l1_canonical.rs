//! L1 canonicalization: provider wire format to canonical types.
//!
//! Runs in a spawned task on the proxy node, off the hot path. Writes every
//! message to the blob store before publishing `ExchangeCaptured`, so a
//! consumer that sees the event can always read the messages.
//!
//! Implementations: one normalizer per [`WireProtocol`], handling every
//! [`Dialect`] of it (vLLM's `reasoning` and SGLang's `reasoning_content`
//! normalize to the same `Reasoning` part). Normalization never drops an
//! exchange for content it does not understand: unknown blocks are kept as
//! `Unknown` parts and reported as warnings.

use crate::interfaces::l0_ingress::RawExchange;
use crate::observed::client::Dialect;
use crate::observed::exchange::{Exchange, WireProtocol};
use crate::observed::message::Message;

/// A normalized exchange plus the message bodies it references by hash.
///
/// Invariant: every hash in `exchange` is the hash of one of `messages`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NormalizedExchange {
    pub exchange: Exchange,
    pub messages: Vec<Message>,
    pub warnings: Vec<NormalizeWarning>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NormalizeWarning {
    /// A block type the normalizer does not know, kept as an `Unknown` part.
    UnknownBlock { kind: String },
    /// A tool result whose call id matches no tool call in the history (the
    /// harness compacted or truncated it).
    OrphanToolResult { call_id: String },
}

pub trait Normalizer {
    fn protocol(&self) -> WireProtocol;

    fn handles(&self, dialect: Dialect) -> bool;

    fn normalize(&self, raw: &RawExchange) -> Result<NormalizedExchange, NormalizeError>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NormalizeError {
    /// The request body is not a valid request of this protocol.
    RequestBody { reason: String },
}
