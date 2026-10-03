//! L1 canonicalization: provider wire format to canonical types.
//!
//! Runs in a spawned task on the proxy node, off the hot path. Writes every
//! message to the blob store before publishing `ExchangeCaptured`, so a
//! consumer that sees the event can always read the messages.
//!
//! Implementations: one normalizer per provider, paired with its
//! [`crate::interfaces::l0_ingress::ProviderAdapter`].

use crate::interfaces::l0_ingress::RawExchange;
use crate::observed::exchange::{Exchange, Provider};
use crate::observed::message::Message;

/// A normalized exchange plus the message bodies it references by hash.
///
/// Invariant: every hash in `exchange` is the hash of one of `messages`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NormalizedExchange {
    pub exchange: Exchange,
    pub messages: Vec<Message>,
}

pub trait Normalizer {
    fn provider(&self) -> Provider;

    fn normalize(&self, raw: &RawExchange) -> Result<NormalizedExchange, NormalizeError>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NormalizeError {
    RequestBody {
        reason: String,
    },
    ResponseBody {
        reason: String,
    },
    /// A content block type the normalizer does not know. Recorded so the
    /// exchange can be re-normalized once support is added.
    UnknownBlock {
        kind: String,
    },
    /// A tool result whose call id matches no tool call in the history.
    OrphanToolResult {
        call_id: String,
    },
}
