//! L1 canonicalization: provider wire format to canonical types.
//!
//! Runs in a spawned task on the proxy node, off the hot path. Writes every
//! message and media blob to the blob store before publishing
//! `ExchangeCaptured`, so a consumer that sees the event can always read
//! them.
//!
//! Implementations: one normalizer per [`WireProtocol`], handling every
//! [`Dialect`] of it (vLLM's `reasoning` and SGLang's `reasoning_content`
//! normalize to the same `Reasoning` part). Normalization never drops an
//! exchange for content it does not understand: unknown blocks are kept as
//! `Unknown` parts and reported as warnings.
//!
//! A [`NormalizedExchange`] crosses only in process (normalizer to capture
//! task); its serde, in the wire contract's conventions, pins the
//! normalizers' goldens.
//!
//! Capture keeps every exchange it announces in the exchange store
//! ([`exchanges::ExchangeStore`], read back through
//! [`exchanges::ExchangeReads`]): the exchange record without its bodies,
//! which stay in the blob store.

pub mod exchanges;

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

use crate::ids::MessageHash;
use crate::interfaces::l0_ingress::RawExchange;
use crate::observed::client::Dialect;
use crate::observed::exchange::{Exchange, ExchangeOutcome, WireProtocol};
use crate::observed::message::{
    AssistantPart, MediaBlob, Message, MessageBody, ToolResult, ToolResultContent, UserPart,
    encoding,
};
use crate::wire::Rejected;

/// A normalized exchange, the message bodies it references by hash and the
/// media bytes those bodies reference by hash: everything L1 writes to the
/// blob store before announcing the exchange.
///
/// Invariants ([`NormalizedExchange::check`], which decoding applies):
/// every message's hash is the BLAKE3 of its body's encoding; `messages`
/// holds each body once, and exactly the bodies `exchange` names; `media`
/// is in ascending hash order, once each, and holds exactly the blobs the
/// messages' `Media` parts name.
///
/// On the wire `{"exchange": .., "messages": [..], "warnings": [..],
/// "media": [..]}`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", try_from = "RawNormalizedExchange")]
pub struct NormalizedExchange {
    pub exchange: Exchange,
    pub messages: Vec<Message>,
    pub warnings: Vec<NormalizeWarning>,
    pub media: Vec<MediaBlob>,
}

/// [`NormalizedExchange`]'s fields, decoded before the check.
#[derive(Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
struct RawNormalizedExchange {
    exchange: Exchange,
    messages: Vec<Message>,
    warnings: Vec<NormalizeWarning>,
    media: Vec<MediaBlob>,
}

impl TryFrom<RawNormalizedExchange> for NormalizedExchange {
    type Error = Rejected<InvalidNormalizedExchange>;

    fn try_from(raw: RawNormalizedExchange) -> Result<Self, Self::Error> {
        let normalized = Self {
            exchange: raw.exchange,
            messages: raw.messages,
            warnings: raw.warnings,
            media: raw.media,
        };
        normalized
            .check()
            .map_err(|error| Rejected::new("normalized exchange", error))?;
        Ok(normalized)
    }
}

/// Why a [`NormalizedExchange`] breaks its invariants.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InvalidNormalizedExchange {
    /// A message's hash is not its body's.
    HashMismatch { hash: MessageHash },
    /// Two messages have one hash.
    DuplicateMessage { hash: MessageHash },
    /// The exchange names a hash no message has.
    UnresolvedMessage { hash: MessageHash },
    /// A message the exchange does not name.
    UnreferencedMessage { hash: MessageHash },
    /// A media blob not after the one before it in hash order.
    MediaOutOfOrder { hash: MessageHash },
    /// A `Media` part names a blob `media` does not hold.
    MissingMedia { hash: MessageHash },
    /// A media blob no `Media` part names.
    UnreferencedMedia { hash: MessageHash },
}

impl NormalizedExchange {
    /// `Ok` when every invariant the type documents holds.
    pub fn check(&self) -> Result<(), InvalidNormalizedExchange> {
        use InvalidNormalizedExchange as Invalid;
        let mut held = BTreeSet::new();
        for message in &self.messages {
            if encoding::hash(&message.body) != message.hash {
                return Err(Invalid::HashMismatch { hash: message.hash });
            }
            if !held.insert(message.hash) {
                return Err(Invalid::DuplicateMessage { hash: message.hash });
            }
        }
        let named: BTreeSet<MessageHash> = self.named_messages().collect();
        if let Some(hash) = named.difference(&held).next() {
            return Err(Invalid::UnresolvedMessage { hash: *hash });
        }
        if let Some(hash) = held.difference(&named).next() {
            return Err(Invalid::UnreferencedMessage { hash: *hash });
        }
        let mut stored = BTreeSet::new();
        for blob in &self.media {
            if stored.last().is_some_and(|last| *last >= blob.hash()) {
                return Err(Invalid::MediaOutOfOrder { hash: blob.hash() });
            }
            stored.insert(blob.hash());
        }
        let mut parts = BTreeSet::new();
        for message in &self.messages {
            media_blobs(&message.body, &mut parts);
        }
        if let Some(hash) = parts.difference(&stored).next() {
            return Err(Invalid::MissingMedia { hash: *hash });
        }
        if let Some(hash) = stored.difference(&parts).next() {
            return Err(Invalid::UnreferencedMedia { hash: *hash });
        }
        Ok(())
    }

    /// Every hash the exchange names: its request, then its response or
    /// partial response.
    fn named_messages(&self) -> impl Iterator<Item = MessageHash> + '_ {
        let response = match &self.exchange.outcome {
            ExchangeOutcome::Completed { response, .. } => Some(*response),
            ExchangeOutcome::Failed {
                partial_response, ..
            } => *partial_response,
        };
        self.exchange.request.iter().copied().chain(response)
    }
}

/// The blobs a body's `Media` parts name, wherever they sit.
fn media_blobs(body: &MessageBody, blobs: &mut BTreeSet<MessageHash>) {
    fn result(result: &ToolResult, blobs: &mut BTreeSet<MessageHash>) {
        for content in &result.content {
            if let ToolResultContent::Media(media) = content {
                blobs.insert(media.blob);
            }
        }
    }
    match body {
        MessageBody::System(_) => {}
        MessageBody::User(parts) => {
            for part in parts {
                if let UserPart::Media(media) = part {
                    blobs.insert(media.blob);
                }
            }
        }
        MessageBody::Assistant(parts) => {
            for part in parts {
                if let AssistantPart::ServerToolResult(server) = part {
                    result(server, blobs);
                }
            }
        }
        MessageBody::Tool(results) => {
            for tool in results.iter() {
                result(tool, blobs);
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "type",
    content = "data",
    rename_all = "snake_case",
    deny_unknown_fields
)]
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
