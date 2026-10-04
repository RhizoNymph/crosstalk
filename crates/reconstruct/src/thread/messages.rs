//! Reading request messages from the blob store: their roles for
//! threading, their bodies for evidence, and whether a user message is a
//! compaction's summary turn.
//!
//! Message bodies are content-addressed and never change, so what a body
//! says about itself ([`Facts`]) is cached by hash. A full-history request
//! repeats its whole history, so after the first exchange only the new
//! messages are read. The cache is bounded and starts over when full.

use std::collections::HashMap;
use std::sync::{Mutex, PoisonError};

use crosstalk_spec::ids::MessageHash;
use crosstalk_spec::interfaces::l2_transport::BlobStore;
use crosstalk_spec::observed::message::encoding::decode;
use crosstalk_spec::observed::message::{Message, MessageBody, Role, UserPart};

use crate::error::StorageFailure;

/// The openings harnesses give the summary turn of a compacted history.
pub const DEFAULT_SUMMARY_PREAMBLES: &[&str] = &[
    // Claude Code, after auto or manual compaction.
    "This session is being continued from a previous conversation",
    // Codex's compacted history.
    "Another language model started to solve this problem and produced a summary",
];

/// What a message body says about itself.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Facts {
    pub role: Role,
    /// A user message opening with a known summary preamble.
    pub summary: bool,
    /// A user message whose text mentions a summary: weaker, counted only
    /// when the harness also hinted the request is a compaction.
    pub mentions_summary: bool,
}

/// How many facts the cache keeps before it starts over.
const CACHE_LIMIT: usize = 1 << 17;

/// Reads message bodies through a [`BlobStore`], caching their facts.
pub struct MessageReader<B> {
    blobs: B,
    preambles: Vec<String>,
    facts: Mutex<HashMap<MessageHash, Facts>>,
}

impl<B> std::fmt::Debug for MessageReader<B> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MessageReader")
            .field("preambles", &self.preambles)
            .finish_non_exhaustive()
    }
}

/// The text of a user message's text parts, joined.
fn user_text(body: &MessageBody) -> Option<String> {
    match body {
        MessageBody::User(parts) => Some(
            parts
                .iter()
                .filter_map(|part| match part {
                    UserPart::Text(text) => Some(text.0.as_str()),
                    UserPart::Media(_) | UserPart::Unknown(_) => None,
                })
                .collect::<Vec<_>>()
                .join("\n"),
        ),
        MessageBody::System(_) | MessageBody::Assistant(_) | MessageBody::Tool(_) => None,
    }
}

impl<B: BlobStore + Send + Sync> MessageReader<B> {
    /// A reader over `blobs` recognizing [`DEFAULT_SUMMARY_PREAMBLES`].
    pub fn new(blobs: B) -> Self {
        Self::with_preambles(
            blobs,
            DEFAULT_SUMMARY_PREAMBLES
                .iter()
                .map(|preamble| (*preamble).to_owned())
                .collect(),
        )
    }

    /// A reader recognizing `preambles` as summary turns.
    pub fn with_preambles(blobs: B, preambles: Vec<String>) -> Self {
        Self {
            blobs,
            preambles,
            facts: Mutex::new(HashMap::new()),
        }
    }

    /// The blob store read through.
    pub fn blobs(&self) -> &B {
        &self.blobs
    }

    /// The body stored under `hash`.
    pub async fn body(&self, hash: MessageHash) -> Result<MessageBody, StorageFailure> {
        let bytes = self
            .blobs
            .get(hash)
            .await
            .map_err(|error| StorageFailure::Blobs {
                reason: format!("{error:?}"),
            })?
            .ok_or_else(|| StorageFailure::MissingBody {
                hash: hash.digest().to_hex(),
            })?;
        decode(&bytes).map_err(|error| StorageFailure::Blobs {
            reason: format!("body {} does not decode: {error}", hash.digest().to_hex()),
        })
    }

    /// The message stored under `hash`.
    pub async fn message(&self, hash: MessageHash) -> Result<Message, StorageFailure> {
        Ok(Message {
            hash,
            body: self.body(hash).await?,
        })
    }

    fn facts_of(&self, body: &MessageBody) -> Facts {
        let text = user_text(body);
        let summary = text.as_deref().is_some_and(|text| {
            let text = text.trim_start();
            self.preambles
                .iter()
                .any(|preamble| text.starts_with(preamble.as_str()))
        });
        let mentions_summary = text
            .as_deref()
            .is_some_and(|text| text.to_lowercase().contains("summary"));
        Facts {
            role: body.role(),
            summary,
            mentions_summary,
        }
    }

    fn cached(&self, hash: &MessageHash) -> Option<Facts> {
        // A poisoned lock only means a reader panicked mid-insert; the map
        // holds whole entries either way.
        let facts = self.facts.lock().unwrap_or_else(PoisonError::into_inner);
        facts.get(hash).copied()
    }

    fn remember(&self, hash: MessageHash, fact: Facts) {
        let mut facts = self.facts.lock().unwrap_or_else(PoisonError::into_inner);
        if facts.len() >= CACHE_LIMIT {
            facts.clear();
        }
        facts.insert(hash, fact);
    }

    /// The facts of the message stored under `hash`.
    pub async fn facts(&self, hash: MessageHash) -> Result<Facts, StorageFailure> {
        if let Some(facts) = self.cached(&hash) {
            return Ok(facts);
        }
        let body = self.body(hash).await?;
        let facts = self.facts_of(&body);
        self.remember(hash, facts);
        Ok(facts)
    }
}
