//! Conversations reconstructed from exchanges.
//!
//! Each request carries the full message history, so a conversation is found
//! by matching a request's message hashes against the known prefixes. System
//! messages, wherever they sit (the top-level prompt first, or a system turn
//! inside the history), are left out of prefix matching: a changed system
//! prompt continues the conversation and is reported as the delta's
//! `new_system`.
//! Matching considers every conversation of the same canonical agent, so a
//! merge does not orphan the merged agent's conversations.
//! Harnesses fork conversations (sub-agents, retries) and compact them
//! (summarize and restart), and both break a plain prefix chain.

use serde::{Deserialize, Serialize};

use crate::ids::{AgentId, ConversationId, MessageHash};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Conversation {
    pub id: ConversationId,
    pub agent: AgentId,
    /// The longest non-system history seen so far, in order. System messages
    /// are not stored here; each delta's `new_system` records them.
    pub messages: Vec<MessageHash>,
    pub origin: ConversationOrigin,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConversationOrigin {
    Root,
    /// Shares the first `shared_prefix` non-system messages with `parent`,
    /// then diverges. (System messages are left out of prefix matching, so
    /// they are left out of this count too.) The shared prefix always
    /// includes at least one assistant message, so conversations that only
    /// share a system prompt and first user turn are separate roots. A retry
    /// (a request that is a prefix of the parent's history) forks with
    /// `shared_prefix` equal to the request's non-system message count.
    Fork {
        parent: ConversationId,
        shared_prefix: u32,
    },
    /// The harness summarized `predecessor` and started over with the
    /// summary. A message is carried over when its hash is in the
    /// predecessor's stored history.
    ///
    /// The stored history (`messages`) is the first request's non-system
    /// messages in request order, carried-over ones included, then that
    /// exchange's output; each later delta's `new_inputs` and then its
    /// `output` follow. It does not start with the predecessor's history:
    /// carried-over messages appear where the first request put them.
    ///
    /// The first delta's `new_inputs` are that first request's non-system
    /// messages minus the carried-over ones, in request order. Later deltas
    /// extend the conversation like any other.
    Compaction {
        predecessor: ConversationId,
    },
}

impl ConversationOrigin {
    /// The origin without its links.
    pub fn kind(&self) -> OriginKind {
        match self {
            Self::Root => OriginKind::Root,
            Self::Fork { .. } => OriginKind::Fork,
            Self::Compaction { .. } => OriginKind::Compaction,
        }
    }

    /// The conversation this one continues: a fork's parent or a
    /// compaction's predecessor; `None` for a root.
    pub fn source(&self) -> Option<ConversationId> {
        match self {
            Self::Root => None,
            Self::Fork { parent, .. } => Some(*parent),
            Self::Compaction { predecessor } => Some(*predecessor),
        }
    }
}

/// A [`ConversationOrigin`] without its links. On the wire, snake_case
/// strings.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OriginKind {
    Root,
    Fork,
    Compaction,
}
