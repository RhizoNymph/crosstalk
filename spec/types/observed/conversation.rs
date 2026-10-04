//! Conversations reconstructed from exchanges.
//!
//! Each request carries the full message history, so a conversation is found
//! by matching a request's message hashes against the known prefixes.
//! Harnesses fork conversations (sub-agents, retries) and compact them
//! (summarize and restart), and both break a plain prefix chain.

use crate::ids::{AgentId, ConversationId, MessageHash};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Conversation {
    pub id: ConversationId,
    pub agent: AgentId,
    /// The longest history seen so far, in order.
    pub messages: Vec<MessageHash>,
    pub origin: ConversationOrigin,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConversationOrigin {
    Root,
    /// Shares the first `shared_prefix` messages with `parent`, then
    /// diverges. The shared prefix always includes at least one assistant
    /// message, so conversations that only share a system prompt and first
    /// user turn are separate roots. A retry (a request that is a prefix of
    /// the parent's history) forks with `shared_prefix` equal to the
    /// request's length.
    Fork {
        parent: ConversationId,
        shared_prefix: u32,
    },
    /// The harness summarized `predecessor` and started over with the
    /// summary. Messages the new conversation carries over from the
    /// predecessor (same hash) are not new inputs.
    Compaction {
        predecessor: ConversationId,
    },
}
