//! L3 reconstruction: who sent an exchange, and which conversation it
//! continues. Consumer group `reconstruct`, triggered by `ExchangeCaptured`.
//!
//! Implementations:
//! - `IdentityResolver`: `ApiKeyResolver`, `HeaderResolver`,
//!   `PromptFingerprintResolver`, and `ChainResolver`, which runs the others
//!   in order and merges their answers.
//! - `Threader`: `PrefixThreader` (message-hash prefix match),
//!   `ResponsesStateThreader` (resolves `Continuation::Increment` exchanges,
//!   from Codex's WebSocket turns, through the stored response chain),
//!   `CompactionThreader` (harness compaction hints and summary heuristics).
//! - `AgentDirectory`: the merge table. Every reader of stored agent ids
//!   resolves them through it.
//!
//! Identity resolution uses the most specific evidence present
//! (`IdentityEvidence::specificity`). Harness ids count only within their
//! `IdentityScope`. Rotating credentials and prompt fingerprints are weak:
//! they attach to an agent but never establish one alone.
//!
//! An increment exchange whose previous response the gateway never saw (it
//! was sent around the proxy) is threaded as `Starts` holding only its
//! increment, so its inputs are still scanned.

use crate::events::ingest::ConversationDelta;
use crate::ids::{AgentId, ConversationId};
use crate::observed::agent::{IdentityEvidence, MergeRequest};
use crate::observed::exchange::{Exchange, ExchangeMeta};
use crate::observed::message::Message;
use crate::support::NonEmpty;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Resolution {
    Known {
        agent: AgentId,
        new_evidence: Vec<IdentityEvidence>,
    },
    New {
        evidence: NonEmpty<IdentityEvidence>,
    },
    /// The evidence points at more than one agent. The chain resolver turns
    /// this into a merge or an operator review.
    Conflict {
        candidates: NonEmpty<AgentId>,
        evidence: NonEmpty<IdentityEvidence>,
    },
}

pub trait AgentDirectory {
    /// The agent `id` resolves to after merges: itself unless merged.
    fn canonical(&self, id: AgentId) -> AgentId;
}

pub trait IdentityResolver {
    /// Apply a merge. Repoints agents already merged into `from`, so no
    /// merge chain is ever longer than one.
    async fn merge(&mut self, request: MergeRequest) -> Result<(), ResolveError>;

    async fn resolve(
        &mut self,
        meta: &ExchangeMeta,
        request: &[Message],
    ) -> Result<Resolution, ResolveError>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ThreadOutcome {
    Starts {
        conversation: ConversationId,
        delta: ConversationDelta,
    },
    Extends {
        conversation: ConversationId,
        delta: ConversationDelta,
    },
    Forks {
        parent: ConversationId,
        shared_prefix: u32,
        conversation: ConversationId,
        delta: ConversationDelta,
    },
    Compacts {
        predecessor: ConversationId,
        conversation: ConversationId,
        delta: ConversationDelta,
    },
}

impl ThreadOutcome {
    pub fn delta(&self) -> &ConversationDelta {
        match self {
            Self::Starts { delta, .. }
            | Self::Extends { delta, .. }
            | Self::Forks { delta, .. }
            | Self::Compacts { delta, .. } => delta,
        }
    }
}

pub trait Threader {
    async fn thread(
        &mut self,
        exchange: &Exchange,
        agent: AgentId,
    ) -> Result<ThreadOutcome, ThreadError>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResolveError {
    Store { reason: String },
    UnknownAgent(AgentId),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ThreadError {
    Store { reason: String },
}
