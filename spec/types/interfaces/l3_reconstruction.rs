//! L3 reconstruction: who sent an exchange, and which conversation it
//! continues. Consumer group `reconstruct`, triggered by `ExchangeCaptured`.
//!
//! Implementations:
//! - `IdentityResolver`: `ApiKeyResolver`, `HeaderResolver`,
//!   `PromptFingerprintResolver`, and `ChainResolver`, which runs the others
//!   in order and merges their answers.
//! - `Threader`: `PrefixThreader` (message-hash prefix match),
//!   `ResponsesStateThreader` (`previous_response_id`), `CompactionThreader`
//!   (summary heuristics).

use crate::events::ingest::ConversationDelta;
use crate::ids::{AgentId, ConversationId};
use crate::observed::agent::IdentityEvidence;
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

pub trait IdentityResolver {
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
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ThreadError {
    Store {
        reason: String,
    },
    /// A Responses API exchange referenced a previous response the gateway
    /// never saw (it was sent around the proxy).
    UnknownPreviousResponse {
        id: String,
    },
}
