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
//! Operators reach L3 through the surface: merges, exact unmerges and
//! display labels. An unmerge changes only the merge table, so graphs and
//! edges split again on their next read.
//!
//! After every committed change to a stored agent (creation, a registered
//! agent from config, a state change, a merge, an unmerge, a label), L3
//! publishes `Changed::Agent` for each agent whose `QueryApi::agents` entry
//! changed: both agents of a merge; the agent, its former target and every
//! restored agent of an unmerge.
//!
//! Identity resolution uses the most specific evidence present
//! (`IdentityEvidence::specificity`). Harness ids count only within their
//! `IdentityScope`. Rotating credentials and prompt fingerprints are weak:
//! they attach to an agent but never establish one alone.
//!
//! An increment exchange whose previous response the gateway never saw (it
//! was sent around the proxy) is threaded as `Starts` holding only its
//! increment, so its inputs are still scanned. A `previous_response_id`
//! resolves only to a response stored under the same upstream and identity
//! scope; anything else counts as unseen, so a forged id cannot attach one
//! caller's turn to another's conversation.
//!
//! A conversation's stored history (`Conversation::messages`) holds non-system
//! messages only. Each `Extends` appends the delta's `new_inputs` and then its
//! output, if any, so the history stays in the order the next request repeats
//! it.
//!
//! A `Compacts` conversation's history starts with its first request's
//! non-system messages in request order, carried-over messages included, then
//! that exchange's output; each later delta's `new_inputs` and then its output
//! follow. Its first delta's `new_inputs` are that request's non-system
//! messages minus the carried-over ones (those whose hash is in the
//! predecessor's stored history), in request order, so read-side detection
//! scans only what the compaction introduced, such as the summary.
//!
//! Deltas and other records carry the agent the exchange was attributed to,
//! not its canonical agent; readers resolve through `AgentDirectory`.

use crate::events::ingest::ConversationDelta;
use crate::ids::{AgentId, ConversationId, OperatorId};
#[cfg(doc)]
use crate::observed::agent::Merged;
use crate::observed::agent::{IdentityEvidence, LabelChange, MergeRequest};
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
    /// Apply a merge and publish one `AgentMerged`. Repoints agents already
    /// merged into the source ([`Merged::repoint`]), so no merge chain is
    /// ever longer than one, and records on the source's [`Merged`] its
    /// prior state and the agents it repointed. A merge into a merged agent
    /// is redirected to that agent's target, recorded as a merge into it
    /// followed by a repoint, and listed in its `repointed`.
    async fn merge(&mut self, request: MergeRequest) -> Result<(), ResolveError>;

    /// Undo `agent`'s merge exactly, as operator `by`. The agent returns to
    /// [`Merged::prior`], and every agent in its [`Merged::repointed`] that
    /// was repointed away from it points at it again
    /// ([`Merged::restore_through`]). Publishes one `AgentUnmerged` listing
    /// those agents. Stored records are untouched; readers see the split on
    /// their next `AgentDirectory::canonical` call.
    ///
    /// `NotMerged` when `agent` is not merged, so a repeated unmerge changes
    /// nothing and publishes nothing.
    async fn unmerge(&mut self, agent: AgentId, by: OperatorId) -> Result<(), ResolveError>;

    /// Record `change` in the label log of `agent`'s canonical agent. A
    /// merged agent's labels never change. Labels are never identity
    /// evidence: `resolve` does not read them.
    async fn set_label(&mut self, agent: AgentId, change: LabelChange) -> Result<(), ResolveError>;

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
    Store {
        reason: String,
    },
    UnknownAgent(AgentId),
    /// An unmerge of an agent that is not merged.
    NotMerged(AgentId),
    /// A label change older than the canonical agent's last one.
    LabelOutOfOrder(AgentId),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ThreadError {
    Store { reason: String },
}
