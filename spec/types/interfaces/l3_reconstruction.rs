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
//! - `ClaimStore`: `PgClaimStore`, the harness claims seen per attributed
//!   agent, recorded for every captured exchange that carries one.
//!
//! Operators reach L3 through the surface: merges, unmerges of one merge
//! record, and renames. A merge or unmerge changes only the merge table, so
//! graphs and edges join or split again on their next read. The merge log,
//! its revert procedure and merge vetoes are in
//! [`crate::observed::agent::merge`].
//!
//! After every committed change to a stored agent (creation, a registered
//! agent from config, a state change, a merge, an unmerge, a rename), L3
//! publishes `Changed::Agent` for each agent whose `QueryApi::agents` entry
//! changed: the source, target and every repointed agent of a merge; the
//! source, its former target and every restored agent of an unmerge; the
//! renamed agent.
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
use crate::ids::{AgentId, ConversationId, MergeId, OperatorId};
#[cfg(doc)]
use crate::observed::agent::Agent;
use crate::observed::agent::{
    AgentLabel, ClaimSet, IdentityEvidence, MergeRecord, MergeRequest, MergeVeto, Reversal,
};
use crate::observed::client::HarnessClaim;
use crate::observed::exchange::{Exchange, ExchangeMeta};
use crate::observed::message::Message;
use crate::support::{Change, NonEmpty, Timestamp};

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

/// Alias resolution. Every reader of stored agent ids resolves them here.
///
/// Implementations that cache the merge table must apply each
/// `AgentMerged` (the source and every agent in `repointed` resolve to
/// `into`) and `AgentUnmerged` (the agent resolves to itself and every agent
/// in `restored` to it) before serving a read that follows the event, or
/// drop the cache. A cache that misses an unmerge keeps showing the agents
/// as one.
pub trait AgentDirectory {
    /// The agent `id` resolves to after merges: itself unless merged.
    fn canonical(&self, id: AgentId) -> AgentId;
}

/// The harness claims seen on each agent's exchanges. Claims are never
/// identity evidence: `IdentityResolver::resolve` never reads this store.
pub trait ClaimStore {
    /// Record that an exchange attributed to `agent`, started at `at`,
    /// carried `claim` (`ClientContext::harness`), keeping the latest time
    /// per distinct claim ([`ClaimSet::observe`]). Called by the
    /// reconstruct consumer for each `ExchangeCaptured` after the agent is
    /// resolved, so it is idempotent under redelivery. `agent` is the
    /// attributed agent, never rewritten by a later merge.
    async fn record(
        &mut self,
        agent: AgentId,
        claim: &HarnessClaim,
        at: Timestamp,
    ) -> Result<(), ResolveError>;

    /// The claims of `agent`'s canonical agent: the [`ClaimSet::union`] of
    /// the claims recorded for it and for every agent that currently
    /// resolves to it. Merges and unmerges change only which sets are
    /// unioned.
    async fn claims(&self, agent: AgentId) -> Result<ClaimSet, ResolveError>;
}

pub trait IdentityResolver {
    /// Apply a merge at `at` and publish one `AgentMerged`, following the
    /// procedure in [`crate::observed::agent::merge`]. Returns the record.
    ///
    /// Refuses, changing nothing and publishing nothing: an unknown agent
    /// (`UnknownAgent`); a source or target that is merged (`AgentMerged`,
    /// naming its canonical agent); and a `MergeAuthor::Resolver` request
    /// between clusters that a [`MergeVeto`] separates (`Vetoed`). A
    /// `MergeAuthor::Operator` request between such clusters goes ahead and
    /// deletes every veto that separated them, in the same transaction.
    async fn merge(
        &mut self,
        request: MergeRequest,
        at: Timestamp,
    ) -> Result<MergeRecord, ResolveError>;

    /// Revert merge record `merge` exactly, as operator `by` at `at`: its
    /// source returns to its prior state, the agents it repointed and that
    /// nothing has moved since point at the source again, and a
    /// [`MergeVeto`] between its source and target is recorded. Publishes
    /// one `AgentUnmerged` listing the restored agents. Stored records are
    /// untouched; readers see the split on their next
    /// `AgentDirectory::canonical` call.
    ///
    /// `UnknownMerge` for an id with no record, and `MergeAlreadyReverted`
    /// for a record already reverted, changing nothing and publishing
    /// nothing.
    async fn unmerge(
        &mut self,
        merge: MergeId,
        by: OperatorId,
        at: Timestamp,
    ) -> Result<Reversal, ResolveError>;

    /// Set or clear the label of `agent` ([`Agent::rename`]) and publish one
    /// `AgentRenamed` when it changed. `AgentMerged` for a merged agent,
    /// which keeps its label; the rename is not redirected to the canonical
    /// agent. Labels are never identity evidence: `resolve` does not read
    /// them.
    async fn rename(
        &mut self,
        agent: AgentId,
        label: Option<AgentLabel>,
        by: OperatorId,
    ) -> Result<Change, ResolveError>;

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
    UnknownMerge(MergeId),
    /// A merge naming, or a rename of, a merged agent.
    AgentMerged {
        agent: AgentId,
        into: AgentId,
    },
    MergeAlreadyReverted(MergeId),
    /// A resolver merge between clusters an operator kept apart.
    Vetoed(MergeVeto),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ThreadError {
    Store { reason: String },
}
