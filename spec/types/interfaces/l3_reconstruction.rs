//! L3 reconstruction: who sent an exchange, and which conversation it
//! continues. Consumer group `reconstruct`, triggered by `ExchangeCaptured`.
//!
//! Implementations:
//! - `EvidenceDeriver`: `ApiKeyEvidence`, `HeaderEvidence`,
//!   `PromptFingerprintEvidence`, and `ChainEvidence`, which runs the others
//!   in order and concatenates their evidence, most specific first. A
//!   computation: it reads no store.
//! - `IdentityResolver`: `PgIdentityResolver`, over the agent table, the
//!   merge log and the vetoes. `resolve` looks the derived evidence up; the
//!   reconstruct consumer turns its answer into an attribution, a new agent
//!   ([`lifecycle::AgentLifecycle::create`]), a merge or an operator review.
//! - [`lifecycle::AgentLifecycle`]: `PgAgentStore`, the writes that create
//!   agents, move them between active states and attach new evidence.
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
//! agent from config, a state change, new evidence, a merge, an unmerge, a
//! rename), the store publishes `Changed::Agent` for each agent whose `QueryApi::agents` row or
//! `QueryApi::agent` detail changed outside its activity: the created agent
//! and its canonical parent (whose children grew); the source, target and
//! every repointed agent of a merge; the source, its former target and
//! every restored agent of an unmerge; for a merge or unmerge also every
//! agent whose stored parent is one of those (its canonical parent moved)
//! and the canonical parent of the source (its children changed); the
//! renamed agent. Activity (harness claims, last-seen times) changes with
//! every exchange and is not announced: the agent reads are `Watermarked`
//! and a client refreshes them on each watermark advance.
//!
//! The agents list, an agent's detail and batch names are read through
//! [`agents::AgentReads`]; [`agents::ActivityStore`] keeps when each agent
//! was last seen.
//!
//! Identity resolution has two halves. An [`EvidenceDeriver`] computes the
//! evidence an exchange carries (its credential by stability, its account,
//! its harness ids scoped by `IdentityScope`, its prompt fingerprint);
//! [`IdentityResolver::resolve`] answers which stored agents hold it. The
//! most specific evidence present decides (`IdentityEvidence::specificity`).
//! Harness ids count only within their `IdentityScope`. Rotating
//! credentials and prompt fingerprints are weak: they attach to an agent but
//! never establish one alone.
//!
//! **Replayed corpora.** An exchange whose `ClientContext::ingress` is
//! `IngressMode::Replay { corpus }` comes from a recorded dataset, whose
//! credentials, accounts and harness ids are the dataset's (often one
//! shared test key for every trajectory). Its evidence is evidence only
//! within its corpus: the consumer never attributes a replayed exchange
//! to, and never merges a replayed agent with, a live agent or an agent of
//! another corpus, and never attributes a live exchange to a replayed
//! agent (`reconstruct.identity.replay-within-corpus`). The evidence values
//! are the ones `EvidenceDeriver` derives for any exchange, and `resolve`
//! takes no population, so the consumer keeps the populations apart around
//! it (P4.1 chooses how: a store per population, or a population key on the
//! stored evidence); a population argument on `resolve` and
//! `attach_evidence` is the spec change to make if that proves awkward.
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
//!
//! Stored conversations, their transcripts and their turns are read back
//! through [`conversations::ConversationReads`].

pub mod agents;
pub mod conversations;
pub mod lifecycle;

use crate::events::ingest::ConversationDelta;
use crate::ids::{AgentId, ConversationId, ExchangeId, MergeId, OperatorId};
#[cfg(doc)]
use crate::observed::agent::Agent;
use crate::observed::agent::{
    AgentLabel, ClaimSet, IdentityEvidence, MergeConflict, MergeRecord, MergeRequest, MergeVeto,
    Reversal,
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
    /// The evidence points at more than one agent. The reconstruct consumer
    /// turns this into a merge or an operator review.
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
    fn record(
        &mut self,
        agent: AgentId,
        claim: &HarnessClaim,
        at: Timestamp,
    ) -> impl Future<Output = Result<(), ResolveError>> + Send;

    /// The claims of `agent`'s canonical agent: the [`ClaimSet::union`] of
    /// the claims recorded for it and for every agent that currently
    /// resolves to it. Merges and unmerges change only which sets are
    /// unioned.
    fn claims(&self, agent: AgentId)
    -> impl Future<Output = Result<ClaimSet, ResolveError>> + Send;
}

pub trait IdentityResolver {
    /// Apply a merge at `at` and publish one `AgentMerged`, following the
    /// procedure in [`crate::observed::agent::merge`]. Returns the record.
    ///
    /// Refuses, changing nothing and publishing nothing, in this order: an
    /// unknown agent (`UnknownAgent`); then whatever
    /// [`MergeRequest::conflict`] finds in the two agents' states
    /// ([`ResolveError::of_conflict`]): two agents that already resolve to
    /// one canonical agent (`MergeIntoSelf`), or a source or target that is
    /// merged (`AgentMerged`, naming its canonical agent); then a
    /// `MergeAuthor::Resolver` request between clusters that a
    /// [`MergeVeto`] separates (`Vetoed`). A `MergeAuthor::Operator` request
    /// between such clusters goes ahead and deletes every veto that
    /// separated them, in the same transaction.
    fn merge(
        &mut self,
        request: MergeRequest,
        at: Timestamp,
    ) -> impl Future<Output = Result<MergeRecord, ResolveError>> + Send;

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
    fn unmerge(
        &mut self,
        merge: MergeId,
        by: OperatorId,
        at: Timestamp,
    ) -> impl Future<Output = Result<Reversal, ResolveError>> + Send;

    /// Set or clear the label of `agent` ([`Agent::rename`]) and publish one
    /// `AgentRenamed` when it changed. `AgentMerged` for a merged agent,
    /// which keeps its label; the rename is not redirected to the canonical
    /// agent. Labels are never identity evidence: `resolve` does not read
    /// them.
    fn rename(
        &mut self,
        agent: AgentId,
        label: Option<AgentLabel>,
        by: OperatorId,
    ) -> impl Future<Output = Result<Change, ResolveError>> + Send;

    /// Which stored agents hold `evidence` (an [`EvidenceDeriver`]'s
    /// output), read in one snapshot of the agent table:
    ///
    /// - only the most specific items decide (the items whose
    ///   `IdentityEvidence::specificity` is the highest present); less
    ///   specific items never make agents conflict;
    /// - an agent holds an item when its evidence contains an equal value,
    ///   so harness ids under two scopes never meet; a `HarnessSession` item
    ///   is held only by an agent holding it and no `HarnessAgent` evidence
    ///   (the session's main agent);
    /// - holders are resolved through the merge table.
    ///
    /// No holder is `New { evidence }`. One canonical holder is
    /// `Known { agent, new_evidence }`, `new_evidence` being the items of
    /// `evidence` that agent's own record does not hold yet, in input order.
    /// Several are `Conflict` with the canonical holders ascending and the
    /// deciding items. Labels and harness claims are never read. Changes
    /// nothing: attaching new evidence is
    /// [`lifecycle::AgentLifecycle::attach_evidence`].
    fn resolve(
        &self,
        evidence: &NonEmpty<IdentityEvidence>,
    ) -> impl Future<Output = Result<Resolution, ResolveError>> + Send;
}

/// The identity evidence one exchange carries, derived from its metadata
/// and request (P4.1's algorithm). A computation: it reads no store, so the
/// same exchange always yields the same evidence.
pub trait EvidenceDeriver {
    /// Every item of evidence `meta` and `request` carry, most specific
    /// first: the credential by its stability (none for a shared or missing
    /// one), the account, harness agent and session ids in the exchange's
    /// `IdentityScope` (also under the previous digests during a secret
    /// rotation), and the prompt fingerprint. Empty when it carries none, in
    /// which case the exchange is not attributed.
    fn derive(&self, meta: &ExchangeMeta, request: &[Message]) -> Vec<IdentityEvidence>;
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

/// Where L3 put one exchange: the agent it was attributed to and the
/// conversation it was threaded into, as the threading call recorded them
/// (the agent as attributed then, not resolved through later merges).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Placement {
    pub agent: AgentId,
    pub conversation: ConversationId,
}

impl Placement {
    /// The placement a threading call's outcome records.
    pub fn of(outcome: &ThreadOutcome) -> Self {
        let delta = outcome.delta();
        Self {
            agent: delta.agent,
            conversation: delta.conversation,
        }
    }
}

/// Threading outcomes read back by exchange: what eval scores L3's
/// identity and threading against, and what a reader uses to place an
/// exchange id it holds.
pub trait ExchangePlacements {
    /// `exchange`'s placement (`reconstruct.placement.as-threaded`): the
    /// agent and conversation of its recorded threading outcome
    /// ([`Placement::of`]). `None` for an exchange never threaded: unknown,
    /// carrying no identity evidence, or left for review.
    fn placement(
        &self,
        exchange: ExchangeId,
    ) -> impl Future<Output = Result<Option<Placement>, ThreadError>> + Send;
}

pub trait Threader {
    fn thread(
        &mut self,
        exchange: &Exchange,
        agent: AgentId,
    ) -> impl Future<Output = Result<ThreadOutcome, ThreadError>> + Send;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResolveError {
    Store {
        reason: String,
    },
    UnknownAgent(AgentId),
    UnknownMerge(MergeId),
    /// A merge naming, or a rename of, a merged agent. For a merge, only
    /// when its two agents resolve to different canonical agents.
    AgentMerged {
        agent: AgentId,
        into: AgentId,
    },
    /// A merge of two different agents that already resolve to one
    /// canonical agent, `canonical` (`MergeConflict::IntoSelf`).
    MergeIntoSelf {
        from: AgentId,
        into: AgentId,
        canonical: AgentId,
    },
    MergeAlreadyReverted(MergeId),
    /// A resolver merge between clusters an operator kept apart.
    Vetoed(MergeVeto),
}

impl ResolveError {
    /// The refusal of `request` for `conflict`.
    pub fn of_conflict(request: &MergeRequest, conflict: MergeConflict) -> Self {
        match conflict {
            MergeConflict::IntoSelf { canonical } => Self::MergeIntoSelf {
                from: request.source(),
                into: request.target(),
                canonical,
            },
            MergeConflict::Merged { agent, into } => Self::AgentMerged { agent, into },
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ThreadError {
    Store { reason: String },
}
