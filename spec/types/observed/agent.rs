//! Agent identity.
//!
//! Requests are stateless, several agents can share a credential, OAuth
//! tokens rotate, and self-hosted servers may have no credential at all. An
//! agent is therefore a claim built from evidence, and the claim can be
//! wrong, so merges are recorded rather than rewriting history.
//!
//! ```text
//! Registered ─first traffic─▶ Provisional ─corroborated─▶ Established
//!                               │    ▲                     │    ▲
//!                               └────┼───── merge ─────────┴────┼──▶ Merged
//!                                    └──────── unmerge ─────────┴──────┘
//! ```
//!
//! An unmerge returns a merged agent to the state it was merged from.
//!
//! `Registered` is an agent the deployment declared in config that has not
//! sent traffic yet. Agents discovered from traffic start in `Provisional`.
//!
//! **Merges are aliases.** Records attributed to a merged agent keep its id.
//! Readers resolve every `AgentId` to its canonical agent through the merge
//! table at read time (see `AgentDirectory` in L3), so a merge is cheap and
//! auditable, and a transmission between two agents that are later merged
//! becomes a self-edge that queries drop.
//!
//! **Unmerges restore exactly.** An operator can undo a merge. The [`Merged`]
//! record holds the agent's state before the merge and the agents repointed
//! through it, so the unmerged agent returns to that state and those agents
//! point at it again. No stored record changes, so graphs and edges split
//! again on their next read.
//!
//! **Labels are for display.** An operator can give the canonical agent a
//! free-text [`AgentLabel`]. It is shown and searchable but is never identity
//! evidence. A merge or unmerge changes no agent's labels: the target keeps
//! its label, and a merged agent keeps its own, which the canonical agent's
//! [`LabelView`] lists in its history when it differs.
//!
//! **Harness claims are aggregated, never evidence.** The harness claims
//! seen on an agent's exchanges are kept per attributed agent as a
//! [`ClaimSet`] and unioned over merge aliases at read time, so graph nodes
//! can show what an agent claimed to be.
//!
//! **Harness ids are scoped.** Session and agent ids sent by a harness are
//! client-asserted (oh-my-pi sends Claude Code's), so they only count as
//! evidence within the [`IdentityScope`] they arrived in: the same session id
//! under two different credentials names two different agents.

use crate::ids::{AccountHash, AgentId, CredentialHash, OperatorId, PromptHash};
use crate::observed::client::UpstreamId;
use crate::support::{NonEmpty, Timestamp};

mod claims;
mod label;

pub use claims::{ClaimSet, DuplicateClaim, SeenClaim};
pub use label::{
    AgentLabel, InvalidLabel, InvalidLabelView, LabelChange, LabelLog, LabelView, Labeled,
    OutOfOrder, PastLabel,
};

/// The authenticated context a harness id is interpreted in: the exchange's
/// account if it has one, else its credential if that is stable, else its
/// upstream (rotating, shared or no credential). Rotating credentials are
/// never a scope, so a token refresh keeps the scope and with it the agent.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum IdentityScope {
    Account(AccountHash),
    Credential(CredentialHash),
    /// No per-caller credential: an unauthenticated or shared-key upstream.
    Upstream(UpstreamId),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum IdentityEvidence {
    /// A harness agent id (`x-claude-code-agent-id`, Codex `thread-id`).
    HarnessAgent {
        scope: IdentityScope,
        agent: String,
    },
    /// A harness session id (`X-Claude-Code-Session-Id`, Codex `session-id`,
    /// pi `session_id`). Sub-agents share their main agent's session id, so a
    /// session id resolves only to the session's main agent: the agent in
    /// that scope holding the session and no `HarnessAgent` evidence.
    HarnessSession {
        scope: IdentityScope,
        session: String,
    },
    Account(AccountHash),
    /// An API key: the same for the life of the caller.
    StableCredential(CredentialHash),
    /// An OAuth or exchanged token: changes on refresh.
    RotatingCredential(CredentialHash),
    /// The system prompt plus first user turn of a conversation.
    PromptFingerprint(PromptHash),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Strength {
    /// Cannot establish an agent on its own.
    Weak,
    Strong,
}

impl IdentityEvidence {
    pub fn strength(&self) -> Strength {
        match self {
            Self::HarnessAgent { .. }
            | Self::HarnessSession { .. }
            | Self::Account(_)
            | Self::StableCredential(_) => Strength::Strong,
            Self::RotatingCredential(_) | Self::PromptFingerprint(_) => Strength::Weak,
        }
    }

    /// Resolution uses the most specific evidence present (highest value).
    /// Less specific evidence is attached to the resolved agent but never
    /// makes two agents conflict: a shared API key carrying two different
    /// harness agent ids names two agents, not a conflict.
    pub fn specificity(&self) -> u8 {
        match self {
            Self::HarnessAgent { .. } => 5,
            Self::HarnessSession { .. } => 4,
            Self::Account(_) => 3,
            Self::StableCredential(_) => 2,
            Self::PromptFingerprint(_) => 1,
            Self::RotatingCredential(_) => 0,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Agent {
    pub id: AgentId,
    pub evidence: NonEmpty<IdentityEvidence>,
    /// The agent that spawned this one, from harness parent ids
    /// (`x-claude-code-parent-agent-id`, `x-codex-parent-thread-id`) in the
    /// same scope, or from the session's main agent for a harness sub-agent.
    pub parent: Option<AgentId>,
    pub state: AgentState,
    /// Operator-chosen display labels. Never identity evidence.
    pub labels: LabelLog,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AgentState {
    Registered {
        at: Timestamp,
    },
    Provisional {
        first_seen: Timestamp,
    },
    /// Holds evidence of at least two variants, at least one of them strong.
    Established {
        since: Timestamp,
    },
    /// This agent turned out to be another. Its records keep its own id and
    /// resolve to [`Merged::into`] at read time.
    Merged(Merged),
}

/// The states an agent can be merged from, and so the states an unmerge
/// returns it to. A `Registered` agent has sent no traffic and a `Merged` one
/// is already an alias, so neither can be merged.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MergeableState {
    Provisional { first_seen: Timestamp },
    Established { since: Timestamp },
}

impl From<MergeableState> for AgentState {
    fn from(state: MergeableState) -> Self {
        match state {
            MergeableState::Provisional { first_seen } => Self::Provisional { first_seen },
            MergeableState::Established { since } => Self::Established { since },
        }
    }
}

/// A merged agent's record: where it resolves to, and what an unmerge needs
/// to restore exactly.
///
/// `into` is never this agent and is never itself `Merged`. A merge into a
/// merged agent `t` is redirected to `t`'s target and recorded as a merge
/// into `t` followed by a repoint ([`Merged::repoint`]), with the source
/// listed in `t`'s `repointed`. When an agent is merged, every agent merged
/// into it is repointed to the new target and listed in its `repointed`.
///
/// Unmerging agent `x` returns it to `prior`, then calls
/// [`Merged::restore_through`] with `x` on each agent in `x`'s `repointed`
/// that is still merged.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Merged {
    pub into: AgentId,
    pub at: Timestamp,
    pub by: MergeAuthor,
    /// The state this agent was in when it was merged.
    pub prior: MergeableState,
    /// The targets this agent pointed at before later merges repointed it,
    /// oldest first. Empty for an agent never repointed.
    pub earlier_targets: Vec<AgentId>,
    /// Agents repointed through this one: those merged into it that its own
    /// merge repointed to its target, and merges into it that were redirected
    /// to its target.
    pub repointed: Vec<AgentId>,
}

impl Merged {
    /// The record of a merge into `into`, before any repointing.
    pub fn new(into: AgentId, at: Timestamp, by: MergeAuthor, prior: MergeableState) -> Self {
        Self {
            into,
            at,
            by,
            prior,
            earlier_targets: Vec::new(),
            repointed: Vec::new(),
        }
    }

    /// This agent's target was merged into `to`: point at `to` and remember
    /// the target it replaces.
    pub fn repoint(&mut self, to: AgentId) {
        self.earlier_targets.push(self.into);
        self.into = to;
    }

    /// `through` was unmerged. If this agent was repointed away from
    /// `through`, point it at `through` again and forget the repoints after
    /// it. Returns whether the target changed. An agent that was unmerged and
    /// merged afresh since then has no earlier targets, so it is untouched.
    pub fn restore_through(&mut self, through: AgentId) -> bool {
        match self.earlier_targets.iter().position(|t| *t == through) {
            Some(index) => {
                self.earlier_targets.truncate(index);
                self.into = through;
                true
            }
            None => false,
        }
    }
}

impl AgentState {
    /// The state an unmerge returns this agent to: `None` unless it is
    /// merged.
    pub fn unmerged(&self) -> Option<AgentState> {
        match self {
            Self::Merged(merged) => Some(merged.prior.into()),
            Self::Registered { .. } | Self::Provisional { .. } | Self::Established { .. } => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MergeAuthor {
    /// The identity resolver found the same strong evidence on two agents.
    Resolver,
    Operator(OperatorId),
}

/// A request to merge `from` into `into`. Built only through
/// [`MergeRequest::new`], which rejects a self-merge.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MergeRequest {
    from: AgentId,
    into: AgentId,
    by: MergeAuthor,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SelfMerge;

impl MergeRequest {
    pub fn new(from: AgentId, into: AgentId, by: MergeAuthor) -> Result<Self, SelfMerge> {
        if from == into {
            return Err(SelfMerge);
        }
        Ok(Self { from, into, by })
    }

    /// The agent being merged away.
    pub fn source(&self) -> AgentId {
        self.from
    }

    /// The agent it becomes.
    pub fn target(&self) -> AgentId {
        self.into
    }

    pub fn by(&self) -> MergeAuthor {
        self.by
    }
}
