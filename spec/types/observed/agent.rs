//! Agent identity.
//!
//! Requests are stateless, several agents can share a credential, OAuth
//! tokens rotate, and self-hosted servers may have no credential at all. An
//! agent is therefore a claim built from evidence, and the claim can be
//! wrong, so merges are recorded rather than rewriting history.
//!
//! ```text
//! Registered ─first traffic─▶ Provisional ─corroborated─▶ Established
//!     │  ▲                       │  ▲                        │  ▲
//!     └──┼───────────────────────┴──┼──── merge ─────────────┴──┼──▶ Merged
//!        └──────────────────────────┴──── unmerge ──────────────┴──────┘
//! ```
//!
//! `Registered` is an agent the deployment declared in config that has not
//! sent traffic yet. Agents discovered from traffic start in `Provisional`.
//! Any of the three active states can be merged, and an unmerge returns the
//! agent to the state it was merged from ([`ActiveAgentState`]).
//!
//! **Merges are aliases.** Records attributed to a merged agent keep its id.
//! Readers resolve every `AgentId` to its canonical agent through the merge
//! table at read time (see `AgentDirectory` in L3), so a merge is cheap and
//! auditable, and a transmission between two agents that are later merged
//! becomes a self-edge that queries drop.
//!
//! **Merges are a log.** Every merge is a [`MergeRecord`]; an unmerge reverts
//! one record exactly, including the agents it repointed, and records a
//! [`MergeVeto`] so the resolver does not merge the pair again on the same
//! evidence. See [`merge`].
//!
//! **Labels are for display.** An operator can give an active agent a
//! free-text [`AgentLabel`]. It is shown and searchable but is never identity
//! evidence. A merged agent cannot be renamed, and a merge or unmerge
//! changes no agent's label. When an agent has no label the UI derives a
//! display name from its evidence and id. Earlier labels are in the audit
//! log, one `RenameAgent` record per change.
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

pub mod merge;

use crate::ids::{AccountHash, AgentId, CredentialHash, OperatorId, PromptHash};
use crate::observed::client::UpstreamId;
use crate::support::{Change, DisplayText, NonEmpty, Timestamp};

mod claims;

pub use claims::{ClaimSet, DuplicateClaim, SeenClaim};
pub use merge::{
    AlreadyReverted, InvalidMergeTransition, MergeConflict, MergeRecord, MergeVeto, MergedInto,
    Reversal,
};

/// An operator's display label for an agent: trimmed, non-empty, at most 64
/// characters and free of control characters.
pub type AgentLabel = DisplayText<64>;

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
    /// The operator-chosen display label. Never identity evidence.
    pub label: Option<AgentLabel>,
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
    /// resolve to [`MergedInto::into`] at read time.
    Merged(MergedInto),
}

/// The states an agent can be merged from, and so the states an unmerge
/// returns it to: every state but `Merged`. A registered agent can be merged
/// by an operator who knows it is the same as one seen in traffic.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ActiveAgentState {
    Registered { at: Timestamp },
    Provisional { first_seen: Timestamp },
    Established { since: Timestamp },
}

impl From<ActiveAgentState> for AgentState {
    fn from(state: ActiveAgentState) -> Self {
        match state {
            ActiveAgentState::Registered { at } => Self::Registered { at },
            ActiveAgentState::Provisional { first_seen } => Self::Provisional { first_seen },
            ActiveAgentState::Established { since } => Self::Established { since },
        }
    }
}

impl AgentState {
    /// The active state, or the merged state of a merged agent.
    pub fn active(&self) -> Result<ActiveAgentState, &MergedInto> {
        match self {
            Self::Registered { at } => Ok(ActiveAgentState::Registered { at: *at }),
            Self::Provisional { first_seen } => Ok(ActiveAgentState::Provisional {
                first_seen: *first_seen,
            }),
            Self::Established { since } => Ok(ActiveAgentState::Established { since: *since }),
            Self::Merged(merged) => Err(merged),
        }
    }

    /// The agent this one resolves to, if merged.
    pub fn merged_into(&self) -> Option<AgentId> {
        match self {
            Self::Merged(merged) => Some(merged.into),
            Self::Registered { .. } | Self::Provisional { .. } | Self::Established { .. } => None,
        }
    }
}

/// A rename of a merged agent. Only canonical agents can be renamed; the
/// operator renames `into` instead.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RenameMerged {
    pub into: AgentId,
}

impl Agent {
    /// Set (`Some`) or clear (`None`) the label. `Unchanged` when the label
    /// already matches. A merged agent is refused and keeps its label.
    pub fn rename(&mut self, label: Option<AgentLabel>) -> Result<Change, RenameMerged> {
        if let Some(into) = self.state.merged_into() {
            return Err(RenameMerged { into });
        }
        if self.label == label {
            return Ok(Change::Unchanged);
        }
        self.label = label;
        Ok(Change::Applied)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MergeAuthor {
    /// The identity resolver found the same strong evidence on two agents.
    Resolver,
    Operator(OperatorId),
}

/// A request to merge `from` into `into`. Built only through
/// [`MergeRequest::new`], which rejects a self-merge. Two different ids of
/// one cluster pass here and are refused by the merge table
/// ([`MergeRequest::conflict`], `MergeConflict::IntoSelf`).
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
