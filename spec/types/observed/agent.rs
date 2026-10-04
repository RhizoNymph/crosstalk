//! Agent identity.
//!
//! Requests are stateless, several agents can share a credential, OAuth
//! tokens rotate, and self-hosted servers may have no credential at all. An
//! agent is therefore a claim built from evidence, and the claim can be
//! wrong, so merges are recorded rather than rewriting history.
//!
//! ```text
//! Registered ─first traffic─▶ Provisional ─corroborated─▶ Established
//!                                  │                          │
//!                                  └──────── merge ───────────┴─▶ Merged
//! ```
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
//! **Harness ids are scoped.** Session and agent ids sent by a harness are
//! client-asserted (oh-my-pi sends Claude Code's), so they only count as
//! evidence within the [`IdentityScope`] they arrived in: the same session id
//! under two different credentials names two different agents.

use crate::ids::{AccountHash, AgentId, CredentialHash, OperatorId, PromptHash};
use crate::observed::client::UpstreamId;
use crate::support::{NonEmpty, Timestamp};

/// The authenticated context a harness id is interpreted in.
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
    /// pi `session_id`).
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
    /// This agent turned out to be `into`. Its records keep its own id and
    /// resolve to `into` at read time.
    ///
    /// `into` is never this agent and is never itself `Merged`: a merge into
    /// a merged agent is redirected to that agent's target, and agents
    /// already merged into this one are repointed to `into`.
    Merged {
        into: AgentId,
        at: Timestamp,
        by: MergeAuthor,
    },
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
