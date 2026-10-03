//! Agent identity.
//!
//! Requests are stateless and several agents can share an API key, so an
//! agent is a claim built from evidence. The claim can be wrong, so merges
//! are recorded as a state rather than rewriting history.
//!
//! ```text
//! Registered ─first traffic─▶ Provisional ─corroborated─▶ Established
//!                                  │                          │
//!                                  └──────── merge ───────────┴─▶ Merged
//! ```
//!
//! `Registered` is an agent the deployment declared in config (an issued
//! key) that has not sent traffic yet. Agents discovered from traffic start
//! in `Provisional`.

use crate::ids::{AgentId, KeyHash, OperatorId, PromptHash};
use crate::observed::exchange::AgentHeader;
use crate::support::{NonEmpty, Timestamp};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum IdentityEvidence {
    ApiKey(KeyHash),
    Header(AgentHeader),
    PromptFingerprint(PromptHash),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Agent {
    pub id: AgentId,
    pub evidence: NonEmpty<IdentityEvidence>,
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
    /// Two or more independent evidence kinds agree.
    Established {
        since: Timestamp,
    },
    /// This agent turned out to be `into`. Its exchanges are attributed to
    /// `into` from now on.
    ///
    /// `into` is never this agent and is never itself `Merged`: a merge into
    /// a merged agent is redirected to that agent's target.
    Merged {
        into: AgentId,
        at: Timestamp,
        by: MergeAuthor,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MergeAuthor {
    /// The identity resolver found the same evidence on two agents.
    Resolver,
    Operator(OperatorId),
}
