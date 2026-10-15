//! Events from capture (L1) and reconstruction (L3).

use serde::{Deserialize, Serialize};

use crate::events::Subject;
use crate::ids::{AgentId, ConversationId, ExchangeId, MergeId, MessageHash, OperatorId};
use crate::observed::agent::{AgentLabel, IdentityEvidence, MergeAuthor};
use crate::observed::exchange::Exchange;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "type",
    content = "data",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum IngestEvent {
    /// A normalized exchange. Every message it references is already in the
    /// blob store when this is published.
    ExchangeCaptured(Box<Exchange>),
    ConversationDelta(ConversationDelta),
    AgentSeen {
        agent: AgentId,
        evidence: IdentityEvidence,
    },
    /// Merge record `merge` was applied: `from` and every agent in
    /// `repointed` now resolve to `into`.
    AgentMerged {
        merge: MergeId,
        from: AgentId,
        into: AgentId,
        repointed: Vec<AgentId>,
        by: MergeAuthor,
    },
    /// An operator reverted merge record `merge`. Readers that cache the
    /// merge table point `agent` at itself and every agent in `restored` at
    /// `agent`.
    AgentUnmerged {
        merge: MergeId,
        /// The record's source.
        agent: AgentId,
        /// The agent it resolved to until now.
        was_into: AgentId,
        /// Agents the record repointed that now point at `agent` again.
        restored: Vec<AgentId>,
        by: OperatorId,
    },
    /// An operator set (`Some`) or cleared (`None`) an agent's label.
    AgentRenamed {
        agent: AgentId,
        label: Option<AgentLabel>,
        by: OperatorId,
    },
}

impl IngestEvent {
    pub fn subject(&self) -> Subject {
        match self {
            Self::ExchangeCaptured(_) => Subject::ExchangeCaptured,
            Self::ConversationDelta(_) => Subject::ConversationDelta,
            Self::AgentSeen { .. } => Subject::AgentSeen,
            Self::AgentMerged { .. } => Subject::AgentMerged,
            Self::AgentUnmerged { .. } => Subject::AgentUnmerged,
            Self::AgentRenamed { .. } => Subject::AgentRenamed,
        }
    }
}

/// The part of one exchange that is new to its conversation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct ConversationDelta {
    pub exchange: ExchangeId,
    pub agent: AgentId,
    pub conversation: ConversationId,
    /// Request messages not seen in this conversation before: tool results
    /// and user turns since the last exchange. Read-side detection scans
    /// these. For an increment exchange, the increment after resolution; for
    /// a compaction's first exchange, its non-system messages whose hash is
    /// not in the predecessor's history, in request order.
    pub new_inputs: Vec<MessageHash>,
    /// The system message, when it is new to the conversation (its first
    /// exchange, or the harness changed it).
    pub new_system: Option<MessageHash>,
    /// The response, or a failed exchange's partial response. Write-side
    /// detection and span extraction read this.
    pub output: Option<MessageHash>,
}
