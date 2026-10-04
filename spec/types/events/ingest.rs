//! Events from capture (L1) and reconstruction (L3).

use crate::events::Subject;
use crate::ids::{AgentId, ConversationId, ExchangeId, MessageHash, OperatorId};
use crate::observed::agent::{IdentityEvidence, MergeAuthor};
use crate::observed::exchange::Exchange;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IngestEvent {
    /// A normalized exchange. Every message it references is already in the
    /// blob store when this is published.
    ExchangeCaptured(Box<Exchange>),
    ConversationDelta(ConversationDelta),
    AgentSeen {
        agent: AgentId,
        evidence: IdentityEvidence,
    },
    AgentMerged {
        from: AgentId,
        into: AgentId,
        by: MergeAuthor,
    },
    /// An operator undid `agent`'s merge. Readers that cache the merge table
    /// point `agent` at itself and every agent in `restored` at `agent`.
    AgentUnmerged {
        agent: AgentId,
        /// The agent it resolved to until now.
        was_into: AgentId,
        /// Agents repointed through `agent` that now point at it again.
        restored: Vec<AgentId>,
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
        }
    }
}

/// The part of one exchange that is new to its conversation.
#[derive(Debug, Clone, PartialEq, Eq)]
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
