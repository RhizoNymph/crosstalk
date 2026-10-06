//! Where threaded conversations are kept: the [`ConversationStore`] trait,
//! what a threading call hands it ([`ThreadInput`]), and what it keeps.
//!
//! A store decides a threading call and records it in one atomic step
//! (one lock section in memory, one `SERIALIZABLE` transaction on
//! Postgres): concurrent calls give the outcomes of some serial order
//! (`reconstruct.thread.serializable`), and a second call for an exchange
//! returns the first call's outcome and changes nothing
//! (`reconstruct.thread.rethread-idempotent`). The decision itself is
//! shared (`plan`), so the stores differ only in how they read and
//! write.
//!
//! Every conversation keeps its messages in order, system turns included,
//! each under an ordinal (0, 1, 2, ...), so a conversation can be paged in
//! order without threading again ([`ConversationStore::transcript`]).
//!
//! Every threading call that records an outcome also records one turn of
//! its conversation (its exchange, the first ordinal and count of the
//! entries it appended, its agent, start, outcome kind and the history
//! length after it), and a call that creates a conversation records the
//! conversation's traffic source. The spec's `ConversationReads` reads
//! them ([`super::reads`]).

use crosstalk_spec::ids::{AgentId, ConversationId, ExchangeId, MessageHash};
use crosstalk_spec::interfaces::l3_reconstruction::{ThreadError, ThreadOutcome};
use crosstalk_spec::observed::agent::IdentityScope;
use crosstalk_spec::observed::client::{TrafficSource, UpstreamId};
use crosstalk_spec::observed::conversation::Conversation;
use crosstalk_spec::observed::exchange::ResponseId;
use crosstalk_spec::support::Timestamp;
use serde::{Deserialize, Serialize};

pub use super::history::Entry;
/// One message of a stored conversation: the spec's
/// [`crosstalk_spec::interfaces::l3_reconstruction::conversations::TranscriptEntry`].
pub use crosstalk_spec::interfaces::l3_reconstruction::conversations::TranscriptEntry;

/// Where a stored response can be found again: the upstream and identity
/// scope it was returned under, and its id. A `previous_response_id`
/// resolves only under the exchange's own upstream and scope
/// (`reconstruct.thread.previous-response-scoped`).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ResponseKey {
    pub upstream: UpstreamId,
    pub scope: IdentityScope,
    pub response: ResponseId,
}

/// What the request is relative to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RequestKind {
    /// The request carries the whole history.
    FullHistory,
    /// The request carries only what was added after `previous`.
    Increment { previous: ResponseKey },
}

/// One threading call, as the threader prepared it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ThreadInput {
    pub exchange: ExchangeId,
    /// When the exchange started: the time its messages are seen at.
    pub at: Timestamp,
    /// The agent the exchange was attributed to.
    pub agent: AgentId,
    /// Every agent of the attributed agent's cluster (its canonical agent
    /// and the agents merged into it): matching considers their
    /// conversations.
    pub members: Vec<AgentId>,
    /// The request's messages with their roles, in order: the whole
    /// history, or the increment.
    pub request: Vec<Entry>,
    pub kind: RequestKind,
    /// The response, or a failed exchange's partial response.
    pub output: Option<MessageHash>,
    /// Where this exchange's response can be continued from, when it has a
    /// response id.
    pub response: Option<ResponseKey>,
    /// A message of the request that carries content evidence of a
    /// compaction (a summary turn), if any.
    pub summary: Option<MessageHash>,
    /// The id a conversation this call creates takes.
    pub conversation: ConversationId,
    /// Where the exchange's traffic came from
    /// (`ClientContext::ingress`): a conversation this call creates
    /// records it.
    pub source: TrafficSource,
}

/// Keeps threaded conversations.
pub trait ConversationStore: Send + Sync {
    /// Decide and record one threading call.
    fn thread(
        &self,
        input: ThreadInput,
    ) -> impl Future<Output = Result<ThreadOutcome, ThreadError>> + Send;

    /// A stored conversation: its attributed agent, origin and non-system
    /// history.
    fn conversation(
        &self,
        id: ConversationId,
    ) -> impl Future<Output = Result<Option<Conversation>, ThreadError>> + Send;

    /// Every message of a stored conversation, by ordinal. Empty for an
    /// unknown id.
    fn transcript(
        &self,
        id: ConversationId,
    ) -> impl Future<Output = Result<Vec<TranscriptEntry>, ThreadError>> + Send;

    /// The ids of every stored conversation, ascending.
    fn conversations(
        &self,
    ) -> impl Future<Output = Result<Vec<ConversationId>, ThreadError>> + Send;
}

/// [`crosstalk_spec::observed::conversation::ConversationOrigin`] as stored.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "type",
    content = "data",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub(crate) enum StoredOrigin {
    Root,
    Fork {
        parent: ConversationId,
        shared_prefix: u32,
    },
    Compaction {
        predecessor: ConversationId,
    },
}

/// [`ThreadOutcome`] as stored.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "type",
    content = "data",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub(crate) enum StoredOutcome {
    Starts {
        conversation: ConversationId,
        delta: crosstalk_spec::events::ingest::ConversationDelta,
    },
    Extends {
        conversation: ConversationId,
        delta: crosstalk_spec::events::ingest::ConversationDelta,
    },
    Forks {
        parent: ConversationId,
        shared_prefix: u32,
        conversation: ConversationId,
        delta: crosstalk_spec::events::ingest::ConversationDelta,
    },
    Compacts {
        predecessor: ConversationId,
        conversation: ConversationId,
        delta: crosstalk_spec::events::ingest::ConversationDelta,
    },
}

impl From<ThreadOutcome> for StoredOutcome {
    fn from(outcome: ThreadOutcome) -> Self {
        match outcome {
            ThreadOutcome::Starts {
                conversation,
                delta,
            } => Self::Starts {
                conversation,
                delta,
            },
            ThreadOutcome::Extends {
                conversation,
                delta,
            } => Self::Extends {
                conversation,
                delta,
            },
            ThreadOutcome::Forks {
                parent,
                shared_prefix,
                conversation,
                delta,
            } => Self::Forks {
                parent,
                shared_prefix,
                conversation,
                delta,
            },
            ThreadOutcome::Compacts {
                predecessor,
                conversation,
                delta,
            } => Self::Compacts {
                predecessor,
                conversation,
                delta,
            },
        }
    }
}

impl From<StoredOutcome> for ThreadOutcome {
    fn from(outcome: StoredOutcome) -> Self {
        match outcome {
            StoredOutcome::Starts {
                conversation,
                delta,
            } => Self::Starts {
                conversation,
                delta,
            },
            StoredOutcome::Extends {
                conversation,
                delta,
            } => Self::Extends {
                conversation,
                delta,
            },
            StoredOutcome::Forks {
                parent,
                shared_prefix,
                conversation,
                delta,
            } => Self::Forks {
                parent,
                shared_prefix,
                conversation,
                delta,
            },
            StoredOutcome::Compacts {
                predecessor,
                conversation,
                delta,
            } => Self::Compacts {
                predecessor,
                conversation,
                delta,
            },
        }
    }
}

/// The conversation an outcome names.
pub(crate) fn outcome_conversation(outcome: &ThreadOutcome) -> ConversationId {
    match outcome {
        ThreadOutcome::Starts { conversation, .. }
        | ThreadOutcome::Extends { conversation, .. }
        | ThreadOutcome::Forks { conversation, .. }
        | ThreadOutcome::Compacts { conversation, .. } => *conversation,
    }
}
