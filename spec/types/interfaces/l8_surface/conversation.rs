//! The conversation view's read models: one agent's conversations
//! ([`ConversationRow`]), one conversation's head ([`ConversationHead`]),
//! its turns with their structure and provenance marks ([`turn`]), their
//! text ([`text`]), and where an exchange or a span sits ([`TurnPoint`],
//! [`SpanPoint`]).
//!
//! **Sources.** Conversations, turns and transcripts are L3's
//! (`ConversationReads`); each turn's exchange record is L1's
//! (`ExchangeReads`); part shapes and text are cut from the blob store's
//! bodies (`Message::part_text`); spans, matches and scan status are L4's
//! (`ProvenanceReads`, `SpanIndex::spans`); the transmission holding a
//! match is L5's (`TransmissionStore::holding`). Nothing here is stored:
//! every read model is assembled on read.
//!
//! **Agents.** Every agent id a read model names is canonical
//! (`AgentDirectory::canonical` at the read) of the id as stored; merges
//! and unmerges change no stored conversation, turn, span or match, so the
//! next read follows them (`surface.conversation.merge-split`).
//!
//! **Permissions.** Every read but `conversation_text` and `part_text` is
//! View and carries no message text (`surface.conversation.view-no-text`):
//! ids, counts, times, roles, part kinds, byte lengths and ranges, tool
//! names and call ids, provider block type names, models and harness
//! claims. The two text reads are Content.
//!
//! **Not watermarked.** Conversations and spans are L3 and L4 state, which
//! L7's watermark does not settle; provenance completeness is reported per
//! turn instead ([`turn::Turn::provenance`]).
//!
//! On the wire every struct is snake_case with unknown fields refused,
//! every data enum adjacently tagged (`{"type": .., "data": ..}`) and every
//! unit enum a snake_case string. Request types are `WireRequest`s;
//! responses are not.

pub mod text;
pub mod turn;

use serde::{Deserialize, Serialize};

use crate::derived::provenance::span::SpanLocation;
use crate::ids::{AgentId, ConversationId, ExchangeId, SpanId, TransmissionId};
use crate::observed::agent::ClaimSet;
use crate::wire::WireRequest;

pub use crate::interfaces::l3_reconstruction::conversations::{
    ExchangePlacement, ReplayFilter, TurnIndex, TurnPoint, TurnWindow,
};
pub use crate::observed::client::{CorpusId, TrafficSource};
pub use crate::observed::conversation::OriginKind;
pub use crate::support::Timestamp;

/// Which conversations `QueryApi::conversations` lists. A request:
/// `{"agent": "01J…", "origins": [], "replay": {"type": "include"}}`.
/// `agent` keeps the conversations whose stored agent resolves to the same
/// canonical agent as it (`None` keeps every agent); empty `origins` keeps
/// every origin; `replay` keeps by traffic source.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct ConversationFilter {
    pub agent: Option<AgentId>,
    pub origins: Vec<OriginKind>,
    pub replay: ReplayFilter,
}

impl WireRequest for ConversationFilter {}

/// One conversation as the list shows it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct ConversationRow {
    pub id: ConversationId,
    /// `AgentDirectory::canonical(Conversation::agent)`.
    pub agent: AgentId,
    pub origin: OriginLink,
    /// The first turn's `ExchangeMeta::started_at`.
    pub started_at: Timestamp,
    /// The last turn's `started_at`.
    pub last_turn_at: Timestamp,
    /// Turns threaded when read.
    pub turns: u32,
    /// Live, or replayed from a dataset corpus: the first turn's
    /// `ClientContext::ingress`.
    pub source: TrafficSource,
}

/// `ConversationOrigin` with its links resolved for display
/// (`surface.conversation.origin-resolved`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "type",
    content = "data",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum OriginLink {
    Root,
    Fork {
        parent: ConversationId,
        /// The parent's canonical agent.
        parent_agent: AgentId,
        /// Non-system messages shared with the parent.
        shared_prefix: u32,
        /// The last turn of `parent` whose history lies wholly inside the
        /// shared prefix (`ConversationReads::branch_turn`): where the
        /// branch leaves the parent. `None` when the prefix ends inside the
        /// parent's first turn.
        branch_turn: Option<TurnIndex>,
    },
    Compaction {
        predecessor: ConversationId,
        /// The predecessor's canonical agent.
        predecessor_agent: AgentId,
        /// Messages of the first request whose hash is in the
        /// predecessor's stored history: the carried-over entries of turn
        /// 0.
        carried_over: u32,
    },
}

/// The head of a conversation page.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct ConversationHead {
    pub row: ConversationRow,
    /// Transmissions in and out of the conversation's turns.
    pub traffic: ConversationTraffic,
    /// Conversations whose origin names this one, oldest first: forks
    /// (with their branch turn here) and compactions
    /// (`surface.conversation.successors-complete`).
    pub successors: Vec<Successor>,
    /// The delegation that started this conversation: a transmission routed
    /// `Delegation(ParentToChild)` whose reader exchange is one of its
    /// turns, the earliest such turn's. `None` when none is known
    /// (`surface.conversation.delegated-from`).
    pub delegated_from: Option<DelegationLink>,
    /// The `ClientContext::harness` claims on its turns, each with the
    /// latest turn start it was seen at. Claims, never identity
    /// (`surface.conversation.claims-only`).
    pub claims: ClaimSet,
}

/// Distinct stored transmissions holding a content match of the
/// conversation, each counted once (`surface.conversation.traffic-counts`).
/// Only transmissions holding content matches can be found from a turn, so
/// a suspected one (co-access evidence only) is not counted.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct ConversationTraffic {
    /// Holding a match whose reader exchange is one of the conversation's
    /// turns.
    pub received: u32,
    /// Holding a match whose origin span is in one of the conversation's
    /// turns' outputs.
    pub sent: u32,
}

/// A conversation that continues this one.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct Successor {
    pub conversation: ConversationId,
    /// Its canonical agent.
    pub agent: AgentId,
    pub kind: SuccessorKind,
    pub started_at: Timestamp,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "type",
    content = "data",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum SuccessorKind {
    /// A fork sharing `shared_prefix` messages, branching after
    /// `branch_turn` of this conversation.
    Fork {
        shared_prefix: u32,
        branch_turn: Option<TurnIndex>,
    },
    Compaction,
}

/// The delegation that spawned a conversation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct DelegationLink {
    pub transmission: TransmissionId,
    /// The parent's span the child read: its agent, exchange and turn.
    pub parent: SpanPoint,
    /// The child's turn that read it.
    pub child: TurnPoint,
}

/// Where a span sits (`surface.conversation.locate`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct SpanPoint {
    pub span: SpanId,
    /// The span's author as recorded when it was indexed
    /// (`IndexedSpan::author`), resolved through `AgentDirectory::canonical`
    /// at read time. Never stored resolved.
    pub agent: AgentId,
    pub exchange: ExchangeId,
    /// `None` while the exchange is not threaded.
    pub turn: Option<TurnPoint>,
    pub location: SpanLocation,
}
