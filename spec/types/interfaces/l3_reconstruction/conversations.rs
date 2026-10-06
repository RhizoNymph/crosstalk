//! L3's conversation reads: the spec lift of the threading store's
//! transcript (every message of a conversation by ordinal, system turns
//! included), with a list, successors, a turn read by index window and a
//! batch locate. The surface's conversation view
//! ([`crate::interfaces::l8_surface::conversation`]) reads through
//! [`ConversationReads`].
//!
//! **Turns.** A conversation's turns are its own threaded exchanges, in
//! threading order: turn `i` is the `i`-th threading call that recorded an
//! outcome naming the conversation (`Starts`, `Forks` or `Compacts` for
//! turn 0, `Extends` after). A fork's inherited messages (its base, the
//! parent's messages through the shared prefix, which carry the parent's
//! exchanges) precede turn 0 and belong to no turn of the fork. A turn's
//! index and exchange never change once threaded, and a redelivered
//! exchange adds no turn (`reconstruct.conversation.turn-index-stable`).
//! Turns are addressed by index ([`TurnWindow`]), not by cursor: the index
//! range is already stable under appends, and a turn link is citeable.
//!
//! **A turn's entries** are the transcript entries its threading call
//! appended, by ordinal: the request's messages new to the conversation in
//! request order, any role (a new or changed top-level system prompt
//! first, then the request's messages after the stored prefix, system
//! turns where the request put them), then the output
//! (`surface.conversation.inputs-are-transcript`). They are contiguous in
//! the transcript, so a store keeps, per turn, its first ordinal and entry
//! count, and reads one window of turns without loading the whole
//! transcript.
//!
//! **Carried over.** A `Compacts` conversation's turn 0 holds the whole
//! first request; the entries whose hash is in the predecessor's stored
//! history are flagged `carried_over`, decided when threading
//! (`surface.conversation.carried-over`). No other entry is.
//!
//! **Agents.** Every agent id is stored as attributed and never rewritten
//! by a merge; readers resolve it through `AgentDirectory`. A list for an
//! agent passes the agent's whole cluster ([`ConversationQuery::agents`]).

use std::collections::{BTreeMap, BTreeSet};
use std::ops::Range;

use serde::{Deserialize, Serialize};

use crate::batch::IdBatch;
use crate::ids::{AgentId, ConversationId, ExchangeId, MessageHash};
use crate::observed::client::{CorpusId, TrafficSource};
use crate::observed::conversation::{Conversation, OriginKind};
use crate::observed::message::Role;
use crate::paging::{ConversationList, Page, PageRequest, PageSize};
use crate::support::Timestamp;
use crate::wire::WireRequest;

use super::ThreadOutcome;

/// A turn's position in its conversation: its threading call's place in
/// threading order, from 0. Dense and immutable once threaded. On the wire
/// a number.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct TurnIndex(pub u32);

/// One turn of one conversation. On the wire
/// `{"conversation": "01J…", "turn": 3}`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct TurnPoint {
    pub conversation: ConversationId,
    pub turn: TurnIndex,
}

/// Where one threaded exchange sits: the agent its turn was attributed
/// to, its conversation and its turn. On the wire
/// `{"agent": "01J…", "conversation": "01J…", "turn": 3}`.
///
/// `ConversationReads::locate` returns the agent as recorded; the
/// surface's `exchange_turns` returns it resolved through
/// `AgentDirectory::canonical` at the read
/// (`surface.conversation.exchange-placement`), never stored resolved.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct ExchangePlacement {
    pub agent: AgentId,
    pub conversation: ConversationId,
    pub turn: TurnIndex,
}

impl ExchangePlacement {
    /// The conversation and turn.
    pub fn point(&self) -> TurnPoint {
        TurnPoint {
            conversation: self.conversation,
            turn: self.turn,
        }
    }
}

/// Turns `from .. from + size`. A request: `{"from": 0, "size": 20}`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct TurnWindow {
    pub from: TurnIndex,
    pub size: PageSize,
}

impl WireRequest for TurnWindow {}

impl TurnWindow {
    /// The turn indexes of this window that exist in a conversation of
    /// `total` turns: `from .. min(from + size, total)`, empty when `from`
    /// is at or past `total`.
    pub fn range(&self, total: u32) -> Range<u32> {
        let start = self.from.0.min(total);
        let end = self
            .from
            .0
            .saturating_add(u32::from(self.size.get().get()))
            .min(total);
        start..end.max(start)
    }
}

/// Which conversations to keep by their [`TrafficSource`]. Default
/// `Include`. On the wire `{"type": "include"}`, `{"type": "exclude"}`,
/// or `{"type": "only", "data": {"corpus": null}}` for every replayed
/// conversation (a corpus name for one corpus).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
#[serde(
    tag = "type",
    content = "data",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum ReplayFilter {
    /// Live and replayed alike.
    #[default]
    Include,
    /// Live only.
    Exclude,
    /// Replayed only: from `corpus`, or from any corpus when `None`.
    Only { corpus: Option<CorpusId> },
}

impl ReplayFilter {
    /// Whether a conversation whose traffic came from `source` is kept
    /// (`surface.conversation.traffic-source`).
    pub fn admits(&self, source: &TrafficSource) -> bool {
        match (self, source) {
            (Self::Include, _) => true,
            (Self::Exclude, TrafficSource::Live) => true,
            (Self::Exclude, TrafficSource::Replay { .. }) => false,
            (Self::Only { .. }, TrafficSource::Live) => false,
            (Self::Only { corpus }, TrafficSource::Replay { corpus: theirs }) => {
                corpus.as_ref().is_none_or(|wanted| wanted == theirs)
            }
        }
    }
}

/// One message of a stored conversation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TranscriptEntry {
    /// From 0, counting every message, system turns included.
    pub ordinal: u32,
    pub message: MessageHash,
    pub role: Role,
    /// The exchange that added it (the parent's, for a fork's inherited
    /// messages).
    pub exchange: ExchangeId,
    /// Its place in the non-system history; `None` for a system message.
    pub history_index: Option<u32>,
    /// Whether it is an exchange's output (its response, or a failed one's
    /// partial response).
    pub output: bool,
    /// On a `Compacts` conversation's turn 0: its hash is in the
    /// predecessor's stored history. `false` everywhere else.
    pub carried_over: bool,
}

/// A [`ThreadOutcome`] without its data ([`ThreadOutcome::kind`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ThreadOutcomeKind {
    Starts,
    Extends,
    Forks,
    Compacts,
}

impl ThreadOutcome {
    /// The outcome without its data.
    pub fn kind(&self) -> ThreadOutcomeKind {
        match self {
            Self::Starts { .. } => ThreadOutcomeKind::Starts,
            Self::Extends { .. } => ThreadOutcomeKind::Extends,
            Self::Forks { .. } => ThreadOutcomeKind::Forks,
            Self::Compacts { .. } => ThreadOutcomeKind::Compacts,
        }
    }
}

/// One of a conversation's own exchanges and the messages it added.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredTurn {
    pub index: TurnIndex,
    pub exchange: ExchangeId,
    /// The agent the exchange was attributed to, as recorded.
    pub agent: AgentId,
    /// When the exchange started (`ExchangeMeta::started_at`).
    pub started_at: Timestamp,
    /// The kind of the threading outcome recorded for the exchange. With
    /// the exchange's `Continuation` it tells an increment whose previous
    /// response was never seen (`Starts`) from one that resolved.
    pub outcome: ThreadOutcomeKind,
    /// The conversation's non-system history length once this turn was
    /// threaded: what a fork's `shared_prefix` is compared with to find the
    /// parent turn it branches from ([`ConversationReads::branch_turn`]).
    pub history_end: u32,
    /// The transcript entries this turn added, by ordinal: its new
    /// messages in request order (any role), then its output.
    pub entries: Vec<TranscriptEntry>,
}

/// A stored conversation with what its list row needs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredConversation {
    /// Its attributed agent (its first exchange's), origin and non-system
    /// history.
    pub conversation: Conversation,
    /// Its first turn's `ClientContext::ingress` source.
    pub source: TrafficSource,
    /// Its first turn's start.
    pub started_at: Timestamp,
    /// Its last turn's start.
    pub last_turn_at: Timestamp,
    /// Turns threaded when read: at least 1.
    pub turns: u32,
}

/// Which conversations [`ConversationReads::list`] returns: every condition
/// holds. A store input, never on the wire; the surface builds it from a
/// `ConversationFilter`.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ConversationQuery {
    /// The stored agents whose conversations are kept: a canonical agent
    /// and every agent that resolves to it, as the surface read the merge
    /// table. `None` keeps every agent.
    pub agents: Option<BTreeSet<AgentId>>,
    /// The origin kinds kept; empty keeps every origin.
    pub origins: BTreeSet<OriginKind>,
    pub replay: ReplayFilter,
}

impl ConversationQuery {
    /// Whether `stored` is one the query keeps.
    pub fn admits(&self, stored: &StoredConversation) -> bool {
        let agent = self
            .agents
            .as_ref()
            .is_none_or(|agents| agents.contains(&stored.conversation.agent));
        let origin =
            self.origins.is_empty() || self.origins.contains(&stored.conversation.origin.kind());
        agent && origin && self.replay.admits(&stored.source)
    }
}

/// A window of a conversation's turns and how many it has.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TurnSlice {
    /// Turns threaded when read.
    pub total: u32,
    /// The turns of [`TurnWindow::range`]`(total)`, ascending and
    /// contiguous.
    pub turns: Vec<StoredTurn>,
}

/// Reads of stored conversations. Each call reads one snapshot.
pub trait ConversationReads {
    /// The conversations `query` admits, newest first (`ConversationId`
    /// descending); a keyset traversal returns each admitted conversation
    /// stored throughout it exactly once. The cursor binds the query: one
    /// presented with another query, or one this store did not issue, is
    /// `InvalidCursor`.
    fn list(
        &self,
        query: &ConversationQuery,
        page: &PageRequest<ConversationList>,
    ) -> impl Future<
        Output = Result<Page<StoredConversation, ConversationList>, ConversationReadError>,
    > + Send;

    /// The conversation stored under `id`; `None` for an unknown id.
    fn conversation(
        &self,
        id: ConversationId,
    ) -> impl Future<Output = Result<Option<StoredConversation>, ConversationReadError>> + Send;

    /// The conversations whose origin names `id` (forks of it and
    /// compactions of it), oldest first (`started_at`, then id); empty for
    /// an unknown id.
    fn successors(
        &self,
        id: ConversationId,
    ) -> impl Future<Output = Result<Vec<StoredConversation>, ConversationReadError>> + Send;

    /// How many turns `id` has, and the turns of `window` that exist
    /// (`TurnWindow::range`), each with its entries, read without loading
    /// the rest of the transcript. A window at or past the last turn is an
    /// empty slice carrying `total`. `None` for an unknown id.
    fn turns(
        &self,
        id: ConversationId,
        window: &TurnWindow,
    ) -> impl Future<Output = Result<Option<TurnSlice>, ConversationReadError>> + Send;

    /// For each exchange of `ids` that has been threaded, the turn it is:
    /// its conversation, its index and the agent the turn was attributed
    /// to, as recorded. Unthreaded and unknown ids are absent, so the
    /// map's keys are a subset of `ids`.
    fn locate(
        &self,
        ids: &IdBatch<ExchangeId>,
    ) -> impl Future<Output = Result<BTreeMap<ExchangeId, ExchangePlacement>, ConversationReadError>>
    + Send;

    /// The last turn of `parent` whose history lies wholly inside its first
    /// `shared_prefix` non-system messages: the greatest turn whose
    /// `history_end` is at most `shared_prefix`, where a fork of `parent`
    /// sharing that prefix leaves it. `None` when no turn qualifies (the
    /// prefix ends inside the first turn) or `parent` is unknown.
    fn branch_turn(
        &self,
        parent: ConversationId,
        shared_prefix: u32,
    ) -> impl Future<Output = Result<Option<TurnIndex>, ConversationReadError>> + Send;
}

/// Why a conversation read failed. Unknown ids are not errors.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConversationReadError {
    /// A store failure; a retry may succeed.
    Store { reason: String },
    /// A `list` cursor this store did not issue for this query.
    InvalidCursor,
}
