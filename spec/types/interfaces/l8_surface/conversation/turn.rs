//! One window of a conversation's turns, with structure and provenance
//! marks and no message text: [`TurnPage`].
//!
//! A [`Turn`] is one of the conversation's own exchanges. Its `inputs`
//! then its `output` are exactly the transcript entries L3 holds for the
//! exchange, in ordinal order, system ones included
//! (`surface.conversation.inputs-are-transcript`): the request's messages
//! new to the conversation in the order the request held them, any role,
//! then the response. Each message lists its parts' shapes, and each part
//! the marks L4 made on it:
//!
//! - [`Inbound`]: another agent's text read here, one per content match
//!   whose `read_at` is the part (on an output part, the `ReaderOutput`
//!   matches), with the transmission holding it, if one does.
//! - [`OutputSpan`]: on an output part, each non-`Common` span cut from
//!   it, originated, forwarded or relayed, with its current state and, for
//!   an indexed one, who read it later ([`ReadBy`]).
//!
//! Marks are complete once the turn's [`ScanStatus`] says so
//! ([`ScanStatus::marks_complete`]); readers of output spans keep arriving
//! afterwards.
//!
//! **Delegation** has no type of its own: L5's route precedence puts
//! `Delegation` first, so a sub-agent's task prompt read by the child is a
//! [`Reader`] whose transmission route is `Delegation(ParentToChild)` on the
//! parent's tool-call span, and the child's answer coming back is an
//! [`Inbound`] with `Delegation(ChildToParent)` on the parent's tool result.

use std::num::NonZeroU32;

use serde::{Deserialize, Serialize};

use crate::derived::flow::transmission::Route;
use crate::derived::provenance::matching::{Carrier, MatchKind};
use crate::derived::provenance::span::{SpanLocation, SpanState};
use crate::ids::{AgentId, ConversationId, ExchangeId, MessageHash, SpanId, TransmissionId};
use crate::interfaces::l4_provenance::reads::{ForwardStatus, ScanStatus};
use crate::interfaces::l8_surface::summary::TransmissionStateKind;
use crate::observed::client::{HarnessClaim, IngressMode};
use crate::observed::exchange::{
    ConnectionId, ExchangeFailure, ModelName, StopReason, TokenUsage, Transport, WireProtocol,
};
use crate::observed::message::{MediaKind, Role, ToolCallId, ToolExecution, ToolName, ToolOutcome};
use crate::support::{ByteRange, Timestamp};
use crate::wire::Rejected;

use super::{SpanPoint, TurnIndex, TurnPoint};

/// The turns one `TurnWindow` names.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct TurnPage {
    pub conversation: ConversationId,
    /// Turns threaded when read. `turns` covers
    /// `window.from .. min(window.from + window.size, total)`
    /// (`surface.conversation.turns-window`).
    pub total: u32,
    pub turns: Vec<Turn>,
}

/// One exchange of the conversation: what was new in its request, its
/// output, and where that text came from and went.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct Turn {
    pub index: TurnIndex,
    pub exchange: ExchangeId,
    /// Canonical agent of the exchange's attributed agent (it can differ
    /// from the head's after a merge threaded an alias's exchange here).
    pub agent: AgentId,
    pub started_at: Timestamp,
    pub protocol: WireProtocol,
    pub transport: Transport,
    pub model: ModelName,
    /// `ClientContext::harness`: what the request said about itself. A
    /// claim, never identity.
    pub harness: Option<HarnessClaim>,
    /// `ClientContext::ingress`: how the exchange reached the gateway
    /// (reverse proxy route, forward proxy host, or replay corpus).
    pub ingress: IngressMode,
    pub continuation: TurnContinuation,
    pub outcome: TurnOutcome,
    /// The request's messages new to the conversation, in request order,
    /// any role: user turns, tool results, and every system message the
    /// transcript records for this exchange, where the request placed it.
    /// A compaction's first turn also lists its carried-over messages,
    /// placed `CarriedOver`.
    pub inputs: Vec<TurnMessage>,
    /// The response, or a failed exchange's partial response
    /// (`surface.conversation.output-is-response`).
    pub output: Option<TurnMessage>,
    /// Whether L4 has finished with the exchange.
    pub provenance: ScanStatus,
}

/// Whether the request carried its whole history.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "type",
    content = "data",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum TurnContinuation {
    FullHistory,
    /// A WebSocket (or `previous_response_id`) turn that sent only its
    /// increment.
    Increment {
        connection: Option<ConnectionId>,
        history: IncrementHistory,
    },
}

/// What an increment continued (`surface.conversation.increment-unseen`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IncrementHistory {
    /// The previous response resolved to a stored conversation.
    Resolved,
    /// The gateway never saw the previous response (it went around the
    /// proxy, or under another scope): threaded as a new conversation
    /// holding only the increment.
    Unseen,
}

/// How the exchange ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "type",
    content = "data",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum TurnOutcome {
    Completed {
        finished_at: Timestamp,
        stop: StopReason,
        usage: Option<TokenUsage>,
    },
    Failed {
        failed_at: Timestamp,
        failure: ExchangeFailure,
    },
}

/// One message of a turn: its role, where it sits, and its parts.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct TurnMessage {
    pub hash: MessageHash,
    pub role: Role,
    pub placement: MessagePlacement,
    pub parts: MessageParts,
}

/// Where a turn's message sits.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MessagePlacement {
    /// New to the conversation in this turn.
    New,
    /// In a compaction's first request and in the predecessor's stored
    /// history (`surface.conversation.carried-over`).
    CarriedOver,
    /// The turn's output.
    Output,
}

/// A message's parts, or the marks of a body content retention dropped.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "type",
    content = "data",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum MessageParts {
    /// One per part, in `PartRef::index` order.
    Shown(Vec<PartShape>),
    /// The body is no longer stored, so its parts are unknown; the marks
    /// L4 made on it are still listed, by part index ascending
    /// (`surface.conversation.body-dropped`).
    BodyDropped(Vec<PartMarks>),
}

/// One part's structure and marks. No text.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct PartShape {
    pub index: u16,
    pub kind: PartKind,
    /// Length in bytes of `Message::part_text`; `None` for a part with no
    /// text.
    pub text_bytes: Option<u32>,
    /// Content matches read here, ordered by range start: on an input
    /// part, every match whose `read_at` is this part; on an output part,
    /// the `ReaderOutput` matches.
    pub inbound: Vec<Inbound>,
    /// On an output part: its non-`Common` spans, ordered by range start.
    pub spans: Vec<OutputSpan>,
}

/// The marks of one part of a dropped body.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct PartMarks {
    pub index: u16,
    pub inbound: Vec<Inbound>,
    pub spans: Vec<OutputSpan>,
}

/// What a part is.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "type",
    content = "data",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum PartKind {
    Text,
    /// Visible reasoning has text; opaque reasoning has none.
    Reasoning {
        visible: bool,
    },
    ToolCall {
        call: ToolCallId,
        name: ToolName,
        execution: ToolExecution,
    },
    /// A tool message's result, or a server-executed tool's result inside
    /// an assistant message.
    ToolResult {
        call: ToolCallId,
        outcome: ToolOutcome,
    },
    Media(MediaKind),
    /// The provider's block type of an unrecognized block.
    Unknown {
        kind: String,
    },
}

/// Text another agent originated, found in this part.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct Inbound {
    /// `ContentMatch::read_at().range`, in this part's text.
    pub range: ByteRange,
    pub matched_bytes: NonZeroU32,
    pub kind: MatchKind,
    pub carrier: Carrier,
    /// The origin span: its author, exchange and turn.
    pub origin: SpanPoint,
    /// The stored transmission holding this match, if one does.
    pub transmission: Option<TransmissionMark>,
}

/// The transmission holding a match, enough to link its evidence page,
/// which reads every state (`surface.evidence.every-state`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct TransmissionMark {
    pub id: TransmissionId,
    /// `Route::resolved`: a channel through supersession; a delegation is
    /// `Delegation(direction)`.
    pub route: Route,
    pub state: TransmissionStateKind,
}

/// One non-`Common` span of an output part.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct OutputSpan {
    pub span: SpanId,
    pub range: ByteRange,
    pub origin: SpanOrigin,
}

/// Where an output span's text came from, with its current state.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "type",
    content = "data",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum SpanOrigin {
    /// This agent wrote it.
    Originated {
        status: OriginatedStatus,
        read_by: ReadBy,
    },
    /// Copied from one of this agent's own inputs that is not a span (a
    /// fetched page, a user turn) and indexed under this agent, so a later
    /// reader of the forwarded text matches it
    /// (`provenance.index.forwarded-indexed`).
    Forwarded {
        input: MessageHash,
        status: ForwardStatus,
        read_by: ReadBy,
    },
    /// Copied from an earlier span (another agent's or its own), or from
    /// an input that is not a span while forwarding is off. Never indexed
    /// under this agent.
    Relayed(RelayedFrom),
}

/// An originated span's state (`SpanState` from `Originated` on).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "type",
    content = "data",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum OriginatedStatus {
    /// Classified, fingerprints not written yet.
    Pending,
    Indexed {
        at: Timestamp,
    },
    Propagated {
        indexed_at: Timestamp,
        first_hit_at: Timestamp,
        hits: NonZeroU32,
    },
    /// Past retention: no later reader will be detected.
    Expired {
        at: Timestamp,
    },
}

impl OriginatedStatus {
    /// The status of a span in `state`; `None` unless it is originated.
    pub fn of(state: &SpanState) -> Option<Self> {
        match state {
            SpanState::Originated => Some(Self::Pending),
            SpanState::Indexed { at } => Some(Self::Indexed { at: *at }),
            SpanState::Propagated {
                indexed_at,
                first_hit_at,
                hits,
            } => Some(Self::Propagated {
                indexed_at: *indexed_at,
                first_hit_at: *first_hit_at,
                hits: *hits,
            }),
            SpanState::Expired { at } => Some(Self::Expired { at: *at }),
            SpanState::Extracted | SpanState::Common | SpanState::Relayed { .. } => None,
        }
    }
}

/// What a relayed span copied.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "type",
    content = "data",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum RelayedFrom {
    /// An indexed span, located.
    Span(SpanPoint),
    /// An input message that is not an indexed span. The UI looks for it
    /// among this conversation's turns.
    Input(MessageHash),
}

/// Readers of one indexed span: up to [`ReadBy::INLINE`] of them, newest
/// reader exchange first, and how many there are. The rest are
/// `QueryApi::span_readers` (`surface.conversation.read-by`).
///
/// Built only through [`ReadBy::new`]; decoding applies it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", try_from = "RawReadBy")]
pub struct ReadBy {
    first: Vec<Reader>,
    total: u32,
}

/// Why a [`ReadBy`] cannot be built.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InvalidReadBy {
    /// More than [`ReadBy::INLINE`] readers inline.
    TooManyInline { max: usize, got: usize },
    /// Fewer readers in all than inline.
    TotalBelowInline { total: u32, inline: usize },
    /// Fewer readers inline than `min(total, INLINE)`.
    InlineShort { total: u32, inline: usize },
}

#[derive(Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
struct RawReadBy {
    first: Vec<Reader>,
    total: u32,
}

impl TryFrom<RawReadBy> for ReadBy {
    type Error = Rejected<InvalidReadBy>;

    fn try_from(raw: RawReadBy) -> Result<Self, Self::Error> {
        Self::new(raw.first, raw.total).map_err(|error| Rejected::new("read by", error))
    }
}

impl ReadBy {
    /// How many readers a turn carries inline per span.
    pub const INLINE: usize = 8;

    /// No readers.
    pub fn none() -> Self {
        Self {
            first: Vec::new(),
            total: 0,
        }
    }

    /// `first` of `total` readers: exactly `min(total, INLINE)` of them.
    pub fn new(first: Vec<Reader>, total: u32) -> Result<Self, InvalidReadBy> {
        let inline = first.len();
        if inline > Self::INLINE {
            return Err(InvalidReadBy::TooManyInline {
                max: Self::INLINE,
                got: inline,
            });
        }
        let total_usize = usize::try_from(total).unwrap_or(usize::MAX);
        if total_usize < inline {
            return Err(InvalidReadBy::TotalBelowInline { total, inline });
        }
        if inline < total_usize.min(Self::INLINE) {
            return Err(InvalidReadBy::InlineShort { total, inline });
        }
        Ok(Self { first, total })
    }

    /// The newest readers, at most [`ReadBy::INLINE`].
    pub fn first(&self) -> &[Reader] {
        &self.first
    }

    /// Every reader there is.
    pub fn total(&self) -> u32 {
        self.total
    }

    /// Readers not inline: what `span_readers` has beyond `first`.
    pub fn more(&self) -> u32 {
        self.total
            .saturating_sub(u32::try_from(self.first.len()).unwrap_or(u32::MAX))
    }
}

/// One reader of a span: one content match whose origin it is.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct Reader {
    /// Canonical `ContentMatch::reader`.
    pub agent: AgentId,
    pub exchange: ExchangeId,
    /// `None` while the reader exchange is not threaded.
    pub turn: Option<TurnPoint>,
    pub read_at: SpanLocation,
    pub carrier: Carrier,
    pub kind: MatchKind,
    pub transmission: Option<TransmissionMark>,
}
