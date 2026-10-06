//! Conversations behind the generated traffic: every exchange the
//! transmissions name, threaded into conversations per agent, with the
//! messages each turn added, the spans each output holds, and where every
//! span was read.
//!
//! The traffic generator knows exchanges only as ids on content matches
//! (the reader's exchange) and spans only as locations. This overlay gives
//! each of them a turn ([`build`]):
//!
//! - a **reader turn** for each reader exchange: its inputs are the
//!   messages the reader's copies arrived in (`ContentMatch::read_at`, the
//!   bodies in [`super::blobs::Blobs`]), and its output a generated reply
//!   (or, for a `ReaderOutput` match, the reader's own output the copy was
//!   found in; the generator stores each such copy as its own message, so
//!   copies beyond the first are listed among the inputs as assistant
//!   messages);
//! - a **writer turn** for each originated span: its output is the
//!   message the span sits in, a little before the first read of it.
//!
//! Turns are grouped per agent into conversations by time (a gap of more
//! than three hours or 24 turns starts a new one; a delegated task starts
//! the child's), each opening with a system prompt and a user task. The
//! named cases the conversation view needs are made on top
//! ([`Cases`]): a fork, a compaction, an increment whose history the
//! gateway never saw, a system turn mid-conversation, a failed exchange, a
//! replayed corpus and a turn still waiting for its scan.
//!
//! Generated messages are real canonical messages ([`Message::new`]); the
//! bodies the traffic generator stored stay in `Blobs` (and stay dropped
//! where retention dropped them), so the evidence page and the
//! conversation view cut the same bytes.
//!
//! The overlay is data only: the reads over it are the fixture's
//! conversation queries.

mod build;
mod messages;

#[cfg(test)]
mod tests;

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

use crosstalk_spec::derived::provenance::span::{RelaySource, SpanLocation};
use crosstalk_spec::ids::{AgentId, ConversationId, ExchangeId, MessageHash, SpanId};
use crosstalk_spec::observed::client::{CorpusId, HarnessClaim, IngressMode};
use crosstalk_spec::observed::conversation::ConversationOrigin;
use crosstalk_spec::observed::exchange::{
    Continuation, ExchangeFailure, ModelName, StopReason, TokenUsage, Transport, WireProtocol,
};
use crosstalk_spec::observed::message::{Message, Role};
use crosstalk_spec::support::Timestamp;

pub use build::build;

/// One message of a turn, as the transcript keeps it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Entry {
    pub message: MessageHash,
    pub role: Role,
    /// A compaction's turn 0: the message is in the predecessor's history.
    pub carried_over: bool,
}

/// How an exchange ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Ending {
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

/// One exchange of a conversation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TurnRecord {
    pub exchange: ExchangeId,
    /// The agent the exchange was attributed to, as recorded.
    pub agent: AgentId,
    pub started_at: Timestamp,
    pub protocol: WireProtocol,
    pub transport: Transport,
    pub model: ModelName,
    pub harness: Option<HarnessClaim>,
    pub continuation: Continuation,
    pub ending: Ending,
    /// The messages new in this turn, in request order, any role.
    pub inputs: Vec<Entry>,
    /// The response, or a failed exchange's partial response.
    pub output: Option<MessageHash>,
}

/// One conversation and its turns, in threading order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConversationRecord {
    pub id: ConversationId,
    /// The agent of its first exchange, as recorded.
    pub agent: AgentId,
    pub origin: ConversationOrigin,
    pub ingress: IngressMode,
    pub turns: Vec<TurnRecord>,
}

impl ConversationRecord {
    /// Its non-system history: each turn's non-system inputs, then its
    /// output (a fork's shared prefix not included).
    pub fn history(&self) -> Vec<MessageHash> {
        let mut out = Vec::new();
        for turn in &self.turns {
            out.extend(
                turn.inputs
                    .iter()
                    .filter(|e| e.role != Role::System)
                    .map(|e| e.message),
            );
            out.extend(turn.output);
        }
        out
    }
}

/// An originated span: where it sits and who wrote it, as recorded
/// (`IndexedSpan`), with what L4 knows of it now.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SpanRecord {
    pub exchange: ExchangeId,
    pub author: AgentId,
    pub location: SpanLocation,
    pub indexed_at: Timestamp,
}

/// A relayed span in one output.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RelayedSpan {
    pub span: SpanId,
    pub location: SpanLocation,
    pub source: RelaySource,
}

/// Where the named cases are.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Cases {
    /// A fork and its parent.
    pub fork: (ConversationId, ConversationId),
    /// A compaction and its predecessor.
    pub compaction: (ConversationId, ConversationId),
    /// A Codex conversation whose first turn continues a response the
    /// gateway never saw.
    pub unseen_increment: ConversationId,
    /// A conversation with a system turn after its first turn, and that
    /// turn's index.
    pub mid_system: (ConversationId, u32),
    /// A conversation with a failed exchange, and that turn's index.
    pub failed: (ConversationId, u32),
    /// The corpus the replayed conversations came from, and one of them.
    pub replay: (CorpusId, ConversationId),
    /// The turn still waiting for its provenance scan.
    pub pending_scan: ExchangeId,
}

/// Every conversation of the fixture. Empty (the default) until
/// [`build`] runs.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Conversations {
    records: BTreeMap<ConversationId, ConversationRecord>,
    /// Each threaded exchange: its conversation and turn index.
    turns: HashMap<ExchangeId, (ConversationId, u32)>,
    spans: HashMap<SpanId, SpanRecord>,
    /// The originated spans of each exchange's output.
    spans_by_exchange: HashMap<ExchangeId, Vec<SpanId>>,
    /// Relayed spans by the exchange whose output holds them.
    relayed: HashMap<ExchangeId, Vec<RelayedSpan>>,
    /// The bodies of generated messages.
    bodies: Arc<HashMap<MessageHash, Message>>,
    /// Exchanges whose provenance scan has not finished.
    pending: HashSet<ExchangeId>,
    cases: Option<Cases>,
}

impl Conversations {
    pub fn records(&self) -> impl Iterator<Item = &ConversationRecord> {
        self.records.values()
    }

    pub fn get(&self, id: ConversationId) -> Option<&ConversationRecord> {
        self.records.get(&id)
    }

    /// The conversation and turn index of a threaded exchange.
    pub fn locate(&self, exchange: ExchangeId) -> Option<(ConversationId, u32)> {
        self.turns.get(&exchange).copied()
    }

    pub fn turn(&self, exchange: ExchangeId) -> Option<&TurnRecord> {
        let (conversation, index) = self.locate(exchange)?;
        self.records
            .get(&conversation)?
            .turns
            .get(usize::try_from(index).ok()?)
    }

    pub fn span(&self, id: SpanId) -> Option<&SpanRecord> {
        self.spans.get(&id)
    }

    /// The originated spans whose output is `exchange`'s.
    pub fn spans_of(&self, exchange: ExchangeId) -> impl Iterator<Item = (SpanId, &SpanRecord)> {
        self.spans_by_exchange
            .get(&exchange)
            .into_iter()
            .flatten()
            .filter_map(|id| self.spans.get(id).map(|record| (*id, record)))
    }

    pub fn relayed_in(&self, exchange: ExchangeId) -> &[RelayedSpan] {
        self.relayed.get(&exchange).map_or(&[], Vec::as_slice)
    }

    /// A generated message's body; `None` for a body the traffic generator
    /// stored (read it from `Blobs`).
    pub fn body(&self, hash: MessageHash) -> Option<&Message> {
        self.bodies.get(&hash)
    }

    pub fn is_pending(&self, exchange: ExchangeId) -> bool {
        self.pending.contains(&exchange)
    }

    /// Conversations whose origin names `id`, oldest first.
    pub fn successors(&self, id: ConversationId) -> Vec<&ConversationRecord> {
        let mut out: Vec<&ConversationRecord> = self
            .records
            .values()
            .filter(|record| match record.origin {
                ConversationOrigin::Root => false,
                ConversationOrigin::Fork { parent, .. } => parent == id,
                ConversationOrigin::Compaction { predecessor } => predecessor == id,
            })
            .collect();
        out.sort_by_key(|record| record.id);
        out
    }

    /// The named cases; `None` only before [`build`].
    pub fn cases(&self) -> Option<&Cases> {
        self.cases.as_ref()
    }

    /// The conversations as they stood at `cutoff`, for a replay: turns
    /// started after it are gone, and so are conversations left empty and
    /// spans of exchanges not yet seen.
    pub fn at(&self, cutoff: Timestamp) -> Conversations {
        let mut records = BTreeMap::new();
        let mut turns = HashMap::new();
        for record in self.records.values() {
            let kept: Vec<TurnRecord> = record
                .turns
                .iter()
                .filter(|turn| turn.started_at <= cutoff)
                .cloned()
                .collect();
            if kept.is_empty() {
                continue;
            }
            for (index, turn) in kept.iter().enumerate() {
                turns.insert(
                    turn.exchange,
                    (record.id, u32::try_from(index).unwrap_or(u32::MAX)),
                );
            }
            records.insert(
                record.id,
                ConversationRecord {
                    turns: kept,
                    ..record.clone()
                },
            );
        }
        let spans: HashMap<SpanId, SpanRecord> = self
            .spans
            .iter()
            .filter(|(_, span)| turns.contains_key(&span.exchange))
            .map(|(id, span)| (*id, *span))
            .collect();
        let spans_by_exchange = self
            .spans_by_exchange
            .iter()
            .filter(|(exchange, _)| turns.contains_key(exchange))
            .map(|(exchange, ids)| (*exchange, ids.clone()))
            .collect();
        let relayed = self
            .relayed
            .iter()
            .filter(|(exchange, _)| turns.contains_key(exchange))
            .map(|(exchange, spans)| (*exchange, spans.clone()))
            .collect();
        Conversations {
            records,
            turns,
            spans,
            spans_by_exchange,
            relayed,
            bodies: Arc::clone(&self.bodies),
            pending: self.pending.clone(),
            cases: self.cases.clone(),
        }
    }
}
