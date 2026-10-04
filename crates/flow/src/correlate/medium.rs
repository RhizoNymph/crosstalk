//! One medium's evidence: its pairable writes and its reads, the channel
//! transmissions their co-accesses opened, and the tool-result matches its
//! reads carried. A shard holds many media; a handoff moves one whole.

use std::collections::BTreeMap;

use crosstalk_spec::derived::flow::access::Access;
use crosstalk_spec::derived::flow::evidence::CoAccess;
use crosstalk_spec::derived::flow::transmission::Confirmed;
use crosstalk_spec::derived::provenance::matching::ContentMatch;
use crosstalk_spec::ids::{AccessId, AgentId, ExchangeId, MessageHash, SpanId, TransmissionId};
use crosstalk_spec::support::Timestamp;

use super::pairing;

/// A channel transmission's identity within its medium: the reader
/// exchange and the sender (the writer). Its route is the medium's.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct Ident {
    pub(crate) exchange: ExchangeId,
    pub(crate) sender: AgentId,
}

impl Ident {
    pub(crate) fn of_match(content: &ContentMatch) -> Self {
        Self {
            exchange: content.reader_exchange(),
            sender: content.origin_agent(),
        }
    }
}

/// A content match's identity, for deduplication and a stable order:
/// what was found, by whom, where.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct MatchKey {
    origin: SpanId,
    reader: AgentId,
    exchange: ExchangeId,
    message: MessageHash,
    index: u16,
    start: u32,
    end: u32,
}

impl MatchKey {
    pub(crate) fn of(content: &ContentMatch) -> Self {
        let read_at = content.read_at();
        Self {
            origin: content.origin(),
            reader: content.reader(),
            exchange: content.reader_exchange(),
            message: read_at.part.message,
            index: read_at.part.index,
            start: read_at.range.start(),
            end: read_at.range.end(),
        }
    }

    pub(crate) fn exchange(&self) -> ExchangeId {
        self.exchange
    }
}

/// Where a channel transmission is.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Phase {
    Awaiting { closes_at: Timestamp },
    Suspected { since: Timestamp },
    Confirmed(Box<Confirmed>),
}

/// An open channel transmission.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Channeled {
    pub(crate) to: AgentId,
    pub(crate) opened_at: Timestamp,
    /// Every co-access backing it, by (write, read).
    pub(crate) co_access: BTreeMap<(AccessId, AccessId), CoAccess>,
    pub(crate) phase: Phase,
}

impl Channeled {
    /// Its co-accesses in a stable order.
    pub(crate) fn co_accesses(&self) -> Vec<CoAccess> {
        self.co_access.values().copied().collect()
    }
}

/// A tool-result match carried by one of the medium's reads.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Held {
    pub(crate) content: ContentMatch,
    /// The carrying read's time: its reader exchange's start.
    pub(crate) read_at: Timestamp,
}

/// One medium's evidence.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Medium {
    pub(crate) writes: BTreeMap<AccessId, Access>,
    pub(crate) reads: BTreeMap<AccessId, Access>,
    pub(crate) open: BTreeMap<(Ident, TransmissionId), Channeled>,
    /// Per identity: how many of its transmissions were discarded, and
    /// when the last one was.
    pub(crate) retired: BTreeMap<Ident, (u32, Timestamp)>,
    pub(crate) held: BTreeMap<MatchKey, Held>,
}

impl Medium {
    pub(crate) fn is_empty(&self) -> bool {
        self.writes.is_empty()
            && self.reads.is_empty()
            && self.open.is_empty()
            && self.retired.is_empty()
            && self.held.is_empty()
    }

    /// The open transmission of `ident`, if any: the earliest opened one
    /// when a handoff merged two.
    pub(crate) fn open_of(&self, ident: Ident) -> Option<TransmissionId> {
        self.open
            .range(
                (ident, TransmissionId::from_ulid(0))
                    ..=(ident, TransmissionId::from_ulid(u128::MAX)),
            )
            .min_by_key(|(_, tx)| tx.opened_at)
            .map(|((_, id), _)| *id)
    }

    /// Whether `content` came through one of the writes backing `tx`.
    pub(crate) fn explains(&self, tx: &Channeled, content: &ContentMatch) -> bool {
        tx.co_access.values().any(|co| {
            self.writes
                .get(&co.write())
                .is_some_and(|write| pairing::links(content, write))
        })
    }
}
