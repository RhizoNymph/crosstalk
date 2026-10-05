//! L4's records: the exchanges it has seen and their scan status, the
//! spans it cut, and the content matches it made. No text: ids, hashes,
//! locations, states and times only.
//!
//! The spec has no trait for these yet (the evidence page reads "L4's span
//! records", and a later `SpanIndex` / `ProvenanceReads` will serve span
//! locations, scan status and matches), so [`ProvenanceStore`] is this
//! crate's. [`MemoryProvenanceStore`] keeps them in memory (tests, the
//! simulation); [`PgProvenanceStore`] in the `provenance` schema.
//!
//! **Scan status.** An exchange is recorded from `ExchangeCaptured` as
//! [`ScanStatus::Pending`]. Its delta's scan commits its spans and matches
//! in one write ([`ProvenanceStore::commit_scan`], status `Scanned`); the
//! originated spans' postings and every observation then go to the
//! fingerprint index, and [`ProvenanceStore::mark_indexed`] advances the
//! originated spans to `Indexed` and the exchange to `Indexed`. A scan that
//! cannot succeed on redelivery is `Failed`. Redelivery of a scanned delta
//! republishes the stored outcome instead of scanning again.
//!
//! **Span states** change only through `SpanState::advance`
//! (`provenance.span-state.legal-transitions`). A forwarded span
//! (`Relayed` from an input) stays `Relayed`; its indexing is the record's
//! [`Forwarding`], advanced by `mark_indexed` and `expire` alongside the
//! originated spans' states.

mod memory;
mod pg;

use std::future::Future;

use crosstalk_spec::derived::provenance::matching::ContentMatch;
use crosstalk_spec::derived::provenance::span::{IllegalTransition, Span, SpanState};
use crosstalk_spec::ids::{AgentId, EventId, ExchangeId, MessageHash, SpanId};
use crosstalk_spec::support::Timestamp;

pub use self::memory::MemoryProvenanceStore;
pub use self::pg::{MIGRATIONS, PgProvenanceStore, migrate};

/// What L4 keeps of an `ExchangeCaptured`: when it started and which
/// messages it sent and received.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExchangeRecord {
    pub id: ExchangeId,
    pub started_at: Timestamp,
    /// The request's messages in order: the full history, or the increment.
    pub request: Vec<MessageHash>,
    /// The response, or a failed exchange's partial response.
    pub output: Option<MessageHash>,
}

/// Why a scan failed for good.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScanFailure {
    /// A body the delta lists is not in the blob store (content retention
    /// dropped it).
    BodyMissing(MessageHash),
    /// A stored body is not a canonical message encoding.
    BodyUndecodable(MessageHash),
    /// The delta names a different agent or output than the exchange.
    Inconsistent { reason: String },
}

/// Where an exchange's scan stands.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScanStatus {
    /// Recorded from `ExchangeCaptured`; its delta not processed yet.
    Pending,
    /// Spans and matches committed; the index writes not yet confirmed.
    Scanned { at: Timestamp },
    /// Spans, matches and index writes all done.
    Indexed { at: Timestamp },
    /// The scan cannot succeed; nothing was committed.
    Failed { at: Timestamp, failure: ScanFailure },
}

/// How a message was scanned in an exchange.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum ScannedAs {
    /// Listed in the delta's `new_inputs`.
    Input,
    /// The delta's `new_system`.
    System,
    /// The delta's `output`.
    Output,
}

/// One message's scan in one exchange.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MessageScan {
    pub message: MessageHash,
    pub exchange: ExchangeId,
    pub scanned_as: ScannedAs,
    pub status: ScanStatus,
}

/// Where a forwarded span stands in the fingerprint index.
///
/// A span classified `Relayed(RelaySource::Input(_))` keeps that state for
/// good (`SpanState::advance` has no edge out of it), yet its text is
/// indexed under the forwarding agent (`provenance.index.forwarded-indexed`).
/// This is the record of that indexing, the forwarded counterpart of
/// `Originated` → `Indexed` → `Expired`. Hits are not counted on it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Forwarding {
    /// Committed; its postings not written yet.
    Pending,
    /// Its postings are in the index.
    Indexed { at: Timestamp },
    /// Past retention: its postings were evicted.
    Expired {
        indexed_at: Timestamp,
        at: Timestamp,
    },
}

/// A stored span: the spec's span, its position in its exchange's output,
/// and the order it was indexed in.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpanRecord {
    pub span: Span,
    /// Position among its exchange's spans (output order).
    pub ordinal: u32,
    /// Set when the span became `Indexed` (or, forwarded, its forwarding
    /// `Indexed`): increasing in indexing order.
    pub index_seq: Option<u64>,
    /// For a forwarded span (`SpanState::is_forwarded`) committed with
    /// forwarding on, its indexing; `None` for every other span.
    pub forward: Option<Forwarding>,
}

impl SpanRecord {
    /// The record `commit_scan` writes for `span`, not indexed yet: a
    /// forwarded span's forwarding is pending when `forwarding` is on, and
    /// absent (the span is never indexed) when it is off.
    pub fn committed(span: Span, ordinal: u32, forwarding: bool) -> Self {
        let forward = (forwarding && span.state.is_forwarded()).then_some(Forwarding::Pending);
        Self {
            span,
            ordinal,
            index_seq: None,
            forward,
        }
    }

    /// When the span's postings were written, while they are in the index:
    /// an `Indexed` or `Propagated` span, or a forwarded span whose
    /// forwarding is `Indexed`. `None` otherwise.
    pub fn indexed_at(&self) -> Option<Timestamp> {
        match (&self.span.state, self.forward) {
            (SpanState::Indexed { at }, _) | (SpanState::Propagated { indexed_at: at, .. }, _) => {
                Some(*at)
            }
            (_, Some(Forwarding::Indexed { at })) => Some(at),
            _ => None,
        }
    }
}

/// A copy of an indexed span in another output: a span classified
/// `Relayed(RelaySource::Span(source))`, the agent whose output holds it,
/// and its exchange's start. The spread rule counts copies as
/// originations (`provenance.match.cross-agent-spread`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Relay {
    pub source: SpanId,
    pub agent: AgentId,
    pub at: Timestamp,
}

/// A stored content match. Its id is its envelope's.
#[derive(Debug, Clone, PartialEq)]
pub struct StoredMatch {
    pub id: EventId,
    /// Position among its reader exchange's matches.
    pub ordinal: u32,
    /// The reader exchange's time.
    pub at: Timestamp,
    pub content: ContentMatch,
}

/// Everything one scan decided, written at once.
#[derive(Debug, Clone, PartialEq)]
pub struct ScanCommit {
    pub exchange: ExchangeId,
    pub agent: AgentId,
    pub at: Timestamp,
    /// Classified spans (`Originated`, `Relayed` or `Common`), in output
    /// order.
    pub spans: Vec<Span>,
    /// The matches, in scan order.
    pub matches: Vec<StoredMatch>,
    /// Every message the delta listed and how it was scanned.
    pub messages: Vec<(MessageHash, ScannedAs)>,
    /// Whether forwarded spans are indexed (`ProvenanceConfig::forwarding`):
    /// their forwarding is recorded pending only then.
    pub forwarding: bool,
}

/// What a commit did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Committed {
    Written,
    /// The exchange was already scanned; nothing changed.
    AlreadyScanned,
}

/// Why a store call failed.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ProvenanceStoreError {
    #[error("the provenance store is unavailable: {reason}")]
    Unavailable { reason: String },
    #[error("a stored provenance record is malformed: {reason}")]
    Corrupt { reason: String },
    /// The database refused a statement (a constraint, a server error).
    #[error("the provenance store refused a write: {reason}")]
    Rejected { reason: String },
    #[error("exchange {exchange:?} is not recorded")]
    UnknownExchange { exchange: ExchangeId },
    #[error("span transition refused: {0:?}")]
    Transition(IllegalTransition),
}

impl ProvenanceStoreError {
    /// Whether retrying later can succeed.
    pub fn is_transient(&self) -> bool {
        matches!(self, Self::Unavailable { .. })
    }
}

/// L4's records. Every time is an argument.
pub trait ProvenanceStore {
    /// Record an exchange from `ExchangeCaptured`. Idempotent: a recorded
    /// exchange keeps its record and status.
    fn record_exchange(
        &mut self,
        record: ExchangeRecord,
    ) -> impl Future<Output = Result<(), ProvenanceStoreError>> + Send;

    /// The exchange's record and scan status; `None` when not recorded.
    /// After [`ProvenanceStore::prune`], `request` is empty.
    fn exchange(
        &self,
        id: ExchangeId,
    ) -> impl Future<Output = Result<Option<(ExchangeRecord, ScanStatus)>, ProvenanceStoreError>> + Send;

    /// The stored spans among `ids` (unknown ids left out), in `ids` order.
    fn spans(
        &self,
        ids: &[SpanId],
    ) -> impl Future<Output = Result<Vec<SpanRecord>, ProvenanceStoreError>> + Send;

    /// Every stored span relayed from one of `sources`
    /// (`Relayed(RelaySource::Span(s))`), with its agent and its
    /// exchange's start, by source then time.
    fn relays(
        &self,
        sources: &[SpanId],
    ) -> impl Future<Output = Result<Vec<Relay>, ProvenanceStoreError>> + Send;

    /// One stored span: its exchange, message, part and range.
    fn span(
        &self,
        id: SpanId,
    ) -> impl Future<Output = Result<Option<SpanRecord>, ProvenanceStoreError>> + Send;

    /// An exchange's spans, in output order.
    fn exchange_spans(
        &self,
        exchange: ExchangeId,
    ) -> impl Future<Output = Result<Vec<SpanRecord>, ProvenanceStoreError>> + Send;

    /// An exchange's matches as reader, in scan order.
    fn exchange_matches(
        &self,
        exchange: ExchangeId,
    ) -> impl Future<Output = Result<Vec<StoredMatch>, ProvenanceStoreError>> + Send;

    /// Matches read in `message`, by part and range.
    fn matches_in_message(
        &self,
        message: MessageHash,
    ) -> impl Future<Output = Result<Vec<StoredMatch>, ProvenanceStoreError>> + Send;

    /// Matches of the origin span `span`, oldest reader first.
    fn matches_of_span(
        &self,
        span: SpanId,
    ) -> impl Future<Output = Result<Vec<StoredMatch>, ProvenanceStoreError>> + Send;

    /// Every scan of `message`, by exchange.
    fn message_scans(
        &self,
        message: MessageHash,
    ) -> impl Future<Output = Result<Vec<MessageScan>, ProvenanceStoreError>> + Send;

    /// The highest index sequence assigned so far (0 when none).
    fn index_watermark(&self) -> impl Future<Output = Result<u64, ProvenanceStoreError>> + Send;

    /// Write a scan's spans and matches, advance each match's origin span
    /// by a hit at the reader's time (a refused hit leaves the span as it
    /// is), and mark the exchange `Scanned`. Nothing changes when the
    /// exchange is already scanned, indexed or failed.
    fn commit_scan(
        &mut self,
        commit: ScanCommit,
    ) -> impl Future<Output = Result<Committed, ProvenanceStoreError>> + Send;

    /// Advance the exchange's `Originated` spans to `Indexed { at }` and
    /// its pending forwarded spans to `Forwarding::Indexed { at }`,
    /// assigning index sequences in output order, and mark the exchange
    /// `Indexed`. Idempotent.
    fn mark_indexed(
        &mut self,
        exchange: ExchangeId,
        at: Timestamp,
    ) -> impl Future<Output = Result<(), ProvenanceStoreError>> + Send;

    /// Mark a pending exchange's scan failed for good.
    fn mark_failed(
        &mut self,
        exchange: ExchangeId,
        at: Timestamp,
        failure: ScanFailure,
    ) -> impl Future<Output = Result<(), ProvenanceStoreError>> + Send;

    /// Indexed or propagated spans, and indexed forwarded spans, whose
    /// retention has run out at `now` (`indexed_at + retention < now`),
    /// oldest first, at most `limit`.
    fn expiring(
        &self,
        now: Timestamp,
        retention_micros: u64,
        limit: usize,
    ) -> impl Future<Output = Result<Vec<SpanId>, ProvenanceStoreError>> + Send;

    /// Advance `spans` to `Expired { at }` (a forwarded span's forwarding
    /// to `Forwarding::Expired`). Call only after their derived data is
    /// evicted from every index (`provenance.match.none-after-expiry`).
    fn expire(
        &mut self,
        spans: &[SpanId],
        at: Timestamp,
    ) -> impl Future<Output = Result<(), ProvenanceStoreError>> + Send;

    /// Forget the request lists of exchanges started before `before` (their
    /// status and records stay).
    fn prune(
        &mut self,
        before: Timestamp,
    ) -> impl Future<Output = Result<(), ProvenanceStoreError>> + Send;
}
