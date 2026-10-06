//! L4's records read back for the conversation view: each exchange's scan
//! status, the spans cut from its output (relayed and forwarded ones
//! included, with their current state), the content matches read in it,
//! and the readers of one span, paged.
//!
//! L4 keeps every span it classifies (`Originated`, `Relayed` with its
//! [`RelaySource`], `Common`) in its records, beside the content matches
//! by reader exchange and by origin span and the per-exchange scan status;
//! [`ProvenanceReads`] serves them. The span locations and authors by id
//! stay [`SpanIndex::spans`], which this trait extends rather than repeats.
//!
//! [`RelaySource`]: crate::derived::provenance::span::RelaySource

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::batch::IdBatch;
use crate::derived::provenance::matching::ContentMatch;
use crate::derived::provenance::span::Span;
use crate::ids::{ExchangeId, SpanId};
use crate::paging::{Page, PageRequest, SpanReaderList};
use crate::support::Timestamp;

use super::SpanIndex;

/// Where L4 stands with one exchange. On the wire adjacently tagged:
/// `{"type": "pending"}`, `{"type": "scanned", "data": {"at": ..}}`.
///
/// The exchange's spans and the matches read in it are committed in one
/// write, so its marks are complete in `Scanned` and `Indexed` and absent
/// before (`provenance.scan.status-after-commit`); readers of its output
/// spans keep arriving afterwards. An exchange L4 has not recorded reads
/// as `Pending`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "type",
    content = "data",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum ScanStatus {
    /// Not scanned yet: no span or match of it is committed.
    Pending,
    /// Spans and matches committed at `at`; the fingerprint index writes
    /// not confirmed yet.
    Scanned { at: Timestamp },
    /// Spans, matches and index writes all done; `at` is when the index
    /// writes were confirmed.
    Indexed { at: Timestamp },
    /// The scan cannot succeed (a body content retention dropped, a body
    /// that does not decode, records that disagree): nothing was committed.
    Failed {
        at: Timestamp,
        failure: ScanFailureKind,
    },
}

impl ScanStatus {
    /// Whether the exchange's spans and the matches read in it are all
    /// committed.
    pub fn marks_complete(&self) -> bool {
        matches!(self, Self::Scanned { .. } | Self::Indexed { .. })
    }
}

/// Why a scan failed for good. On the wire, snake_case strings.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ScanFailureKind {
    /// A body the delta lists is not in the blob store.
    BodyMissing,
    /// A stored body is not a canonical message encoding.
    BodyUndecodable,
    /// The delta names a different agent or output than the exchange.
    Inconsistent,
}

/// Where a forwarded span stands in the fingerprint index: a span relayed
/// from one of its agent's own inputs, indexed under that agent while its
/// state stays `Relayed` (`provenance.index.forwarded-indexed`). On the
/// wire adjacently tagged.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "type",
    content = "data",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum ForwardStatus {
    /// Committed; its postings not written yet.
    Pending,
    /// Its postings are in the index.
    Indexed { at: Timestamp },
    /// Past retention: no later reader will be detected.
    Expired {
        indexed_at: Timestamp,
        at: Timestamp,
    },
}

/// One stored span with its current state, as L4 keeps it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredSpan {
    /// Its id, location, recorded author, exchange and current state.
    pub span: Span,
    /// For a forwarded span committed with forwarding on, its indexing;
    /// `None` for every other span.
    pub forward: Option<ForwardStatus>,
}

/// One page of a span's readers and how many it has in all.
#[derive(Debug, Clone, PartialEq)]
pub struct ReaderPage {
    /// Every match whose origin is the span, when read.
    pub total: u32,
    pub page: Page<ContentMatch, SpanReaderList>,
}

/// L4's records, read back. Each call reads one snapshot.
pub trait ProvenanceReads: SpanIndex {
    /// For each exchange of `exchanges` L4 recorded, its output's
    /// non-`Common` spans (originated, relayed and forwarded), in output
    /// order, each with its current state. An exchange not recorded is
    /// absent; a recorded one with no such span maps to an empty list.
    fn output_spans(
        &self,
        exchanges: &IdBatch<ExchangeId>,
    ) -> impl Future<Output = Result<BTreeMap<ExchangeId, Vec<StoredSpan>>, ProvenanceReadError>> + Send;

    /// For each exchange of `exchanges` L4 recorded, every content match
    /// whose `reader_exchange` it is (read in its inputs, or in its output
    /// for `ReaderOutput`), in scan order. Absent when not recorded.
    fn matches_read_in(
        &self,
        exchanges: &IdBatch<ExchangeId>,
    ) -> impl Future<Output = Result<BTreeMap<ExchangeId, Vec<ContentMatch>>, ProvenanceReadError>> + Send;

    /// The matches whose origin is `span`, newest reader exchange first
    /// (the reader exchange's start, then the match, descending), and their
    /// total; a keyset traversal returns each exactly once. `None` when L4
    /// keeps no span `span`. The cursor binds the span: one presented for
    /// another span, or one this store did not issue, is `InvalidCursor`.
    fn readers(
        &self,
        span: SpanId,
        page: &PageRequest<SpanReaderList>,
    ) -> impl Future<Output = Result<Option<ReaderPage>, ProvenanceReadError>> + Send;

    /// For each exchange of `exchanges` L4 recorded, its scan status.
    /// Absent when not recorded (which a reader shows as `Pending`).
    fn scan_status(
        &self,
        exchanges: &IdBatch<ExchangeId>,
    ) -> impl Future<Output = Result<BTreeMap<ExchangeId, ScanStatus>, ProvenanceReadError>> + Send;
}

/// Why a provenance read failed. Unknown ids are not errors.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProvenanceReadError {
    /// A store failure; a retry may succeed.
    Store { reason: String },
    /// A `readers` cursor this store did not issue for this span.
    InvalidCursor,
}
