//! The spec's L4 reads over L4's records: [`ProvenanceReads`] (scan
//! status, output spans of every origin with their state, matches by
//! reader exchange, a span's readers, paged) and, on Postgres,
//! [`SpanIndex`] over the committed spans.
//!
//! Both stores answer through the same functions over [`ProvenanceStore`],
//! so they agree by construction; the stores differ only in how they read
//! their records.
//!
//! **Readers' cursors** are `<micros>-<match id>_<tag>`: the reader
//! exchange's start and the match id of the last reader served (the
//! list's key, newest first), and a keyed BLAKE3 tag over them and the
//! span, so a cursor issued for another span, or not by a store with this
//! key, is `InvalidCursor`.

use std::collections::BTreeMap;

use crosstalk_spec::batch::IdBatch;
use crosstalk_spec::derived::provenance::matching::ContentMatch;
use crosstalk_spec::derived::provenance::span::{Origin, OriginatedSpan, SpanState};
use crosstalk_spec::ids::{EventId, ExchangeId, SpanId};
use crosstalk_spec::interfaces::l4_provenance::reads::{
    ForwardStatus, ProvenanceReadError, ProvenanceReads, ReaderPage, ScanFailureKind,
    ScanStatus as SpecScanStatus, StoredSpan,
};
use crosstalk_spec::interfaces::l4_provenance::{IndexedSpan, SpanIndex, SpanIndexError};
use crosstalk_spec::paging::{Cursor, Page, PageRequest, SpanReaderList};
use crosstalk_spec::support::{Blake3, NonEmpty, Timestamp};

use super::{
    Forwarding, MemoryProvenanceStore, PgProvenanceStore, ProvenanceStore, ProvenanceStoreError,
    ScanFailure, ScanStatus, SpanRecord, StoredMatch,
};

/// The key readers' cursors are tagged with: they bind a span and a
/// position in a list View already shows, so the key keeps cursors from
/// one span off another, not secrets.
const CURSOR_KEY: &[u8] = b"crosstalk.provenance.readers.v1";

fn read_failed(error: ProvenanceStoreError) -> ProvenanceReadError {
    ProvenanceReadError::Store {
        reason: error.to_string(),
    }
}

/// The spec's scan status of a stored one.
pub fn scan_status(status: &ScanStatus) -> SpecScanStatus {
    match status {
        ScanStatus::Pending => SpecScanStatus::Pending,
        ScanStatus::Scanned { at } => SpecScanStatus::Scanned { at: *at },
        ScanStatus::Indexed { at } => SpecScanStatus::Indexed { at: *at },
        ScanStatus::Failed { at, failure } => SpecScanStatus::Failed {
            at: *at,
            failure: match failure {
                ScanFailure::BodyMissing(_) => ScanFailureKind::BodyMissing,
                ScanFailure::BodyUndecodable(_) => ScanFailureKind::BodyUndecodable,
                ScanFailure::Inconsistent { .. } => ScanFailureKind::Inconsistent,
            },
        },
    }
}

fn forward_status(forwarding: Forwarding) -> ForwardStatus {
    match forwarding {
        Forwarding::Pending => ForwardStatus::Pending,
        Forwarding::Indexed { at } => ForwardStatus::Indexed { at },
        Forwarding::Expired { indexed_at, at } => ForwardStatus::Expired { indexed_at, at },
    }
}

/// Whether a span is one `SpanIndex` keeps a record of: originated, or
/// forwarded with forwarding on.
fn indexed(record: &SpanRecord) -> bool {
    record.span.state.origin() == Some(Origin::Originated) || record.forward.is_some()
}

async fn output_spans<S: ProvenanceStore>(
    store: &S,
    exchanges: &IdBatch<ExchangeId>,
) -> Result<BTreeMap<ExchangeId, Vec<StoredSpan>>, ProvenanceReadError> {
    let mut spans = BTreeMap::new();
    for exchange in exchanges.ids() {
        if store
            .exchange(*exchange)
            .await
            .map_err(read_failed)?
            .is_none()
        {
            continue;
        }
        let records = store.exchange_spans(*exchange).await.map_err(read_failed)?;
        let kept = records
            .into_iter()
            .filter(|record| !matches!(record.span.state, SpanState::Common | SpanState::Extracted))
            .map(|record| StoredSpan {
                forward: record.forward.map(forward_status),
                span: record.span,
            })
            .collect();
        spans.insert(*exchange, kept);
    }
    Ok(spans)
}

async fn matches_read_in<S: ProvenanceStore>(
    store: &S,
    exchanges: &IdBatch<ExchangeId>,
) -> Result<BTreeMap<ExchangeId, Vec<ContentMatch>>, ProvenanceReadError> {
    let mut read = BTreeMap::new();
    for exchange in exchanges.ids() {
        if store
            .exchange(*exchange)
            .await
            .map_err(read_failed)?
            .is_none()
        {
            continue;
        }
        let mut matches = store
            .exchange_matches(*exchange)
            .await
            .map_err(read_failed)?;
        matches.sort_by_key(|stored| stored.ordinal);
        read.insert(
            *exchange,
            matches.into_iter().map(|stored| stored.content).collect(),
        );
    }
    Ok(read)
}

async fn scan_statuses<S: ProvenanceStore>(
    store: &S,
    exchanges: &IdBatch<ExchangeId>,
) -> Result<BTreeMap<ExchangeId, SpecScanStatus>, ProvenanceReadError> {
    let mut statuses = BTreeMap::new();
    for exchange in exchanges.ids() {
        if let Some((_, status)) = store.exchange(*exchange).await.map_err(read_failed)? {
            statuses.insert(*exchange, scan_status(&status));
        }
    }
    Ok(statuses)
}

/// A reader's place in the list: newest reader exchange first, then the
/// newest match.
type ReaderKey = (Timestamp, EventId);

fn key_of(stored: &StoredMatch) -> ReaderKey {
    (stored.at, stored.id)
}

fn tag(span: SpanId, key: ReaderKey) -> String {
    let mut bytes = CURSOR_KEY.to_vec();
    bytes.extend_from_slice(span.ulid_text().as_bytes());
    bytes.push(0);
    bytes.extend_from_slice(key.0.as_micros().to_string().as_bytes());
    bytes.push(0);
    bytes.extend_from_slice(key.1.ulid_text().as_bytes());
    Blake3::of(&bytes).to_hex()[..32].to_owned()
}

fn issue(span: SpanId, key: ReaderKey) -> Result<Cursor<SpanReaderList>, ProvenanceReadError> {
    let token = format!(
        "{}-{}_{}",
        key.0.as_micros(),
        key.1.ulid_text(),
        tag(span, key)
    );
    Cursor::from_token(token).map_err(|error| ProvenanceReadError::Store {
        reason: format!("cursor not issued: {error:?}"),
    })
}

fn resume(span: SpanId, cursor: &Cursor<SpanReaderList>) -> Result<ReaderKey, ProvenanceReadError> {
    let invalid = || ProvenanceReadError::InvalidCursor;
    let (position, given) = cursor.token().split_once('_').ok_or_else(invalid)?;
    let (micros, id) = position.split_once('-').ok_or_else(invalid)?;
    let micros: u64 = micros.parse().map_err(|_| invalid())?;
    let id = EventId::from_ulid_text(id).map_err(|_| invalid())?;
    let key = (Timestamp::from_micros(micros), id);
    if tag(span, key) == given {
        Ok(key)
    } else {
        Err(invalid())
    }
}

async fn readers<S: ProvenanceStore>(
    store: &S,
    span: SpanId,
    page: &PageRequest<SpanReaderList>,
) -> Result<Option<ReaderPage>, ProvenanceReadError> {
    let after = page
        .after
        .as_ref()
        .map(|cursor| resume(span, cursor))
        .transpose()?;
    if store.span(span).await.map_err(read_failed)?.is_none() {
        return Ok(None);
    }
    let mut matches = store.matches_of_span(span).await.map_err(read_failed)?;
    let total = u32::try_from(matches.len()).map_err(|_| ProvenanceReadError::Store {
        reason: format!(
            "span {} has more readers than a count holds",
            span.ulid_text()
        ),
    })?;
    matches.sort_by_key(|stored| std::cmp::Reverse(key_of(stored)));
    let rest: Vec<StoredMatch> = matches
        .into_iter()
        .filter(|stored| after.is_none_or(|after| key_of(stored) < after))
        .collect();
    let size = page.size;
    let limit = usize::from(size.get().get());
    let overflow = |_| ProvenanceReadError::Store {
        reason: "page larger than its size".to_owned(),
    };
    let page = if rest.len() <= limit {
        Page::last(
            size,
            rest.into_iter().map(|stored| stored.content).collect(),
        )
        .map_err(overflow)?
    } else {
        let served = &rest[..limit];
        let last = served
            .last()
            .map(key_of)
            .ok_or_else(|| ProvenanceReadError::Store {
                reason: "empty page with more to follow".to_owned(),
            })?;
        let items =
            NonEmpty::from_vec(served.iter().map(|stored| stored.content.clone()).collect())
                .ok_or_else(|| ProvenanceReadError::Store {
                    reason: "empty page with more to follow".to_owned(),
                })?;
        Page::more(size, items, issue(span, last)?).map_err(overflow)?
    };
    Ok(Some(ReaderPage { total, page }))
}

impl ProvenanceReads for MemoryProvenanceStore {
    async fn output_spans(
        &self,
        exchanges: &IdBatch<ExchangeId>,
    ) -> Result<BTreeMap<ExchangeId, Vec<StoredSpan>>, ProvenanceReadError> {
        output_spans(self, exchanges).await
    }

    async fn matches_read_in(
        &self,
        exchanges: &IdBatch<ExchangeId>,
    ) -> Result<BTreeMap<ExchangeId, Vec<ContentMatch>>, ProvenanceReadError> {
        matches_read_in(self, exchanges).await
    }

    async fn readers(
        &self,
        span: SpanId,
        page: &PageRequest<SpanReaderList>,
    ) -> Result<Option<ReaderPage>, ProvenanceReadError> {
        readers(self, span, page).await
    }

    async fn scan_status(
        &self,
        exchanges: &IdBatch<ExchangeId>,
    ) -> Result<BTreeMap<ExchangeId, SpecScanStatus>, ProvenanceReadError> {
        scan_statuses(self, exchanges).await
    }
}

/// The spec's `SpanIndex` over the spans the scans committed, as
/// [`MemoryProvenanceStore`]'s: `record` adds nothing (`commit_scan`
/// wrote every classified span), and `spans` reads back the originated and
/// forwarded ones as recorded.
impl SpanIndex for PgProvenanceStore {
    async fn record(&mut self, _span: &OriginatedSpan) -> Result<(), SpanIndexError> {
        Ok(())
    }

    async fn spans(
        &self,
        ids: &IdBatch<SpanId>,
    ) -> Result<BTreeMap<SpanId, IndexedSpan>, SpanIndexError> {
        let records = ProvenanceStore::spans(self, ids.ids())
            .await
            .map_err(|error| SpanIndexError::Store {
                reason: error.to_string(),
            })?;
        Ok(records
            .into_iter()
            .filter(indexed)
            .map(|record| {
                (
                    record.span.id,
                    IndexedSpan {
                        exchange: record.span.exchange,
                        author: record.span.agent,
                        location: record.span.location,
                    },
                )
            })
            .collect())
    }
}

impl ProvenanceReads for PgProvenanceStore {
    async fn output_spans(
        &self,
        exchanges: &IdBatch<ExchangeId>,
    ) -> Result<BTreeMap<ExchangeId, Vec<StoredSpan>>, ProvenanceReadError> {
        output_spans(self, exchanges).await
    }

    async fn matches_read_in(
        &self,
        exchanges: &IdBatch<ExchangeId>,
    ) -> Result<BTreeMap<ExchangeId, Vec<ContentMatch>>, ProvenanceReadError> {
        matches_read_in(self, exchanges).await
    }

    async fn readers(
        &self,
        span: SpanId,
        page: &PageRequest<SpanReaderList>,
    ) -> Result<Option<ReaderPage>, ProvenanceReadError> {
        readers(self, span, page).await
    }

    async fn scan_status(
        &self,
        exchanges: &IdBatch<ExchangeId>,
    ) -> Result<BTreeMap<ExchangeId, SpecScanStatus>, ProvenanceReadError> {
        scan_statuses(self, exchanges).await
    }
}
