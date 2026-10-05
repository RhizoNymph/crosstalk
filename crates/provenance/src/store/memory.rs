//! [`MemoryProvenanceStore`]: L4's records in memory, for tests and the
//! simulation. Clones are handles on one store; its state sits behind a
//! std `Mutex` held only for synchronous critical sections.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex, MutexGuard};

use crosstalk_spec::batch::IdBatch;
use crosstalk_spec::derived::provenance::span::{
    Origin, OriginatedSpan, RelaySource, SpanEvent, SpanState,
};
use crosstalk_spec::ids::{ExchangeId, MessageHash, SpanId};
use crosstalk_spec::interfaces::l4_provenance::{IndexedSpan, SpanIndex, SpanIndexError};
use crosstalk_spec::support::Timestamp;

use super::{
    Committed, ExchangeRecord, Forwarding, MessageScan, ProvenanceStore, ProvenanceStoreError,
    Relay, ScanCommit, ScanFailure, ScanStatus, ScannedAs, SpanRecord, StoredMatch,
};

#[derive(Debug, Default)]
struct Tables {
    exchanges: BTreeMap<ExchangeId, (ExchangeRecord, ScanStatus)>,
    spans: BTreeMap<SpanId, SpanRecord>,
    /// Each exchange's spans in output order.
    by_exchange: BTreeMap<ExchangeId, Vec<SpanId>>,
    matches: Vec<StoredMatch>,
    scanned: BTreeSet<(MessageHash, ExchangeId, ScannedAs)>,
    sequence: u64,
    /// The spans relayed from each span.
    relayed_from: BTreeMap<SpanId, Vec<SpanId>>,
}

/// L4's records in memory.
#[derive(Debug, Clone, Default)]
pub struct MemoryProvenanceStore {
    tables: Arc<Mutex<Tables>>,
}

impl MemoryProvenanceStore {
    pub fn new() -> Self {
        Self::default()
    }

    fn lock(&self) -> MutexGuard<'_, Tables> {
        // A poisoned lock means a panic mid-write in a test; the tables are
        // still the last consistent state, since every write checks before
        // it changes anything.
        self.tables
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Every stored span (tests).
    pub fn all_spans(&self) -> Vec<SpanRecord> {
        self.lock().spans.values().cloned().collect()
    }

    /// Every stored match (tests).
    pub fn all_matches(&self) -> Vec<StoredMatch> {
        self.lock().matches.clone()
    }
}

fn advance(record: &mut SpanRecord, event: SpanEvent) -> Result<(), ProvenanceStoreError> {
    let next = record
        .span
        .state
        .advance(event)
        .map_err(ProvenanceStoreError::Transition)?;
    record.span.state = next;
    Ok(())
}

/// The spec's `SpanIndex` over the spans the scans committed: `commit_scan`
/// already wrote every classified span, so `record` adds nothing (it is
/// idempotent by construction), and `spans` reads back the indexed ones as
/// recorded: the originated ones (`Originated`, `Indexed`, `Propagated` or
/// `Expired`) and the forwarded ones committed with forwarding on
/// (`Relayed` from an input, `provenance.index.forwarded-indexed`). Spans relayed from another span,
/// common spans and unknown ids are absent.
impl SpanIndex for MemoryProvenanceStore {
    async fn record(&mut self, _span: &OriginatedSpan) -> Result<(), SpanIndexError> {
        Ok(())
    }

    async fn spans(
        &self,
        ids: &IdBatch<SpanId>,
    ) -> Result<BTreeMap<SpanId, IndexedSpan>, SpanIndexError> {
        let tables = self.lock();
        Ok(ids
            .ids()
            .iter()
            .filter_map(|id| tables.spans.get(id))
            .filter(|record| {
                record.span.state.origin() == Some(Origin::Originated) || record.forward.is_some()
            })
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

impl ProvenanceStore for MemoryProvenanceStore {
    async fn record_exchange(
        &mut self,
        record: ExchangeRecord,
    ) -> Result<(), ProvenanceStoreError> {
        self.lock()
            .exchanges
            .entry(record.id)
            .or_insert((record, ScanStatus::Pending));
        Ok(())
    }

    async fn exchange(
        &self,
        id: ExchangeId,
    ) -> Result<Option<(ExchangeRecord, ScanStatus)>, ProvenanceStoreError> {
        Ok(self.lock().exchanges.get(&id).cloned())
    }

    async fn spans(&self, ids: &[SpanId]) -> Result<Vec<SpanRecord>, ProvenanceStoreError> {
        let tables = self.lock();
        Ok(ids
            .iter()
            .filter_map(|id| tables.spans.get(id).cloned())
            .collect())
    }

    async fn relays(&self, sources: &[SpanId]) -> Result<Vec<Relay>, ProvenanceStoreError> {
        let tables = self.lock();
        let mut relays: Vec<Relay> = sources
            .iter()
            .flat_map(|source| {
                tables
                    .relayed_from
                    .get(source)
                    .into_iter()
                    .flatten()
                    .filter_map(|id| {
                        let record = tables.spans.get(id)?;
                        let (exchange, _) = tables.exchanges.get(&record.span.exchange)?;
                        Some(Relay {
                            source: *source,
                            agent: record.span.agent,
                            at: exchange.started_at,
                        })
                    })
            })
            .collect();
        relays.sort_by_key(|relay| (relay.source, relay.at, relay.agent));
        Ok(relays)
    }

    async fn span(&self, id: SpanId) -> Result<Option<SpanRecord>, ProvenanceStoreError> {
        Ok(self.lock().spans.get(&id).cloned())
    }

    async fn exchange_spans(
        &self,
        exchange: ExchangeId,
    ) -> Result<Vec<SpanRecord>, ProvenanceStoreError> {
        let tables = self.lock();
        Ok(tables
            .by_exchange
            .get(&exchange)
            .into_iter()
            .flatten()
            .filter_map(|id| tables.spans.get(id).cloned())
            .collect())
    }

    async fn exchange_matches(
        &self,
        exchange: ExchangeId,
    ) -> Result<Vec<StoredMatch>, ProvenanceStoreError> {
        let mut matches: Vec<StoredMatch> = self
            .lock()
            .matches
            .iter()
            .filter(|stored| stored.content.reader_exchange() == exchange)
            .cloned()
            .collect();
        matches.sort_by_key(|stored| stored.ordinal);
        Ok(matches)
    }

    async fn matches_in_message(
        &self,
        message: MessageHash,
    ) -> Result<Vec<StoredMatch>, ProvenanceStoreError> {
        let mut matches: Vec<StoredMatch> = self
            .lock()
            .matches
            .iter()
            .filter(|stored| stored.content.read_at().part.message == message)
            .cloned()
            .collect();
        matches.sort_by_key(|stored| {
            let at = stored.content.read_at();
            (at.part.index, at.range.start(), at.range.end(), stored.id)
        });
        Ok(matches)
    }

    async fn matches_of_span(
        &self,
        span: SpanId,
    ) -> Result<Vec<StoredMatch>, ProvenanceStoreError> {
        let mut matches: Vec<StoredMatch> = self
            .lock()
            .matches
            .iter()
            .filter(|stored| stored.content.origin() == span)
            .cloned()
            .collect();
        matches.sort_by_key(|stored| (stored.at, stored.id));
        Ok(matches)
    }

    async fn message_scans(
        &self,
        message: MessageHash,
    ) -> Result<Vec<MessageScan>, ProvenanceStoreError> {
        let tables = self.lock();
        Ok(tables
            .scanned
            .iter()
            .filter(|(hash, _, _)| *hash == message)
            .filter_map(|(hash, exchange, scanned_as)| {
                let (_, status) = tables.exchanges.get(exchange)?;
                Some(MessageScan {
                    message: *hash,
                    exchange: *exchange,
                    scanned_as: *scanned_as,
                    status: status.clone(),
                })
            })
            .collect())
    }

    async fn index_watermark(&self) -> Result<u64, ProvenanceStoreError> {
        Ok(self.lock().sequence)
    }

    async fn commit_scan(&mut self, commit: ScanCommit) -> Result<Committed, ProvenanceStoreError> {
        let mut tables = self.lock();
        let Some((_, status)) = tables.exchanges.get(&commit.exchange) else {
            return Err(ProvenanceStoreError::UnknownExchange {
                exchange: commit.exchange,
            });
        };
        if *status != ScanStatus::Pending {
            return Ok(Committed::AlreadyScanned);
        }
        let mut ids = Vec::with_capacity(commit.spans.len());
        for (ordinal, span) in commit.spans.iter().enumerate() {
            ids.push(span.id);
            if let SpanState::Relayed {
                source: RelaySource::Span(source),
            } = span.state
            {
                tables.relayed_from.entry(source).or_default().push(span.id);
            }
            tables.spans.insert(
                span.id,
                SpanRecord::committed(
                    span.clone(),
                    u32::try_from(ordinal).unwrap_or(u32::MAX),
                    commit.forwarding,
                ),
            );
        }
        tables.by_exchange.insert(commit.exchange, ids);
        for stored in &commit.matches {
            if let Some(origin) = tables.spans.get_mut(&stored.content.origin())
                && origin.forward.is_none()
                && let Err(error) = advance(origin, SpanEvent::Hit { at: commit.at })
            {
                tracing::debug!(span = ?stored.content.origin(), error = %error, "hit not recorded on the origin span");
            }
            tables.matches.push(stored.clone());
        }
        for (message, scanned_as) in &commit.messages {
            tables
                .scanned
                .insert((*message, commit.exchange, *scanned_as));
        }
        if let Some((_, status)) = tables.exchanges.get_mut(&commit.exchange) {
            *status = ScanStatus::Scanned { at: commit.at };
        }
        Ok(Committed::Written)
    }

    async fn mark_indexed(
        &mut self,
        exchange: ExchangeId,
        at: Timestamp,
    ) -> Result<(), ProvenanceStoreError> {
        let mut tables = self.lock();
        let Some((_, status)) = tables.exchanges.get(&exchange) else {
            return Err(ProvenanceStoreError::UnknownExchange { exchange });
        };
        if !matches!(status, ScanStatus::Scanned { .. }) {
            return Ok(());
        }
        let ids = tables
            .by_exchange
            .get(&exchange)
            .cloned()
            .unwrap_or_default();
        for id in ids {
            let sequence = tables.sequence + 1;
            let Some(record) = tables.spans.get_mut(&id) else {
                continue;
            };
            if record.span.state == SpanState::Originated {
                advance(record, SpanEvent::Index { at })?;
            } else if record.forward == Some(Forwarding::Pending) {
                record.forward = Some(Forwarding::Indexed { at });
            } else {
                continue;
            }
            record.index_seq = Some(sequence);
            tables.sequence = sequence;
        }
        if let Some((_, status)) = tables.exchanges.get_mut(&exchange) {
            *status = ScanStatus::Indexed { at };
        }
        Ok(())
    }

    async fn mark_failed(
        &mut self,
        exchange: ExchangeId,
        at: Timestamp,
        failure: ScanFailure,
    ) -> Result<(), ProvenanceStoreError> {
        let mut tables = self.lock();
        let Some((_, status)) = tables.exchanges.get_mut(&exchange) else {
            return Err(ProvenanceStoreError::UnknownExchange { exchange });
        };
        if *status == ScanStatus::Pending {
            *status = ScanStatus::Failed { at, failure };
        }
        Ok(())
    }

    async fn expiring(
        &self,
        now: Timestamp,
        retention_micros: u64,
        limit: usize,
    ) -> Result<Vec<SpanId>, ProvenanceStoreError> {
        let tables = self.lock();
        let mut due: Vec<(Timestamp, SpanId)> = tables
            .spans
            .values()
            .filter_map(|record| {
                let at = record.indexed_at()?;
                (at.as_micros().saturating_add(retention_micros) < now.as_micros())
                    .then_some((at, record.span.id))
            })
            .collect();
        due.sort_unstable();
        Ok(due.into_iter().take(limit).map(|(_, id)| id).collect())
    }

    async fn expire(
        &mut self,
        spans: &[SpanId],
        at: Timestamp,
    ) -> Result<(), ProvenanceStoreError> {
        let mut tables = self.lock();
        for id in spans {
            let Some(record) = tables.spans.get_mut(id) else {
                continue;
            };
            if let Some(Forwarding::Indexed { at: indexed_at }) = record.forward {
                record.forward = Some(Forwarding::Expired { indexed_at, at });
            } else if let Err(error) = advance(record, SpanEvent::Expire { at }) {
                tracing::debug!(span = ?id, error = %error, "span not expired");
            }
        }
        Ok(())
    }

    async fn prune(&mut self, before: Timestamp) -> Result<(), ProvenanceStoreError> {
        let mut tables = self.lock();
        for (record, _) in tables.exchanges.values_mut() {
            if record.started_at < before {
                record.request.clear();
            }
        }
        Ok(())
    }
}
