//! [`Provenance`]: L4's processing of one event at a time, over the
//! fingerprint index, the records, the semantic matcher and the message
//! bodies it is handed.
//!
//! - `ExchangeCaptured` → [`Provenance::record_exchange`]: the exchange's
//!   start and messages are recorded, status pending.
//! - `ConversationDelta` → [`Provenance::process`], by the exchange's scan
//!   status:
//!   - pending: load the delta's bodies (a body that is gone or undecodable
//!     fails the scan for good: status failed, nothing published), scan,
//!     commit spans and matches, write the index (postings, then
//!     observations), mark indexed, and return the events;
//!   - scanned (a crash after the commit): redo the index writes from the
//!     stored spans, mark indexed, return the stored outcome's events;
//!   - indexed or failed: return the stored outcome's events.
//!
//!   The events are built from the stored records with deterministic ids,
//!   so every delivery of one delta returns the same envelopes
//!   (`provenance.delta.redelivery-idempotent`).
//! - Eviction ([`Provenance::expire`]): spans past retention are evicted
//!   from the fingerprint index and the semantic matcher, and only then
//!   advanced to `Expired` (`provenance.match.none-after-expiry`); the
//!   index ages out old observations on the same call, even when no span
//!   is due (`provenance.index.retention-bound`).

use crosstalk_spec::derived::provenance::span::{Origin, SpanState};
use crosstalk_spec::events::detect::DetectEvent;
use crosstalk_spec::events::ingest::ConversationDelta;
use crosstalk_spec::events::{BusEvent, Envelope};
use crosstalk_spec::ids::{ExchangeId, MessageHash};
use crosstalk_spec::interfaces::l4_provenance::{FingerprintIndex, IndexError, SemanticMatcher};
use crosstalk_spec::observed::exchange::{Exchange, ExchangeOutcome};
use crosstalk_spec::observed::message::Message;
use crosstalk_spec::support::Timestamp;

use crate::config::ProvenanceConfig;
use crate::scan::messages::{LoadError, MessageSource};
use crate::scan::{Loaded, ScanEnv, ScanError, Scanner};
use crate::span::span_event_id;
use crate::store::{
    Committed, ExchangeRecord, ProvenanceStore, ProvenanceStoreError, ScanFailure, ScanStatus,
    SpanRecord, StoredMatch,
};

/// How many spans one eviction pass handles at a time.
const EVICTION_BATCH: usize = 512;

/// Why an event could not be processed. Transient errors are retried by
/// redelivery.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum EngineError {
    /// The delta arrived before its `ExchangeCaptured`.
    #[error("exchange {exchange:?} is not recorded yet")]
    NotRecorded { exchange: ExchangeId },
    #[error("fingerprint index: {0:?}")]
    Index(IndexError),
    #[error("semantic matcher: {0:?}")]
    Semantic(IndexError),
    #[error(transparent)]
    Store(#[from] ProvenanceStoreError),
    #[error(transparent)]
    Load(#[from] LoadError),
}

impl EngineError {
    /// Whether redelivering the event later can succeed.
    pub fn is_transient(&self) -> bool {
        match self {
            Self::NotRecorded { .. } => true,
            Self::Index(error) | Self::Semantic(error) => matches!(error, IndexError::Store { .. }),
            Self::Store(error) => error.is_transient(),
            Self::Load(error) => error.is_transient(),
        }
    }
}

impl From<ScanError> for EngineError {
    fn from(error: ScanError) -> Self {
        match error {
            ScanError::Index(error) => Self::Index(error),
            ScanError::Semantic(error) => Self::Semantic(error),
            ScanError::Store(error) => Self::Store(error),
            ScanError::Load(error) => Self::Load(error),
        }
    }
}

/// What processing a delta did.
#[derive(Debug, Clone, PartialEq)]
pub enum Processed {
    /// Scanned now; publish `events`.
    Scanned { events: Vec<Envelope> },
    /// Scanned before (a redelivery); publish `events` again.
    Replayed { events: Vec<Envelope> },
    /// The scan cannot succeed; nothing to publish.
    Failed { failure: ScanFailure },
}

impl Processed {
    pub fn events(&self) -> &[Envelope] {
        match self {
            Self::Scanned { events } | Self::Replayed { events } => events,
            Self::Failed { .. } => &[],
        }
    }
}

/// L4 over its stores.
#[derive(Debug)]
pub struct Provenance<I, S, M, L> {
    scanner: Scanner,
    retention_micros: u64,
    index: I,
    store: S,
    semantic: M,
    messages: L,
}

/// The envelopes announcing an exchange's stored outcome: each span's
/// origin (`SpanOriginated`, `SpanRelayed`; nothing for `Common`), then each
/// match, stamped with the exchange's start.
pub fn envelopes(
    exchange: ExchangeId,
    at: Timestamp,
    spans: &[SpanRecord],
    matches: &[StoredMatch],
) -> Vec<Envelope> {
    let mut events = Vec::with_capacity(spans.len() + matches.len());
    for record in spans {
        let span = &record.span;
        let event = match span.state.origin() {
            Some(Origin::Originated) => DetectEvent::SpanOriginated {
                span: span.id,
                agent: span.agent,
            },
            Some(Origin::Relayed(source)) => DetectEvent::SpanRelayed {
                span: span.id,
                source,
            },
            Some(Origin::Common) | None => continue,
        };
        events.push(Envelope {
            id: span_event_id(exchange, span.id),
            at,
            event: BusEvent::Detect(event),
        });
    }
    for stored in matches {
        events.push(Envelope {
            id: stored.id,
            at,
            event: BusEvent::Detect(DetectEvent::ContentMatched(stored.content.clone())),
        });
    }
    events
}

/// The record L4 keeps of `exchange`.
pub fn exchange_record(exchange: &Exchange) -> ExchangeRecord {
    let output = match &exchange.outcome {
        ExchangeOutcome::Completed { response, .. } => Some(*response),
        ExchangeOutcome::Failed {
            partial_response, ..
        } => *partial_response,
    };
    ExchangeRecord {
        id: exchange.meta.id,
        started_at: exchange.meta.started_at,
        request: exchange.request.clone(),
        output,
    }
}

/// A load that cannot succeed, or one to retry.
enum LoadOutcome {
    Loaded(Box<Loaded>),
    Failed(ScanFailure),
}

impl<I, S, M, L> Provenance<I, S, M, L>
where
    I: FingerprintIndex + Send + Sync,
    S: ProvenanceStore + Send + Sync,
    M: SemanticMatcher + Send + Sync,
    L: MessageSource + Send + Sync,
{
    pub fn new(config: &ProvenanceConfig, index: I, store: S, semantic: M, messages: L) -> Self {
        Self {
            scanner: Scanner::new(config),
            retention_micros: config.index().retention_micros(),
            index,
            store,
            semantic,
            messages,
        }
    }

    pub fn scanner(&self) -> &Scanner {
        &self.scanner
    }

    pub fn index(&self) -> &I {
        &self.index
    }

    pub fn store(&self) -> &S {
        &self.store
    }

    pub fn semantic(&self) -> &M {
        &self.semantic
    }

    /// The fingerprint index, giving the engine up.
    pub fn into_index(self) -> I {
        self.index
    }

    /// Record an exchange from `ExchangeCaptured`.
    pub async fn record_exchange(&mut self, exchange: &Exchange) -> Result<(), EngineError> {
        self.store
            .record_exchange(exchange_record(exchange))
            .await?;
        Ok(())
    }

    /// When the recorded exchange `exchange` started; `None` when its
    /// `ExchangeCaptured` was not recorded. Read from the store, so it
    /// survives a restart over a durable store.
    pub async fn started_at(&self, exchange: ExchangeId) -> Result<Option<Timestamp>, EngineError> {
        Ok(self.store.started_at(exchange).await?)
    }

    /// Process one `ConversationDelta`.
    pub async fn process(&mut self, delta: &ConversationDelta) -> Result<Processed, EngineError> {
        let Some((record, status)) = self.store.exchange(delta.exchange).await? else {
            return Err(EngineError::NotRecorded {
                exchange: delta.exchange,
            });
        };
        let at = record.started_at;
        match status {
            ScanStatus::Indexed { .. } => {
                let events = self.stored_events(delta.exchange, at).await?;
                Ok(Processed::Replayed { events })
            }
            ScanStatus::Failed { failure, .. } => Ok(Processed::Failed { failure }),
            ScanStatus::Scanned { .. } => {
                let loaded = match self.load(delta, &record).await? {
                    LoadOutcome::Loaded(loaded) => loaded,
                    LoadOutcome::Failed(failure) => return Ok(Processed::Failed { failure }),
                };
                self.write_index(delta.exchange, &loaded, at).await?;
                let events = self.stored_events(delta.exchange, at).await?;
                Ok(Processed::Replayed { events })
            }
            ScanStatus::Pending => {
                let loaded = match self.load(delta, &record).await? {
                    LoadOutcome::Loaded(loaded) => loaded,
                    LoadOutcome::Failed(failure) => {
                        self.store
                            .mark_failed(delta.exchange, at, failure.clone())
                            .await?;
                        tracing::warn!(exchange = ?delta.exchange, failure = ?failure, "provenance scan failed");
                        return Ok(Processed::Failed { failure });
                    }
                };
                let env = ScanEnv {
                    index: &self.index,
                    store: &self.store,
                    semantic: &self.semantic,
                    messages: &self.messages,
                };
                let commit = self.scanner.scan(delta, &record, &loaded, env).await?;
                let spans = commit.spans.len();
                let matches = commit.matches.len();
                let committed = self.store.commit_scan(commit).await?;
                self.write_index(delta.exchange, &loaded, at).await?;
                let events = self.stored_events(delta.exchange, at).await?;
                tracing::debug!(exchange = ?delta.exchange, spans, matches, committed = ?committed, "delta scanned");
                Ok(match committed {
                    Committed::Written => Processed::Scanned { events },
                    Committed::AlreadyScanned => Processed::Replayed { events },
                })
            }
        }
    }

    /// The index writes for the exchange's stored spans, then mark it
    /// indexed.
    async fn write_index(
        &mut self,
        exchange: ExchangeId,
        loaded: &Loaded,
        at: Timestamp,
    ) -> Result<(), EngineError> {
        let spans: Vec<_> = self
            .store
            .exchange_spans(exchange)
            .await?
            .into_iter()
            .map(|record| record.span)
            .collect();
        let work = self.scanner.index_work(loaded, &spans);
        for (span, fingerprints) in &work.postings {
            self.index
                .insert(span, fingerprints, at)
                .await
                .map_err(EngineError::Index)?;
        }
        for fingerprints in &work.observations {
            self.index
                .observe(fingerprints, at, at)
                .await
                .map_err(EngineError::Index)?;
        }
        self.store.mark_indexed(exchange, at).await?;
        Ok(())
    }

    async fn stored_events(
        &self,
        exchange: ExchangeId,
        at: Timestamp,
    ) -> Result<Vec<Envelope>, EngineError> {
        let spans = self.store.exchange_spans(exchange).await?;
        let matches = self.store.exchange_matches(exchange).await?;
        Ok(envelopes(exchange, at, &spans, &matches))
    }

    async fn required(
        &self,
        hash: MessageHash,
    ) -> Result<Result<Message, ScanFailure>, EngineError> {
        match self.messages.message(hash).await {
            Ok(Some(message)) => Ok(Ok(message)),
            Ok(None) => Ok(Err(ScanFailure::BodyMissing(hash))),
            Err(LoadError::Undecodable(hash) | LoadError::Corrupt(hash)) => {
                Ok(Err(ScanFailure::BodyUndecodable(hash)))
            }
            Err(error) => Err(EngineError::Load(error)),
        }
    }

    async fn load(
        &self,
        delta: &ConversationDelta,
        record: &ExchangeRecord,
    ) -> Result<LoadOutcome, EngineError> {
        let mut loaded = Loaded::default();
        for hash in &delta.new_inputs {
            match self.required(*hash).await? {
                Ok(message) => loaded.new_inputs.push(message),
                Err(failure) => return Ok(LoadOutcome::Failed(failure)),
            }
        }
        if let Some(hash) = delta.new_system {
            match self.required(hash).await? {
                Ok(message) => loaded.new_system = Some(message),
                Err(failure) => return Ok(LoadOutcome::Failed(failure)),
            }
        }
        if let Some(hash) = delta.output {
            match self.required(hash).await? {
                Ok(message) => loaded.output = Some(message),
                Err(failure) => return Ok(LoadOutcome::Failed(failure)),
            }
        }
        for hash in &record.request {
            let known = loaded
                .new_inputs
                .iter()
                .chain(loaded.new_system.iter())
                .find(|message| message.hash == *hash)
                .cloned();
            if let Some(message) = known {
                loaded.history.push(message);
                continue;
            }
            match self.messages.message(*hash).await {
                Ok(Some(message)) => loaded.history.push(message),
                Ok(None) | Err(LoadError::Undecodable(_) | LoadError::Corrupt(_)) => {
                    tracing::debug!(exchange = ?record.id, message = ?hash, "history body unavailable; classifying without it");
                }
                Err(error) => return Err(EngineError::Load(error)),
            }
        }
        Ok(LoadOutcome::Loaded(Box::new(loaded)))
    }

    /// Evict every span past retention at `now` from the index and the
    /// semantic matcher, then mark it expired; age out old observations and
    /// forget old request lists. Returns how many spans expired.
    pub async fn expire(&mut self, now: Timestamp) -> Result<usize, EngineError> {
        let mut expired = 0;
        loop {
            let due = self
                .store
                .expiring(now, self.retention_micros, EVICTION_BATCH)
                .await?;
            self.index
                .evict(&due, now)
                .await
                .map_err(EngineError::Index)?;
            if due.is_empty() {
                break;
            }
            self.semantic
                .evict(&due)
                .await
                .map_err(EngineError::Semantic)?;
            self.store.expire(&due, now).await?;
            expired += due.len();
            if due.len() < EVICTION_BATCH {
                break;
            }
        }
        let horizon = now.as_micros().saturating_sub(self.retention_micros);
        self.store.prune(Timestamp::from_micros(horizon)).await?;
        if expired > 0 {
            tracing::info!(expired, "provenance spans expired");
        }
        Ok(expired)
    }
}

/// Whether a span record's state is one the index holds postings for.
pub fn is_live(state: &SpanState) -> bool {
    matches!(
        state,
        SpanState::Indexed { .. } | SpanState::Propagated { .. }
    )
}
