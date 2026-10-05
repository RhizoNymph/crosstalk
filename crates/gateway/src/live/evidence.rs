//! Feeding the surface's span records from the bus.
//!
//! The evidence page reads spans, accesses and resources by id
//! ([`MemoryEvidence`]). Accesses and resources it reads from the registry
//! that recorded them; spans this stage copies in as L4 announces them:
//! on `SpanOriginated`, and on `SpanRelayed` from an input (a forwarded
//! span, indexed under the forwarding agent:
//! `provenance.index.forwarded-indexed`), the span's record from L4's
//! `SpanIndex` ([`SpanSource`], [`IndexedSpans`]). Provenance commits a span
//! before it publishes the event naming it, and only originated and
//! forwarded spans are ever the origin of a content match.
//!
//! Kept to this one module: once the surface reads `SpanIndex` itself,
//! this stage goes away.

use std::future::Future;

use crosstalk_api::in_process::MemoryEvidence;
use crosstalk_spec::batch::IdBatch;
use crosstalk_spec::derived::provenance::span::{RelaySource, Span, SpanState};
use crosstalk_spec::events::detect::DetectEvent;
use crosstalk_spec::events::{BusEvent, Envelope, Subject};
use crosstalk_spec::ids::SpanId;
use crosstalk_spec::interfaces::l4_provenance::SpanIndex;

use super::stage::{Stage, StageError};

/// Why a span could not be read.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("span source unavailable: {reason}")]
pub struct SpanSourceError {
    pub reason: String,
}

/// Where span records are read by id: L4's span store.
pub trait SpanSource: Send + Sync + 'static {
    fn span(
        &self,
        id: SpanId,
    ) -> impl Future<Output = Result<Option<Span>, SpanSourceError>> + Send;
}

/// Spans read through the spec's `SpanIndex` (L4's provenance store): an
/// originated or forwarded span's record, as the evidence page needs it
/// (its location, author and exchange). The index keeps no state, so the
/// span is rebuilt as `Originated`; the feeder restates a forwarded span's
/// `Relayed` state from its event. Spans relayed from another span and
/// common spans are not in the index and never the origin of a content
/// match.
#[derive(Debug, Clone, Default)]
pub struct IndexedSpans<I>(pub I);

impl<I: SpanIndex + Send + Sync + 'static> SpanSource for IndexedSpans<I> {
    async fn span(&self, id: SpanId) -> Result<Option<Span>, SpanSourceError> {
        let batch = IdBatch::new([id]).map_err(|error| SpanSourceError {
            reason: format!("one id is a batch: {error:?}"),
        })?;
        let spans = self
            .0
            .spans(&batch)
            .await
            .map_err(|error| SpanSourceError {
                reason: format!("{error:?}"),
            })?;
        Ok(spans.get(&id).map(|indexed| Span {
            id,
            location: indexed.location,
            agent: indexed.author,
            exchange: indexed.exchange,
            state: SpanState::Originated,
        }))
    }
}

/// The evidence slot's stage.
pub struct EvidenceFeeder<S> {
    evidence: MemoryEvidence,
    spans: S,
}

impl<S: SpanSource> EvidenceFeeder<S> {
    pub fn new(evidence: MemoryEvidence, spans: S) -> Self {
        Self { evidence, spans }
    }

    async fn span(&self, id: SpanId, state: Option<SpanState>) -> Result<(), StageError> {
        match self.spans.span(id).await {
            Ok(Some(mut span)) => {
                if let Some(state) = state {
                    span.state = state;
                }
                self.evidence.insert_span(span);
                Ok(())
            }
            Ok(None) => {
                tracing::warn!(span = %id.ulid_text(), "announced span not in the span source");
                Ok(())
            }
            Err(error) => Err(StageError::Retry {
                reason: error.to_string(),
            }),
        }
    }
}

impl<S: SpanSource> Stage for EvidenceFeeder<S> {
    fn subjects(&self) -> Vec<Subject> {
        vec![Subject::SpanOriginated, Subject::SpanRelayed]
    }

    async fn handle(&mut self, envelope: &Envelope) -> Result<(), StageError> {
        match &envelope.event {
            BusEvent::Detect(DetectEvent::SpanOriginated { span, .. }) => {
                self.span(*span, None).await
            }
            BusEvent::Detect(DetectEvent::SpanRelayed {
                span,
                source: source @ RelaySource::Input(_),
            }) => {
                self.span(*span, Some(SpanState::Relayed { source: *source }))
                    .await
            }
            _ => Ok(()),
        }
    }
}
