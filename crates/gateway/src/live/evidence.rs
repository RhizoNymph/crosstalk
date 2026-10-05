//! Feeding the surface's evidence records from the bus.
//!
//! The evidence page reads spans, accesses and resources by id
//! ([`MemoryEvidence`]). This stage copies them in as L4 and L5 announce
//! them, reading each record from the store that wrote it:
//!
//! - `SpanOriginated`: the span's record from L4's `SpanIndex`
//!   ([`SpanSource`], [`IndexedSpans`]); provenance commits a span before
//!   it publishes the event naming it, and only originated spans are ever
//!   the origin of a content match;
//! - `AccessRecorded`: the access and its resource from the registry's
//!   batch read (`AccessStore::accesses`), as recorded.
//!
//! Kept to this one module: once the surface reads `SpanIndex` and
//! `AccessStore` itself, this stage goes away.

use std::future::Future;

use crosstalk_api::in_process::MemoryEvidence;
use crosstalk_memory::flow::MemoryChannels;
use crosstalk_memory::reconstruct::MemoryAgents;
use crosstalk_spec::batch::IdBatch;
use crosstalk_spec::derived::provenance::span::{Span, SpanState};
use crosstalk_spec::events::detect::DetectEvent;
use crosstalk_spec::events::{BusEvent, Envelope, Subject};
use crosstalk_spec::ids::{AccessId, SpanId};
use crosstalk_spec::interfaces::l4_provenance::SpanIndex;
use crosstalk_spec::interfaces::l5_flow::channels::AccessStore;

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
/// originated span's record, as the evidence page needs it (its location,
/// author and exchange). The index keeps no state, so the span is rebuilt
/// as `Originated`, the state it was recorded in; relayed and common spans
/// are not in the index and never the origin of a content match.
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
    accesses: MemoryChannels<MemoryAgents>,
    spans: S,
}

impl<S: SpanSource> EvidenceFeeder<S> {
    pub fn new(evidence: MemoryEvidence, accesses: MemoryChannels<MemoryAgents>, spans: S) -> Self {
        Self {
            evidence,
            accesses,
            spans,
        }
    }

    async fn span(&self, id: SpanId) -> Result<(), StageError> {
        match self.spans.span(id).await {
            Ok(Some(span)) => {
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

    async fn access(&self, id: AccessId) -> Result<(), StageError> {
        let batch = IdBatch::new([id]).map_err(|error| StageError::Reject {
            reason: format!("one id is a batch: {error:?}"),
        })?;
        let read = self
            .accesses
            .accesses(&batch)
            .await
            .map_err(|error| StageError::Retry {
                reason: format!("reading the access: {error:?}"),
            })?;
        match read.get(&id) {
            Some((access, resource)) => {
                self.evidence.insert_access(access.clone());
                self.evidence.insert_resource(resource.clone());
            }
            None => {
                tracing::warn!(access = %id.ulid_text(), "announced access not recorded");
            }
        }
        Ok(())
    }
}

impl<S: SpanSource> Stage for EvidenceFeeder<S> {
    fn subjects(&self) -> Vec<Subject> {
        vec![Subject::SpanOriginated, Subject::AccessRecorded]
    }

    async fn handle(&mut self, envelope: &Envelope) -> Result<(), StageError> {
        match &envelope.event {
            BusEvent::Detect(DetectEvent::SpanOriginated { span, .. }) => self.span(*span).await,
            BusEvent::Detect(DetectEvent::AccessRecorded { access, .. }) => {
                self.access(access.id).await
            }
            _ => Ok(()),
        }
    }
}
