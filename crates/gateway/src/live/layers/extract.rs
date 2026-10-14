//! L5's extraction step, run by the L4 stage right after provenance
//! processed the same delta (`Slot::L4Provenance`), so the writer's spans
//! of the exchange are stored when its write calls are extracted.
//!
//! The step itself is `crosstalk_flow::extract::step` (see its docs for
//! what one delta yields). Here it runs over the memory ledger, the live
//! process's blob store and its provenance store, and hands its inputs to
//! the flow consumer over an unbounded channel, which confirms them at
//! once: in memory mode nothing outlives the process.

use crosstalk_flow::consumer::Extracted;
use crosstalk_flow::extract::ExtractConfig;
use crosstalk_flow::extract::step::{
    ExtractionStep, MemoryExtractionLedger, MessageReader, PortError, SpanReader,
};
use crosstalk_provenance::scan::messages::{BlobMessages, MessageSource};
use crosstalk_provenance::store::{MemoryProvenanceStore, ProvenanceStore};
use crosstalk_spec::derived::provenance::span::Span;
use crosstalk_spec::events::ingest::ConversationDelta;
use crosstalk_spec::ids::{AgentId, ExchangeId, MessageHash, SpanId};
use crosstalk_spec::observed::message::Message;
use crosstalk_spec::support::Timestamp;
use tokio::sync::mpsc::UnboundedSender;

use crate::live::blobs::LiveBlobs;

pub use crosstalk_flow::extract::step::ExtractStepError;

/// Message bodies from the live blob store.
pub(crate) struct LiveMessages(pub(crate) BlobMessages<LiveBlobs>);

impl MessageReader for LiveMessages {
    async fn message(&self, hash: MessageHash) -> Result<Option<Message>, PortError> {
        self.0
            .message(hash)
            .await
            .map_err(|error| PortError::new(format!("{error:?}")))
    }
}

/// Spans from the live process's provenance store (memory or Postgres).
pub(crate) struct LiveSpans<P = MemoryProvenanceStore>(pub(crate) P);

impl<P: ProvenanceStore + Send + Sync> SpanReader for LiveSpans<P> {
    async fn exchange_spans(&self, exchange: ExchangeId) -> Result<Vec<Span>, PortError> {
        self.0
            .exchange_spans(exchange)
            .await
            .map(|records| records.into_iter().map(|record| record.span).collect())
            .map_err(|error| PortError::new(format!("{error:?}")))
    }

    async fn span_agent(&self, span: SpanId) -> Result<Option<AgentId>, PortError> {
        self.0
            .span(span)
            .await
            .map(|record| record.map(|record| record.span.agent))
            .map_err(|error| PortError::new(format!("{error:?}")))
    }
}

/// The extraction step over the memory ledger.
pub struct Extraction {
    step: ExtractionStep<MemoryExtractionLedger, LiveSpans, LiveMessages>,
    flow: UnboundedSender<Extracted>,
}

impl Extraction {
    /// The step under `config` (the MCP tools, HTTP and fetch tools and
    /// site rules the extractors know).
    pub fn new(
        blobs: LiveBlobs,
        spans: MemoryProvenanceStore,
        config: ExtractConfig,
        flow: UnboundedSender<Extracted>,
    ) -> Self {
        Self {
            step: ExtractionStep::new(
                MemoryExtractionLedger::new(),
                LiveSpans(spans),
                LiveMessages(BlobMessages::new(blobs)),
                config,
            ),
            flow,
        }
    }

    /// Extract `delta` of an exchange that started at `at`, and hand what
    /// it yields to the flow consumer, in order.
    pub async fn delta(
        &mut self,
        delta: &ConversationDelta,
        at: Timestamp,
    ) -> Result<(), ExtractStepError> {
        self.step.delta(delta, at, &mut self.flow).await.map(|_| ())
    }
}

#[cfg(test)]
mod tests;
