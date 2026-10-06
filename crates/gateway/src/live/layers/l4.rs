//! L4: `crosstalk-provenance`'s engine over the live process's provenance
//! store and a reference fingerprint index, then L5's extraction step over
//! the same delta (see [`super::extract`]).
//!
//! The engine runs here rather than under the crate's own `run` loop so
//! eviction follows the process's ticks (`Live::settle` and the periodic
//! ticker) instead of a wall-time interval:
//!
//! - `ExchangeCaptured`: record the exchange (its start is what the
//!   extraction step stamps accesses with, read back from the store with
//!   `Provenance::started_at`, so a restart loses nothing);
//! - `ConversationDelta`: scan it, extract it, then publish the envelopes
//!   the scan yielded (`SpanOriginated`, `SpanRelayed`, `ContentMatched`);
//! - a tick at `now`: expire spans past retention.

use crosstalk_memory::provenance::{IndexConfig, MemoryFingerprintIndex};
use crosstalk_provenance::config::ProvenanceConfig;
use crosstalk_provenance::engine::{EngineError, Provenance};
use crosstalk_provenance::scan::messages::BlobMessages;
use crosstalk_provenance::semantic::DisabledSemanticMatcher;
use crosstalk_provenance::store::MemoryProvenanceStore;
use crosstalk_spec::events::ingest::IngestEvent;
use crosstalk_spec::events::{BusEvent, Envelope, Subject};
use crosstalk_spec::interfaces::l2_transport::EventBus;
use crosstalk_spec::support::Timestamp;
use crosstalk_transport::MpscBus;

use super::extract::Extraction;
use crate::live::blobs::LiveBlobs;
use crate::live::stage::{Stage, StageContext, StageError};

type Engine = Provenance<
    MemoryFingerprintIndex,
    MemoryProvenanceStore,
    DisabledSemanticMatcher,
    BlobMessages<LiveBlobs>,
>;

/// The L4 slot's stage.
pub struct ProvenanceStage {
    engine: Engine,
    bus: MpscBus,
    extraction: Extraction,
}

impl ProvenanceStage {
    pub fn new(ctx: &StageContext, config: &ProvenanceConfig, extraction: Extraction) -> Self {
        let index = MemoryFingerprintIndex::new(IndexConfig::single_node(
            config.index().cutoff(),
            config.index().retention(),
        ));
        Self {
            engine: Provenance::new(
                config,
                index,
                ctx.layers.provenance.clone(),
                DisabledSemanticMatcher,
                BlobMessages::new(ctx.stores.blobs.clone()),
            ),
            bus: ctx.stores.bus.clone(),
            extraction,
        }
    }
}

fn engine_error(error: EngineError) -> StageError {
    match error.is_transient() {
        true => StageError::Retry {
            reason: error.to_string(),
        },
        false => StageError::Reject {
            reason: error.to_string(),
        },
    }
}

impl Stage for ProvenanceStage {
    fn subjects(&self) -> Vec<Subject> {
        crosstalk_provenance::consumer::SUBJECTS.to_vec()
    }

    async fn handle(&mut self, envelope: &Envelope) -> Result<(), StageError> {
        match &envelope.event {
            BusEvent::Ingest(IngestEvent::ExchangeCaptured(exchange)) => {
                self.engine
                    .record_exchange(exchange)
                    .await
                    .map_err(engine_error)
            }
            BusEvent::Ingest(IngestEvent::ConversationDelta(delta)) => {
                let processed = self.engine.process(delta).await.map_err(engine_error)?;
                // Extracted before provenance's events are published: the
                // flow consumer takes queued extracted inputs before its
                // next delivery, so it sees a read's access before the
                // content match the read carried.
                let at = self
                    .engine
                    .started_at(delta.exchange)
                    .await
                    .map_err(engine_error)?
                    .unwrap_or(envelope.at);
                self.extraction
                    .delta(delta, at)
                    .await
                    .map_err(|error| StageError::Retry {
                        reason: error.to_string(),
                    })?;
                for event in processed.events() {
                    self.bus
                        .publish(event.clone())
                        .await
                        .map_err(|error| StageError::Retry {
                            reason: format!("publishing provenance's events: {error:?}"),
                        })?;
                }
                Ok(())
            }
            _ => Ok(()),
        }
    }

    async fn tick(&mut self, now: Timestamp) {
        if let Err(error) = self.engine.expire(now).await {
            tracing::warn!(error = %error, "provenance eviction failed; retried next tick");
        }
    }
}
