//! L4 in Postgres mode: `crosstalk-provenance`'s engine over
//! `PgProvenanceStore` and `PgFingerprintIndex`, then L5's extraction step
//! (`crosstalk_flow::extract::step`) over `PgExtractionLedger`, handing its
//! inputs to the durable flow consumer through `DurableInputs`.
//!
//! - `ExchangeCaptured`: record the exchange (idempotent).
//! - `ConversationDelta`: scan it (replay-complete: a redelivered delta
//!   returns the stored envelopes), extract it, wait until the flow
//!   consumer holds the extracted inputs durably (`deliver` answers once
//!   their accesses, held writes and tool calls are stored), commit the
//!   ledger, then publish provenance's envelopes (ids derived from the
//!   input). Only then is the delta acked; `NotDurable` retries it.
//! - A tick at `now`: expire spans past retention, and the ledger's
//!   records older than `content_retention`.
//!
//! The exchange's start the step stamps accesses with is read back from
//! the store (`Provenance::started_at`), so a restart loses nothing.

use std::time::Duration;

use crosstalk_flow::consumer::DurableInputs;
use crosstalk_flow::extract::ExtractConfig;
use crosstalk_flow::extract::step::ExtractionStep;
use crosstalk_flow::store::PgExtractionLedger;
use crosstalk_provenance::config::ProvenanceConfig;
use crosstalk_provenance::engine::{EngineError, Provenance};
use crosstalk_provenance::index::PgFingerprintIndex;
use crosstalk_provenance::scan::messages::BlobMessages;
use crosstalk_provenance::semantic::DisabledSemanticMatcher;
use crosstalk_provenance::store::PgProvenanceStore;
use crosstalk_spec::events::ingest::IngestEvent;
use crosstalk_spec::events::{BusEvent, Envelope, Subject};
use crosstalk_spec::interfaces::l2_transport::EventBus;
use crosstalk_spec::support::Timestamp;

use crate::live::blobs::LiveBlobs;
use crate::live::layers::extract::{LiveMessages, LiveSpans};
use crate::live::stage::{Stage, StageError};
use crate::spool::LiveBus;

type Engine = Provenance<
    PgFingerprintIndex,
    PgProvenanceStore,
    DisabledSemanticMatcher,
    BlobMessages<LiveBlobs>,
>;
type Step = ExtractionStep<PgExtractionLedger, LiveSpans<PgProvenanceStore>, LiveMessages>;

/// What the stage is built from.
pub struct PgProvenanceParts {
    pub provenance: PgProvenanceStore,
    pub index: PgFingerprintIndex,
    pub ledger: PgExtractionLedger,
    pub blobs: LiveBlobs,
    pub bus: LiveBus,
    pub flow: DurableInputs,
    /// How long the ledger keeps deliveries, contexts and done marks.
    pub content_retention: Duration,
}

/// The L4 slot's stage over Postgres.
pub struct PgProvenanceStage {
    engine: Engine,
    step: Step,
    bus: LiveBus,
    flow: DurableInputs,
    content_retention: Duration,
}

impl PgProvenanceStage {
    pub fn new(
        config: &ProvenanceConfig,
        extract: &ExtractConfig,
        parts: PgProvenanceParts,
    ) -> Self {
        Self {
            engine: Provenance::new(
                config,
                parts.index,
                parts.provenance.clone(),
                DisabledSemanticMatcher,
                BlobMessages::new(parts.blobs.clone()),
            ),
            step: ExtractionStep::new(
                parts.ledger,
                LiveSpans(parts.provenance),
                LiveMessages(BlobMessages::new(parts.blobs)),
                extract.clone(),
            ),
            bus: parts.bus,
            flow: parts.flow,
            content_retention: parts.content_retention,
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

impl Stage for PgProvenanceStage {
    fn subjects(&self) -> Vec<Subject> {
        crosstalk_provenance::consumer::SUBJECTS.to_vec()
    }

    async fn handle(&mut self, envelope: &Envelope) -> Result<(), StageError> {
        match &envelope.event {
            BusEvent::Ingest(IngestEvent::ExchangeCaptured(exchange)) => self
                .engine
                .record_exchange(exchange)
                .await
                .map_err(engine_error),
            BusEvent::Ingest(IngestEvent::ConversationDelta(delta)) => {
                let processed = self.engine.process(delta).await.map_err(engine_error)?;
                let at = self
                    .engine
                    .started_at(delta.exchange)
                    .await
                    .map_err(engine_error)?
                    .unwrap_or(envelope.at);
                self.step
                    .delta(delta, at, &mut self.flow)
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
        if let Err(error) = self.step.expire(now, self.content_retention).await {
            tracing::warn!(error = %error, "extraction ledger expiry failed; retried next tick");
        }
    }
}
