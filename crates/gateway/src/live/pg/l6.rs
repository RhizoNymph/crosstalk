//! L6 in Postgres mode: `crosstalk-analysis`'s classification step
//! (`classify::Classifier`) in place of the gateway's memory classifier.
//!
//! The step decides once and saves the decision before any other effect,
//! so a redelivery redoes nothing, and the `TransmissionClassified` it
//! returns has an id derived from the delivery (`EventId::derive`,
//! `transport.consumer.derived-envelope-ids`), which the bus
//! deduplicates. Publish, then ack.

use crosstalk_analysis::classify::Classifier;
use crosstalk_api::pg::{PgCatalog, PgTransmissions};
use crosstalk_spec::events::{Envelope, Subject};
use crosstalk_spec::interfaces::l2_transport::EventBus;

use crate::live::stage::{Stage, StageError};
use crate::spool::LiveBus;

/// The L6 slot's stage over Postgres.
pub struct PgClassify {
    step: Classifier<PgCatalog<LiveBus>, PgTransmissions<LiveBus>>,
    bus: LiveBus,
}

impl PgClassify {
    pub fn new(
        catalog: PgCatalog<LiveBus>,
        transmissions: PgTransmissions<LiveBus>,
        bus: LiveBus,
    ) -> Self {
        Self {
            step: Classifier::new(catalog, transmissions),
            bus,
        }
    }
}

impl Stage for PgClassify {
    fn subjects(&self) -> Vec<Subject> {
        Classifier::<PgCatalog<LiveBus>, PgTransmissions<LiveBus>>::subjects()
    }

    async fn handle(&mut self, envelope: &Envelope) -> Result<(), StageError> {
        let classified = self
            .step
            .classify(envelope)
            .await
            .map_err(|error| StageError::Retry {
                reason: error.to_string(),
            })?;
        if let Some(classified) = classified {
            self.bus
                .publish(classified)
                .await
                .map_err(|error| StageError::Retry {
                    reason: format!("publishing TransmissionClassified: {error:?}"),
                })?;
        }
        Ok(())
    }
}
