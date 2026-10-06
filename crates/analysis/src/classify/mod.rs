//! The classification step of the `analyze` consumer: a
//! `TransmissionConfirmed` delivery in, the topic assignment and the
//! `Classified` state stored, and the `TransmissionClassified` envelope to
//! publish out. The composer publishes the envelope, then acks the
//! delivery (write, publish, ack).
//!
//! It runs no topic model: nothing in this step embeds a transmission's
//! content, so a fresh classification is an outlier (`topic: None`) under
//! the catalog's active version, and never watched. A step with a topic
//! model replaces how a fresh classification is decided; the redelivery
//! discipline below stays.
//!
//! **Redelivery** (the bus is at least once). Every effect is keyed so a
//! redelivered confirmation redoes nothing and republishes the same
//! envelope, whatever happened since the first delivery:
//!
//! ```text
//! decide:   the stored transmission already Classified or Aggregated ─▶ its classification
//!           otherwise ─▶ outlier under the active version
//! 1. save Classified { confirmed, classification }   (only when the stored state is Confirmed)
//! 2. assign (transmission, version)                   Unchanged on a redelivery
//! 3. return Envelope { id: EventId::derive(delivery id, CLASSIFIED_LABEL, 0),
//!                      at: the delivery's time, TransmissionClassified { Confirmation, .. } }
//! ```
//!
//! The decision is saved before anything else, so a crash after the save
//! makes every redelivery read it back, and a crash before it left nothing
//! behind: a topic-model version activated between two deliveries never
//! splits one confirmation into assignments or events under two versions.
//! The envelope id is a function of the delivery's id
//! (`transport.consumer.derived-envelope-ids`), so a republish lands on an
//! id the bus already holds (`transport.publish.idempotent-on-id`).
//!
//! One case cannot be replayed exactly: a confirmed transmission the store
//! does not hold (logged at warn) has nowhere to record its decision, so a
//! redelivery after a version change classifies it under the newer
//! version. Its envelope keeps its id, so the bus keeps the first one.

#[cfg(test)]
mod tests;

use crosstalk_spec::derived::flow::transmission::{Classification, TransmissionState};
use crosstalk_spec::events::detect::DetectEvent;
use crosstalk_spec::events::insight::{ClassificationCause, InsightEvent};
use crosstalk_spec::events::{BusEvent, Envelope, Subject};
use crosstalk_spec::ids::{EventId, TransmissionId};
use crosstalk_spec::interfaces::l5_flow::transmissions::{
    TransmissionStore, TransmissionStoreError,
};
use crosstalk_spec::interfaces::l6_analysis::CatalogError;
use crosstalk_spec::interfaces::l6_analysis::TopicCatalog;
use crosstalk_spec::interfaces::l6_analysis::lifecycle::{
    StoredAssignment, TopicLifecycle, TopicLifecycleError,
};
use crosstalk_spec::support::Change;

/// The label of the one `TransmissionClassified` a confirmation derives
/// (`EventId::derive(delivery, CLASSIFIED_LABEL, 0)`).
pub const CLASSIFIED_LABEL: &str = "transmission-classified";

/// Why a delivery was not classified. Every one is retryable: nothing the
/// step decided was lost, and a redelivery redoes it.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ClassifyError {
    #[error("reading the topic versions: {0:?}")]
    Catalog(CatalogError),
    #[error("storing the assignment: {0:?}")]
    Assignment(TopicLifecycleError),
    #[error("reading or saving the transmission: {0:?}")]
    Transmissions(TransmissionStoreError),
}

/// The classification step over a topic catalog and the transmission store.
#[derive(Debug, Clone)]
pub struct Classifier<C, T> {
    catalog: C,
    transmissions: T,
}

impl<C, T> Classifier<C, T>
where
    C: TopicCatalog + TopicLifecycle + Send,
    T: TransmissionStore + Send,
{
    pub fn new(catalog: C, transmissions: T) -> Self {
        Self {
            catalog,
            transmissions,
        }
    }

    /// The subjects the step consumes.
    pub fn subjects() -> Vec<Subject> {
        vec![Subject::TransmissionConfirmed]
    }

    pub fn catalog(&self) -> &C {
        &self.catalog
    }

    pub fn transmissions(&self) -> &T {
        &self.transmissions
    }

    /// Classify one delivery. Returns the envelope to publish, or `None`
    /// for an event the step does not consume.
    pub async fn classify(
        &mut self,
        delivery: &Envelope,
    ) -> Result<Option<Envelope>, ClassifyError> {
        let BusEvent::Detect(DetectEvent::TransmissionConfirmed {
            transmission,
            from,
            to,
            route,
            at,
            matched_bytes,
        }) = &delivery.event
        else {
            return Ok(None);
        };
        let classification = self.decide(*transmission).await?;
        let stored = StoredAssignment {
            topic: classification.topic,
            confirmed_at: *at,
            matched_bytes: *matched_bytes,
            from: *from,
            to: *to,
        };
        match self
            .catalog
            .assign(*transmission, classification.version, stored)
            .await
        {
            Ok(Change::Applied | Change::Unchanged) => {}
            // Another classifier stored a different assignment first. The
            // event is still published: L7 applies one contribution per
            // (transmission, version) and ignores the rest.
            Err(error @ TopicLifecycleError::Conflicting { .. }) => {
                tracing::warn!(
                    transmission = %transmission.ulid_text(),
                    version = classification.version.0,
                    error = ?error,
                    "assignment already stored differently"
                );
            }
            // Retention dropped the version after the decision was saved:
            // its assignments are gone and its sizes frozen, so there is
            // nothing left to store under it. A retry would fail the same
            // way forever.
            Err(error @ TopicLifecycleError::VersionNotRetained(_)) => {
                tracing::warn!(
                    transmission = %transmission.ulid_text(),
                    version = classification.version.0,
                    error = ?error,
                    "classified under a version retention has since dropped"
                );
            }
            Err(error) => return Err(ClassifyError::Assignment(error)),
        }
        let classified = InsightEvent::TransmissionClassified {
            cause: ClassificationCause::Confirmation,
            transmission: *transmission,
            from: *from,
            to: *to,
            route: route.clone(),
            at: *at,
            matched_bytes: *matched_bytes,
            classification: classification.clone(),
        };
        tracing::info!(
            transmission = %transmission.ulid_text(),
            version = classification.version.0,
            "transmission classified"
        );
        Ok(Some(Envelope {
            id: EventId::derive(delivery.id, CLASSIFIED_LABEL, 0),
            at: delivery.at,
            event: BusEvent::Insight(classified),
        }))
    }

    /// The transmission's classification: the stored one when an earlier
    /// delivery saved it, otherwise a fresh one, saved before it is used.
    async fn decide(&mut self, id: TransmissionId) -> Result<Classification, ClassifyError> {
        let stored = self
            .transmissions
            .transmission(id)
            .await
            .map_err(ClassifyError::Transmissions)?;
        if let Some(stored) = &stored
            && let TransmissionState::Classified { classification, .. }
            | TransmissionState::Aggregated { classification, .. } = &stored.state
        {
            return Ok(classification.clone());
        }
        let history = self
            .catalog
            .versions()
            .await
            .map_err(ClassifyError::Catalog)?;
        let classification = Classification {
            version: history.active().version(),
            topic: None,
            watched: false,
        };
        match stored {
            None => {
                tracing::warn!(transmission = %id.ulid_text(), "confirmed transmission not stored; classified without a state");
            }
            Some(mut transmission) => {
                if let TransmissionState::Confirmed(confirmed) = &transmission.state {
                    transmission.state = TransmissionState::Classified {
                        confirmed: confirmed.clone(),
                        classification: classification.clone(),
                    };
                    self.transmissions
                        .save(transmission)
                        .await
                        .map_err(ClassifyError::Transmissions)?;
                }
            }
        }
        Ok(classification)
    }
}
