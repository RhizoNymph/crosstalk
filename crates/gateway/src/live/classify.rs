//! The minimal L6 classifier: every confirmed transmission classified under
//! the topic catalog's active version, so L7 has `TransmissionClassified`
//! to aggregate edges from.
//!
//! On `TransmissionConfirmed` it reads the active version from the
//! catalog, stores the transmission's assignment under it
//! (`TopicLifecycle::assign`, so topic sizes count it), saves the
//! transmission as `Classified` (`TransmissionStore::save`) and publishes
//! `TransmissionClassified { cause: Confirmation, .. }` with the
//! confirmation's facts.
//!
//! It runs no topic model: nothing in this process embeds a
//! transmission's content, so the assignment is unassigned (`topic:
//! None`, the outlier bucket) whatever the version, and never watched.
//! When `crosstalk-analysis` has a consumer, it takes this slot.

use crosstalk_memory::analysis::catalog::InMemoryTopicCatalog;
use crosstalk_memory::flow::MemoryVerdicts;
use crosstalk_spec::derived::flow::transmission::{Classification, TransmissionState};
use crosstalk_spec::events::detect::DetectEvent;
use crosstalk_spec::events::insight::{ClassificationCause, InsightEvent};
use crosstalk_spec::events::{BusEvent, Envelope, Subject};
use crosstalk_spec::ids::TransmissionId;
use crosstalk_spec::interfaces::l5_flow::transmissions::TransmissionStore;
use crosstalk_spec::interfaces::l6_analysis::TopicCatalog;
use crosstalk_spec::interfaces::l6_analysis::lifecycle::{
    StoredAssignment, TopicLifecycle, TopicLifecycleError,
};
use crosstalk_spec::support::Change;

use super::stage::{Publisher, Stage, StageError};

/// The L6 slot's stage.
pub struct Classifier {
    catalog: InMemoryTopicCatalog,
    transmissions: MemoryVerdicts,
    publisher: Publisher,
}

impl Classifier {
    pub fn new(
        catalog: InMemoryTopicCatalog,
        transmissions: MemoryVerdicts,
        publisher: Publisher,
    ) -> Self {
        Self {
            catalog,
            transmissions,
            publisher,
        }
    }

    /// Store the confirmed transmission as classified: `TransmissionStore`
    /// holds the latest state, and analysis saves each classification. A
    /// transmission already past `Confirmed` (classified before, or
    /// aggregated) keeps its state.
    async fn store_state(
        &mut self,
        id: TransmissionId,
        classification: &Classification,
    ) -> Result<(), StageError> {
        let stored =
            self.transmissions
                .transmission(id)
                .await
                .map_err(|error| StageError::Retry {
                    reason: format!("reading the transmission: {error:?}"),
                })?;
        let Some(mut transmission) = stored else {
            tracing::warn!(transmission = %id.ulid_text(), "confirmed transmission not stored; classified without a state");
            return Ok(());
        };
        let TransmissionState::Confirmed(confirmed) = &transmission.state else {
            return Ok(());
        };
        transmission.state = TransmissionState::Classified {
            confirmed: confirmed.clone(),
            classification: classification.clone(),
        };
        self.transmissions
            .save(transmission)
            .await
            .map_err(|error| StageError::Retry {
                reason: format!("saving the classified transmission: {error:?}"),
            })
    }
}

impl Stage for Classifier {
    fn subjects(&self) -> Vec<Subject> {
        vec![Subject::TransmissionConfirmed]
    }

    async fn handle(&mut self, envelope: &Envelope) -> Result<(), StageError> {
        let BusEvent::Detect(DetectEvent::TransmissionConfirmed {
            transmission,
            from,
            to,
            route,
            at,
            matched_bytes,
        }) = &envelope.event
        else {
            return Ok(());
        };
        let history = self
            .catalog
            .versions()
            .await
            .map_err(|error| StageError::Retry {
                reason: format!("reading the topic versions: {error:?}"),
            })?;
        let version = history.active().version();
        let stored = StoredAssignment {
            topic: None,
            confirmed_at: *at,
            matched_bytes: *matched_bytes,
            from: *from,
            to: *to,
        };
        match self.catalog.assign(*transmission, version, stored).await {
            Ok(Change::Applied | Change::Unchanged) => {}
            // Another classifier stored a different assignment first. The
            // event is still published: L7 applies one contribution per
            // (transmission, version) and ignores the rest.
            Err(error @ TopicLifecycleError::Conflicting { .. }) => {
                tracing::warn!(
                    transmission = %transmission.ulid_text(),
                    version = version.0,
                    error = ?error,
                    "assignment already stored differently"
                );
            }
            Err(error) => {
                return Err(StageError::Retry {
                    reason: format!("storing the assignment: {error:?}"),
                });
            }
        }
        let classification = Classification {
            version,
            topic: None,
            watched: false,
        };
        self.store_state(*transmission, &classification).await?;
        let classified = InsightEvent::TransmissionClassified {
            cause: ClassificationCause::Confirmation,
            transmission: *transmission,
            from: *from,
            to: *to,
            route: route.clone(),
            at: *at,
            matched_bytes: *matched_bytes,
            classification,
        };
        self.publisher
            .publish(BusEvent::Insight(classified))
            .await
            .map_err(|error| StageError::Retry {
                reason: format!("publishing TransmissionClassified: {error}"),
            })?;
        tracing::info!(
            transmission = %transmission.ulid_text(),
            version = version.0,
            "transmission classified"
        );
        Ok(())
    }
}
