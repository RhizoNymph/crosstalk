//! The minimal L6 classifier: every confirmed transmission classified under
//! the topic catalog's active version, so L7 has `TransmissionClassified`
//! to aggregate edges from.
//!
//! On `TransmissionConfirmed` it reads the active version from the
//! catalog, stores the transmission's assignment under it
//! (`TopicLifecycle::assign`, so topic sizes count it) and publishes
//! `TransmissionClassified { cause: Confirmation, .. }` with the
//! confirmation's facts.
//!
//! It runs no topic model: nothing in this process embeds a
//! transmission's content, so the assignment is unassigned (`topic:
//! None`, the outlier bucket) whatever the version, and never watched.
//! When `crosstalk-analysis` has a consumer, it takes this slot.

use crosstalk_memory::analysis::catalog::InMemoryTopicCatalog;
use crosstalk_spec::derived::flow::transmission::Classification;
use crosstalk_spec::events::detect::DetectEvent;
use crosstalk_spec::events::insight::{ClassificationCause, InsightEvent};
use crosstalk_spec::events::{BusEvent, Envelope, Subject};
use crosstalk_spec::interfaces::l6_analysis::TopicCatalog;
use crosstalk_spec::interfaces::l6_analysis::lifecycle::{
    StoredAssignment, TopicLifecycle, TopicLifecycleError,
};
use crosstalk_spec::support::Change;

use super::stage::{Publisher, Stage, StageError};

/// The L6 slot's stage.
pub struct Classifier {
    catalog: InMemoryTopicCatalog,
    publisher: Publisher,
}

impl Classifier {
    pub fn new(catalog: InMemoryTopicCatalog, publisher: Publisher) -> Self {
        Self { catalog, publisher }
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
        let classified = InsightEvent::TransmissionClassified {
            cause: ClassificationCause::Confirmation,
            transmission: *transmission,
            from: *from,
            to: *to,
            route: route.clone(),
            at: *at,
            matched_bytes: *matched_bytes,
            classification: Classification {
                version,
                topic: None,
                watched: false,
            },
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
