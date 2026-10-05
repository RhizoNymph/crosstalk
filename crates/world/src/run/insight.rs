//! L6 and L7 writes: topic fits, assignments and activation, the search
//! corpus, edge contributions and the watermark.

use crosstalk_spec::aggregates::retention::{Pin, PinChange};
use crosstalk_spec::interfaces::l6_analysis::TopicCatalog;
use crosstalk_spec::interfaces::l6_analysis::alerts::AlertRuleMaintenance;
use crosstalk_spec::interfaces::l6_analysis::corpus::SearchCorpus;
use crosstalk_spec::interfaces::l6_analysis::lifecycle::{CatalogActivation, TopicLifecycle};
use crosstalk_spec::interfaces::l7_topology::{Activation, EdgeError, EdgeStore};
use crosstalk_spec::interfaces::l8_surface::{ActionOutcome, OperatorAction};
use crosstalk_spec::support::Timestamp;

use crate::error::WorldError;
use crate::script::Op;
use crate::stores::WorldStores;

use super::Runner;

impl<S: WorldStores> Runner<'_, S> {
    pub(super) async fn insight(&mut self, at: Timestamp, op: Op) -> Result<(), WorldError> {
        match op {
            Op::SetModel => {
                let model = self.config.embedding.clone();
                self.stores
                    .search()
                    .set_model(model)
                    .await
                    .map_err(|e| WorldError::store("SearchCorpus::set_model", at, e))
            }
            Op::BeginFit { version } => {
                let got = self
                    .stores
                    .catalog()
                    .begin_fit(at)
                    .await
                    .map_err(|e| WorldError::store("TopicLifecycle::begin_fit", at, e))?;
                if got != version {
                    return Err(WorldError::diverged(
                        "TopicLifecycle::begin_fit",
                        at,
                        version,
                        got,
                    ));
                }
                Ok(())
            }
            Op::CompleteFit { version, topics } => {
                let ids = topics.iter().map(|t| t.id).collect();
                let lineage = self
                    .stores
                    .catalog()
                    .complete_fit(version, topics, at)
                    .await
                    .map_err(|e| WorldError::store("TopicLifecycle::complete_fit", at, e))?;
                self.ledger.lineages.insert(version, lineage);
                self.ledger.topics.insert(version, ids);
                Ok(())
            }
            Op::Assign {
                transmission,
                version,
                assignment,
            } => self
                .stores
                .catalog()
                .assign(transmission, version, assignment)
                .await
                .map(drop)
                .map_err(|e| WorldError::store("TopicLifecycle::assign", at, e)),
            Op::Ready { version, count } => {
                self.stores
                    .catalog()
                    .mark_ready(version, at)
                    .await
                    .map_err(|e| WorldError::store("TopicLifecycle::mark_ready", at, e))?;
                self.stores
                    .edges()
                    .version_ready(version, count)
                    .await
                    .map_err(|e| WorldError::store("EdgeStore::version_ready", at, e))?;
                let lineage = self
                    .ledger
                    .lineages
                    .get(&version)
                    .cloned()
                    .ok_or_else(|| WorldError::missing(format!("lineage to {version:?}")))?;
                let topics = self
                    .ledger
                    .topics
                    .get(&version)
                    .cloned()
                    .unwrap_or_default();
                self.stores
                    .alerts()
                    .topic_version_ready(&lineage, &topics)
                    .await
                    .map(drop)
                    .map_err(|e| {
                        WorldError::store("AlertRuleMaintenance::topic_version_ready", at, e)
                    })
            }
            Op::Activate { version } => {
                let activation = self
                    .stores
                    .edges()
                    .activate(version)
                    .await
                    .map_err(|e| WorldError::store("EdgeStore::activate", at, e))?;
                if !matches!(activation, Activation::Switched { .. }) {
                    return Err(WorldError::diverged(
                        "EdgeStore::activate",
                        at,
                        "Switched",
                        activation,
                    ));
                }
                let activated = self
                    .stores
                    .catalog()
                    .mark_active(version, at)
                    .await
                    .map_err(|e| WorldError::store("TopicLifecycle::mark_active", at, e))?;
                let CatalogActivation::Switched { dropped, .. } = activated else {
                    return Err(WorldError::diverged(
                        "TopicLifecycle::mark_active",
                        at,
                        "Switched",
                        activated,
                    ));
                };
                for version in dropped {
                    self.stores
                        .edges()
                        .drop_version(version)
                        .await
                        .map_err(|e| WorldError::store("EdgeStore::drop_version", at, e))?;
                }
                Ok(())
            }
            Op::Pin { version, by } => {
                let change = self
                    .stores
                    .catalog()
                    .pin(version, Pin { by, at })
                    .await
                    .map_err(|e| WorldError::store("TopicCatalog::pin", at, e))?;
                let result = match change {
                    PinChange::Changed => ActionOutcome::Applied,
                    PinChange::Unchanged => ActionOutcome::Unchanged,
                };
                self.audit(
                    at,
                    by,
                    OperatorAction::PinTopicVersion { version },
                    Ok(result),
                )
                .await
            }
            Op::Index(document) => self
                .stores
                .search()
                .index(*document)
                .await
                .map_err(|e| WorldError::store("SearchCorpus::index", at, e)),
            Op::Edge(contribution) => match self.stores.edges().apply(&contribution).await {
                // A merge can make sender and reader one agent; L7 counts
                // the transmission as processed and draws no edge.
                Ok(_) | Err(EdgeError::SelfEdge) => Ok(()),
                Err(error) => Err(WorldError::store("EdgeStore::apply", at, error)),
            },
            Op::Watermark(frontier) => self
                .stores
                .edges()
                .advance_watermark(frontier)
                .await
                .map(drop)
                .map_err(|e| WorldError::store("EdgeStore::advance_watermark", at, e)),
            other => Err(WorldError::missing(format!("an insight op, not {other:?}"))),
        }
    }
}
