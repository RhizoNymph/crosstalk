//! L8 and L2 writes: config loads and entries, sink deliveries, rules,
//! triage and alert actions, projection jobs, message bodies and dead
//! letters.

use crosstalk_spec::aggregates::alert::{AlertDraft, TriageOutcome};
use crosstalk_spec::interfaces::l2_transport::{BlobStore, DeadLetterStore};
use crosstalk_spec::interfaces::l6_analysis::alerts::AlertActions;
use crosstalk_spec::interfaces::l6_analysis::{AlertRuleStore, AlertTriage, ProjectionStore};
use crosstalk_spec::interfaces::l8_surface::audit::{AuditBody, ConfigOutcome, ConfigRecord};
use crosstalk_spec::interfaces::l8_surface::operators::OperatorStore;
use crosstalk_spec::interfaces::l8_surface::sinks::SinkRegistry;
use crosstalk_spec::interfaces::l8_surface::{ActionError, ActionOutcome, OperatorAction};
use crosstalk_spec::support::Timestamp;

use crate::error::WorldError;
use crate::script::{AlertKey, Op, RuleRef};
use crate::stores::WorldStores;

use super::{Runner, Tracked, outcome};

impl<S: WorldStores> Runner<'_, S> {
    pub(super) async fn surface(&mut self, at: Timestamp, op: Op) -> Result<(), WorldError> {
        match op {
            Op::LoadAccess { hash } => {
                let access = self.config.access.clone();
                self.stores
                    .operators()
                    .load(&access, hash, at)
                    .await
                    .map(drop)
                    .map_err(|e| WorldError::store("OperatorStore::load", at, e))
            }
            Op::ConfigEntry { hash, change } => {
                let record = ConfigRecord {
                    config: hash,
                    change,
                    outcome: ConfigOutcome::Applied,
                };
                self.append(at, AuditBody::Config(record)).await
            }
            Op::Delivery { sink, outcome } => self
                .stores
                .sinks()
                .record_delivery(sink, outcome)
                .await
                .map_err(|e| WorldError::store("SinkRegistry::record_delivery", at, e)),
            Op::CreateRule {
                key,
                name,
                rule,
                sinks,
                by,
            } => {
                let action = OperatorAction::CreateRule {
                    name: name.clone(),
                    rule: rule.clone(),
                    sinks: sinks.clone(),
                };
                let id = self
                    .stores
                    .alerts()
                    .create(name, rule, sinks, by, at)
                    .await
                    .map_err(|e| WorldError::store("AlertRuleStore::create", at, e))?;
                self.ledger.rules.insert(key, id);
                self.audit(at, by, action, Ok(ActionOutcome::RuleCreated(id)))
                    .await
            }
            Op::SetRuleEnabled { rule, enabled, by } => {
                let id = self.ledger.rule(RuleRef::User(rule))?;
                let change = self
                    .stores
                    .alerts()
                    .set_enabled(id, enabled, by, at)
                    .await
                    .map_err(|e| WorldError::store("AlertRuleStore::set_enabled", at, e))?;
                if !enabled {
                    self.ledger.rule_disabled(id);
                }
                let action = OperatorAction::SetRuleEnabled { id, enabled };
                self.audit(at, by, action, Ok(outcome(change))).await
            }
            Op::Triage {
                alert,
                rule,
                subject,
            } => self.triage(at, alert, rule, subject).await,
            Op::Acknowledge { alert, by } => {
                let Some(tracked) = self.ledger.active(alert) else {
                    self.ledger.skipped += 1;
                    return Ok(());
                };
                let change = self
                    .stores
                    .alerts()
                    .acknowledge(tracked.id, by, at)
                    .await
                    .map_err(|e| WorldError::store("AlertActions::acknowledge", at, e))?;
                let action = OperatorAction::Acknowledge { alert: tracked.id };
                self.audit(at, by, action, Ok(outcome(change))).await
            }
            Op::Resolve { alert, by, note } => {
                let Some(tracked) = self.ledger.active(alert) else {
                    self.ledger.skipped += 1;
                    return Ok(());
                };
                let change = self
                    .stores
                    .alerts()
                    .resolve(tracked.id, by, at, note.clone())
                    .await
                    .map_err(|e| WorldError::store("AlertActions::resolve", at, e))?;
                if let Some(book) = self.ledger.alerts.get_mut(&alert) {
                    book.active = false;
                }
                let action = OperatorAction::Resolve {
                    alert: tracked.id,
                    note,
                };
                self.audit(at, by, action, Ok(outcome(change))).await
            }
            Op::RefusedAcknowledge { alert, by } => {
                let id = self
                    .ledger
                    .alerts
                    .get(&alert)
                    .map(|tracked| tracked.id)
                    .ok_or_else(|| WorldError::missing(format!("alert {alert:?}")))?;
                let result = self
                    .stores
                    .alerts()
                    .acknowledge(id, by, at)
                    .await
                    .map(outcome)
                    .map_err(ActionError::from);
                self.audit(at, by, OperatorAction::Acknowledge { alert: id }, result)
                    .await
            }
            Op::Enqueue(info) => self
                .stores
                .projections()
                .enqueue(*info)
                .await
                .map_err(|e| WorldError::store("ProjectionStore::enqueue", at, e)),
            Op::StartFit { job } => {
                let claimed = self
                    .stores
                    .projections()
                    .claim(at)
                    .await
                    .map_err(|e| WorldError::store("ProjectionStore::claim", at, e))?;
                let got = claimed.as_ref().map(|info| info.id());
                if got != Some(job) {
                    return Err(WorldError::diverged(
                        "ProjectionStore::claim",
                        at,
                        Some(job),
                        got,
                    ));
                }
                Ok(())
            }
            Op::CompleteJob { job, frame } => self
                .stores
                .projections()
                .complete(job, *frame, at)
                .await
                .map_err(|e| WorldError::store("ProjectionStore::complete", at, e)),
            Op::FailFit { job, failure } => self
                .stores
                .projections()
                .fail(job, failure, at)
                .await
                .map_err(|e| WorldError::store("ProjectionStore::fail", at, e)),
            Op::ExpireFrames => self
                .stores
                .projections()
                .expire(at)
                .await
                .map(drop)
                .map_err(|e| WorldError::store("ProjectionStore::expire", at, e)),
            Op::Body { hash, bytes } => {
                let stored = self
                    .stores
                    .blobs()
                    .put(&bytes)
                    .await
                    .map_err(|e| WorldError::store("BlobStore::put", at, e))?;
                if stored != hash {
                    return Err(WorldError::diverged("BlobStore::put", at, hash, stored));
                }
                Ok(())
            }
            Op::DeadLetter(letter) => self
                .stores
                .letters()
                .put(*letter)
                .await
                .map_err(|e| WorldError::store("DeadLetterStore::put", at, e)),
            other => Err(WorldError::missing(format!("a surface op, not {other:?}"))),
        }
    }

    /// One draft, as the alerts consumer triages it: it opens the planned
    /// alert, or deduplicates into it while it is active.
    async fn triage(
        &mut self,
        at: Timestamp,
        key: AlertKey,
        rule: RuleRef,
        subject: crosstalk_spec::aggregates::alert::AlertSubject,
    ) -> Result<(), WorldError> {
        let rule = self.ledger.rule(rule)?;
        let draft = AlertDraft {
            rule,
            subject,
            raised_at: at,
        };
        let triaged = self
            .stores
            .alerts()
            .triage(draft)
            .await
            .map_err(|e| WorldError::store("AlertTriage::triage", at, e))?;
        let active = self.ledger.active(key);
        match (triaged, active) {
            (TriageOutcome::Opened(alert), None) => {
                self.ledger.alerts.insert(
                    key,
                    Tracked {
                        id: alert.id,
                        rule,
                        subject,
                        active: true,
                    },
                );
                Ok(())
            }
            (TriageOutcome::Deduplicated { into }, Some(tracked)) if into == tracked.id => Ok(()),
            (triaged, active) => Err(WorldError::diverged(
                "AlertTriage::triage",
                at,
                active.map_or("Opened".to_owned(), |t| {
                    format!("Deduplicated into {:?}", t.id)
                }),
                triaged,
            )),
        }
    }
}
