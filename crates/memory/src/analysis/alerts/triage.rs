//! `AlertTriage` on the reference alert store, and `AlertActions`, the
//! surface's acknowledge and resolve, which change the same alerts.
//!
//! ```text
//! draft ─▶ rule unknown or not evaluating ─▶ RuleInactive
//!       ─▶ subject held as FalseDetection ─▶ OperatorRejected
//!       ─▶ active alert with the same rule and stored subject ─▶ Deduplicated (occurrences + 1)
//!       ─▶ otherwise ─▶ Opened (Open, one occurrence)
//! Open ─acknowledge─▶ Acknowledged ─resolve─▶ Resolved
//! Open | Acknowledged ─sanction, disable, false detection─▶ Suppressed
//! ```

use crosstalk_spec::aggregates::alert::{
    Alert, AlertDraft, AlertRevision, AlertState, AlertSubject, SuppressReason, TriageOutcome,
};
use crosstalk_spec::derived::flow::verdict::{CurrentVerdict, Observed, Verdict, VerdictRevision};
use crosstalk_spec::events::changed::Changed;
use crosstalk_spec::events::insight::InsightEvent;
use crosstalk_spec::ids::{AlertId, AlertRuleId, ChannelId, OperatorId, TransmissionId};
use crosstalk_spec::interfaces::l3_reconstruction::AgentDirectory;
use crosstalk_spec::interfaces::l5_flow::ChannelDirectory;
use crosstalk_spec::interfaces::l6_analysis::alerts::{AlertActionError, AlertActions};
use crosstalk_spec::interfaces::l6_analysis::{AlertTriage, Embedder, TriageError};
use crosstalk_spec::support::{Change, Timestamp};

use super::{AlertsState, CommitRefused, InMemoryAlertStore, StoredAlert, is_active};
use crate::analysis::aliases::Directories;
use crate::support::{Outbox, lock};

fn triage_error(error: CommitRefused) -> TriageError {
    TriageError::Store {
        reason: error.to_string(),
    }
}

fn action_error(error: CommitRefused) -> AlertActionError {
    AlertActionError::Store {
        reason: error.to_string(),
    }
}

impl AlertsState {
    /// Triage's copy of `transmission`'s verdict holds `FalseDetection`.
    fn rejected(&self, subject: AlertSubject) -> bool {
        match subject {
            AlertSubject::Transmission(transmission) => {
                CurrentVerdict::is_false_detection(self.verdicts.get(&transmission))
            }
            AlertSubject::Channel(_) | AlertSubject::Agent(_) => false,
        }
    }

    fn open(&mut self, draft: AlertDraft, outbox: &Outbox) -> Alert {
        let alert = Alert {
            id: AlertId::from_ulid(self.alert_ids.next_ulid()),
            rule: draft.rule,
            subject: draft.subject,
            raised_at: draft.raised_at,
            occurrences: 1,
            state: AlertState::Open,
        };
        self.alerts.insert(
            alert.id,
            StoredAlert {
                alert: alert.clone(),
                revision: AlertRevision::OPENED,
            },
        );
        outbox.insight(InsightEvent::AlertOpened(alert.clone()));
        outbox.changed(Changed::Alert(alert.id));
        alert
    }
}

impl<E, D> AlertActions for InMemoryAlertStore<E, D>
where
    E: Embedder + Send + Sync,
    D: AgentDirectory + ChannelDirectory + Send + Sync,
{
    async fn acknowledge(
        &mut self,
        alert: AlertId,
        by: OperatorId,
        at: Timestamp,
    ) -> Result<Change, AlertActionError> {
        let mut state = lock(&self.state);
        let stored = state
            .alerts
            .get(&alert)
            .ok_or(AlertActionError::UnknownAlert(alert))?;
        let mut next = stored.alert.clone();
        match next.state {
            AlertState::Open => next.state = AlertState::Acknowledged { by, at },
            AlertState::Acknowledged { .. } => return Ok(Change::Unchanged),
            AlertState::Resolved { .. } | AlertState::Suppressed { .. } => {
                return Err(AlertActionError::NotActive(alert));
            }
        }
        state
            .commit_alert(next, &self.outbox)
            .map_err(action_error)?;
        Ok(Change::Applied)
    }

    async fn resolve(
        &mut self,
        alert: AlertId,
        by: OperatorId,
        at: Timestamp,
        note: Option<String>,
    ) -> Result<Change, AlertActionError> {
        let mut state = lock(&self.state);
        let stored = state
            .alerts
            .get(&alert)
            .ok_or(AlertActionError::UnknownAlert(alert))?;
        let mut next = stored.alert.clone();
        match next.state {
            AlertState::Acknowledged { .. } => {
                next.state = AlertState::Resolved { by, at, note };
            }
            AlertState::Open => return Err(AlertActionError::NotAcknowledged(alert)),
            AlertState::Resolved { .. } | AlertState::Suppressed { .. } => {
                return Err(AlertActionError::NotActive(alert));
            }
        }
        state
            .commit_alert(next, &self.outbox)
            .map_err(action_error)?;
        Ok(Change::Applied)
    }
}

impl<E, D> AlertTriage for InMemoryAlertStore<E, D>
where
    E: Embedder + Send + Sync,
    D: AgentDirectory + ChannelDirectory + Send + Sync,
{
    async fn triage(&mut self, draft: AlertDraft) -> Result<TriageOutcome, TriageError> {
        let mut state = lock(&self.state);
        let evaluates = state
            .rules
            .get(draft.rule)
            .is_some_and(crosstalk_spec::aggregates::alert::AlertRuleDef::evaluates);
        if !evaluates {
            return Ok(TriageOutcome::RuleInactive);
        }
        if state.rejected(draft.subject) {
            return Ok(TriageOutcome::OperatorRejected);
        }
        let active = state
            .alerts
            .values()
            .find(|stored| {
                is_active(&stored.alert.state)
                    && stored.alert.rule == draft.rule
                    && stored.alert.subject == draft.subject
            })
            .map(|stored| stored.alert.clone());
        match active {
            Some(mut alert) => {
                alert.occurrences = alert
                    .occurrences
                    .checked_add(1)
                    .ok_or(CommitRefused::RevisionExhausted)
                    .map_err(triage_error)?;
                let into = alert.id;
                state
                    .commit_alert(alert, &self.outbox)
                    .map_err(triage_error)?;
                Ok(TriageOutcome::Deduplicated { into })
            }
            None => Ok(TriageOutcome::Opened(state.open(draft, &self.outbox))),
        }
    }

    async fn channel_sanctioned(
        &mut self,
        channel: ChannelId,
        at: Timestamp,
    ) -> Result<u32, TriageError> {
        let aliases = Directories(&self.directory);
        let sanctioned = AlertSubject::Channel(channel).resolved(aliases);
        let mut state = lock(&self.state);
        state
            .suppress(
                |alert| {
                    matches!(alert.subject, AlertSubject::Channel(_))
                        && alert.subject.resolved(aliases) == sanctioned
                },
                SuppressReason::ChannelSanctioned,
                at,
                &self.outbox,
            )
            .map_err(triage_error)
    }

    async fn rule_disabled(
        &mut self,
        rule: AlertRuleId,
        at: Timestamp,
    ) -> Result<u32, TriageError> {
        let mut state = lock(&self.state);
        state
            .suppress(
                |alert| alert.rule == rule,
                SuppressReason::RuleDisabled,
                at,
                &self.outbox,
            )
            .map_err(triage_error)
    }

    async fn transmission_judged(
        &mut self,
        transmission: TransmissionId,
        verdict: Option<Verdict>,
        revision: VerdictRevision,
        at: Timestamp,
    ) -> Result<u32, TriageError> {
        let mut state = lock(&self.state);
        let held = state.verdicts.get(&transmission).copied();
        let mut copy = held.unwrap_or(CurrentVerdict { verdict, revision });
        let observed = match held {
            Some(_) => copy.observe(verdict, revision),
            None => Observed::Newer,
        };
        if observed == Observed::Stale {
            return Ok(0);
        }
        let suppressed = if verdict == Some(Verdict::FalseDetection) {
            state
                .suppress(
                    |alert| alert.subject == AlertSubject::Transmission(transmission),
                    SuppressReason::OperatorRejected,
                    at,
                    &self.outbox,
                )
                .map_err(triage_error)?
        } else {
            0
        };
        state.verdicts.insert(transmission, copy);
        Ok(suppressed)
    }
}
