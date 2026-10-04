//! `AlertTriage` on [`PgAlertStore`], and `AlertActions`, the surface's
//! acknowledge and resolve, which change the same alerts.
//!
//! ```text
//! draft ─▶ rule unknown or not evaluating ─▶ RuleInactive
//!       ─▶ subject held as FalseDetection ─▶ OperatorRejected
//!       ─▶ active alert with the same rule and stored subject ─▶ Deduplicated (occurrences + 1)
//!       ─▶ otherwise ─▶ Opened (Open, one occurrence)
//! Open ─acknowledge─▶ Acknowledged ─resolve─▶ Resolved
//! Open | Acknowledged ─sanction, disable, false detection─▶ Suppressed
//! ```
//!
//! Each call is one `SERIALIZABLE` transaction that reads the rule, the
//! verdict copy and the alerts it decides on; the partial unique index on
//! active (rule, subject) backs the one-active-alert rule against
//! concurrent drafts.

use crosstalk_spec::aggregates::alert::{
    Alert, AlertDraft, AlertState, AlertSubject, SuppressReason, TriageOutcome,
};
use crosstalk_spec::derived::flow::verdict::{CurrentVerdict, Observed, Verdict, VerdictRevision};
use crosstalk_spec::ids::{AlertId, AlertRuleId, ChannelId, OperatorId, TransmissionId};
use crosstalk_spec::interfaces::l3_reconstruction::AgentDirectory;
use crosstalk_spec::interfaces::l5_flow::ChannelDirectory;
use crosstalk_spec::interfaces::l6_analysis::alerts::{AlertActionError, AlertActions};
use crosstalk_spec::interfaces::l6_analysis::{AlertTriage, Embedder, TriageError};
use crosstalk_spec::support::{Change, Timestamp};
use crosstalk_store::{TxError, retry_serializable};

use super::store::{self, Active, abort, finish};
use super::{PgAlertStore, SubjectFacts};
use crate::pg::codec::to_json;
use crate::pg::outbox::{self, Pending};
use crate::pg::{EventSink, StorageFailure};

/// How an acknowledge or resolve changes an alert's state.
#[derive(Debug, Clone)]
enum Action {
    Acknowledge {
        by: OperatorId,
        at: Timestamp,
    },
    Resolve {
        by: OperatorId,
        at: Timestamp,
        note: Option<String>,
    },
}

impl Action {
    /// The state after the action, `None` when it changes nothing, or the
    /// refusal.
    fn apply(
        &self,
        alert: AlertId,
        state: &AlertState,
    ) -> Result<Option<AlertState>, AlertActionError> {
        match (self, state) {
            (Self::Acknowledge { by, at }, AlertState::Open) => {
                Ok(Some(AlertState::Acknowledged { by: *by, at: *at }))
            }
            (Self::Acknowledge { .. }, AlertState::Acknowledged { .. }) => Ok(None),
            (Self::Resolve { by, at, note }, AlertState::Acknowledged { .. }) => {
                Ok(Some(AlertState::Resolved {
                    by: *by,
                    at: *at,
                    note: note.clone(),
                }))
            }
            (Self::Resolve { .. }, AlertState::Open) => {
                Err(AlertActionError::NotAcknowledged(alert))
            }
            (_, AlertState::Resolved { .. } | AlertState::Suppressed { .. }) => {
                Err(AlertActionError::NotActive(alert))
            }
        }
    }
}

impl<E, D, F, S> PgAlertStore<E, D, F, S>
where
    E: Embedder + Send + Sync,
    D: AgentDirectory + ChannelDirectory + Clone + Send + Sync + 'static,
    F: SubjectFacts,
    S: EventSink,
{
    async fn act(&self, alert: AlertId, action: Action) -> Result<Change, AlertActionError> {
        let (change, pending) = finish(
            retry_serializable(&self.pool, &self.retry, |conn| {
                let action = action.clone();
                Box::pin(async move {
                    let (mut stored, revision) = store::alert(conn, alert)
                        .await
                        .map_err(abort)?
                        .ok_or(TxError::Abort(AlertActionError::UnknownAlert(alert)))?;
                    let Some(state) = action.apply(alert, &stored.state).map_err(TxError::Abort)?
                    else {
                        return Ok((Change::Unchanged, Pending::default()));
                    };
                    stored.state = state;
                    let events = store::save_alert(conn, &stored, revision)
                        .await
                        .map_err(abort)?;
                    let pending = outbox::append(conn, events).await.map_err(abort)?;
                    Ok((Change::Applied, pending))
                })
            })
            .await,
        )?;
        self.deliver(pending).await;
        Ok(change)
    }

    /// Suppress the active alerts `target` selects at `at`, in one
    /// transaction. Returns how many.
    async fn suppress_target(
        &self,
        target: Target,
        reason: SuppressReason,
        at: Timestamp,
    ) -> Result<u32, TriageError> {
        let directory = self.directory.clone();
        let (count, pending) = finish(
            retry_serializable(&self.pool, &self.retry, |conn| {
                let directory = directory.clone();
                Box::pin(async move {
                    let targets: Vec<_> = match target {
                        Target::Rule(rule) => store::active(conn, Active::Rule(rule))
                            .await
                            .map_err(abort)?,
                        Target::Channel(channel) => {
                            let resolve = |subject: AlertSubject| match subject {
                                AlertSubject::Channel(channel) => {
                                    Some(ChannelDirectory::canonical(&directory, channel))
                                }
                                AlertSubject::Transmission(_) | AlertSubject::Agent(_) => None,
                            };
                            let sanctioned = ChannelDirectory::canonical(&directory, channel);
                            store::active(conn, Active::Channels)
                                .await
                                .map_err(abort)?
                                .into_iter()
                                .filter(|(alert, _)| resolve(alert.subject) == Some(sanctioned))
                                .collect()
                        }
                    };
                    let count = u32::try_from(targets.len()).unwrap_or(u32::MAX);
                    let events = store::suppress(conn, targets, reason, at)
                        .await
                        .map_err(abort)?;
                    let pending = outbox::append(conn, events).await.map_err(abort)?;
                    Ok((count, pending))
                })
            })
            .await,
        )?;
        self.deliver(pending).await;
        Ok(count)
    }
}

/// Which active alerts a suppression targets.
#[derive(Debug, Clone, Copy)]
enum Target {
    /// Whose subject resolves to this channel's canonical channel.
    Channel(ChannelId),
    /// Raised by this rule.
    Rule(AlertRuleId),
}

impl<E, D, F, S> AlertActions for PgAlertStore<E, D, F, S>
where
    E: Embedder + Send + Sync,
    D: AgentDirectory + ChannelDirectory + Clone + Send + Sync + 'static,
    F: SubjectFacts,
    S: EventSink,
{
    async fn acknowledge(
        &mut self,
        alert: AlertId,
        by: OperatorId,
        at: Timestamp,
    ) -> Result<Change, AlertActionError> {
        self.act(alert, Action::Acknowledge { by, at }).await
    }

    async fn resolve(
        &mut self,
        alert: AlertId,
        by: OperatorId,
        at: Timestamp,
        note: Option<String>,
    ) -> Result<Change, AlertActionError> {
        self.act(alert, Action::Resolve { by, at, note }).await
    }
}

impl<E, D, F, S> AlertTriage for PgAlertStore<E, D, F, S>
where
    E: Embedder + Send + Sync,
    D: AgentDirectory + ChannelDirectory + Clone + Send + Sync + 'static,
    F: SubjectFacts,
    S: EventSink,
{
    async fn triage(&mut self, draft: AlertDraft) -> Result<TriageOutcome, TriageError> {
        let subject = to_json("alert subject", &draft.subject).map_err(store::fail)?;
        let id = self.alert_id(draft.raised_at).map_err(store::fail)?;
        let (outcome, pending) = finish(
            retry_serializable(&self.pool, &self.retry, |conn| {
                let (draft, subject) = (draft.clone(), subject.clone());
                Box::pin(async move {
                    let evaluates = store::rule(conn, draft.rule)
                        .await
                        .map_err(abort)?
                        .is_some_and(|(rule, _)| rule.evaluates());
                    if !evaluates {
                        return Ok((TriageOutcome::RuleInactive, Pending::default()));
                    }
                    if let AlertSubject::Transmission(transmission) = draft.subject {
                        let copy = store::verdict(conn, transmission).await.map_err(abort)?;
                        if CurrentVerdict::is_false_detection(copy.as_ref()) {
                            return Ok((TriageOutcome::OperatorRejected, Pending::default()));
                        }
                    }
                    let active = store::active(conn, Active::Key(draft.rule, &subject))
                        .await
                        .map_err(abort)?;
                    let (outcome, events) = match active.into_iter().next() {
                        Some((mut alert, revision)) => {
                            alert.occurrences =
                                alert.occurrences.checked_add(1).ok_or_else(|| {
                                    abort(StorageFailure::RevisionExhausted(format!(
                                        "occurrences of alert {:?}",
                                        alert.id
                                    )))
                                })?;
                            let into = alert.id;
                            let events = store::save_alert(conn, &alert, revision)
                                .await
                                .map_err(abort)?;
                            (TriageOutcome::Deduplicated { into }, events)
                        }
                        None => {
                            let alert = Alert {
                                id,
                                rule: draft.rule,
                                subject: draft.subject,
                                raised_at: draft.raised_at,
                                occurrences: 1,
                                state: AlertState::Open,
                            };
                            let events = store::open_alert(conn, &alert).await.map_err(abort)?;
                            (TriageOutcome::Opened(alert), events)
                        }
                    };
                    let pending = outbox::append(conn, events).await.map_err(abort)?;
                    Ok((outcome, pending))
                })
            })
            .await,
        )?;
        self.deliver(pending).await;
        Ok(outcome)
    }

    async fn channel_sanctioned(
        &mut self,
        channel: ChannelId,
        at: Timestamp,
    ) -> Result<u32, TriageError> {
        self.suppress_target(
            Target::Channel(channel),
            SuppressReason::ChannelSanctioned,
            at,
        )
        .await
    }

    async fn rule_disabled(
        &mut self,
        rule: AlertRuleId,
        at: Timestamp,
    ) -> Result<u32, TriageError> {
        self.suppress_target(Target::Rule(rule), SuppressReason::RuleDisabled, at)
            .await
    }

    async fn transmission_judged(
        &mut self,
        transmission: TransmissionId,
        verdict: Option<Verdict>,
        revision: VerdictRevision,
        at: Timestamp,
    ) -> Result<u32, TriageError> {
        let (count, pending) = finish(
            retry_serializable(&self.pool, &self.retry, |conn| {
                Box::pin(async move {
                    let held = store::verdict(conn, transmission).await.map_err(abort)?;
                    let mut copy = held.unwrap_or(CurrentVerdict { verdict, revision });
                    let observed = match held {
                        Some(_) => copy.observe(verdict, revision),
                        None => Observed::Newer,
                    };
                    if observed == Observed::Stale {
                        return Ok((0, Pending::default()));
                    }
                    let (count, events) = if verdict == Some(Verdict::FalseDetection) {
                        let targets = store::active(conn, Active::Transmission(transmission))
                            .await
                            .map_err(abort)?;
                        let count = u32::try_from(targets.len()).unwrap_or(u32::MAX);
                        let events =
                            store::suppress(conn, targets, SuppressReason::OperatorRejected, at)
                                .await
                                .map_err(abort)?;
                        (count, events)
                    } else {
                        (0, Vec::new())
                    };
                    store::save_verdict(conn, transmission, &copy)
                        .await
                        .map_err(abort)?;
                    let pending = outbox::append(conn, events).await.map_err(abort)?;
                    Ok((count, pending))
                })
            })
            .await,
        )?;
        self.deliver(pending).await;
        Ok(count)
    }
}
