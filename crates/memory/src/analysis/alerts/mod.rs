//! [`InMemoryAlertStore`]: the reference [`AlertRuleStore`],
//! [`AlertTriage`], `AlertRuleMaintenance`, `AlertActions` and
//! `AlertReads`, in one store.
//!
//! The spec reads rules and alerts "in the same transaction": triage
//! re-checks a draft's rule, a disable suppresses the rule's active alerts,
//! and a false-detection verdict suppresses alerts and blocks new ones. One
//! lock over the rule set, the alerts and the verdict copy makes each call
//! one transaction.
//!
//! - [`rules`]: `AlertRuleStore`, and `AlertRuleMaintenance`, the `alerts`
//!   consumer's rule changes: remapping on `TopicVersionReady`, staleness
//!   on an embedding model change.
//! - [`triage`]: `AlertTriage`, and `AlertActions`, the surface's
//!   acknowledge and resolve.
//! - This module: `AlertReads`. The channel filter matches a transmission
//!   subject by its stored route, read from the L5 transmission store the
//!   alert store is given (the reference reads [`MemoryVerdicts`] directly).
//!
//! Every stored change bumps the rule's or alert's revision by one and
//! publishes the matching `AlertRuleChanged`, `AlertOpened` or
//! `AlertChanged` and `Changed` into the outbox, in the same critical
//! section. A change that would exhaust a revision counter is refused
//! before anything is touched.
//!
//! [`AlertRuleStore`]: crosstalk_spec::interfaces::l6_analysis::AlertRuleStore
//! [`AlertTriage`]: crosstalk_spec::interfaces::l6_analysis::AlertTriage
//! [`MemoryVerdicts`]: crate::flow::MemoryVerdicts

pub mod rules;
pub mod triage;

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};

use crosstalk_spec::aggregates::alert::{
    Alert, AlertRevision, AlertRuleConfig, AlertRuleDef, AlertRuleSet, AlertState, AlertSubject,
    BuiltinRule, InsertError, RuleRevision, RuleStatus, SuppressReason,
};
use crosstalk_spec::aggregates::topic::{EmbeddingModel, TopicModelVersion};
use crosstalk_spec::derived::flow::transmission::Route;
use crosstalk_spec::derived::flow::verdict::CurrentVerdict;
use crosstalk_spec::events::changed::Changed;
use crosstalk_spec::events::insight::InsightEvent;
use crosstalk_spec::ids::{AlertId, AlertRuleId, SinkId, TopicId, TransmissionId};
use crosstalk_spec::interfaces::l3_reconstruction::AgentDirectory;
use crosstalk_spec::interfaces::l5_flow::ChannelDirectory;
use crosstalk_spec::interfaces::l6_analysis::Embedder;
use crosstalk_spec::interfaces::l6_analysis::alerts::{AlertReadError, AlertReads};
use crosstalk_spec::interfaces::l8_surface::lists::AlertRuleFilter;
use crosstalk_spec::interfaces::l8_surface::{AlertFilter, AlertStateKind};
use crosstalk_spec::paging::{AlertList, AlertRuleList, Page, PageRequest};
use crosstalk_spec::support::Timestamp;

use crate::flow::MemoryVerdicts;
use crate::support::{CursorBook, IdSequence, Outbox, PageError, lock, page_after};

/// The alert store's configuration.
#[derive(Debug, Clone, PartialEq)]
pub struct AlertStoreConfig {
    pub rules: AlertRuleConfig,
    /// The configured sinks: every sink a rule lists must be one of these.
    pub sinks: BTreeSet<SinkId>,
    /// Each built-in rule's status and sinks; a rule not listed is enabled
    /// with no sinks.
    pub builtins: BTreeMap<BuiltinRule, (RuleStatus, Vec<SinkId>)>,
}

/// The reference alert store. Cloning shares the store.
#[derive(Clone)]
pub struct InMemoryAlertStore<E, D> {
    config: AlertStoreConfig,
    embedder: E,
    directory: D,
    transmissions: MemoryVerdicts,
    outbox: Outbox,
    state: Arc<Mutex<AlertsState>>,
}

#[derive(Debug)]
struct AlertsState {
    rules: AlertRuleSet,
    rule_revisions: BTreeMap<AlertRuleId, RuleRevision>,
    alerts: BTreeMap<AlertId, StoredAlert>,
    /// Triage's copy of each transmission's current verdict.
    verdicts: BTreeMap<TransmissionId, CurrentVerdict>,
    /// The topic-model version the consumer last made current, and its
    /// topics.
    version: TopicModelVersion,
    topics: BTreeSet<TopicId>,
    /// The embedder's model as the store last saw it.
    model: EmbeddingModel,
    rule_ids: IdSequence,
    alert_ids: IdSequence,
    rule_cursors: CursorBook<AlertRuleFilter, AlertRuleId>,
    alert_cursors: CursorBook<AlertFilter, AlertId>,
}

#[derive(Debug, Clone)]
struct StoredAlert {
    alert: Alert,
    revision: AlertRevision,
}

#[cfg(test)]
impl<E, D> InMemoryAlertStore<E, D> {
    /// The alert's current revision, for tests of the revision sequence (it
    /// travels in `AlertOpened` and `AlertChanged`).
    pub(crate) fn alert_revision(&self, id: AlertId) -> Option<AlertRevision> {
        lock(&self.state)
            .alerts
            .get(&id)
            .map(|stored| stored.revision)
    }
}

fn page_error(error: PageError) -> AlertReadError {
    AlertReadError::Store {
        reason: error.to_string(),
    }
}

/// Whether an alert is open or acknowledged.
pub fn is_active(state: &AlertState) -> bool {
    matches!(state, AlertState::Open | AlertState::Acknowledged { .. })
}

/// The kind of an alert state, as `AlertFilter::states` lists it.
pub fn state_kind(state: &AlertState) -> AlertStateKind {
    match state {
        AlertState::Open => AlertStateKind::Open,
        AlertState::Acknowledged { .. } => AlertStateKind::Acknowledged,
        AlertState::Resolved { .. } => AlertStateKind::Resolved,
        AlertState::Suppressed { .. } => AlertStateKind::Suppressed,
    }
}

impl<E, D> InMemoryAlertStore<E, D>
where
    E: Embedder + Send + Sync,
    D: AgentDirectory + ChannelDirectory + Send + Sync,
{
    /// A store holding every built-in rule, provisioned by config (each
    /// publishes `AlertRuleChanged` at `RuleRevision::CREATED` to `outbox`),
    /// no user rule and no alert. Topic-model version 0 is current, and the
    /// embedder's model. Transmission subjects' routes are read from
    /// `transmissions`.
    pub fn new(
        config: AlertStoreConfig,
        embedder: E,
        directory: D,
        transmissions: MemoryVerdicts,
        outbox: Outbox,
    ) -> Self {
        let rules = AlertRuleSet::new(|rule| {
            config
                .builtins
                .get(&rule)
                .cloned()
                .unwrap_or((RuleStatus::Enabled, Vec::new()))
        });
        let mut rule_revisions = BTreeMap::new();
        for rule in rules.iter() {
            rule_revisions.insert(rule.id(), RuleRevision::CREATED);
            outbox.insight(InsightEvent::AlertRuleChanged {
                rule: rule.clone(),
                revision: RuleRevision::CREATED,
            });
            outbox.changed(Changed::Rule(rule.id()));
        }
        let model = embedder.model();
        Self {
            config,
            embedder,
            directory,
            transmissions,
            outbox,
            state: Arc::new(Mutex::new(AlertsState {
                rules,
                rule_revisions,
                alerts: BTreeMap::new(),
                verdicts: BTreeMap::new(),
                version: TopicModelVersion(0),
                topics: BTreeSet::new(),
                model,
                rule_ids: IdSequence::default(),
                alert_ids: IdSequence::default(),
                rule_cursors: CursorBook::default(),
                alert_cursors: CursorBook::default(),
            })),
        }
    }

    fn alert_matches(&self, filter: &AlertFilter, alert: &Alert) -> bool {
        let by_state =
            filter.states.is_empty() || filter.states.contains(&state_kind(&alert.state));
        let by_channel = filter.channel.is_none_or(|listed| {
            let listed = ChannelDirectory::canonical(&self.directory, listed);
            match alert.subject {
                AlertSubject::Channel(channel) => {
                    ChannelDirectory::canonical(&self.directory, channel) == listed
                }
                AlertSubject::Transmission(transmission) => {
                    matches!(
                        self.transmissions.route(transmission),
                        Some(Route::Channel(channel))
                            if ChannelDirectory::canonical(&self.directory, channel) == listed
                    )
                }
                AlertSubject::Agent(_) => false,
            }
        });
        by_state && by_channel
    }
}

impl AlertsState {
    /// Store `rule` (already changed) under its id, at its next revision,
    /// and publish the change. Refused, changing nothing, when the
    /// revision counter is exhausted.
    fn commit_rule(
        &mut self,
        rule: AlertRuleDef,
        outbox: &Outbox,
    ) -> Result<RuleRevision, CommitRefused> {
        let id = rule.id();
        let revision = match self.rule_revisions.get(&id) {
            Some(current) => current.next().ok_or(CommitRefused::RevisionExhausted)?,
            None => RuleRevision::CREATED,
        };
        match self.rules.get_mut(id) {
            Some(stored) => *stored = rule.clone(),
            None => self
                .rules
                .insert(rule.clone())
                .map_err(CommitRefused::Insert)?,
        }
        self.rule_revisions.insert(id, revision);
        outbox.insight(InsightEvent::AlertRuleChanged { rule, revision });
        outbox.changed(Changed::Rule(id));
        Ok(revision)
    }

    /// Replace a stored alert with `alert` at its next revision and publish
    /// `AlertChanged`. Refused, changing nothing, when the counter is
    /// exhausted.
    fn commit_alert(
        &mut self,
        alert: Alert,
        outbox: &Outbox,
    ) -> Result<AlertRevision, CommitRefused> {
        let stored = self
            .alerts
            .get_mut(&alert.id)
            .ok_or(CommitRefused::UnknownAlert(alert.id))?;
        let revision = stored
            .revision
            .next()
            .ok_or(CommitRefused::RevisionExhausted)?;
        stored.alert = alert.clone();
        stored.revision = revision;
        let id = alert.id;
        outbox.insight(InsightEvent::AlertChanged { alert, revision });
        outbox.changed(Changed::Alert(id));
        Ok(revision)
    }

    /// Suppress every active alert `selected` picks, at `at`, all or none.
    fn suppress(
        &mut self,
        selected: impl Fn(&Alert) -> bool,
        reason: SuppressReason,
        at: Timestamp,
        outbox: &Outbox,
    ) -> Result<u32, CommitRefused> {
        let targets: Vec<Alert> = self
            .alerts
            .values()
            .filter(|stored| is_active(&stored.alert.state) && selected(&stored.alert))
            .map(|stored| stored.alert.clone())
            .collect();
        if targets.iter().any(|alert| {
            self.alerts
                .get(&alert.id)
                .is_none_or(|stored| stored.revision.next().is_none())
        }) {
            return Err(CommitRefused::RevisionExhausted);
        }
        for mut alert in targets.iter().cloned() {
            alert.state = AlertState::Suppressed { at, reason };
            self.commit_alert(alert, outbox)?;
        }
        Ok(u32::try_from(targets.len()).unwrap_or(u32::MAX))
    }
}

/// Why a change could not be stored. Each refusal changes nothing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum CommitRefused {
    #[error("the revision counter is exhausted")]
    RevisionExhausted,
    #[error("the rule set refused the rule: {0:?}")]
    Insert(InsertError),
    #[error("no stored alert {0:?}")]
    UnknownAlert(AlertId),
}

impl<E, D> AlertReads for InMemoryAlertStore<E, D>
where
    E: Embedder + Send + Sync,
    D: AgentDirectory + ChannelDirectory + Send + Sync,
{
    async fn rule(&self, id: AlertRuleId) -> Result<Option<AlertRuleDef>, AlertReadError> {
        Ok(lock(&self.state).rules.get(id).cloned())
    }

    /// Newest id first, so user rules before the built-in rules.
    async fn rules(
        &self,
        filter: &AlertRuleFilter,
        page: &PageRequest<AlertRuleList>,
    ) -> Result<Page<AlertRuleDef, AlertRuleList>, AlertReadError> {
        let mut state = lock(&self.state);
        let after = match &page.after {
            None => None,
            Some(cursor) => Some(
                state
                    .rule_cursors
                    .resolve(cursor, filter)
                    .ok_or(AlertReadError::InvalidCursor)?,
            ),
        };
        let mut remaining: Vec<AlertRuleDef> = state
            .rules
            .iter()
            .filter(|rule| filter.matches(rule))
            .filter(|rule| after.is_none_or(|after| rule.id() < after))
            .cloned()
            .collect();
        remaining.sort_by_key(|rule| std::cmp::Reverse(rule.id()));
        page_after(
            &mut state.rule_cursors,
            remaining,
            page.size,
            filter.clone(),
            AlertRuleDef::id,
        )
        .map_err(page_error)
    }

    async fn alert(&self, id: AlertId) -> Result<Option<Alert>, AlertReadError> {
        Ok(lock(&self.state)
            .alerts
            .get(&id)
            .map(|stored| stored.alert.clone()))
    }

    async fn alerts(
        &self,
        filter: &AlertFilter,
        page: &PageRequest<AlertList>,
    ) -> Result<Page<Alert, AlertList>, AlertReadError> {
        let mut state = lock(&self.state);
        let after = match &page.after {
            None => None,
            Some(cursor) => Some(
                state
                    .alert_cursors
                    .resolve(cursor, filter)
                    .ok_or(AlertReadError::InvalidCursor)?,
            ),
        };
        let remaining: Vec<Alert> = state
            .alerts
            .values()
            .rev()
            .map(|stored| &stored.alert)
            .filter(|alert| after.is_none_or(|after| alert.id < after))
            .filter(|alert| self.alert_matches(filter, alert))
            .cloned()
            .collect();
        page_after(
            &mut state.alert_cursors,
            remaining,
            page.size,
            filter.clone(),
            |alert| alert.id,
        )
        .map_err(page_error)
    }

    async fn rule_version(&self) -> Result<TopicModelVersion, AlertReadError> {
        Ok(lock(&self.state).version)
    }
}
