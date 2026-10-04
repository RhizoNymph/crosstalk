//! [`InMemoryAlertStore`]: the reference [`AlertRuleStore`] and
//! [`AlertTriage`], in one store.
//!
//! The spec reads rules and alerts "in the same transaction": triage
//! re-checks a draft's rule, a disable suppresses the rule's active alerts,
//! and a false-detection verdict suppresses alerts and blocks new ones. One
//! lock over the rule set, the alerts and the verdict copy makes each call
//! one transaction.
//!
//! - [`rules`]: `AlertRuleStore`, and the `alerts` consumer's rule
//!   changes: remapping on `TopicVersionReady`, staleness on an embedding
//!   model change.
//! - [`triage`]: `AlertTriage`, and the surface's acknowledge and resolve.
//!
//! Every stored change bumps the rule's or alert's revision by one and
//! publishes the matching `AlertRuleChanged`, `AlertOpened` or
//! `AlertChanged` and `Changed` into the outbox, in the same critical
//! section. A change that would exhaust a revision counter is refused
//! before anything is touched.
//!
//! [`AlertRuleStore`]: crosstalk_spec::interfaces::l6_analysis::AlertRuleStore
//! [`AlertTriage`]: crosstalk_spec::interfaces::l6_analysis::AlertTriage

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
use crosstalk_spec::interfaces::l8_surface::lists::AlertRuleFilter;
use crosstalk_spec::interfaces::l8_surface::{AlertFilter, AlertStateKind};
use crosstalk_spec::paging::{AlertList, AlertRuleList, Page, PageRequest};
use crosstalk_spec::support::Timestamp;

use super::support::{Clock, IdSequence, Outbox, Published, lock};
use crate::surface::paging::{CursorBook, PageError, page_after};

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
    clock: Arc<dyn Clock>,
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
    outbox: Outbox,
}

#[derive(Debug, Clone)]
struct StoredAlert {
    alert: Alert,
    revision: AlertRevision,
}

/// Why a read of the store failed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum AlertReadError {
    #[error("a cursor this store did not issue, or issued for another filter")]
    InvalidCursor,
    #[error("the page could not be built: {0}")]
    Page(PageError),
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
    /// publishes `AlertRuleChanged` at `RuleRevision::CREATED`), no user
    /// rule and no alert. Topic-model version 0 is current, and the
    /// embedder's model.
    pub fn new(config: AlertStoreConfig, embedder: E, directory: D, clock: Arc<dyn Clock>) -> Self {
        let rules = AlertRuleSet::new(|rule| {
            config
                .builtins
                .get(&rule)
                .cloned()
                .unwrap_or((RuleStatus::Enabled, Vec::new()))
        });
        let mut outbox = Outbox::default();
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
            clock,
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
                outbox,
            })),
        }
    }

    /// Everything published since the last drain, in commit order.
    pub fn drain_published(&self) -> Vec<Published> {
        lock(&self.state).outbox.drain()
    }

    /// The topic-model version rules must currently name.
    pub fn current_version(&self) -> TopicModelVersion {
        lock(&self.state).version
    }

    pub fn rule(&self, id: AlertRuleId) -> Option<AlertRuleDef> {
        lock(&self.state).rules.get(id).cloned()
    }

    pub fn rule_revision(&self, id: AlertRuleId) -> Option<RuleRevision> {
        lock(&self.state).rule_revisions.get(&id).copied()
    }

    /// Every rule: built-in rules in `BuiltinRule::ALL` order, then user
    /// rules by id.
    pub fn all_rules(&self) -> Vec<AlertRuleDef> {
        lock(&self.state).rules.iter().cloned().collect()
    }

    /// The rules `filter` matches, newest id first (`QueryApi::alert_rules`).
    pub fn rules_page(
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
        .map_err(AlertReadError::Page)
    }

    pub fn alert(&self, id: AlertId) -> Option<Alert> {
        lock(&self.state)
            .alerts
            .get(&id)
            .map(|stored| stored.alert.clone())
    }

    pub fn alert_revision(&self, id: AlertId) -> Option<AlertRevision> {
        lock(&self.state)
            .alerts
            .get(&id)
            .map(|stored| stored.revision)
    }

    /// Every alert, by id.
    pub fn all_alerts(&self) -> Vec<Alert> {
        lock(&self.state)
            .alerts
            .values()
            .map(|stored| stored.alert.clone())
            .collect()
    }

    /// The alerts `filter` matches, newest id first (`QueryApi::alerts`).
    /// The channel filter resolves the listed channel, a channel subject
    /// and a transmission subject's route through supersession; `route_of`
    /// reads a transmission's stored route.
    pub fn alerts_page(
        &self,
        filter: &AlertFilter,
        route_of: impl Fn(TransmissionId) -> Option<Route>,
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
            .filter(|alert| self.alert_matches(filter, alert, &route_of))
            .cloned()
            .collect();
        page_after(
            &mut state.alert_cursors,
            remaining,
            page.size,
            filter.clone(),
            |alert| alert.id,
        )
        .map_err(AlertReadError::Page)
    }

    fn alert_matches(
        &self,
        filter: &AlertFilter,
        alert: &Alert,
        route_of: &impl Fn(TransmissionId) -> Option<Route>,
    ) -> bool {
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
                        route_of(transmission),
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
    fn commit_rule(&mut self, rule: AlertRuleDef) -> Result<RuleRevision, CommitRefused> {
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
        self.outbox
            .insight(InsightEvent::AlertRuleChanged { rule, revision });
        self.outbox.changed(Changed::Rule(id));
        Ok(revision)
    }

    /// Replace a stored alert with `alert` at its next revision and publish
    /// `AlertChanged`. Refused, changing nothing, when the counter is
    /// exhausted.
    fn commit_alert(&mut self, alert: Alert) -> Result<AlertRevision, CommitRefused> {
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
        self.outbox
            .insight(InsightEvent::AlertChanged { alert, revision });
        self.outbox.changed(Changed::Alert(id));
        Ok(revision)
    }

    /// Suppress every active alert `selected` picks, at `at`, all or none.
    fn suppress(
        &mut self,
        selected: impl Fn(&Alert) -> bool,
        reason: SuppressReason,
        at: Timestamp,
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
            self.commit_alert(alert)?;
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
