//! The alert store's other writes and its reads: the `alerts` consumer's
//! rule upkeep, the surface's acknowledge and resolve, and the rule and
//! alert reads behind `QueryApi::alert_rules`, `alert_rule`, `alerts`,
//! `alert` and `present`.
//!
//! One store holds the rules, the alerts and triage's verdict copy and
//! implements these with `AlertRuleStore` and `AlertTriage`, so each call
//! is one transaction over all three. Every stored change bumps the rule's
//! or alert's revision by one and publishes `AlertRuleChanged` (with
//! `Changed::Rule`) or `AlertChanged` (with `Changed::Alert`) from that
//! transaction; a change that would exhaust a revision counter is refused
//! as a store failure before anything changes.
//!
//! ```text
//! Open ─acknowledge─▶ Acknowledged ─resolve─▶ Resolved
//! Open | Acknowledged ─sanction, disable, false detection─▶ Suppressed
//! ```

use crate::aggregates::alert::{Alert, AlertRuleDef};
#[cfg(doc)]
use crate::aggregates::alert::{BuiltinRule, UserRule};
use crate::aggregates::topic::{EmbeddingModel, TopicModelVersion};
use crate::aggregates::topic_history::TopicLineage;
use crate::ids::{AlertId, AlertRuleId, OperatorId, TopicId};
use crate::interfaces::l8_surface::AlertFilter;
use crate::interfaces::l8_surface::lists::AlertRuleFilter;
use crate::paging::{AlertList, AlertRuleList, Page, PageRequest};
use crate::support::{Change, Timestamp};

use super::RuleError;

/// The rule changes the `alerts` consumer makes. Each returns the rules it
/// changed, ascending, after publishing `AlertRuleChanged` and
/// `Changed::Rule` for each; a redelivered event changes nothing.
pub trait AlertRuleMaintenance {
    /// `TopicVersionReady` for `lineage.to()`, whose topics are `topics`:
    /// every watched-topic rule current on `lineage.from()` is carried over
    /// with `AlertRuleDef::remap` (current on the new version, or stale),
    /// and `lineage.to()` becomes the version a [`UserRule`] must name (with
    /// topics among `topics`). A version not newer than the one rules name
    /// changes nothing.
    fn topic_version_ready(
        &mut self,
        lineage: &TopicLineage,
        topics: &[TopicId],
    ) -> impl Future<Output = Result<Vec<AlertRuleId>, RuleError>> + Send;

    /// The embedder now uses `model` (called when `alerts` starts, before
    /// it evaluates any event): every current semantic rule embedded with
    /// another model becomes stale, keeping its status.
    fn embedding_model_changed(
        &mut self,
        model: &EmbeddingModel,
    ) -> impl Future<Output = Result<Vec<AlertRuleId>, RuleError>> + Send;
}

/// The surface's alert actions (`Acknowledge`, `Resolve`). Each publishes
/// `AlertChanged` with the alert's next revision and `Changed::Alert` when
/// it changed the alert.
pub trait AlertActions {
    /// `Open` becomes `Acknowledged { by, at }`; an acknowledged alert is
    /// `Unchanged`. `NotActive` for a resolved or suppressed alert.
    fn acknowledge(
        &mut self,
        alert: AlertId,
        by: OperatorId,
        at: Timestamp,
    ) -> impl Future<Output = Result<Change, AlertActionError>> + Send;

    /// `Acknowledged` becomes `Resolved { by, at, note }`. An open alert is
    /// refused with `NotAcknowledged` (it is acknowledged first), and a
    /// resolved or suppressed one with `NotActive`.
    fn resolve(
        &mut self,
        alert: AlertId,
        by: OperatorId,
        at: Timestamp,
        note: Option<String>,
    ) -> impl Future<Output = Result<Change, AlertActionError>> + Send;
}

/// Rules and alerts as the store holds them, each list read in one
/// snapshot.
pub trait AlertReads {
    /// The rule stored under `id`, built in or not, stale or not; `None`
    /// for an id no rule ever had (rules are never deleted).
    fn rule(
        &self,
        id: AlertRuleId,
    ) -> impl Future<Output = Result<Option<AlertRuleDef>, AlertReadError>> + Send;

    /// The rules [`AlertRuleFilter::matches`] keeps, in the order
    /// [`QueryApi::alert_rules`] lists them: built-in rules first, in
    /// [`BuiltinRule::ALL`] order, then user rules newest first. The cursor
    /// binds the filter.
    ///
    /// [`QueryApi::alert_rules`]: crate::interfaces::l8_surface::QueryApi::alert_rules
    /// [`BuiltinRule::ALL`]: crate::aggregates::alert::BuiltinRule::ALL
    fn rules(
        &self,
        filter: &AlertRuleFilter,
        page: &PageRequest<AlertRuleList>,
    ) -> impl Future<Output = Result<Page<AlertRuleDef, AlertRuleList>, AlertReadError>> + Send;

    /// The alert stored under `id`, its subject as raised; `None` for an
    /// unknown id.
    fn alert(
        &self,
        id: AlertId,
    ) -> impl Future<Output = Result<Option<Alert>, AlertReadError>> + Send;

    /// The alerts `filter` keeps, newest id first: its states, and its
    /// channel matched as [`AlertFilter`] defines (the listed channel, a
    /// channel subject and a transmission subject's stored route all
    /// resolved through `ChannelDirectory`). The cursor binds the filter.
    fn alerts(
        &self,
        filter: &AlertFilter,
        page: &PageRequest<AlertList>,
    ) -> impl Future<Output = Result<Page<Alert, AlertList>, AlertReadError>> + Send;

    /// The topic-model version a watched-topic [`UserRule`] must name: the
    /// one the consumer last made current
    /// ([`AlertRuleMaintenance::topic_version_ready`]), version 0 at first.
    /// `QueryApi::present` reports it.
    fn rule_version(
        &self,
    ) -> impl Future<Output = Result<TopicModelVersion, AlertReadError>> + Send;
}

/// Why an acknowledge or resolve was refused. Nothing changed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AlertActionError {
    Store {
        reason: String,
    },
    UnknownAlert(AlertId),
    /// Resolved or suppressed: no action leaves those states.
    NotActive(AlertId),
    /// Resolving an open alert: it must be acknowledged first.
    NotAcknowledged(AlertId),
}

/// Why a rule or alert read failed. Unknown ids are not errors.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AlertReadError {
    Store {
        reason: String,
    },
    /// A cursor this store did not issue, or issued for another filter.
    InvalidCursor,
}
