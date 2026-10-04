//! Running the script: each step's writes through the spec's write traits.
//!
//! The runner is what the pipeline's consumers and the surface would have
//! done at each step's time, collapsed into direct trait calls: the
//! correlator's state changes (`TransmissionStore::save`), the flow
//! consumer's registry writes, `analyze`'s fits and assignments, L7's
//! contributions and activations, the alerts consumer's triage, and
//! operator actions with their audit entries (as `OperatorActions::act`
//! records them: the caller the directory gives the operator, the action
//! and `AuditOutcome::of` its result).
//!
//! It keeps what later steps need from earlier writes: the ids stores
//! assign (merge records, user rules, alerts), each fit's lineage, and an
//! alert book mirroring which planned alerts are still active, so a planned
//! acknowledgement of an alert a sanction already suppressed is skipped as
//! an operator would no longer see it, and a triage is checked against the
//! outcome the plan expects (opened, or deduplicated into the alert in
//! force).

mod insight;
mod pipeline;
mod surface;

use std::collections::BTreeMap;
use std::sync::Arc;

use crosstalk_spec::aggregates::alert::AlertSubject;
use crosstalk_spec::aggregates::topic::TopicModelVersion;
use crosstalk_spec::aggregates::topic_history::TopicLineage;
use crosstalk_spec::ids::{
    AlertId, AlertRuleId, ChannelId, MergeId, OperatorId, TopicId, TransmissionId,
};
use crosstalk_spec::interfaces::l8_surface::audit::{
    AuditBody, AuditEntry, AuditLog, AuditOutcome, OperatorRecord,
};
use crosstalk_spec::interfaces::l8_surface::operators::{OperatorStore, RequestIdentity};
use crosstalk_spec::interfaces::l8_surface::{ActionError, ActionOutcome, Caller, OperatorAction};
use crosstalk_spec::support::Timestamp;

use crate::clock::{Anchor, WorldClock};
use crate::config::WorldConfig;
use crate::error::WorldError;
use crate::mint::Mint;
use crate::scenario::{MergeKey, RuleKey};
use crate::script::{AlertKey, Op, RuleRef, Step};
use crate::stores::WorldStores;

/// A planned alert as triage opened it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Tracked {
    id: AlertId,
    rule: AlertRuleId,
    subject: AlertSubject,
    active: bool,
}

/// What the runner learned from the writes so far.
#[derive(Debug, Default)]
pub(crate) struct Ledger {
    pub merges: BTreeMap<MergeKey, MergeId>,
    pub rules: BTreeMap<RuleKey, AlertRuleId>,
    pub lineages: BTreeMap<TopicModelVersion, TopicLineage>,
    pub topics: BTreeMap<TopicModelVersion, Vec<TopicId>>,
    alerts: BTreeMap<AlertKey, Tracked>,
    /// Each superseded channel and the promoted channel that superseded it.
    superseded: BTreeMap<ChannelId, ChannelId>,
    callers: BTreeMap<OperatorId, Caller>,
    /// Acknowledgements and resolutions skipped because the alert was no
    /// longer active.
    pub skipped: u32,
}

impl Ledger {
    pub fn rule(&self, rule: RuleRef) -> Result<AlertRuleId, WorldError> {
        match rule {
            RuleRef::Builtin(builtin) => Ok(builtin.id()),
            RuleRef::User(key) => self
                .rules
                .get(&key)
                .copied()
                .ok_or_else(|| WorldError::missing(format!("rule {key:?}"))),
        }
    }

    pub fn merge(&self, key: MergeKey) -> Result<MergeId, WorldError> {
        self.merges
            .get(&key)
            .copied()
            .ok_or_else(|| WorldError::missing(format!("merge {key:?}")))
    }

    /// The alert of `key`, while it is active.
    fn active(&self, key: AlertKey) -> Option<Tracked> {
        self.alerts.get(&key).copied().filter(|alert| alert.active)
    }

    /// Every alert about `channel` or a channel it superseded is no longer
    /// active: `AlertTriage::channel_sanctioned` suppressed it.
    fn sanctioned(&mut self, channel: ChannelId) {
        let superseded = &self.superseded;
        for alert in self.alerts.values_mut() {
            if let AlertSubject::Channel(subject) = alert.subject
                && (subject == channel || superseded.get(&subject) == Some(&channel))
            {
                alert.active = false;
            }
        }
    }

    fn suppress(&mut self, keep: impl Fn(&Tracked) -> bool) {
        for alert in self.alerts.values_mut() {
            if !keep(alert) {
                alert.active = false;
            }
        }
    }

    fn judged_false(&mut self, transmission: TransmissionId) {
        self.suppress(|alert| alert.subject != AlertSubject::Transmission(transmission));
    }

    fn rule_disabled(&mut self, rule: AlertRuleId) {
        self.suppress(|alert| alert.rule != rule);
    }
}

/// Runs steps against `stores`.
pub(crate) struct Runner<'a, S> {
    pub stores: &'a mut S,
    pub config: &'a WorldConfig,
    /// Audit entry ids.
    audit_ids: Mint,
    pub ledger: Ledger,
}

impl<'a, S: WorldStores> Runner<'a, S> {
    pub fn new(stores: &'a mut S, config: &'a WorldConfig, seed: u64, anchor: Anchor) -> Self {
        Self {
            stores,
            config,
            audit_ids: Mint::new(seed, "audit", Arc::new(WorldClock::Fixed(anchor))),
            ledger: Ledger::default(),
        }
    }

    /// Runs every step, in order.
    pub async fn run(&mut self, steps: Vec<Step>) -> Result<(), WorldError> {
        for step in steps {
            self.step(step).await?;
        }
        Ok(())
    }

    async fn step(&mut self, Step { at, op }: Step) -> Result<(), WorldError> {
        match op {
            Op::LoadAccess { .. }
            | Op::ConfigEntry { .. }
            | Op::Delivery { .. }
            | Op::CreateRule { .. }
            | Op::SetRuleEnabled { .. }
            | Op::Triage { .. }
            | Op::Acknowledge { .. }
            | Op::Resolve { .. }
            | Op::RefusedAcknowledge { .. }
            | Op::Enqueue(_)
            | Op::StartFit { .. }
            | Op::CompleteJob { .. }
            | Op::FailFit { .. }
            | Op::ExpireFrames
            | Op::Body { .. }
            | Op::DeadLetter(_) => self.surface(at, op).await,
            Op::SetModel
            | Op::BeginFit { .. }
            | Op::CompleteFit { .. }
            | Op::Assign { .. }
            | Op::Ready { .. }
            | Op::Activate { .. }
            | Op::Pin { .. }
            | Op::Index(_)
            | Op::Edge(_)
            | Op::Watermark(_) => self.insight(at, op).await,
            Op::CreateAgent(_)
            | Op::Advance { .. }
            | Op::Claim { .. }
            | Op::Activity { .. }
            | Op::Merge { .. }
            | Op::Unmerge { .. }
            | Op::Rename { .. }
            | Op::Discover { .. }
            | Op::AddResource { .. }
            | Op::Access { .. }
            | Op::Detection { .. }
            | Op::Save(_)
            | Op::Confirm { .. }
            | Op::Policy { .. }
            | Op::Promote { .. }
            | Op::ForbiddenPolicy { .. }
            | Op::Verdict { .. } => self.pipeline(at, op).await,
        }
    }

    /// The caller the directory gives `operator`'s verified session.
    async fn caller(&mut self, operator: OperatorId, at: Timestamp) -> Result<Caller, WorldError> {
        if let Some(caller) = self.ledger.callers.get(&operator) {
            return Ok(caller.clone());
        }
        let caller = self
            .stores
            .operators()
            .caller(RequestIdentity::Verified(operator))
            .await
            .map_err(|e| WorldError::store("OperatorStore::caller", at, e))?;
        self.ledger.callers.insert(operator, caller.clone());
        Ok(caller)
    }

    /// Appends the audit entry of `by`'s call of `action` at `at`, which
    /// returned `result`.
    async fn audit(
        &mut self,
        at: Timestamp,
        by: OperatorId,
        action: OperatorAction,
        result: Result<ActionOutcome, ActionError>,
    ) -> Result<(), WorldError> {
        let caller = self.caller(by, at).await?;
        let record = OperatorRecord::new(&caller, action, AuditOutcome::of(&result))
            .map_err(|e| WorldError::invalid("OperatorRecord", e))?;
        self.append(at, AuditBody::Operator(record)).await
    }

    async fn append(&mut self, at: Timestamp, body: AuditBody) -> Result<(), WorldError> {
        let entry = AuditEntry {
            id: self.audit_ids.at(at)?,
            at,
            body,
        };
        self.stores
            .audit()
            .append(entry)
            .await
            .map_err(|e| WorldError::store("AuditLog::append", at, e))
    }
}

/// `Applied` or `Unchanged`, as an action reports a store's `Change`.
fn outcome(change: crosstalk_spec::support::Change) -> ActionOutcome {
    match change {
        crosstalk_spec::support::Change::Applied => ActionOutcome::Applied,
        crosstalk_spec::support::Change::Unchanged => ActionOutcome::Unchanged,
    }
}
