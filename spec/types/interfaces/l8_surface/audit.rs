//! The audit log: one entry per operator action call, one per change
//! config made, and one or two per export (its refusal, or its start and
//! its end; see [`super::export::record`]).
//!
//! ```text
//! act(caller, action) ─permission─┬─ missing ───────────────────────────▶ Operator entry: Forbidden, no effect
//!                                 └─ held ─▶ intend ─▶ apply ─┬─ Ok ──▶ complete: Operator entry Succeeded
//!                                            (AuditIntent)    │         (appended, intent removed: one txn)
//!                                                             └─ Err ─▶ complete: Operator entry Rejected
//! start ─▶ recover_interrupted: each leftover intent ─▶ Operator entry: Interrupted (effect may have applied)
//! config load ─ diff against stored state ─▶ ConfigChange* ─▶ Config entry each: Applied or Rejected
//!                                                             (same transaction as the change)
//! ```
//!
//! **Atomicity with the effect.** An action's effect is committed by the
//! layer that owns it (L3, L5 or L6), in its own store and transaction; the
//! spec's traits cannot carry one transaction across those calls. So a
//! durable surface records a write-ahead [`AuditIntent`] before the effect
//! ([`AuditIntents::intend`]), and afterwards appends the entry and removes
//! the intent in one transaction ([`AuditIntents::complete`]). At start,
//! every intent left by a process that stopped mid-call is appended as an
//! [`AuditOutcome::Interrupted`] entry ([`AuditIntents::recover_interrupted`]):
//! the effect may or may not have applied, and the log says so rather than
//! nothing (`surface.audit.no-silent-effect`).
//!
//! An entry's [`AuditBody`] is either an operator's call or a change config
//! made, never a mix: a config change is a [`ConfigChange`], not an
//! [`OperatorAction`] with a made-up author, and an operator entry always
//! carries a [`CallerSnapshot`] of the [`Caller`](super::Caller) as
//! authenticated: its operator and the permissions it held.
//! [`AuditEntry::by`] derives the author from the body, so the two cannot
//! disagree.
//!
//! A config load records only what it changes: loading a config the stored
//! state already reflects records nothing.
//!
//! The log is append-only: [`AuditLog`] has no update or delete, and the
//! store's role has no `UPDATE` or `DELETE` grant on it. Per-channel policy
//! decisions are also kept in the channel's `PolicyHistory`; the log answers
//! "who did what, when, to what, and what came of it" across everything.

use serde::{Deserialize, Serialize};

use crate::aggregates::alert::AlertRuleKind;
use crate::aggregates::projection::FrameRetention;
use crate::aggregates::retention::RetentionPolicy;
use crate::aggregates::topic::TopicModelVersion;
use crate::derived::flow::channel::policy::PolicyAuthor;
use crate::derived::flow::resource::ResourcePattern;
use crate::ids::{
    AgentId, AlertId, AlertRuleId, AuditId, ChannelId, ConfigHash, ExportId, MergeId, OperatorId,
    ProjectionId, SinkId, TransmissionId,
};
use crate::interfaces::l8_surface::export::ExportRecord;
use crate::interfaces::l8_surface::operators::{AccessMode, OperatorName};
use crate::interfaces::l8_surface::sinks::SinkKind;
use crate::interfaces::l8_surface::{
    ActionError, ActionOutcome, CallerSnapshot, ConflictKind, InputError, OperatorAction,
    Permission, PermissionSet, PolicyKind,
};
use crate::observed::agent::IdentityEvidence;
use crate::paging::{AuditList, Page, PageRequest};
use crate::support::{NonEmpty, TimeWindow, Timestamp};
use crate::wire::{Rejected, WireRequest};

/// Who made an audited change: config, or an operator. The same type that
/// authors a policy decision, so a `SetPolicy` entry and the decision it
/// made name the same author.
pub type AuditAuthor = PolicyAuthor;

/// An entity an audited action or change touched. The log's subject filter
/// matches on these.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(
    tag = "type",
    content = "data",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum AuditSubject {
    Agent(AgentId),
    Channel(ChannelId),
    Alert(AlertId),
    Rule(AlertRuleId),
    Transmission(TransmissionId),
    Merge(MergeId),
    Operator(OperatorId),
    /// A topic-model version an operator pinned or unpinned.
    TopicVersion(TopicModelVersion),
    /// An export: its start and its end.
    Export(ExportId),
    /// A stored projection an export read.
    Projection(ProjectionId),
    /// An alert sink config defined, changed or removed.
    Sink(SinkId),
}

/// What an operator call came to: the exact result `act` returned, split so
/// the log can tell refusals from failures.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "type",
    content = "data",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum AuditOutcome {
    Succeeded(ActionOutcome),
    /// A permitted action that was refused or failed. It had no effect.
    Rejected(Rejection),
    /// The caller lacked the action's required permission.
    Forbidden {
        missing: Permission,
    },
    /// The process stopped between the call's [`AuditIntent`] and its
    /// entry: the effect may or may not have applied, and the caller never
    /// received a result. Recorded only at start, from a leftover intent
    /// ([`AuditIntents::recover_interrupted`]); `AuditOutcome::of` never
    /// returns it.
    Interrupted,
}

/// The reason [`AuditOutcome::result`] gives for an `Interrupted` call.
pub const INTERRUPTED_REASON: &str =
    "interrupted: the process stopped during the call; its effect may or may not have applied";

/// Why a permitted action, or a config change, was refused or failed:
/// every `ActionError` except `Forbidden`, which is its own outcome.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "type",
    content = "data",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum Rejection {
    NotFound,
    Conflict(ConflictKind),
    InvalidInput(InputError),
    /// A store or bus failure; the transaction rolled back.
    Failed {
        reason: String,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum OutcomeKind {
    Applied,
    Unchanged,
    Rejected,
    Forbidden,
    Interrupted,
}

impl AuditOutcome {
    /// The outcome recorded for what `OperatorActions::act` returned.
    pub fn of(result: &Result<ActionOutcome, ActionError>) -> Self {
        match result {
            Ok(outcome) => Self::Succeeded(outcome.clone()),
            Err(ActionError::Forbidden { missing }) => Self::Forbidden { missing: *missing },
            Err(ActionError::NotFound) => Self::Rejected(Rejection::NotFound),
            Err(ActionError::Conflict(kind)) => Self::Rejected(Rejection::Conflict(kind.clone())),
            Err(ActionError::InvalidInput(input)) => {
                Self::Rejected(Rejection::InvalidInput(input.clone()))
            }
            Err(ActionError::Store { reason } | ActionError::Unavailable { reason, .. }) => {
                Self::Rejected(Rejection::Failed {
                    reason: reason.clone(),
                })
            }
        }
    }

    /// What `act` returned for a call recorded with this outcome. The exact
    /// inverse of [`AuditOutcome::of`] over everything a surface's `act`
    /// returns; the client-only `ActionError::Unavailable`, which no
    /// surface returns or records, reads back as the `Store` it is served
    /// as. An `Interrupted` call returned nothing; it reads back as a
    /// `Store` error with [`INTERRUPTED_REASON`], what a caller that lost
    /// the connection would have to assume.
    pub fn result(&self) -> Result<ActionOutcome, ActionError> {
        match self {
            Self::Succeeded(outcome) => Ok(outcome.clone()),
            Self::Forbidden { missing } => Err(ActionError::Forbidden { missing: *missing }),
            Self::Rejected(Rejection::NotFound) => Err(ActionError::NotFound),
            Self::Rejected(Rejection::Conflict(kind)) => Err(ActionError::Conflict(kind.clone())),
            Self::Rejected(Rejection::InvalidInput(input)) => {
                Err(ActionError::InvalidInput(input.clone()))
            }
            Self::Rejected(Rejection::Failed { reason }) => Err(ActionError::Store {
                reason: reason.clone(),
            }),
            Self::Interrupted => Err(ActionError::Store {
                reason: INTERRUPTED_REASON.to_owned(),
            }),
        }
    }

    pub fn kind(&self) -> OutcomeKind {
        match self {
            Self::Succeeded(ActionOutcome::Unchanged) => OutcomeKind::Unchanged,
            Self::Succeeded(_) => OutcomeKind::Applied,
            Self::Rejected(_) => OutcomeKind::Rejected,
            Self::Forbidden { .. } => OutcomeKind::Forbidden,
            Self::Interrupted => OutcomeKind::Interrupted,
        }
    }
}

/// One operator action call.
///
/// Built only through [`OperatorRecord::new`]: the outcome is `Forbidden`
/// exactly when the caller (as its [`CallerSnapshot`] records it) lacks the
/// action's required permission, and then
/// names that permission, because the permission is checked before anything
/// else. A record that says an action was attempted by a caller who could
/// not attempt it, or forbidden to one who could, cannot be built.
///
/// On the wire, `{"caller": {"operator": .., "permissions": [..]}, "action":
/// .., "outcome": ..}`, decoded through [`OperatorRecord::new`]. A response
/// (inside `AuditEntry`), never a request.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", try_from = "RawOperatorRecord")]
pub struct OperatorRecord {
    caller: CallerSnapshot,
    action: OperatorAction,
    outcome: AuditOutcome,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InvalidOperatorRecord {
    /// `Forbidden`, but the caller holds the required permission.
    ForbiddenButPermitted { required: Permission },
    /// Succeeded or rejected, but the caller lacks the required permission,
    /// so the action could not have been attempted.
    AttemptedWithoutPermission { required: Permission },
    /// `Forbidden`, but naming a permission other than the one the action
    /// requires.
    WrongMissingPermission { required: Permission },
}

/// [`OperatorRecord`]'s fields, decoded without the check.
#[derive(Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
struct RawOperatorRecord {
    caller: CallerSnapshot,
    action: OperatorAction,
    outcome: AuditOutcome,
}

impl TryFrom<RawOperatorRecord> for OperatorRecord {
    type Error = Rejected<InvalidOperatorRecord>;

    fn try_from(raw: RawOperatorRecord) -> Result<Self, Self::Error> {
        Self::new(raw.caller, raw.action, raw.outcome)
            .map_err(|error| Rejected::new("operator record", error))
    }
}

impl OperatorRecord {
    /// The record of `caller`'s call: the surface passes the
    /// [`Caller`](super::Caller) of the call, and the record keeps its
    /// [`CallerSnapshot`]; decoding passes the snapshot it read.
    pub fn new(
        caller: impl Into<CallerSnapshot>,
        action: OperatorAction,
        outcome: AuditOutcome,
    ) -> Result<Self, InvalidOperatorRecord> {
        let caller = caller.into();
        let required = action.required_permission();
        let permitted = caller.has(required);
        match (&outcome, permitted) {
            (AuditOutcome::Forbidden { .. }, true) => {
                Err(InvalidOperatorRecord::ForbiddenButPermitted { required })
            }
            (AuditOutcome::Forbidden { missing }, false) if *missing != required => {
                Err(InvalidOperatorRecord::WrongMissingPermission { required })
            }
            (AuditOutcome::Forbidden { .. }, false) => Ok(()),
            (_, false) => Err(InvalidOperatorRecord::AttemptedWithoutPermission { required }),
            (_, true) => Ok(()),
        }?;
        Ok(Self {
            caller,
            action,
            outcome,
        })
    }

    /// The caller as authenticated for the call: its operator and the
    /// permissions it held then.
    pub fn caller(&self) -> &CallerSnapshot {
        &self.caller
    }

    pub fn action(&self) -> &OperatorAction {
        &self.action
    }

    pub fn outcome(&self) -> &AuditOutcome {
        &self.outcome
    }

    /// The action's subjects, then those its outcome names
    /// ([`ActionOutcome::subjects`]), each once.
    pub fn subjects(&self) -> Vec<AuditSubject> {
        let mut subjects = self.action.subjects();
        if let AuditOutcome::Succeeded(outcome) = &self.outcome {
            for named in outcome.subjects() {
                if !subjects.contains(&named) {
                    subjects.push(named);
                }
            }
        }
        subjects
    }
}

/// One change config made, on load or reload. Each names its target, so
/// the log reads as a list of facts, not as a copy of the config file.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "type",
    content = "data",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum ConfigChange {
    /// Declared a channel before traffic (`ChannelRegistry::declare`). An
    /// `Unreviewed` policy records no decision.
    DeclareChannel {
        channel: ChannelId,
        pattern: ResourcePattern,
        policy: PolicyKind,
        note: Option<String>,
    },
    /// Recorded a config policy decision for a declared channel
    /// (`ChannelRegistry::set_policy` with `PolicyAuthor::Config`).
    SetPolicy {
        channel: ChannelId,
        policy: PolicyKind,
        note: Option<String>,
    },
    /// Registered an agent before its traffic (`AgentState::Registered`).
    RegisterAgent {
        agent: AgentId,
        evidence: NonEmpty<IdentityEvidence>,
    },
    /// Provisioned a built-in alert rule.
    ProvisionRule {
        rule: AlertRuleId,
        kind: AlertRuleKind,
    },
    /// Switched how requests are authenticated. Switching to `Trusted`
    /// turns off login; it is always recorded on its own entry.
    SetAccessMode(AccessMode),
    /// Defined an operator, or changed its name or permissions. In trusted
    /// mode this is the trusted operator with every permission.
    SetOperator {
        operator: OperatorId,
        name: OperatorName,
        permissions: PermissionSet,
    },
    /// Config no longer defines this operator: it keeps its name, loses
    /// every permission, and gets no further `Caller`.
    RemoveOperator { operator: OperatorId },
    /// Defined an alert sink, or changed its kind, name or endpoint. The
    /// endpoint itself is not recorded: a webhook URL or a Slack token can
    /// carry a credential, and the log is readable with Audit alone.
    /// From the change on, alerts of the rules listing the sink are
    /// delivered as it now says.
    SetSink {
        sink: SinkId,
        kind: SinkKind,
        name: String,
    },
    /// Config no longer defines this sink: `QueryApi::sinks` stops listing
    /// it and a rule naming it in `CreateRule` or `UpdateRule` is
    /// `InvalidInput(UnknownSink)`.
    RemoveSink { sink: SinkId },
    /// Changed how many topic-model versions retention keeps
    /// ([`RetentionPolicy`]). The catalog enforces the new policy when it
    /// starts with it, so a lower `keep_last` drops versions then, each
    /// with its own `TopicVersionDropped`.
    SetTopicRetention(RetentionPolicy),
    /// Changed how long a ready projection's frame is kept after its fit
    /// ([`FrameRetention`], `Present::frame_retention`). It applies to
    /// every stored frame from the change on.
    SetFrameRetention {
        frame_retention_micros: FrameRetention,
    },
}

impl ConfigChange {
    pub fn subjects(&self) -> Vec<AuditSubject> {
        match self {
            Self::DeclareChannel { channel, .. } | Self::SetPolicy { channel, .. } => {
                vec![AuditSubject::Channel(*channel)]
            }
            Self::RegisterAgent { agent, .. } => vec![AuditSubject::Agent(*agent)],
            Self::ProvisionRule { rule, .. } => vec![AuditSubject::Rule(*rule)],
            Self::SetAccessMode(_) => Vec::new(),
            Self::SetOperator { operator, .. } | Self::RemoveOperator { operator } => {
                vec![AuditSubject::Operator(*operator)]
            }
            Self::SetSink { sink, .. } | Self::RemoveSink { sink } => {
                vec![AuditSubject::Sink(*sink)]
            }
            Self::SetTopicRetention(_) | Self::SetFrameRetention { .. } => Vec::new(),
        }
    }
}

/// A config change either took effect or was refused (a declared pattern
/// overlapping another channel's, for example). A change the stored state
/// already reflects is not a change and is not recorded.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "type",
    content = "data",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum ConfigOutcome {
    Applied,
    Rejected(Rejection),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct ConfigRecord {
    /// The config document whose load made the change.
    pub config: ConfigHash,
    pub change: ConfigChange,
    pub outcome: ConfigOutcome,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "type",
    content = "data",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum AuditBody {
    Operator(OperatorRecord),
    Config(ConfigRecord),
    /// An export refused, started, ended or abandoned
    /// ([`crate::interfaces::l8_surface::export::record`]).
    Export(ExportRecord),
}

/// One entry of the audit log.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct AuditEntry {
    pub id: AuditId,
    /// When the action was accepted or refused, or when config made the
    /// change. For an applied `SetPolicy` it equals the decision's time.
    pub at: Timestamp,
    pub body: AuditBody,
}

impl AuditEntry {
    pub fn by(&self) -> AuditAuthor {
        match &self.body {
            AuditBody::Operator(record) => AuditAuthor::Operator(record.caller().operator()),
            AuditBody::Export(record) => AuditAuthor::Operator(record.caller().operator()),
            AuditBody::Config(_) => AuditAuthor::Config,
        }
    }

    /// Every entity the entry touched, without duplicates.
    pub fn subjects(&self) -> Vec<AuditSubject> {
        match &self.body {
            AuditBody::Operator(record) => record.subjects(),
            AuditBody::Config(record) => record.change.subjects(),
            AuditBody::Export(record) => record.subjects(),
        }
    }
}

/// `QueryApi::audit`'s filter. An empty `by` and a `None` do not restrict;
/// the fields combine with AND.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct AuditFilter {
    /// Keep entries by any of these authors. `AuditAuthor::Config` selects
    /// config changes.
    pub by: Vec<AuditAuthor>,
    /// Keep entries that touched this entity, as named in the action or
    /// change or created by it. Ids are matched as recorded, not resolved
    /// through merges.
    pub subject: Option<AuditSubject>,
    /// Keep entries whose `at` lies in the window.
    pub window: Option<TimeWindow>,
}

impl AuditFilter {
    pub fn matches(&self, entry: &AuditEntry) -> bool {
        let by = self.by.is_empty() || self.by.contains(&entry.by());
        let subject = self
            .subject
            .is_none_or(|subject| entry.subjects().contains(&subject));
        let in_window = self.window.is_none_or(|window| window.contains(entry.at));
        by && subject && in_window
    }
}

/// A client chooses every field of the filter, `by` included: which
/// authors to list, not who is asking.
impl WireRequest for AuditFilter {}

/// A write-ahead record of an operator action call whose permission check
/// passed and whose effect is about to be applied: the entry the call will
/// leave, but for its outcome. Kept until the call's entry is appended
/// ([`AuditIntents::complete`]), or turned into an `Interrupted` entry at
/// start ([`AuditIntent::interrupted`]).
///
/// Built only through [`AuditIntent::new`]: the caller holds the action's
/// required permission, because a forbidden call has no effect and needs
/// no intent. On the wire, `{"id": .., "at": .., "caller": {..}, "action":
/// ..}`, decoded through [`AuditIntent::new`]; stored, never served.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", try_from = "RawAuditIntent")]
pub struct AuditIntent {
    id: AuditId,
    at: Timestamp,
    caller: CallerSnapshot,
    action: OperatorAction,
}

/// [`AuditIntent`]'s fields, decoded without the check.
#[derive(Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
struct RawAuditIntent {
    id: AuditId,
    at: Timestamp,
    caller: CallerSnapshot,
    action: OperatorAction,
}

impl TryFrom<RawAuditIntent> for AuditIntent {
    type Error = Rejected<InvalidOperatorRecord>;

    fn try_from(raw: RawAuditIntent) -> Result<Self, Self::Error> {
        Self::new(raw.id, raw.at, raw.caller, raw.action)
            .map_err(|error| Rejected::new("audit intent", error))
    }
}

impl AuditIntent {
    /// The intent of `caller`'s call of `action`, accepted at `at`, whose
    /// entry will have id `id`. `AttemptedWithoutPermission` when the
    /// caller lacks the action's required permission.
    pub fn new(
        id: AuditId,
        at: Timestamp,
        caller: impl Into<CallerSnapshot>,
        action: OperatorAction,
    ) -> Result<Self, InvalidOperatorRecord> {
        let caller = caller.into();
        let required = action.required_permission();
        if !caller.has(required) {
            return Err(InvalidOperatorRecord::AttemptedWithoutPermission { required });
        }
        Ok(Self {
            id,
            at,
            caller,
            action,
        })
    }

    /// The id the call's entry is appended under.
    pub const fn id(&self) -> AuditId {
        self.id
    }

    /// When the surface accepted the call: the entry's `at`.
    pub const fn at(&self) -> Timestamp {
        self.at
    }

    pub fn caller(&self) -> &CallerSnapshot {
        &self.caller
    }

    pub fn action(&self) -> &OperatorAction {
        &self.action
    }

    /// The call's entry with `outcome`: same id, time, caller and action.
    /// `Forbidden` cannot be the outcome of a call that had an intent, and
    /// is refused as `ForbiddenButPermitted`.
    pub fn entry(&self, outcome: AuditOutcome) -> Result<AuditEntry, InvalidOperatorRecord> {
        let record = OperatorRecord::new(self.caller, self.action.clone(), outcome)?;
        Ok(AuditEntry {
            id: self.id,
            at: self.at,
            body: AuditBody::Operator(record),
        })
    }

    /// The entry recovery appends for an intent a stopped process left:
    /// its call with outcome [`AuditOutcome::Interrupted`].
    pub fn interrupted(&self) -> AuditEntry {
        AuditEntry {
            id: self.id,
            at: self.at,
            // Built directly: `new` checked that the caller holds the
            // action's permission, which is all `OperatorRecord::new`
            // checks for a non-`Forbidden` outcome.
            body: AuditBody::Operator(OperatorRecord {
                caller: self.caller,
                action: self.action.clone(),
                outcome: AuditOutcome::Interrupted,
            }),
        }
    }
}

/// The write-ahead half of a durable audit log
/// (`surface.audit.no-silent-effect`). Implemented by the store that also
/// implements [`AuditLog`] (`PgAuditLog`, and the memory reference), so an
/// intent and its entry live in one store.
pub trait AuditIntents: AuditLog {
    /// Record `intent` durably before the call's effect is applied.
    /// Idempotent on [`AuditIntent::id`]; a different intent under a used
    /// id, or an id an entry already has, is `IdReused`.
    fn intend(
        &mut self,
        intent: &AuditIntent,
    ) -> impl Future<Output = Result<(), AuditError>> + Send;

    /// Append `entry` and remove the intent with its id, in one
    /// transaction. Without such an intent it is [`AuditLog::append`].
    fn complete(
        &mut self,
        entry: AuditEntry,
    ) -> impl Future<Output = Result<(), AuditError>> + Send;

    /// At start, before the surface accepts a call: append every leftover
    /// intent as [`AuditIntent::interrupted`] and remove it, each in one
    /// transaction. Returns the ids appended, oldest intent first.
    /// Idempotent: a second call finds no intent.
    fn recover_interrupted(
        &mut self,
    ) -> impl Future<Output = Result<Vec<AuditId>, AuditError>> + Send;
}

/// Append-only storage for audit entries. There is no update or delete.
pub trait AuditLog {
    /// Append one entry. Idempotent on `AuditEntry::id`: appending the same
    /// entry again is a no-op, and a different entry with a used id is
    /// `IdReused`.
    fn append(&mut self, entry: AuditEntry) -> impl Future<Output = Result<(), AuditError>> + Send;

    /// The entries `filter` matches, a page at a time with the cursors of
    /// [`crate::paging`]: newest first by `(at, id)`, so entries appended
    /// during a traversal never shift a page.
    fn query(
        &self,
        filter: &AuditFilter,
        page: &PageRequest<AuditList>,
    ) -> impl Future<Output = Result<Page<AuditEntry, AuditList>, AuditError>> + Send;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuditError {
    Store {
        reason: String,
    },
    IdReused(AuditId),
    /// A cursor the log did not issue, or issued for another filter.
    InvalidCursor,
}
