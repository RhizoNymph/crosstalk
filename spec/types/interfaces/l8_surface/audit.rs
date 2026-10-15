//! The audit log: one entry per operator action call, one per change
//! config made, and one or two per export (its refusal, or its start and
//! its end; see [`super::export::record`]).
//!
//! ```text
//! act(caller, action) ─permission─┬─ missing ──────────────▶ Operator entry: Forbidden, no effect
//!                                 └─ held ─▶ apply ─┬─ Ok ──▶ Operator entry: Succeeded
//!                                                   │         (same transaction as the effect)
//!                                                   └─ Err ─▶ Operator entry: Rejected, no effect
//! config load ─ diff against stored state ─▶ ConfigChange* ─▶ Config entry each: Applied or Rejected
//!                                                             (same transaction as the change)
//! ```
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
use crate::aggregates::topic::TopicModelVersion;
use crate::derived::flow::channel::policy::PolicyAuthor;
use crate::derived::flow::resource::ResourcePattern;
use crate::ids::{
    AgentId, AlertId, AlertRuleId, AuditId, ChannelId, ConfigHash, ExportId, MergeId, OperatorId,
    ProjectionId, TransmissionId,
};
use crate::interfaces::l8_surface::export::ExportRecord;
use crate::interfaces::l8_surface::operators::{AccessMode, OperatorName};
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
}

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
            Err(ActionError::Store { reason }) => Self::Rejected(Rejection::Failed {
                reason: reason.clone(),
            }),
        }
    }

    /// What `act` returned for a call recorded with this outcome. The exact
    /// inverse of [`AuditOutcome::of`].
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
        }
    }

    pub fn kind(&self) -> OutcomeKind {
        match self {
            Self::Succeeded(ActionOutcome::Unchanged) => OutcomeKind::Unchanged,
            Self::Succeeded(_) => OutcomeKind::Applied,
            Self::Rejected(_) => OutcomeKind::Rejected,
            Self::Forbidden { .. } => OutcomeKind::Forbidden,
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

/// Append-only storage for audit entries. There is no update or delete.
pub trait AuditLog {
    /// Append one entry. Idempotent on `AuditEntry::id`: appending the same
    /// entry again is a no-op, and a different entry with a used id is
    /// `IdReused`.
    async fn append(&mut self, entry: AuditEntry) -> Result<(), AuditError>;

    /// The entries `filter` matches, a page at a time with the cursors of
    /// [`crate::paging`]: newest first by `(at, id)`, so entries appended
    /// during a traversal never shift a page.
    async fn query(
        &self,
        filter: &AuditFilter,
        page: &PageRequest<AuditList>,
    ) -> Result<Page<AuditEntry, AuditList>, AuditError>;
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
