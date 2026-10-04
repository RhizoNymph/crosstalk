//! The audit log: one record per operator action call.
//!
//! ```text
//! act(caller, action) ─permission─┬─ missing ──────────────▶ record Forbidden, no effect
//!                                 └─ held ─▶ apply ─┬─ Ok ──▶ record Succeeded (Applied, Unchanged, …)
//!                                                   │         (same transaction as the effect)
//!                                                   └─ Err ─▶ record Rejected, no effect
//! ```
//!
//! A record holds the [`OperatorAction`] value itself, not a per-action
//! shape, so every action, including ones added later, is audited the same
//! way. The log is append-only: [`AuditLog`] has no update or delete, and
//! the store's role has no `UPDATE` or `DELETE` grant on it.
//!
//! The log answers "who did what, when, and what came of it" across all
//! actions. The per-channel record of policy decisions, including the ones
//! config makes, is the channel's `PolicyHistory`.

use crate::ids::{AuditId, OperatorId};
use crate::interfaces::l8_surface::{
    ActionError, ActionKind, ActionOutcome, Caller, ConflictKind, InputError, OperatorAction,
    Permission,
};
use crate::paging::{AuditList, Page, PageRequest};
use crate::support::{TimeWindow, Timestamp};

/// What a call recorded in the log came to: the exact result `act`
/// returned, split so the log can be filtered by kind.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuditOutcome {
    Succeeded(ActionOutcome),
    /// A permitted action that was refused or failed. It had no effect.
    Rejected(Rejection),
    /// The caller lacked the action's required permission.
    Forbidden {
        missing: Permission,
    },
}

/// Why a permitted action was refused or failed: every `ActionError` except
/// `Forbidden`, which is its own outcome.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Rejection {
    NotFound,
    Conflict(ConflictKind),
    InvalidInput(InputError),
    /// A store or bus failure; the action's transaction rolled back.
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
            Ok(outcome) => Self::Succeeded(*outcome),
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
            Self::Succeeded(outcome) => Ok(*outcome),
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
/// Built only through [`AuditRecord::new`]: the outcome is `Forbidden`
/// exactly when the caller lacks the action's required permission, because
/// the permission is checked before anything else. A record that says an
/// action was applied for a caller who could not apply it, or forbidden for
/// one who could, cannot be built.
#[derive(Debug, Clone, PartialEq)]
pub struct AuditRecord {
    id: AuditId,
    at: Timestamp,
    caller: Caller,
    action: OperatorAction,
    outcome: AuditOutcome,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InvalidAuditRecord {
    /// `Forbidden`, but the caller holds the required permission.
    ForbiddenButPermitted { required: Permission },
    /// Applied, unchanged or rejected, but the caller lacks the required
    /// permission, so the action could not have been attempted.
    AttemptedWithoutPermission { required: Permission },
    /// `Forbidden`, but naming a permission other than the one the action
    /// requires.
    WrongMissingPermission { required: Permission },
}

impl AuditRecord {
    /// `at` is when the action was accepted or refused; for an applied
    /// `SetPolicy` it equals the decision's time.
    pub fn new(
        id: AuditId,
        at: Timestamp,
        caller: Caller,
        action: OperatorAction,
        outcome: AuditOutcome,
    ) -> Result<Self, InvalidAuditRecord> {
        let required = action.required_permission();
        let permitted = caller.has(required);
        match (&outcome, permitted) {
            (AuditOutcome::Forbidden { .. }, true) => {
                Err(InvalidAuditRecord::ForbiddenButPermitted { required })
            }
            (AuditOutcome::Forbidden { missing }, false) if *missing != required => {
                Err(InvalidAuditRecord::WrongMissingPermission { required })
            }
            (AuditOutcome::Forbidden { .. }, false) => Ok(()),
            (_, false) => Err(InvalidAuditRecord::AttemptedWithoutPermission { required }),
            (_, true) => Ok(()),
        }?;
        Ok(Self {
            id,
            at,
            caller,
            action,
            outcome,
        })
    }

    pub fn id(&self) -> AuditId {
        self.id
    }

    pub fn at(&self) -> Timestamp {
        self.at
    }

    /// The caller as authenticated for the call, with the permissions it
    /// held then.
    pub fn caller(&self) -> &Caller {
        &self.caller
    }

    pub fn action(&self) -> &OperatorAction {
        &self.action
    }

    pub fn outcome(&self) -> &AuditOutcome {
        &self.outcome
    }
}

/// Empty lists do not restrict; non-empty ones combine with AND.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct AuditFilter {
    /// Keep records whose `at` lies in the window.
    pub window: Option<TimeWindow>,
    pub operators: Vec<OperatorId>,
    pub actions: Vec<ActionKind>,
    pub outcomes: Vec<OutcomeKind>,
}

impl AuditFilter {
    pub fn matches(&self, record: &AuditRecord) -> bool {
        let in_window = self
            .window
            .is_none_or(|window| window.contains(record.at()));
        let by_operator =
            self.operators.is_empty() || self.operators.contains(&record.caller().operator);
        let of_action = self.actions.is_empty() || self.actions.contains(&record.action().kind());
        let with_outcome =
            self.outcomes.is_empty() || self.outcomes.contains(&record.outcome().kind());
        in_window && by_operator && of_action && with_outcome
    }
}

/// Append-only storage for audit records. There is no update or delete.
pub trait AuditLog {
    /// Append one record. Idempotent on `AuditRecord::id`: appending the
    /// same record again is a no-op, and a different record with a used id
    /// is `IdReused`.
    async fn append(&mut self, record: AuditRecord) -> Result<(), AuditError>;

    /// The records `filter` matches, a page at a time with the cursors of
    /// [`crate::paging`]: newest first by `(at, id)`, so records appended
    /// during a traversal never shift a page.
    async fn query(
        &self,
        filter: &AuditFilter,
        page: &PageRequest<AuditList>,
    ) -> Result<Page<AuditRecord, AuditList>, AuditError>;
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
