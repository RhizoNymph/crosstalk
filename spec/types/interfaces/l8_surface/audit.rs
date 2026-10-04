//! The audit log: one record per operator action call.
//!
//! ```text
//! act(caller, action) ─permission─┬─ missing ──────────────▶ record Forbidden, no effect
//!                                 └─ held ─▶ apply ─┬─ Ok ──▶ record Applied / Unchanged
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
use crate::interfaces::l8_surface::{ActionKind, Caller, OperatorAction, Permission, QueryError};
use crate::paging::{AuditList, Page, PageRequest};
use crate::support::{TimeWindow, Timestamp};

/// What an accepted action did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ActionEffect {
    /// The action changed state (a decision published, a merge forwarded,
    /// an alert moved, a dead letter replayed).
    Applied,
    /// Accepted, but the state already matched: acknowledging an
    /// acknowledged alert, or the losing request of a race.
    Unchanged,
}

/// Why a permitted action was refused or failed. It had no effect.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Rejection {
    /// What the action names does not exist.
    NotFound,
    /// The request is invalid in the current state, e.g. acknowledging a
    /// suppressed alert.
    Invalid { reason: String },
    /// A store or bus failure; the action's transaction rolled back.
    Failed { reason: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuditOutcome {
    Applied,
    Unchanged,
    Rejected(Rejection),
    /// The caller lacked the action's required permission.
    Forbidden,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum OutcomeKind {
    Applied,
    Unchanged,
    Rejected,
    Forbidden,
}

impl AuditOutcome {
    /// The outcome recorded for what `OperatorActions::act` returns.
    ///
    /// `act` never returns `InvalidCursor` or `StaleProjection` (they belong
    /// to list and projection queries); should one reach the log it is
    /// recorded as an invalid request, so [`AuditOutcome::result`] inverts
    /// this only for the errors `act` can return.
    pub fn of(result: &Result<ActionEffect, QueryError>) -> Self {
        match result {
            Ok(ActionEffect::Applied) => Self::Applied,
            Ok(ActionEffect::Unchanged) => Self::Unchanged,
            Err(QueryError::Forbidden) => Self::Forbidden,
            Err(QueryError::NotFound) => Self::Rejected(Rejection::NotFound),
            Err(QueryError::BadRequest { reason }) => Self::Rejected(Rejection::Invalid {
                reason: reason.clone(),
            }),
            Err(QueryError::Store { reason }) => Self::Rejected(Rejection::Failed {
                reason: reason.clone(),
            }),
            Err(QueryError::InvalidCursor) => Self::Rejected(Rejection::Invalid {
                reason: "an operator action takes no cursor".to_owned(),
            }),
            Err(QueryError::StaleProjection { .. }) => Self::Rejected(Rejection::Invalid {
                reason: "an operator action reads no projection".to_owned(),
            }),
        }
    }

    /// What `act` returned for a call recorded with this outcome. The
    /// inverse of [`AuditOutcome::of`].
    pub fn result(&self) -> Result<ActionEffect, QueryError> {
        match self {
            Self::Applied => Ok(ActionEffect::Applied),
            Self::Unchanged => Ok(ActionEffect::Unchanged),
            Self::Forbidden => Err(QueryError::Forbidden),
            Self::Rejected(Rejection::NotFound) => Err(QueryError::NotFound),
            Self::Rejected(Rejection::Invalid { reason }) => Err(QueryError::BadRequest {
                reason: reason.clone(),
            }),
            Self::Rejected(Rejection::Failed { reason }) => Err(QueryError::Store {
                reason: reason.clone(),
            }),
        }
    }

    pub fn kind(&self) -> OutcomeKind {
        match self {
            Self::Applied => OutcomeKind::Applied,
            Self::Unchanged => OutcomeKind::Unchanged,
            Self::Rejected(_) => OutcomeKind::Rejected,
            Self::Forbidden => OutcomeKind::Forbidden,
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
            (AuditOutcome::Forbidden, true) => {
                Err(InvalidAuditRecord::ForbiddenButPermitted { required })
            }
            (AuditOutcome::Forbidden, false) => Ok(()),
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
