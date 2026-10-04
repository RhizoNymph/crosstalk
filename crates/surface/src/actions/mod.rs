//! Operator actions ([`OperatorActions`]): permission, effect, audit.
//!
//! ```text
//! ActionRequest ─into_action(caller)─┬─ Err(SelfMerge) ─▶ InvalidInput(SelfMerge), not audited
//!                                    └─ Ok(action) ─▶ act(caller, action)
//! act: at = clock.now()
//!      permission held? ─ no ─▶ Forbidden { missing }
//!                       └ yes ─▶ apply (one store call, stamped with caller and at) ─▶ outcome or ActionError::from
//!      append OperatorRecord(caller, action, AuditOutcome::of(result)) at `at`
//!      return result (AuditOutcome::result of the entry is exactly it)
//! ```
//!
//! The entry is appended after the store call returns. The in-memory
//! stores share no transaction, so a store that applied the effect and an
//! audit log that then refuses the entry leave the effect unaudited; the
//! call then reports `Store`. A database implementation of `SurfaceStores`
//! makes both one transaction.

mod apply;
mod errors;

use crosstalk_spec::interfaces::l8_surface::audit::{AuditBody, AuditOutcome, OperatorRecord};
use crosstalk_spec::interfaces::l8_surface::{
    ActionError, ActionOutcome, ActionRequest, Caller, OperatorAction, OperatorActions,
};

use crate::service::Surface;
use crate::stores::SurfaceStores;

impl<S: SurfaceStores> Surface<S> {
    /// Stamp `request` with `caller` ([`ActionRequest::into_action`]) and
    /// act on it. A merge naming one agent twice is `InvalidInput(SelfMerge)`
    /// and never reaches `act`, so it is not audited.
    pub async fn request(
        &self,
        caller: &Caller,
        request: ActionRequest,
    ) -> Result<ActionOutcome, ActionError> {
        let action = request.into_action(caller).map_err(ActionError::from)?;
        self.act(caller, action).await
    }
}

impl<S: SurfaceStores> OperatorActions for Surface<S> {
    async fn act(
        &self,
        caller: &Caller,
        action: OperatorAction,
    ) -> Result<ActionOutcome, ActionError> {
        let at = self.now();
        let kind = action.kind();
        let required = action.required_permission();
        let result = if caller.has(required) {
            self.apply(caller, &action, at).await
        } else {
            Err(ActionError::Forbidden { missing: required })
        };
        let outcome = AuditOutcome::of(&result);
        let record = match OperatorRecord::new(caller, action, outcome) {
            Ok(record) => record,
            // Unreachable: the outcome is `Forbidden` exactly when the
            // caller lacks the required permission, naming it.
            Err(invalid) => {
                tracing::error!(?kind, ?invalid, "operator record refused");
                return Err(ActionError::Store {
                    reason: format!("operator record refused: {invalid:?}"),
                });
            }
        };
        match self.audit_append(at, AuditBody::Operator(record)).await {
            Ok(entry) => {
                tracing::info!(
                    ?kind,
                    operator = ?caller.operator(),
                    audit = ?entry,
                    ok = result.is_ok(),
                    "operator action"
                );
                result
            }
            Err(failed) => {
                tracing::warn!(?kind, operator = ?caller.operator(), error = %failed, "operator action not audited");
                match result {
                    Err(ActionError::Store { reason }) => Err(ActionError::Store { reason }),
                    Ok(_) | Err(_) => Err(ActionError::Store {
                        reason: failed.reason(),
                    }),
                }
            }
        }
    }
}
