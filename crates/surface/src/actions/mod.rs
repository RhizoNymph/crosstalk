//! Operator actions ([`OperatorActions`]): permission, write-ahead intent,
//! effect, audit.
//!
//! ```text
//! ActionRequest ─into_action(caller)─┬─ Err(SelfMerge) ─▶ InvalidInput(SelfMerge), not audited
//!                                    └─ Ok(action) ─▶ act(caller, action)
//! act: at = clock.now()
//!      permission held? ─ no ─▶ append OperatorRecord(caller, action, Forbidden { missing }) at `at`
//!                       └ yes ─▶ id = mint; intend(AuditIntent(id, at, caller, action))
//!                                ├─ Err ─▶ Store, nothing applied, nothing recorded
//!                                └─ Ok ─▶ apply (one store call, stamped with caller and at) ─▶ outcome or ActionError::from
//!                                         complete(intent.entry(AuditOutcome::of(result))): entry appended, intent removed
//!      return result (AuditOutcome::result of the entry is exactly it)
//! start: recover_interrupted ─▶ each leftover intent appended as Interrupted
//! ```
//!
//! An action's effect commits in the store of the layer that owns it, its
//! entry in the audit log; no transaction spans both. So the intent is
//! durable before the effect, and the entry replaces it afterwards: a
//! process that stops in between leaves the intent, which
//! [`Surface::recover_interrupted`] records as `Interrupted` at the next
//! start (`surface.audit.no-silent-effect`). A completion the log refuses
//! also leaves the intent; the call then reports `Store`, and the next
//! start records it as interrupted, since the effect may have applied.

mod apply;
mod errors;

use crosstalk_spec::ids::AuditId;
use crosstalk_spec::interfaces::l8_surface::audit::{
    AuditBody, AuditError, AuditIntent, AuditIntents, AuditOutcome, OperatorRecord,
};
use crosstalk_spec::interfaces::l8_surface::{
    ActionError, ActionOutcome, ActionRequest, Caller, OperatorAction, OperatorActions, Permission,
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

impl<S: SurfaceStores> Surface<S> {
    /// At start, before the surface accepts a call: record every operator
    /// action a stopped process left between its intent and its entry as
    /// [`AuditOutcome::Interrupted`] (`AuditIntents::recover_interrupted`).
    /// Returns the entries' ids, oldest first.
    pub async fn recover_interrupted(&self) -> Result<Vec<AuditId>, AuditError> {
        let mut log = self.stores.audit().clone();
        let recovered = log.recover_interrupted().await?;
        if !recovered.is_empty() {
            tracing::warn!(
                count = recovered.len(),
                "operator actions interrupted by a stop recorded as interrupted"
            );
        }
        Ok(recovered)
    }

    /// The forbidden call's entry: no intent, no effect.
    async fn forbidden(
        &self,
        caller: &Caller,
        action: OperatorAction,
        at: crosstalk_spec::support::Timestamp,
        missing: Permission,
    ) -> Result<ActionOutcome, ActionError> {
        let kind = action.kind();
        let result = Err(ActionError::Forbidden { missing });
        let record = match OperatorRecord::new(caller, action, AuditOutcome::of(&result)) {
            Ok(record) => record,
            // Unreachable: the caller lacks `missing`, the action's
            // required permission.
            Err(invalid) => {
                tracing::error!(?kind, ?invalid, "operator record refused");
                return Err(ActionError::Store {
                    reason: format!("operator record refused: {invalid:?}"),
                });
            }
        };
        match self.audit_append(at, AuditBody::Operator(record)).await {
            Ok(entry) => {
                tracing::info!(?kind, operator = ?caller.operator(), audit = ?entry, ok = false, "operator action forbidden");
                result
            }
            Err(failed) => {
                tracing::warn!(?kind, operator = ?caller.operator(), error = %failed, "operator action not audited");
                Err(ActionError::Store {
                    reason: failed.reason(),
                })
            }
        }
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
        if !caller.has(required) {
            return self.forbidden(caller, action, at, required).await;
        }
        let id: AuditId = self.ids.mint().map_err(|error| ActionError::Store {
            reason: format!("audit append failed: no audit id could be minted: {error}"),
        })?;
        let intent = match AuditIntent::new(id, at, caller, action.clone()) {
            Ok(intent) => intent,
            // Unreachable: the caller holds the required permission.
            Err(invalid) => {
                tracing::error!(?kind, ?invalid, "audit intent refused");
                return Err(ActionError::Store {
                    reason: format!("audit intent refused: {invalid:?}"),
                });
            }
        };
        let mut log = self.stores.audit().clone();
        if let Err(error) = log.intend(&intent).await {
            tracing::warn!(?kind, operator = ?caller.operator(), ?error, "operator action refused: intent not recorded");
            return Err(ActionError::Store {
                reason: format!("audit intent not recorded: {error:?}"),
            });
        }
        let result = self.apply(caller, &action, at).await;
        let entry = match intent.entry(AuditOutcome::of(&result)) {
            Ok(entry) => entry,
            // Unreachable: a permitted call's outcome is never `Forbidden`.
            Err(invalid) => {
                tracing::error!(?kind, ?invalid, "operator record refused");
                return Err(ActionError::Store {
                    reason: format!("operator record refused: {invalid:?}"),
                });
            }
        };
        match log.complete(entry).await {
            Ok(()) => {
                tracing::info!(
                    ?kind,
                    operator = ?caller.operator(),
                    audit = ?id,
                    ok = result.is_ok(),
                    "operator action"
                );
                result
            }
            Err(error) => {
                // The intent stays: the next start records the call as
                // interrupted, since its effect may have applied.
                tracing::warn!(?kind, operator = ?caller.operator(), ?error, "operator action not audited; left as an intent");
                match result {
                    Err(ActionError::Store { reason }) => Err(ActionError::Store { reason }),
                    Ok(_) | Err(_) => Err(ActionError::Store {
                        reason: format!(
                            "audit append failed: the audit log refused the entry: {error:?}"
                        ),
                    }),
                }
            }
        }
    }
}
