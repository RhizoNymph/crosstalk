//! [`InMemoryOperatorStore`]: the reference [`OperatorStore`], the store
//! behind the surface's [`OperatorDirectory`].
//!
//! The directory itself is a spec value built only by
//! [`OperatorDirectory::load`]; the store keeps the current one and applies
//! each config load to it, recording every change as a config audit entry
//! in the same transaction that stores the new directory. A config the
//! directory already reflects records nothing; a config `load` refuses
//! changes nothing and records nothing.

use std::sync::{Arc, Mutex};

use crosstalk_spec::ids::{AuditId, ConfigHash};
use crosstalk_spec::interfaces::l8_surface::Caller;
use crosstalk_spec::interfaces::l8_surface::audit::{
    AuditBody, AuditEntry, ConfigChange, ConfigOutcome, ConfigRecord,
};
use crosstalk_spec::interfaces::l8_surface::operators::{
    AccessConfig, CallerError, Operator, OperatorDirectory, OperatorLoadError, OperatorStore,
    OperatorStoreError, RequestIdentity,
};
use crosstalk_spec::support::Timestamp;

use super::audit::InMemoryAuditLog;
use crate::support::{IdSequence, lock};

/// The operator directory and the audit log its loads are recorded in.
/// Cloning shares the store.
#[derive(Debug, Clone)]
pub struct InMemoryOperatorStore {
    audit: InMemoryAuditLog,
    state: Arc<Mutex<OperatorState>>,
}

#[derive(Debug)]
struct OperatorState {
    directory: Option<OperatorDirectory>,
    audit_ids: IdSequence,
}

impl InMemoryOperatorStore {
    /// A store with no directory yet, recording into `audit`. Audit ids
    /// come from `audit_ids`, which must not overlap the ids anything else
    /// appends to the same log.
    pub fn new(audit: InMemoryAuditLog, audit_ids: IdSequence) -> Self {
        Self {
            audit,
            state: Arc::new(Mutex::new(OperatorState {
                directory: None,
                audit_ids,
            })),
        }
    }

    /// The stored directory, if a config was ever loaded.
    pub fn directory(&self) -> Option<OperatorDirectory> {
        lock(&self.state).directory.clone()
    }
}

impl OperatorStore for InMemoryOperatorStore {
    /// The new directory is stored and each change is appended as an
    /// applied config entry, together; audit ids are drawn only when both
    /// succeed.
    async fn load(
        &mut self,
        config: &AccessConfig,
        hash: ConfigHash,
        at: Timestamp,
    ) -> Result<Vec<ConfigChange>, OperatorLoadError> {
        let mut state = lock(&self.state);
        let (directory, changes) = OperatorDirectory::load(state.directory.as_ref(), config)
            .map_err(OperatorLoadError::Invalid)?;
        let ids = state.audit_ids.peek(changes.len());
        let entries: Vec<AuditEntry> = changes
            .iter()
            .zip(ids)
            .map(|(change, id)| AuditEntry {
                id: AuditId::from_ulid(id),
                at,
                body: AuditBody::Config(ConfigRecord {
                    config: hash,
                    change: change.clone(),
                    outcome: ConfigOutcome::Applied,
                }),
            })
            .collect();
        self.audit
            .append_all(&entries)
            .map_err(OperatorLoadError::Audit)?;
        state.audit_ids.skip(entries.len());
        state.directory = Some(directory);
        Ok(changes)
    }

    async fn operators(&self) -> Result<Vec<Operator>, OperatorStoreError> {
        Ok(lock(&self.state)
            .directory
            .as_ref()
            .map(|directory| directory.operators().cloned().collect())
            .unwrap_or_default())
    }

    async fn caller(&self, identity: RequestIdentity) -> Result<Caller, CallerError> {
        let state = lock(&self.state);
        let directory = state.directory.as_ref().ok_or(CallerError::NotLoaded)?;
        directory
            .caller(identity)
            .map_err(CallerError::Unauthenticated)
    }
}
