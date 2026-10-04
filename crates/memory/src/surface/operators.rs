//! [`InMemoryOperatorStore`]: the store behind the surface's
//! [`OperatorDirectory`].
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
    AuditBody, AuditEntry, AuditError, ConfigChange, ConfigOutcome, ConfigRecord,
};
use crosstalk_spec::interfaces::l8_surface::operators::{
    AccessConfig, InvalidAccessConfig, Operator, OperatorDirectory, RequestIdentity,
    Unauthenticated,
};
use crosstalk_spec::support::Timestamp;

use super::audit::InMemoryAuditLog;
use crate::analysis::support::{IdSequence, lock};

/// Why a config load changed nothing.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum LoadError {
    #[error("the access config cannot be used: {0:?}")]
    Invalid(InvalidAccessConfig),
    #[error("the audit log refused the load's entries: {0:?}")]
    Audit(AuditError),
}

/// Why a request got no caller.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum CallerError {
    #[error("no access config has been loaded")]
    NotLoaded,
    #[error("the request is not authenticated: {0:?}")]
    Unauthenticated(Unauthenticated),
}

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

    /// Apply `config` (the document hashed `hash`) at `at`: the new
    /// directory is stored and each change is appended as an applied config
    /// entry, together. Returns the changes, in `OperatorDirectory::load`'s
    /// order.
    pub fn load(
        &self,
        config: &AccessConfig,
        hash: ConfigHash,
        at: Timestamp,
    ) -> Result<Vec<ConfigChange>, LoadError> {
        let mut state = lock(&self.state);
        let (directory, changes) = OperatorDirectory::load(state.directory.as_ref(), config)
            .map_err(LoadError::Invalid)?;
        let mut ids = state.audit_ids;
        let entries: Vec<AuditEntry> = changes
            .iter()
            .map(|change| AuditEntry {
                id: AuditId::from_ulid(ids.next_raw()),
                at,
                body: AuditBody::Config(ConfigRecord {
                    config: hash,
                    change: change.clone(),
                    outcome: ConfigOutcome::Applied,
                }),
            })
            .collect();
        self.audit.append_all(&entries).map_err(LoadError::Audit)?;
        state.audit_ids = ids;
        state.directory = Some(directory);
        Ok(changes)
    }

    /// The stored directory, if a config was ever loaded.
    pub fn directory(&self) -> Option<OperatorDirectory> {
        lock(&self.state).directory.clone()
    }

    /// Every operator, current and former, by id (`QueryApi::operators`).
    pub fn operators(&self) -> Vec<Operator> {
        lock(&self.state)
            .directory
            .as_ref()
            .map(|directory| directory.operators().cloned().collect())
            .unwrap_or_default()
    }

    /// The caller for one request ([`OperatorDirectory::caller`]).
    pub fn caller(&self, identity: RequestIdentity) -> Result<Caller, CallerError> {
        let state = lock(&self.state);
        let directory = state.directory.as_ref().ok_or(CallerError::NotLoaded)?;
        directory
            .caller(identity)
            .map_err(CallerError::Unauthenticated)
    }
}
