//! Appending to the audit log: one entry per action call and per export
//! event, each under a fresh `AuditId` and stamped with the time the call
//! was accepted.

use crosstalk_spec::ids::AuditId;
use crosstalk_spec::interfaces::l8_surface::audit::{AuditBody, AuditEntry, AuditError, AuditLog};
use crosstalk_spec::support::Timestamp;

use crate::service::Surface;
use crate::stores::SurfaceStores;

/// Why an entry could not be appended.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub(crate) enum AppendFailed {
    #[error("no audit id could be minted: {reason}")]
    Mint { reason: String },
    #[error("the audit log refused the entry: {0:?}")]
    Log(AuditError),
}

impl AppendFailed {
    /// The reason a call that fails on it reports.
    pub(crate) fn reason(&self) -> String {
        format!("audit append failed: {self}")
    }
}

impl<S: SurfaceStores> Surface<S> {
    /// Append `body` as a new entry dated `at`.
    pub(crate) async fn audit_append(
        &self,
        at: Timestamp,
        body: AuditBody,
    ) -> Result<AuditId, AppendFailed> {
        let id: AuditId = self.ids.mint().map_err(|error| AppendFailed::Mint {
            reason: error.to_string(),
        })?;
        let entry = AuditEntry { id, at, body };
        let mut log = self.stores.audit().clone();
        log.append(entry).await.map_err(AppendFailed::Log)?;
        Ok(id)
    }
}
