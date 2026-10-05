//! Verdict logs, detection quality, the audit log, the operator directory
//! and the caller's own operator.

use crosstalk_spec::aggregates::quality::DetectionQuality;
use crosstalk_spec::derived::flow::verdict::VerdictLog;
use crosstalk_spec::ids::TransmissionId;
use crosstalk_spec::interfaces::l5_flow::verdicts::{TransmissionVerdicts, VerdictError};
use crosstalk_spec::interfaces::l8_surface::audit::{AuditEntry, AuditFilter, AuditLog};
use crosstalk_spec::interfaces::l8_surface::operators::{Operator, OperatorStore};
use crosstalk_spec::interfaces::l8_surface::{Caller, Permission, QueryError};
use crosstalk_spec::paging::{AuditList, Page, PageRequest};
use crosstalk_spec::support::TimeWindow;

use crate::service::{Surface, require};
use crate::stores::SurfaceStores;

impl<S: SurfaceStores> Surface<S> {
    /// The transmission's verdict log; `None` for an unknown transmission.
    pub(crate) async fn verdicts_query(
        &self,
        caller: &Caller,
        transmission: TransmissionId,
    ) -> Result<Option<VerdictLog>, QueryError> {
        require(caller, Permission::View)?;
        match self.stores.transmissions().log(transmission).await {
            Ok(log) => Ok(Some(log)),
            Err(VerdictError::UnknownTransmission(_)) => Ok(None),
            Err(error) => Err(error.into()),
        }
    }

    pub(crate) async fn detection_quality_query(
        &self,
        caller: &Caller,
        window: TimeWindow,
    ) -> Result<DetectionQuality, QueryError> {
        require(caller, Permission::View)?;
        Ok(self.stores.transmissions().quality(window).await?)
    }

    pub(crate) async fn audit_query(
        &self,
        caller: &Caller,
        filter: &AuditFilter,
        page: &PageRequest<AuditList>,
    ) -> Result<Page<AuditEntry, AuditList>, QueryError> {
        require(caller, Permission::Audit)?;
        Ok(self.stores.audit().query(filter, page).await?)
    }

    pub(crate) async fn operators_query(
        &self,
        caller: &Caller,
    ) -> Result<Vec<Operator>, QueryError> {
        require(caller, Permission::View)?;
        Ok(self.stores.operators().operators().await?)
    }

    /// The caller's own operator: its name from the directory, the
    /// permissions the caller was authenticated with. No permission is
    /// checked: every caller may read itself.
    pub(crate) async fn me_query(&self, caller: &Caller) -> Result<Operator, QueryError> {
        let id = caller.operator();
        let Some(listed) = self
            .stores
            .operators()
            .operators()
            .await?
            .into_iter()
            .find(|operator| operator.id == id)
        else {
            tracing::warn!(operator = ?id, "caller's operator missing from the directory");
            return Err(QueryError::NotFound);
        };
        Ok(Operator {
            permissions: caller.permissions(),
            ..listed
        })
    }
}
