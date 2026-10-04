//! The stream `export` returns: the sealed rows, with the export's end
//! recorded in the audit log.
//!
//! The trailer is recorded as `Ended` when the sealer builds it. A stream
//! dropped before its trailer (the client went away, or the future awaiting
//! a row was cancelled) is recorded as `Abandoned` with the rows it handed
//! out by its [`EndGuard`], from a task spawned on the current runtime,
//! since `Drop` cannot await.

use std::sync::Arc;

use crosstalk_spec::ids::{AuditId, ExportId};
use crosstalk_spec::interfaces::l8_surface::CallerSnapshot;
use crosstalk_spec::interfaces::l8_surface::audit::{AuditBody, AuditEntry, AuditLog};
use crosstalk_spec::interfaces::l8_surface::export::{
    ExportEvent, ExportRecord, ExportRequest, ExportStep, ExportStream, RowSource, SealedRows,
};
use crosstalk_spec::support::Clock;

use super::hasher::Blake3RowHasher;
use crate::ids::IdMinter;

/// What a stream needs to record its end: the audit log, the id minter,
/// the clock, and the export's caller, request and id.
pub struct ExportAudit<A> {
    pub(crate) log: A,
    pub(crate) ids: IdMinter,
    pub(crate) clock: Arc<dyn Clock>,
    pub(crate) caller: CallerSnapshot,
    pub(crate) request: ExportRequest,
    pub(crate) export: ExportId,
}

impl<A> std::fmt::Debug for ExportAudit<A> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ExportAudit")
            .field("caller", &self.caller)
            .field("export", &self.export)
            .finish_non_exhaustive()
    }
}

impl<A: AuditLog + Send + 'static> ExportAudit<A> {
    /// Append `event` for this export, now. A failed append is logged: the
    /// rows are already out, so it cannot fail the export.
    async fn record(mut self, event: ExportEvent) {
        let entry = match self.entry(event) {
            Ok(entry) => entry,
            Err(reason) => {
                tracing::warn!(export = ?self.export, %reason, "export end not audited");
                return;
            }
        };
        if let Err(error) = self.log.append(entry).await {
            tracing::warn!(export = ?self.export, error = ?error, "export end not audited");
        }
    }

    fn entry(&self, event: ExportEvent) -> Result<AuditEntry, String> {
        let record = ExportRecord::new(self.caller, self.request.clone(), event)
            .map_err(|invalid| format!("export record refused: {invalid:?}"))?;
        let id: AuditId = self.ids.mint().map_err(|error| error.to_string())?;
        Ok(AuditEntry {
            id,
            at: self.clock.now(),
            body: AuditBody::Export(record),
        })
    }
}

/// Records how an export ended: `Ended` through [`EndGuard::ended`], or
/// `Abandoned` when dropped first.
#[derive(Debug)]
pub struct EndGuard<A: AuditLog + Send + 'static> {
    audit: Option<ExportAudit<A>>,
    /// Rows handed out so far.
    sent: u64,
}

impl<A: AuditLog + Send + 'static> EndGuard<A> {
    async fn ended(mut self, event: ExportEvent) {
        if let Some(audit) = self.audit.take() {
            audit.record(event).await;
        }
    }
}

impl<A: AuditLog + Send + 'static> Drop for EndGuard<A> {
    fn drop(&mut self) {
        let Some(audit) = self.audit.take() else {
            return;
        };
        let event = ExportEvent::Abandoned {
            export: audit.export,
            rows: self.sent,
        };
        match tokio::runtime::Handle::try_current() {
            Ok(runtime) => {
                tracing::info!(export = ?audit.export, rows = self.sent, "export abandoned");
                runtime.spawn(audit.record(event));
            }
            Err(_) => {
                tracing::warn!(
                    export = ?audit.export,
                    "export abandoned outside a runtime; not audited"
                );
            }
        }
    }
}

/// The rows of one started export ([`ExportStream`]), ending with its
/// trailer.
#[derive(Debug)]
pub struct SurfaceExport<R, A: AuditLog + Send + 'static> {
    rows: SealedRows<R, Blake3RowHasher>,
    end: EndGuard<A>,
}

impl<R, A: AuditLog + Send + 'static> SurfaceExport<R, A> {
    pub(crate) fn new(rows: SealedRows<R, Blake3RowHasher>, audit: ExportAudit<A>) -> Self {
        Self {
            rows,
            end: EndGuard {
                audit: Some(audit),
                sent: 0,
            },
        }
    }
}

impl<R, A> ExportStream for SurfaceExport<R, A>
where
    R: RowSource + Send,
    A: AuditLog + Send + 'static,
{
    async fn next(self) -> ExportStep<Self> {
        let Self { rows, mut end } = self;
        match rows.next().await {
            ExportStep::Row(row, rest) => {
                end.sent += 1;
                ExportStep::Row(row, Self { rows: rest, end })
            }
            ExportStep::End(trailer) => {
                end.ended(ExportEvent::Ended(trailer.clone())).await;
                ExportStep::End(trailer)
            }
        }
    }
}
