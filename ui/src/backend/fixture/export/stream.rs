//! The stream `export` returns: the spec's [`SealedRows`] over the planned
//! rows, with the fixture's row hasher, and a ledger that audits how the
//! export ended.
//!
//! The trailer is the sealer's, so the stream ends exactly as the spec's
//! sealed stream does. When it yields the trailer the ledger appends
//! `Ended(trailer)`; when it is dropped before the trailer (the client went
//! away) the ledger appends `Abandoned { rows }` with the rows already
//! yielded. Either way the export's audit entries are its `Started` and one
//! end.

use std::sync::Arc;

use crosstalk_spec::ids::ExportId;
use crosstalk_spec::interfaces::l8_surface::Caller;
use crosstalk_spec::interfaces::l8_surface::export::{
    ExportEvent, ExportHeader, ExportRequest, ExportStep, ExportStream, ExportTrailer, SealedRows,
};
use tokio::sync::RwLock;

use super::digest::RowDigest;
use super::plan::PlannedRows;
use super::record;
use crate::backend::fixture::store::State;

/// The rows of one fixture export, ending with its trailer.
#[derive(Debug)]
pub struct ExportRows {
    rows: SealedRows<PlannedRows, RowDigest>,
    ledger: Ledger,
}

impl ExportRows {
    pub(super) fn new(
        header: &ExportHeader,
        rows: PlannedRows,
        state: Arc<RwLock<State>>,
        caller: Caller,
        request: ExportRequest,
    ) -> Self {
        Self {
            rows: SealedRows::new(header, rows, RowDigest::new()),
            ledger: Ledger {
                state,
                caller,
                request,
                export: header.id(),
                sent: 0,
                ended: false,
            },
        }
    }
}

impl ExportStream for ExportRows {
    async fn next(self) -> ExportStep<Self> {
        let Self { rows, mut ledger } = self;
        match rows.next().await {
            ExportStep::Row(row, rows) => {
                ledger.sent = ledger.sent.saturating_add(1);
                ExportStep::Row(row, Self { rows, ledger })
            }
            ExportStep::End(trailer) => {
                ledger.end(&trailer).await;
                ExportStep::End(trailer)
            }
        }
    }
}

/// Audits the end of one started export.
#[derive(Debug)]
struct Ledger {
    state: Arc<RwLock<State>>,
    caller: Caller,
    request: ExportRequest,
    export: ExportId,
    /// Rows yielded so far.
    sent: u64,
    /// Whether the trailer was yielded (and `Ended` appended).
    ended: bool,
}

impl Ledger {
    async fn end(&mut self, trailer: &ExportTrailer) {
        self.ended = true;
        let mut state = self.state.write().await;
        let event = ExportEvent::Ended(trailer.clone());
        if let Err(error) = record(&mut state, &self.caller, &self.request, event) {
            tracing::error!(export = ?self.export, error = ?error, "export end not audited");
        }
    }

    fn abandoned(&self) -> ExportEvent {
        ExportEvent::Abandoned {
            export: self.export,
            rows: self.sent,
        }
    }
}

impl Drop for Ledger {
    /// Appends `Abandoned` for a stream dropped before its trailer: at
    /// once when the log is free, otherwise from a task on the current
    /// runtime.
    fn drop(&mut self) {
        if self.ended {
            return;
        }
        let event = self.abandoned();
        if let Ok(mut state) = self.state.try_write() {
            if let Err(error) = record(&mut state, &self.caller, &self.request, event) {
                tracing::error!(export = ?self.export, error = ?error, "export abandonment not audited");
            }
            return;
        }
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            tracing::error!(export = ?self.export, "export abandoned outside a runtime: not audited");
            return;
        };
        let state = Arc::clone(&self.state);
        let (caller, request, export) = (self.caller.clone(), self.request.clone(), self.export);
        runtime.spawn(async move {
            let mut state = state.write().await;
            if let Err(error) = record(&mut state, &caller, &request, event) {
                tracing::error!(export = ?export, error = ?error, "export abandonment not audited");
            }
        });
    }
}
