//! `QueryApi::export`: refuse or plan, audit the start, and stream the
//! sealed rows, auditing the end.
//!
//! ```text
//! export(caller, request)
//!   permission ── missing ──▶ Refused(Forbidden)              (nothing read)
//!   format     ── unwritten ▶ Refused(UnsupportedFormat)      (nothing read)
//!   W = EdgeStore::watermark()
//!   ExportSource::plan(request, W) ── Err ─▶ Refused(QueryError::from)
//!   ExportLimits::check(rows) ── over ─▶ Refused(Conflict(ExportTooLarge))
//!   header = ExportHeader::new(..) ─▶ append Started(header) ── fails ─▶ Err(Store), nothing sent
//!   rows = SealedRows(source, BLAKE3 row hasher)
//!   stream: … rows … trailer ─▶ append Ended(trailer)
//!           dropped before its trailer ─▶ append Abandoned { rows sent }
//! ```
//!
//! - [`hasher`]: the BLAKE3 row hasher the digest is defined with.
//! - [`stream`]: [`SurfaceExport`], the stream `export` returns.
//! - [`source`]: [`SpecExportSource`], an `ExportSource` over the spec's
//!   store traits for the datasets they can produce.

pub mod hasher;
pub mod source;
pub mod stream;

use crosstalk_spec::ids::ExportId;
use crosstalk_spec::interfaces::l7_topology::EdgeStore;
use crosstalk_spec::interfaces::l8_surface::audit::AuditBody;
use crosstalk_spec::interfaces::l8_surface::export::{
    Export, ExportEvent, ExportHeader, ExportHeaderParts, ExportRecord, ExportRequest,
    ExportSource, SealedRows,
};
use crosstalk_spec::interfaces::l8_surface::{Caller, CallerSnapshot, QueryError};
use crosstalk_spec::support::Timestamp;

use crate::service::Surface;
use crate::stores::SurfaceStores;

pub use hasher::Blake3RowHasher;
pub use source::SpecExportSource;
pub use stream::SurfaceExport;

/// The stream `export` returns over `S`'s export source.
pub type ExportRowsOf<S> = SurfaceExport<
    <<S as SurfaceStores>::Export as ExportSource>::Rows,
    <S as SurfaceStores>::Audit,
>;

impl<S: SurfaceStores> Surface<S> {
    pub(crate) async fn export_query(
        &self,
        caller: &Caller,
        request: &ExportRequest,
    ) -> Result<Export<ExportRowsOf<S>>, QueryError> {
        let at = self.now();
        let required = request.required_permission();
        if !caller.has(required) {
            return Err(self
                .refuse(
                    caller,
                    request,
                    at,
                    QueryError::Forbidden { missing: required },
                )
                .await);
        }
        if let Err(unsupported) = self.config.export_formats.check(request.format()) {
            return Err(self.refuse(caller, request, at, unsupported.into()).await);
        }
        let started = match self.start(caller, request, at).await {
            Ok(started) => started,
            Err(error) => return Err(self.refuse(caller, request, at, error).await),
        };
        let (header, source) = started;
        let event = ExportEvent::Started(Box::new(header.clone()));
        let record = ExportRecord::new(caller, request.clone(), event).map_err(|invalid| {
            QueryError::Store {
                reason: format!("export record refused: {invalid:?}"),
            }
        })?;
        if let Err(failed) = self.audit_append(at, AuditBody::Export(record)).await {
            tracing::warn!(export = ?header.id(), error = %failed, "export start not audited");
            return Err(QueryError::Store {
                reason: failed.reason(),
            });
        }
        tracing::info!(
            export = ?header.id(),
            operator = ?caller.operator(),
            rows = header.rows(),
            "export started"
        );
        let rows = SealedRows::new(&header, source, Blake3RowHasher::new());
        let stream = SurfaceExport::new(
            rows,
            stream::ExportAudit {
                log: self.stores.audit().clone(),
                ids: self.ids.clone(),
                clock: std::sync::Arc::clone(&self.clock),
                caller: CallerSnapshot::of(caller),
                request: request.clone(),
                export: header.id(),
            },
        );
        Ok(Export {
            header,
            rows: stream,
        })
    }

    /// Read the watermark, plan the export, check its size and build its
    /// header. Nothing is audited here.
    async fn start(
        &self,
        caller: &Caller,
        request: &ExportRequest,
        at: Timestamp,
    ) -> Result<
        (
            ExportHeader,
            <<S as SurfaceStores>::Export as ExportSource>::Rows,
        ),
        QueryError,
    > {
        let watermark = self.stores.edges().watermark().await?;
        let plan = self
            .stores
            .export_source()
            .plan(request, watermark)
            .await?;
        self.config
            .export_limits
            .check(plan.rows)
            .map_err(QueryError::Conflict)?;
        let id: ExportId = self.mint()?;
        let header = ExportHeader::new(ExportHeaderParts {
            id,
            request: request.clone(),
            by: caller.operator(),
            started_at: at.max(watermark.at()),
            watermark,
            basis: plan.basis,
            embedding_model: plan.embedding_model,
            gateway: self.config.gateway.clone(),
            rows: plan.rows,
        })
        .map_err(|invalid| QueryError::Store {
            reason: format!("export header refused: {invalid:?}"),
        })?;
        Ok((header, plan.source))
    }

    /// Record a refused export and return the error the call returns: the
    /// refusal itself, or `Store` when the refusal could not be recorded.
    async fn refuse(
        &self,
        caller: &Caller,
        request: &ExportRequest,
        at: Timestamp,
        error: QueryError,
    ) -> QueryError {
        let event = ExportEvent::Refused(error.clone());
        let record = match ExportRecord::new(caller, request.clone(), event) {
            Ok(record) => record,
            Err(invalid) => {
                return QueryError::Store {
                    reason: format!("export record refused: {invalid:?}"),
                };
            }
        };
        match self.audit_append(at, AuditBody::Export(record)).await {
            Ok(_) => error,
            Err(failed) => {
                tracing::warn!(error = %failed, "export refusal not audited");
                match error {
                    QueryError::Store { .. } => error,
                    _ => QueryError::Store {
                        reason: failed.reason(),
                    },
                }
            }
        }
    }
}
