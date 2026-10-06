//! `QueryApi::export` over the fixture, as `l8_surface::export` defines it:
//!
//! ```text
//! export(caller, request)
//!   1. request.required_permission() ── missing ─▶ Forbidden; audit Refused
//!   2. Parquet ── the fixture writes JSONL only ─▶ Store; audit Refused
//!   3. W = the fixture's watermark
//!   4. Snapshot::plan(request, W) ── Err ─▶ QueryError::from; audit Refused
//!   5. limits.check(rows) ── over ─▶ Conflict(ExportTooLarge); audit Refused
//!   6. ExportHeader::new(..); audit Started; return Export { header, rows }
//! stream: rows ─▶ trailer; audit Ended (or Abandoned when dropped first)
//! ```
//!
//! Everything before the header runs under the state's write lock, so the
//! plan reads one snapshot and the `Started` entry is appended before the
//! header is returned. Rows are sealed with the surface's own row hasher
//! (`crosstalk_surface::export::Blake3RowHasher`: BLAKE3 under the spec's
//! `ROW_DIGEST_CONTEXT`), so a fixture export verifies wherever a surface
//! export does, through `crosstalk-client` included.

mod plan;
mod rows;
mod stream;

use std::num::NonZeroU64;
use std::sync::Arc;

use crosstalk_spec::ids::ExportId;
use crosstalk_spec::interfaces::l8_surface::audit::AuditError;
use crosstalk_spec::interfaces::l8_surface::export::{
    Export, ExportEvent, ExportFormat, ExportHeader, ExportHeaderParts, ExportLimits, ExportRecord,
    ExportRequest, ExportSource, GatewayVersion, InvalidExportRecord,
};
use crosstalk_spec::interfaces::l8_surface::{Caller, QueryError};
use tokio::sync::RwLock;

use super::queries::graph::watermark;
use super::queries::{Ctx, require};
use super::store::State;
use super::world::World;
use crate::backend::Result;

pub use stream::ExportRows;

/// The fixture's `export.max_rows`. A week of transmissions or edge
/// buckets (about 4,600 rows each) fits; a week of access buckets (about
/// 9,500) does not, so the refusal can be seen.
pub const MAX_ROWS: NonZeroU64 = match NonZeroU64::new(5_000) {
    Some(rows) => rows,
    None => NonZeroU64::MIN,
};

pub fn limits() -> ExportLimits {
    ExportLimits::new(MAX_ROWS)
}

/// The formats the fixture writes. Parquet needs a Parquet writer (Thrift
/// footer, column encodings), which the UI crate does not depend on.
pub const FORMATS: &[ExportFormat] = &[ExportFormat::Jsonl];

/// The gateway version the header records.
fn gateway() -> Result<GatewayVersion> {
    GatewayVersion::new(concat!("crosstalk-ui-fixture/", env!("CARGO_PKG_VERSION"))).map_err(|e| {
        QueryError::Store {
            reason: format!("fixture gateway version: {e:?}"),
        }
    })
}

/// Why an export event could not be audited. A fixture fault either way.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuditFault {
    Record(InvalidExportRecord),
    Log(AuditError),
}

impl From<AuditFault> for QueryError {
    fn from(fault: AuditFault) -> Self {
        Self::Store {
            reason: format!("fixture export audit: {fault:?}"),
        }
    }
}

/// Appends one export event to the audit log, at the fixture's clock.
fn record(
    state: &mut State,
    caller: &Caller,
    request: &ExportRequest,
    event: ExportEvent,
) -> std::result::Result<(), AuditFault> {
    let record =
        ExportRecord::new(caller.clone(), request.clone(), event).map_err(AuditFault::Record)?;
    let at = state.clock.now();
    state
        .audit
        .export(&mut state.mint, at, record)
        .map(|_| ())
        .map_err(AuditFault::Log)
}

/// Steps 1 to 6 up to the header, under the write lock.
async fn start(
    world: &World,
    state: &mut State,
    limits: ExportLimits,
    caller: &Caller,
    request: &ExportRequest,
) -> Result<(ExportHeader, plan::PlannedRows)> {
    require(caller, request.required_permission())?;
    if !FORMATS.contains(&request.format()) {
        return Err(QueryError::Store {
            reason: format!(
                "the fixture backend cannot write {:?}; it writes JSONL only",
                request.format()
            ),
        });
    }
    let watermark = watermark();
    let plan = {
        let snapshot = plan::Snapshot {
            ctx: Ctx::new(world, state),
        };
        snapshot.plan(request, watermark).await?
    };
    limits.check(plan.rows).map_err(QueryError::Conflict)?;
    let started_at = state.clock.now();
    let header = ExportHeader::new(ExportHeaderParts {
        id: ExportId::from_ulid(state.mint.ulid(started_at)),
        request: request.clone(),
        by: caller.operator(),
        started_at,
        watermark,
        basis: plan.basis,
        embedding_model: plan.embedding_model,
        gateway: gateway()?,
        rows: plan.rows,
    })
    .map_err(|e| QueryError::Store {
        reason: format!("fixture export header: {e:?}"),
    })?;
    Ok((header, plan.source))
}

/// `QueryApi::export`: the header, audited `Started`, and its rows; or the
/// refusal, audited `Refused`.
pub async fn export(
    world: &World,
    shared: &Arc<RwLock<State>>,
    limits: ExportLimits,
    caller: &Caller,
    request: &ExportRequest,
) -> Result<Export<ExportRows>> {
    let mut state = shared.write().await;
    match start(world, &mut state, limits, caller, request).await {
        Ok((header, rows)) => {
            let started = ExportEvent::Started(Box::new(header.clone()));
            record(&mut state, caller, request, started)?;
            let rows = ExportRows::new(
                &header,
                rows,
                Arc::clone(shared),
                caller.clone(),
                request.clone(),
            );
            Ok(Export { header, rows })
        }
        Err(error) => {
            record(
                &mut state,
                caller,
                request,
                ExportEvent::Refused(error.clone()),
            )?;
            Err(error)
        }
    }
}
