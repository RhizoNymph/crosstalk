//! The export request the export page builds (item 11). The spec's
//! `export::ExportRequest` replaces it when the export area moves onto
//! `QueryApi::export`; until then the page answers 501 with this request.
//! The audit log and the operator directory are the spec's
//! (`l8_surface::{audit, operators}`).

use crosstalk_spec::ids::ProjectionId;

use crate::url::scope::Scope;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExportDataset {
    Transmissions,
    Edges,
    Accesses,
    Topics,
    Projection(ProjectionId),
    Verdicts,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExportFormat {
    Jsonl,
    Parquet,
}

/// An export (item 11). `include_content` needs `Content`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExportRequest {
    pub dataset: ExportDataset,
    pub scope: Scope,
    pub format: ExportFormat,
    pub include_content: bool,
}
