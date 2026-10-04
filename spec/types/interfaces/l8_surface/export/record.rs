//! How exports appear in the audit log.
//!
//! Every export is audited, View and Content alike: an export moves the
//! gateway's records out in bulk, which is what the log is for. One call
//! leaves either one `Refused` entry, or a `Started` entry and then one
//! `Ended` or `Abandoned` entry:
//!
//! ```text
//! export(caller, request) ─permission─┬─ missing ─▶ Refused(Forbidden)
//!                                     └─ held ─▶ plan ─┬─ Err ─▶ Refused(error)
//!                                                      └─ Ok ──▶ Started(header)   appended before the first row
//!                                       stream ─┬─ trailer sent ─────▶ Ended(trailer)
//!                                               └─ client went away ─▶ Abandoned { rows sent }
//! ```
//!
//! `Started` is appended before the first row is sent; if the append fails
//! the export fails with `Store` and sends nothing, so no row leaves
//! unaudited. A `Store` refusal had no effect and leaves at most one entry,
//! as for actions. The entries of one export share its [`ExportId`], their
//! subject, so the log's subject filter finds its start and end together.

use crate::ids::ExportId;
use crate::interfaces::l8_surface::audit::AuditSubject;
use crate::interfaces::l8_surface::{Caller, Permission, QueryError};

use super::manifest::{ExportHeader, ExportTrailer};
use super::request::{ExportDataset, ExportRequest};

/// What an audit entry records about an export.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExportEvent {
    /// Refused before anything was sent: exactly what `export` returned.
    Refused(QueryError),
    /// Accepted; this header was sent first. Boxed: it is the largest event.
    Started(Box<ExportHeader>),
    /// The trailer was sent.
    Ended(ExportTrailer),
    /// The client went away after `rows` rows and before the trailer.
    Abandoned { export: ExportId, rows: u64 },
}

/// One audited export event.
///
/// Built only through [`ExportRecord::new`]: `Refused(Forbidden)` exactly
/// when the caller lacks the request's required permission, naming it;
/// every other event only for a caller holding it; and a `Started` header
/// is for this request and this caller's operator.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExportRecord {
    caller: Caller,
    request: ExportRequest,
    event: ExportEvent,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InvalidExportRecord {
    ForbiddenButPermitted {
        required: Permission,
    },
    AttemptedWithoutPermission {
        required: Permission,
    },
    WrongMissingPermission {
        required: Permission,
    },
    /// The header describes another request or another requester.
    HeaderMismatch,
}

impl ExportRecord {
    pub fn new(
        caller: Caller,
        request: ExportRequest,
        event: ExportEvent,
    ) -> Result<Self, InvalidExportRecord> {
        let required = request.required_permission();
        let permitted = caller.has(required);
        match (&event, permitted) {
            (ExportEvent::Refused(QueryError::Forbidden { .. }), true) => {
                Err(InvalidExportRecord::ForbiddenButPermitted { required })
            }
            (ExportEvent::Refused(QueryError::Forbidden { missing }), false)
                if *missing != required =>
            {
                Err(InvalidExportRecord::WrongMissingPermission { required })
            }
            (ExportEvent::Refused(QueryError::Forbidden { .. }), false) => Ok(()),
            (_, false) => Err(InvalidExportRecord::AttemptedWithoutPermission { required }),
            (ExportEvent::Started(header), true)
                if *header.request() != request || header.by() != caller.operator() =>
            {
                Err(InvalidExportRecord::HeaderMismatch)
            }
            (_, true) => Ok(()),
        }?;
        Ok(Self {
            caller,
            request,
            event,
        })
    }

    pub fn caller(&self) -> &Caller {
        &self.caller
    }

    pub fn request(&self) -> &ExportRequest {
        &self.request
    }

    pub fn event(&self) -> &ExportEvent {
        &self.event
    }

    /// The export's id (once it has one), then the projection a projection
    /// export reads.
    pub fn subjects(&self) -> Vec<AuditSubject> {
        let export = match &self.event {
            ExportEvent::Refused(_) => None,
            ExportEvent::Started(header) => Some(header.id()),
            ExportEvent::Ended(trailer) => Some(trailer.export()),
            ExportEvent::Abandoned { export, .. } => Some(*export),
        };
        let projection = match self.request.dataset() {
            ExportDataset::Projection(projection) => Some(*projection),
            ExportDataset::Transmissions(_)
            | ExportDataset::Edges(_)
            | ExportDataset::Accesses(_)
            | ExportDataset::Topics(_)
            | ExportDataset::Verdicts(_) => None,
        };
        export
            .map(AuditSubject::Export)
            .into_iter()
            .chain(projection.map(AuditSubject::Projection))
            .collect()
    }
}
