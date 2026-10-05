//! `POST /exports`: the content type follows the format, the file name the
//! dataset and export id, and the route's errors before streaming are the
//! usual statuses.

use crate::interfaces::l8_surface::export::{ExportDataset, ExportFormat};
use crate::interfaces::l8_surface::http::export::{
    JSONL, PARQUET, content_disposition, content_type, file_name,
};
use crate::interfaces::l8_surface::http::{
    ErrorStatus, ResponseBody, Route, RoutePermission, Status,
};
use crate::interfaces::l8_surface::{ConflictKind, Permission, QueryError};
use crate::tests::export::{header, scope, window};

#[test]
fn the_content_type_is_the_format() {
    assert_eq!(content_type(ExportFormat::Jsonl), "application/x-ndjson");
    assert_eq!(
        content_type(ExportFormat::Parquet),
        "application/vnd.apache.parquet"
    );
    assert_eq!(
        (JSONL, PARQUET),
        (
            content_type(ExportFormat::Jsonl),
            content_type(ExportFormat::Parquet)
        )
    );
}

#[test]
fn the_file_is_named_by_dataset_and_export_id() {
    let edges = header(ExportDataset::Edges(scope()), false, 3);
    let id = edges.id().ulid_text();
    assert_eq!(file_name(&edges), format!("crosstalk-edges-{id}.jsonl"));
    assert_eq!(
        content_disposition(&edges),
        format!("attachment; filename=\"crosstalk-edges-{id}.jsonl\"")
    );
    let verdicts = header(ExportDataset::Verdicts(window(0, 1_500)), false, 0);
    assert!(file_name(&verdicts).starts_with("crosstalk-verdicts-"));
    for name in [file_name(&edges), file_name(&verdicts)] {
        assert!(
            name.bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'.'),
            "{name}"
        );
    }
}

/// The route streams the export; its permission is the request's, and a
/// refusal before streaming is answered with its status.
#[test]
fn errors_before_the_stream_are_statuses() {
    let spec = Route::Export.spec();
    assert_eq!(spec.success.body, ResponseBody::Export);
    assert_eq!(spec.success.status, Status::Ok);
    assert_eq!(spec.permission, RoutePermission::ByExportRequest);
    let too_large = QueryError::Conflict(ConflictKind::ExportTooLarge {
        rows: 11,
        limit: 10,
    });
    assert_eq!(too_large.status(), Status::Conflict);
    let forbidden = QueryError::Forbidden {
        missing: Permission::Content,
    };
    assert_eq!(forbidden.status(), Status::Forbidden);
}
