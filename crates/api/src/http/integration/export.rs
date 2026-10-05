//! `POST /exports`: refused before any byte, or 200 with the export's
//! headers and its JSONL lines streamed, the trailer last, a failure
//! after the header included.

use std::sync::Arc;

use axum::http::StatusCode;
use axum::http::header::{
    CACHE_CONTROL, CONTENT_DISPOSITION, CONTENT_LENGTH, CONTENT_TYPE, X_CONTENT_TYPE_OPTIONS,
};
use crosstalk_spec::interfaces::l8_surface::export::{
    ExportEnd, ExportFormat, ExportHeader, ExportRequest, ExportTrailer, read_jsonl,
};
use crosstalk_spec::interfaces::l8_surface::http::export::content_disposition;
use crosstalk_spec::interfaces::l8_surface::http::{RequestBuilder, Route};
use crosstalk_spec::interfaces::l8_surface::{InputError, Permission, QueryError};
use serde_json::json;

use super::fake::{Call, ExportParts, Fake};
use super::{FULL, error_json, golden, golden_bytes, operator, request, send, server, without};
use crate::http::written_formats;

fn complete() -> ExportParts {
    let read = read_jsonl(&golden_bytes("surface_reads/export/export_complete.jsonl"))
        .expect("the golden export reads");
    ExportParts {
        header: read.header,
        rows: read.rows,
        trailer: read.trailer.expect("the golden has its trailer"),
    }
}

fn export_request(
    parts: &ExportParts,
) -> crosstalk_spec::interfaces::l8_surface::http::EncodedRequest {
    RequestBuilder::new(Route::Export)
        .body(parts.header.request())
        .build()
        .expect("a request")
}

/// A complete export is the golden JSONL byte for byte, with the
/// download's headers; a failure after the header is still a 200 whose
/// body ends with the trailer recording it.
#[tokio::test]
async fn export_streams_with_trailer_after_a_mid_stream_failure() {
    let parts = complete();
    let fake = Arc::new(Fake::default());
    fake.export_with(parts.clone());
    let reply = send(&server(&fake), request(&export_request(&parts), FULL)).await;
    assert_eq!(reply.status, StatusCode::OK);
    assert_eq!(
        reply.header(CONTENT_TYPE.as_str()),
        Some("application/x-ndjson")
    );
    let disposition = content_disposition(&parts.header);
    assert_eq!(
        reply.header(CONTENT_DISPOSITION.as_str()),
        Some(disposition.as_str())
    );
    assert!(disposition.starts_with("attachment; filename=\"crosstalk-"));
    assert_eq!(reply.header(CACHE_CONTROL.as_str()), Some("no-store"));
    assert_eq!(
        reply.header(X_CONTENT_TYPE_OPTIONS.as_str()),
        Some("nosniff")
    );
    assert_eq!(reply.header(CONTENT_LENGTH.as_str()), None, "streamed");
    assert_eq!(
        reply.body.to_vec(),
        golden_bytes("surface_reads/export/export_complete.jsonl"),
        "the export's JSONL, unchanged"
    );
    assert_eq!(
        fake.calls(),
        vec![Call {
            method: "export",
            operator: operator(FULL),
            args: json!({ "body": parts.header.request() }),
        }]
    );

    // The store fails after the first row: the status is already sent.
    let failed: ExportTrailer = golden("surface_reads/export/export_trailer_store");
    assert!(matches!(failed.end(), ExportEnd::Failed(_)));
    let mut cut = parts.clone();
    cut.rows.truncate(1);
    cut.trailer = failed.clone();
    let fake = Arc::new(Fake::default());
    fake.export_with(cut.clone());
    let reply = send(&server(&fake), request(&export_request(&cut), FULL)).await;
    assert_eq!(reply.status, StatusCode::OK);
    let read = read_jsonl(&reply.body).expect("JSONL framing");
    assert_eq!(read.header, cut.header);
    assert_eq!(read.rows, cut.rows);
    assert_eq!(read.trailer, Some(failed));
    let lines: Vec<&[u8]> = reply.body.split(|byte| *byte == b'\n').collect();
    assert_eq!(
        lines.len(),
        1 + 1 + 1 + 1,
        "header, one row, trailer, then nothing"
    );
    assert!(lines[3].is_empty(), "the trailer line ends with a newline");
}

/// A refused export is its status and JSON, and no export byte.
#[tokio::test]
async fn a_refused_export_sends_no_body() {
    let parts = complete();
    let refusals = [
        json!({"type": "conflict", "data": {"type": "export_too_large", "data": {"rows": 2000001, "limit": 2000000}}}),
        json!({"type": "invalid_input", "data": {"type": "unsupported_format", "data": {"format": "parquet"}}}),
        json!({"type": "store", "data": {"reason": "the audit log is unreachable"}}),
    ];
    for refusal in refusals {
        let error: QueryError = serde_json::from_value(refusal).expect("a query error");
        let fake = Arc::new(Fake::default());
        fake.export_with(parts.clone());
        fake.fail_with(error.clone());
        let reply = send(&server(&fake), request(&export_request(&parts), FULL)).await;
        assert_eq!(reply.json(), error_json(&error));
        assert_ne!(reply.status, StatusCode::OK);
        assert_eq!(reply.header(CONTENT_DISPOSITION.as_str()), None);
    }
    // Content columns need Content, before anything is read.
    let with_content: ExportRequest =
        golden("surface_reads/export/export_request_transmissions_with_content");
    assert_eq!(with_content.required_permission(), Permission::Content);
    let encoded = RequestBuilder::new(Route::Export)
        .body(&with_content)
        .build()
        .expect("a request");
    let fake = Arc::new(Fake::default());
    fake.export_with(parts);
    let reply = send(
        &server(&fake),
        request(&encoded, without(Permission::Content)),
    )
    .await;
    assert_eq!(reply.status, StatusCode::FORBIDDEN);
    assert!(fake.untouched());
}

/// The server writes JSONL only; a Parquet export the surface accepted is
/// refused as `UnsupportedFormat` before any byte.
#[tokio::test]
async fn a_format_the_server_does_not_write_is_unsupported() {
    assert_eq!(written_formats().as_slice(), &[ExportFormat::Jsonl]);
    let mut parts = complete();
    let mut header = serde_json::to_value(&parts.header).expect("JSON");
    header["request"]["format"] = json!("parquet");
    let Ok(parquet) = serde_json::from_value::<ExportHeader>(header) else {
        panic!("a header for a Parquet export decodes");
    };
    parts.header = parquet;
    let fake = Arc::new(Fake::default());
    fake.export_with(parts.clone());
    let reply = send(&server(&fake), request(&export_request(&parts), FULL)).await;
    assert_eq!(reply.status, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(
        reply.json(),
        error_json(&QueryError::InvalidInput(InputError::UnsupportedFormat {
            format: ExportFormat::Parquet
        }))
    );
}
