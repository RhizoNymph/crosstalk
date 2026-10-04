//! `POST /exports`: one export as a download.
//!
//! ```text
//! POST /exports, body: ExportRequest
//!   ─▶ QueryApi::export(caller, request)
//!        ├─ Err(e) ─▶ e.status() with e's JSON (Forbidden 403, ExportTooLarge 409, …): nothing streamed
//!        └─ Ok(export) ─▶ 200 with the headers below, sent before any row is read
//!             body: the export in its format (JSONL lines, or one Parquet file), streamed chunked
//! ```
//!
//! **Headers.** `Content-Type` is the format's ([`content_type`]),
//! `Content-Disposition: attachment; filename="<name>"` names the file
//! ([`file_name`]: `crosstalk-<dataset>-<export id>.<jsonl|parquet>`, so two
//! exports never share a name), `Cache-Control: no-store` and
//! `X-Content-Type-Options: nosniff`. There is no `Content-Length`: the
//! size is not known until the trailer.
//!
//! **A failure mid-stream.** The status and headers are sent when the
//! header is known, before the first row, so a failure after that cannot
//! change them: the response stays `200`. The export's trailer records the
//! failure instead (`ExportEnd::Failed`: a store failure, the version
//! dropped, a count mismatch, a refused row), as the last JSONL line or in
//! the Parquet footer, and the body then ends normally. A reader therefore
//! never trusts the status: it checks the trailer (`verify_export`), and
//! only a `Complete` trailer whose count and digest match is a complete
//! export. When the server cannot write even the trailer (the process
//! stops), it aborts the response without its terminating chunk (HTTP/2:
//! a `RST_STREAM`), so the client's HTTP stack reports an error; a body
//! read up to there has no trailer, which `read_jsonl` and
//! `verify_export` already refuse (`NoTrailer`), and a Parquet file cut
//! before its footer does not open. HTTP trailers are not used.

use super::super::export::{ExportDatasetKind, ExportFormat, ExportHeader};

/// The content type of a JSONL export: newline-delimited JSON.
pub const JSONL: &str = "application/x-ndjson";

/// The content type of a Parquet export (IANA-registered).
pub const PARQUET: &str = "application/vnd.apache.parquet";

/// What the route table shows for the export route's content type.
pub const EITHER_CONTENT_TYPE: &str = "application/x-ndjson | application/vnd.apache.parquet";

/// The `Content-Type` of an export in `format`.
pub fn content_type(format: ExportFormat) -> &'static str {
    match format {
        ExportFormat::Jsonl => JSONL,
        ExportFormat::Parquet => PARQUET,
    }
}

/// The file extension of `format`.
pub fn extension(format: ExportFormat) -> &'static str {
    match format {
        ExportFormat::Jsonl => "jsonl",
        ExportFormat::Parquet => "parquet",
    }
}

/// The dataset's name in a file name.
pub fn dataset_name(dataset: ExportDatasetKind) -> &'static str {
    match dataset {
        ExportDatasetKind::Transmissions => "transmissions",
        ExportDatasetKind::Edges => "edges",
        ExportDatasetKind::Accesses => "accesses",
        ExportDatasetKind::Topics => "topics",
        ExportDatasetKind::Projection => "projection",
        ExportDatasetKind::Verdicts => "verdicts",
    }
}

/// `crosstalk-<dataset>-<export id>.<extension>`: ASCII letters, digits,
/// `-` and `.` only, so it needs no quoting beyond the header's quotes.
pub fn file_name(header: &ExportHeader) -> String {
    let request = header.request();
    format!(
        "crosstalk-{}-{}.{}",
        dataset_name(request.dataset().kind()),
        header.id().ulid_text(),
        extension(request.format())
    )
}

/// `attachment; filename="<file_name>"`.
pub fn content_disposition(header: &ExportHeader) -> String {
    format!("attachment; filename=\"{}\"", file_name(header))
}
