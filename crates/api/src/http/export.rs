//! `POST /exports`: one export as a streamed download
//! (`surface.http.export-download`).
//!
//! ```text
//! decode ExportRequest ─▶ QueryApi::export(caller, request)
//!   ├─ Err(e) ─▶ e.status(), e's JSON: no body byte of the export
//!   └─ Ok(Export { header, rows }) ─▶ 200, the format's Content-Type,
//!        Content-Disposition: attachment; filename="crosstalk-<dataset>-<id>.<ext>",
//!        no-store, nosniff, no Content-Length (chunked)
//!        body: {"type":"header",..}\n  {"type":"row",..}\n …  {"type":"trailer",..}\n
//! ```
//!
//! The status is sent with the header, before any row is read, so a
//! failure later is not a status: the stream's trailer records it
//! (`ExportEnd::Failed`) and the body ends normally. A line that cannot be
//! written (a value with no JSON) aborts the body without its terminating
//! chunk, so the client's HTTP stack reports the cut and the body has no
//! trailer, which `read_jsonl` and `verify_export` refuse.
//!
//! The server writes JSONL ([`written_formats`]). A Parquet request the
//! surface accepted is answered `InvalidInput(UnsupportedFormat)` before
//! any byte is sent, and the export is dropped without a trailer; the
//! gateway avoids it by offering only what this server writes in
//! `Present::export_formats`.

use axum::body::{Body, Bytes};
use axum::http::HeaderValue;
use axum::http::header::{CONTENT_DISPOSITION, X_CONTENT_TYPE_OPTIONS};
use axum::response::Response;
use crosstalk_spec::interfaces::l8_surface::export::{
    Export, ExportFormat, ExportFormats, ExportHeader, ExportLine, ExportRequest, ExportStep,
    ExportStream, UnsupportedFormat,
};
use crosstalk_spec::interfaces::l8_surface::http::Status;
use crosstalk_spec::interfaces::l8_surface::http::export::{content_disposition, content_type};
use crosstalk_spec::interfaces::l8_surface::{Caller, QueryError};
use futures_util::stream;

use super::input::Input;
use super::{Surface, respond};

/// The formats this server writes, in offer order: what the gateway gives
/// the surface as `Present::export_formats`.
pub fn written_formats() -> ExportFormats {
    ExportFormats::new(vec![ExportFormat::Jsonl]).unwrap_or_else(|_| {
        // One format, so neither empty nor repeated.
        unreachable!("one export format is a valid list")
    })
}

/// Why an export body stopped before its trailer.
#[derive(Debug, thiserror::Error)]
#[error("export line has no JSON: {0}")]
struct Unwritable(String);

pub(super) async fn serve<S: Surface>(
    surface: &S,
    caller: &Caller,
    input: &Input,
) -> Result<Response, QueryError> {
    let request: ExportRequest = input.body()?;
    let Export { header, rows } = surface.export(caller, &request).await?;
    let format = header.request().format();
    if !written_formats().offers(format) {
        tracing::error!(
            format = ?format,
            export = %header.id().ulid_text(),
            "the surface accepted an export format this server does not write"
        );
        return Err(QueryError::from(UnsupportedFormat { format }));
    }
    let disposition = respond::header_value(&content_disposition(&header))?;
    let body = Body::from_stream(stream::unfold(
        Next::Header(Box::new(header), rows),
        |next| async move { next.step().await },
    ));
    let mut response = respond::with_body(Status::Ok, content_type(format), body);
    let headers = response.headers_mut();
    headers.insert(CONTENT_DISPOSITION, disposition);
    headers.insert(X_CONTENT_TYPE_OPTIONS, HeaderValue::from_static("nosniff"));
    Ok(response)
}

/// Where the JSONL body is.
enum Next<R> {
    Header(Box<ExportHeader>, R),
    Rows(R),
    Done,
}

type Chunk = Result<Bytes, Unwritable>;

impl<R: ExportStream + Send> Next<R> {
    /// The next line and what follows it; `None` once the trailer is out
    /// (or the body was aborted).
    async fn step(self) -> Option<(Chunk, Self)> {
        match self {
            Self::Header(header, rows) => Some(line(&ExportLine::Header(header), Self::Rows(rows))),
            Self::Rows(rows) => Some(match rows.next().await {
                ExportStep::Row(row, rows) => line(&ExportLine::Row(row), Self::Rows(rows)),
                ExportStep::End(trailer) => line(&ExportLine::Trailer(trailer), Self::Done),
            }),
            Self::Done => None,
        }
    }
}

/// One line, compact JSON and `\n`; a line with no JSON aborts the body.
fn line<R>(line: &ExportLine, then: Next<R>) -> (Chunk, Next<R>) {
    match serde_json::to_vec(line) {
        Ok(mut bytes) => {
            bytes.push(b'\n');
            (Ok(Bytes::from(bytes)), then)
        }
        Err(error) => {
            tracing::warn!(error = %error, "export body aborted before its trailer");
            (Err(Unwritable(error.to_string())), Next::Done)
        }
    }
}
