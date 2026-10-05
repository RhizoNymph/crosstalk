//! `POST /exports` over HTTP.
//!
//! ```text
//! QueryApi::export(request)            JSONL only: a Parquet request is refused here, unsent
//!   ─▶ POST /exports, body: ExportRequest
//!        ├─ error status ─▶ the error (403, 409 ExportTooLarge, 422 UnsupportedFormat, …): nothing streamed
//!        └─ 200 application/x-ndjson ─▶ line 1: the header (for this request, named as Content-Disposition says)
//!             ─▶ Export { header, rows: HttpExportRows }
//!                  rows ─▶ each line a row, run through an ExportSealer as it passes
//!                       ─▶ the trailer, checked against what the sealer counted and digested
//! HttpClient::download_export(request) ─▶ the response's bytes as they come, either format
//! ```
//!
//! **The trailer is the verdict.** A failure after the header cannot change
//! the `200` already sent, so the surface records it in the trailer, and a
//! server that cannot write even that cuts the response
//! (`surface.http.export-download`). The client therefore never trusts the
//! status: [`HttpExportRows`] checks every row with the binding's own
//! sealer as it passes (dataset, content columns, key order, the planned
//! count) and ends with
//! - the surface's trailer, when it is `Failed` (the surface's own account
//!   of the failure), or when it is `Complete` and agrees with what the
//!   client counted and digested;
//! - otherwise a `Failed` trailer of the client's sealer over the rows it
//!   yielded: `InvalidRow` for a row the sealer refused (the row is not
//!   yielded), `CountMismatch` when fewer or more rows than planned
//!   arrived, and `Store` naming the cause for a body cut before its
//!   trailer, a trailer of another export, a digest that does not match,
//!   or a line that is not the export's framing.
//!
//! So a stream that ends `Complete` is exactly the export the surface
//! sealed: `verify_export` of the rows yielded and the trailer holds
//! (`surface.http.client-export-verified`).
//!
//! **Formats.** The trait's stream yields decoded rows, which the client
//! reads from JSONL; it has no Parquet reader, so a Parquet request through
//! `QueryApi::export` is `InvalidInput(UnsupportedFormat)` before anything
//! is sent. [`HttpClient::download_export`] passes either format through
//! as bytes, for a page that hands the file to a browser.

mod download;
mod rows;

pub use download::ExportDownload;
pub use rows::HttpExportRows;

use crosstalk_spec::interfaces::l8_surface::QueryError;
use crosstalk_spec::interfaces::l8_surface::export::{
    Export, ExportFormat, ExportRequest, RowHasher, UnsupportedFormat,
};
use crosstalk_spec::interfaces::l8_surface::http::export::content_type;
use crosstalk_spec::interfaces::l8_surface::http::{RequestBuilder, Route};
use hyper::StatusCode;
use hyper::body::Incoming;
use hyper::header::{CONTENT_DISPOSITION, HeaderMap};

use crate::body;
use crate::client::{HttpClient, expect_content_type};
use crate::error::{ClientError, TransportError, decode_error};

type Result<T> = std::result::Result<T, ClientError<QueryError>>;

/// The accepted export's response head and its body, not yet read.
struct Accepted {
    content_disposition: Option<String>,
    body: Incoming,
}

impl<H> HttpClient<H> {
    /// `POST /exports` up to the response head: an error status is the
    /// error, read whole; a `200` must carry the format's content type.
    async fn open_export(&self, request: &ExportRequest) -> Result<Accepted> {
        let route = Route::Export;
        let encoded = RequestBuilder::new(route)
            .body(request)
            .build()
            .map_err(ClientError::Encode)?;
        let accept = content_type(request.format());
        let config = *self.config();
        let timeout = TransportError::Timeout {
            millis: config.request_timeout().as_millis(),
        };
        let response = tokio::time::timeout(
            config.request_timeout(),
            self.send(route, encoded, accept, HeaderMap::new()),
        )
        .await
        .map_err(|_| timeout)??;
        let (parts, body) = response.into_parts();
        let status = parts.status.as_u16();
        if parts.status != StatusCode::OK {
            let error_body = tokio::time::timeout(
                config.request_timeout(),
                body::collect(body, config.max_response_bytes()),
            )
            .await
            .map_err(|_| TransportError::Timeout {
                millis: config.request_timeout().as_millis(),
            })??;
            return Err(decode_error(route, status, &error_body));
        }
        expect_content_type(route, status, &parts.headers, accept)?;
        let content_disposition = parts
            .headers
            .get(CONTENT_DISPOSITION)
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned);
        tracing::info!(format = ?request.format(), dataset = ?request.dataset().kind(), "export accepted");
        Ok(Accepted {
            content_disposition,
            body,
        })
    }

    /// The export as decoded rows; `request` is JSONL.
    pub(crate) async fn start_export(
        &self,
        request: &ExportRequest,
    ) -> Result<Export<HttpExportRows<H>>>
    where
        H: RowHasher + Default,
    {
        if request.format() != ExportFormat::Jsonl {
            // The client's own refusal, as the trait's error: it reads no
            // Parquet. Nothing is sent.
            return Err(ClientError::Api(QueryError::from(UnsupportedFormat {
                format: request.format(),
            })));
        }
        let accepted = self.open_export(request).await?;
        rows::open(
            request,
            accepted.content_disposition.as_deref(),
            accepted.body,
            *self.config(),
        )
        .await
    }

    /// The export's response as bytes, for passing the file on: its
    /// content type, `Content-Disposition` and body as they arrive. Either
    /// format. Nothing here checks the trailer; whoever reads the file does
    /// (`verify_export`, or the Parquet footer).
    pub async fn download_export(&self, request: &ExportRequest) -> Result<ExportDownload> {
        let accepted = self.open_export(request).await?;
        let content_disposition = accepted
            .content_disposition
            .ok_or_else(|| ClientError::unexpected(Route::Export, 200, "no Content-Disposition"))?;
        Ok(ExportDownload::new(
            request.format(),
            content_disposition,
            accepted.body,
            *self.config(),
        ))
    }
}
