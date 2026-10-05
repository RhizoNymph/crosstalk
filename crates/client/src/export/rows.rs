//! [`HttpExportRows`]: a JSONL export read line by line, every row checked
//! by an [`ExportSealer`] as it passes, and the trailer checked against
//! what the sealer counted and digested (see the module docs of
//! [`crate::export`]).

use crosstalk_spec::interfaces::l8_surface::QueryError;
use crosstalk_spec::interfaces::l8_surface::export::digest::hash_row;
use crosstalk_spec::interfaces::l8_surface::export::{
    Export, ExportEnd, ExportHeader, ExportLine, ExportRequest, ExportSealer, ExportStep,
    ExportStream, ExportTrailer, RowHasher, SourceFailure,
};
use crosstalk_spec::interfaces::l8_surface::http::Route;
use crosstalk_spec::interfaces::l8_surface::http::export::content_disposition;
use hyper::body::Incoming;

use crate::body::{ChunkReader, LineReader};
use crate::config::ClientConfig;
use crate::error::ClientError;

/// The rows of an export read over HTTP, ending with its trailer: the
/// surface's when it holds, else a `Failed` one of the client's sealer.
pub struct HttpExportRows<H> {
    lines: LineReader,
    header: ExportHeader,
    sealer: ExportSealer<H>,
    /// The same digest as the sealer's, readable before the sealer is
    /// consumed: the trailer is compared with it.
    shadow: H,
    scratch: Vec<u8>,
}

impl<H: RowHasher> std::fmt::Debug for HttpExportRows<H> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HttpExportRows")
            .field("export", &self.header.id())
            .field("rows", &self.sealer.rows())
            .finish_non_exhaustive()
    }
}

/// Reads the header line of an accepted export and returns the export.
pub(super) async fn open<H: RowHasher + Default>(
    request: &ExportRequest,
    disposition: Option<&str>,
    body: Incoming,
    config: ClientConfig,
) -> Result<Export<HttpExportRows<H>>, ClientError<QueryError>> {
    let route = Route::Export;
    let unexpected = |reason: String| ClientError::unexpected(route, 200, reason);
    let mut lines = LineReader::new(
        ChunkReader::new(body, config.idle_timeout()),
        config.max_response_bytes(),
    );
    let first = lines
        .line()
        .await?
        .ok_or_else(|| unexpected("the export ended before its header".to_owned()))?;
    let header = match serde_json::from_slice::<ExportLine>(&first) {
        Ok(ExportLine::Header(header)) => *header,
        Ok(ExportLine::Row(_) | ExportLine::Trailer(_)) => {
            return Err(unexpected("the first line is not the header".to_owned()));
        }
        Err(error) => return Err(unexpected(format!("the header line: {error}"))),
    };
    if header.request() != request {
        return Err(unexpected("the header is another request's".to_owned()));
    }
    let expected = content_disposition(&header);
    if disposition != Some(expected.as_str()) {
        return Err(unexpected(format!(
            "Content-Disposition {disposition:?}, expected {expected}"
        )));
    }
    tracing::debug!(export = %header.id().ulid_text(), planned = header.rows(), "export header read");
    let rows = HttpExportRows {
        lines,
        sealer: ExportSealer::new(&header, H::default()),
        shadow: H::default(),
        scratch: Vec::new(),
        header: header.clone(),
    };
    Ok(Export { header, rows })
}

impl<H: RowHasher> HttpExportRows<H> {
    /// Ends the stream with the client's own `Failed` trailer: the rows
    /// yielded so far, their digest, and why.
    fn fail(self, reason: String) -> ExportTrailer {
        tracing::warn!(
            export = %self.header.id().ulid_text(),
            rows = self.sealer.rows(),
            reason = %reason,
            "export incomplete"
        );
        self.sealer.fail(SourceFailure::Store {
            reason: format!("export received incomplete: {reason}"),
        })
    }

    /// The trailer to end with, given the surface's.
    async fn conclude(mut self, trailer: ExportTrailer) -> ExportTrailer {
        match self.lines.line().await {
            Ok(None) => {}
            Ok(Some(_)) => return self.fail("a line after the trailer".to_owned()),
            Err(error) => {
                tracing::warn!(error = %error, "the export was cut after its trailer");
            }
        }
        if trailer.export() != self.header.id() {
            return self.fail("the trailer of another export".to_owned());
        }
        if matches!(trailer.end(), ExportEnd::Failed(_)) {
            tracing::warn!(end = ?trailer.end(), "the export failed mid-stream");
            return trailer;
        }
        let received = self.sealer.rows();
        if received != self.header.rows() {
            // `finish` records the count mismatch against the plan.
            return self.sealer.finish();
        }
        if trailer.rows() != received {
            let reason = format!(
                "the trailer counts {} rows, {received} were received",
                trailer.rows()
            );
            return self.fail(reason);
        }
        if trailer.digest().digest() != &self.shadow.finalize() {
            return self.fail("the rows do not match the trailer's digest".to_owned());
        }
        trailer
    }
}

impl<H: RowHasher + Send> ExportStream for HttpExportRows<H> {
    async fn next(mut self) -> ExportStep<Self> {
        let line = match self.lines.line().await {
            Ok(Some(line)) => line,
            Ok(None) => return ExportStep::End(self.fail("cut off before its trailer".to_owned())),
            Err(error) => return ExportStep::End(self.fail(format!("cut off: {error}"))),
        };
        match serde_json::from_slice::<ExportLine>(&line) {
            Ok(ExportLine::Row(row)) => match self.sealer.push(&row) {
                Ok(()) => {
                    hash_row(&mut self.shadow, &row, &mut self.scratch);
                    ExportStep::Row(row, self)
                }
                Err(refused) => {
                    tracing::warn!(refused = ?refused, "export row refused");
                    ExportStep::End(self.sealer.finish())
                }
            },
            Ok(ExportLine::Trailer(trailer)) => ExportStep::End(self.conclude(trailer).await),
            Ok(ExportLine::Header(_)) => ExportStep::End(self.fail("a second header".to_owned())),
            Err(error) => ExportStep::End(self.fail(format!("an undecodable line: {error}"))),
        }
    }
}
