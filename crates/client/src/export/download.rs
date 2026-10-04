//! [`ExportDownload`]: an accepted export's response passed on as bytes.

use bytes::Bytes;
use crosstalk_spec::interfaces::l8_surface::export::ExportFormat;
use crosstalk_spec::interfaces::l8_surface::http::export::content_type;
use hyper::body::Incoming;

use crate::body::ChunkReader;
use crate::config::ClientConfig;
use crate::error::TransportError;

/// An accepted export as the surface sent it: the format, the
/// `Content-Disposition` naming the file, and the body a chunk at a time.
/// A body cut short is a [`TransportError`] from [`ExportDownload::chunk`];
/// the bytes before it hold no trailer, which whoever reads the file
/// refuses.
#[derive(Debug)]
pub struct ExportDownload {
    format: ExportFormat,
    content_disposition: String,
    chunks: ChunkReader,
}

impl ExportDownload {
    pub(super) fn new(
        format: ExportFormat,
        content_disposition: String,
        body: Incoming,
        config: ClientConfig,
    ) -> Self {
        Self {
            format,
            content_disposition,
            chunks: ChunkReader::new(body, config.idle_timeout()),
        }
    }

    pub fn format(&self) -> ExportFormat {
        self.format
    }

    /// `application/x-ndjson` or `application/vnd.apache.parquet`.
    pub fn content_type(&self) -> &'static str {
        content_type(self.format)
    }

    /// `attachment; filename="crosstalk-<dataset>-<export id>.<extension>"`,
    /// as the surface sent it.
    pub fn content_disposition(&self) -> &str {
        &self.content_disposition
    }

    /// The next chunk of the file; `None` once the body ended normally.
    pub async fn chunk(&mut self) -> Result<Option<Bytes>, TransportError> {
        self.chunks.chunk().await
    }
}
