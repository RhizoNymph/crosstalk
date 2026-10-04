//! Reading response bodies: whole (JSON, a frame), or a chunk at a time
//! with an idle timeout (the live feed, an export), and split into lines.

use std::time::Duration;

use bytes::{Buf, Bytes, BytesMut};
use http_body_util::BodyExt;
use hyper::body::Incoming;

use crate::error::TransportError;

/// The whole body, refusing one over `limit` bytes.
pub(crate) async fn collect(body: Incoming, limit: usize) -> Result<Bytes, TransportError> {
    match http_body_util::Limited::new(body, limit).collect().await {
        Ok(collected) => Ok(collected.to_bytes()),
        Err(error) if error.is::<http_body_util::LengthLimitError>() => {
            Err(TransportError::TooLarge { limit })
        }
        Err(error) => Err(TransportError::Body(error)),
    }
}

/// A streamed body read a chunk at a time. A chunk that does not arrive
/// within `idle` is a [`TransportError::Timeout`]: the stream is treated
/// as cut.
#[derive(Debug)]
pub(crate) struct ChunkReader {
    body: Incoming,
    idle: Duration,
    ended: bool,
}

impl ChunkReader {
    pub(crate) fn new(body: Incoming, idle: Duration) -> Self {
        Self {
            body,
            idle,
            ended: false,
        }
    }

    /// The next data chunk; `None` once the body ended normally. HTTP
    /// trailers are skipped: the binding sends none.
    pub(crate) async fn chunk(&mut self) -> Result<Option<Bytes>, TransportError> {
        while !self.ended {
            let frame = tokio::time::timeout(self.idle, self.body.frame())
                .await
                .map_err(|_| TransportError::Timeout {
                    millis: self.idle.as_millis(),
                })?;
            match frame {
                None => self.ended = true,
                Some(Err(error)) => return Err(TransportError::Body(Box::new(error))),
                Some(Ok(frame)) => {
                    if let Ok(data) = frame.into_data()
                        && !data.is_empty()
                    {
                        return Ok(Some(data));
                    }
                }
            }
        }
        Ok(None)
    }
}

/// A streamed body split at each `\n`, as JSONL framing writes it
/// (`export::framing`). A last fragment with no `\n` was cut off mid-line
/// and is dropped, as the reference reader drops it.
#[derive(Debug)]
pub(crate) struct LineReader {
    chunks: ChunkReader,
    buffer: BytesMut,
    limit: usize,
}

impl LineReader {
    pub(crate) fn new(chunks: ChunkReader, limit: usize) -> Self {
        Self {
            chunks,
            buffer: BytesMut::new(),
            limit,
        }
    }

    /// The next complete line without its `\n`; `None` at the end of the
    /// body (a cut-off fragment dropped).
    pub(crate) async fn line(&mut self) -> Result<Option<Bytes>, TransportError> {
        let mut searched = 0;
        loop {
            if let Some(at) = self.buffer[searched..].iter().position(|&b| b == b'\n') {
                let line = self.buffer.split_to(searched + at).freeze();
                self.buffer.advance(1);
                return Ok(Some(line));
            }
            searched = self.buffer.len();
            if searched > self.limit {
                return Err(TransportError::TooLarge { limit: self.limit });
            }
            match self.chunks.chunk().await? {
                Some(chunk) => self.buffer.extend_from_slice(&chunk),
                None => {
                    self.buffer.clear();
                    return Ok(None);
                }
            }
        }
    }
}
