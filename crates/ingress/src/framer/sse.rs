//! The Anthropic Messages event-stream framer.
//!
//! Splits the bytes into server-sent events by the WHATWG rules (lines end
//! with LF, CRLF or CR; a blank line dispatches; `:` lines are comments; one
//! space after the field's colon is dropped) and reports:
//!
//! - [`FrameEvent::FirstContent`] at the first message event
//!   (`message_start`, `content_block_*`, `message_delta`, `message_stop`);
//! - [`FrameEvent::Finished`] at `message_stop`, after which every byte is
//!   ignored;
//! - [`FrameError::UpstreamErrorEvent`] at an `error` event;
//! - [`FrameError::MalformedFrame`] at an event whose data is not JSON, whose
//!   `event` field is not UTF-8, or that grows past the configured size, with
//!   the offset of the event's first byte.
//!
//! `ping` and unknown event types are skipped. The framer keeps only the
//! current line and the current event, so its state does not depend on where
//! chunks split the stream (`ingress.framer.chunking-invariant`).

use std::num::NonZeroUsize;

use crosstalk_spec::interfaces::l0_ingress::{FrameError, FrameEvent};
use serde::de::IgnoredAny;

use super::{Emitted, Progress};

/// The event being read.
#[derive(Debug, Default)]
struct Event {
    /// Offset of its first byte; `None` before any line of it.
    start: Option<u64>,
    kind: Option<String>,
    data: Option<Vec<u8>>,
    /// Bytes of it seen so far, terminators included.
    bytes: usize,
}

#[derive(Debug)]
pub struct SseFramer {
    max_event: NonZeroUsize,
    /// Offset of the next byte.
    offset: u64,
    /// The current line, without its terminator.
    line: Vec<u8>,
    line_start: u64,
    /// The last byte was CR: a following LF belongs to it.
    after_cr: bool,
    event: Event,
    progress: Progress,
}

impl SseFramer {
    pub fn new(max_event: NonZeroUsize) -> Self {
        Self {
            max_event,
            offset: 0,
            line: Vec::new(),
            line_start: 0,
            after_cr: false,
            event: Event::default(),
            progress: Progress::NotStarted,
        }
    }

    /// Scan `chunk`, appending events; stop at `Finished` or the first error.
    pub(super) fn scan(&mut self, chunk: &[u8], emitted: &mut Emitted) -> Result<(), FrameError> {
        let mut rest = chunk;
        while !rest.is_empty() {
            if self.after_cr {
                self.after_cr = false;
                if rest[0] == b'\n' {
                    self.offset += 1;
                    rest = &rest[1..];
                    self.line_start = self.offset;
                    continue;
                }
            }
            match rest.iter().position(|&byte| byte == b'\n' || byte == b'\r') {
                Some(end) => {
                    self.extend_line(&rest[..end])?;
                    self.after_cr = rest[end] == b'\r';
                    self.offset += end as u64 + 1;
                    self.count(1)?;
                    rest = &rest[end + 1..];
                    let line = std::mem::take(&mut self.line);
                    let line_start = self.line_start;
                    self.line_start = self.offset;
                    self.line(&line, line_start, emitted)?;
                    if self.progress == Progress::Finished {
                        return Ok(());
                    }
                }
                None => {
                    self.extend_line(rest)?;
                    self.offset += rest.len() as u64;
                    rest = &[];
                }
            }
        }
        Ok(())
    }

    fn extend_line(&mut self, bytes: &[u8]) -> Result<(), FrameError> {
        if bytes.is_empty() {
            return Ok(());
        }
        if self.event.start.is_none() {
            self.event.start = Some(self.line_start);
        }
        self.count(bytes.len())?;
        self.line.extend_from_slice(bytes);
        Ok(())
    }

    /// Count `bytes` against the event bound. Bytes between events (blank
    /// lines) count against nothing.
    fn count(&mut self, bytes: usize) -> Result<(), FrameError> {
        let Some(start) = self.event.start else {
            return Ok(());
        };
        self.event.bytes = self.event.bytes.saturating_add(bytes);
        if self.event.bytes > self.max_event.get() {
            return Err(FrameError::MalformedFrame { offset: start });
        }
        Ok(())
    }

    fn line(&mut self, line: &[u8], start: u64, emitted: &mut Emitted) -> Result<(), FrameError> {
        if line.is_empty() {
            let event = std::mem::take(&mut self.event);
            return self.dispatch(event, emitted);
        }
        if self.event.start.is_none() {
            self.event.start = Some(start);
        }
        if line[0] == b':' {
            return Ok(());
        }
        let (field, value) = match line.iter().position(|&byte| byte == b':') {
            Some(colon) => {
                let value = &line[colon + 1..];
                (&line[..colon], value.strip_prefix(b" ").unwrap_or(value))
            }
            None => (line, &[][..]),
        };
        match field {
            b"event" => {
                let start = self.event.start.unwrap_or(start);
                let kind = std::str::from_utf8(value)
                    .map_err(|_| FrameError::MalformedFrame { offset: start })?;
                self.event.kind = Some(kind.to_owned());
            }
            b"data" => match &mut self.event.data {
                Some(data) => {
                    data.push(b'\n');
                    data.extend_from_slice(value);
                }
                None => self.event.data = Some(value.to_vec()),
            },
            _ => {}
        }
        Ok(())
    }

    fn dispatch(&mut self, event: Event, emitted: &mut Emitted) -> Result<(), FrameError> {
        let (Some(start), Some(data)) = (event.start, event.data) else {
            return Ok(());
        };
        let malformed = FrameError::MalformedFrame { offset: start };
        match event.kind.as_deref().unwrap_or("message") {
            "ping" => Ok(()),
            "error" => {
                let value: serde_json::Value =
                    serde_json::from_slice(&data).map_err(|_| malformed)?;
                let error = value.get("error");
                let message = error
                    .and_then(|error| error.get("message").or_else(|| error.get("type")))
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or_default()
                    .to_owned();
                Err(FrameError::UpstreamErrorEvent { message })
            }
            kind @ ("message_start"
            | "content_block_start"
            | "content_block_delta"
            | "content_block_stop"
            | "message_delta"
            | "message_stop") => {
                serde_json::from_slice::<IgnoredAny>(&data).map_err(|_| malformed)?;
                if self.progress == Progress::NotStarted {
                    self.progress = Progress::Started;
                    emitted.push(FrameEvent::FirstContent);
                }
                if kind == "message_stop" {
                    self.progress = Progress::Finished;
                    emitted.push(FrameEvent::Finished);
                }
                Ok(())
            }
            _ => Ok(()),
        }
    }
}
