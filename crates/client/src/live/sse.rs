//! An incremental Server-Sent Events parser, as the WHATWG HTML standard
//! reads an event stream: lines end at `\r\n`, `\n` or `\r` (a pair split
//! across chunks included); a leading byte-order mark is dropped; a line
//! starting with `:` is a comment; `field: value` loses one space after
//! the colon; `data` lines join with `\n`; a blank line dispatches the
//! event, unless it had no `data`. Unknown fields and `retry` are ignored.
//!
//! Unlike a browser's `EventSource`, each event keeps the `id` field it
//! carried itself (`None` when it had none) rather than the last one seen,
//! so the client can check that the stream's `end` event has no id and
//! every item's id is its cursor.

use std::collections::VecDeque;

/// One dispatched event.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SseEvent {
    /// The `event` field, `message` when there was none.
    pub(crate) name: String,
    /// The event's own `id` field.
    pub(crate) id: Option<String>,
    /// The `data` lines joined with `\n`.
    pub(crate) data: String,
}

/// Why the bytes are not an event stream the client will read.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub(crate) enum SseError {
    /// A line, or one event's data, over the limit.
    #[error("an event stream line or event over {limit} bytes")]
    LineTooLong { limit: usize },
    #[error("an event stream line that is not UTF-8")]
    NotUtf8,
}

#[derive(Debug)]
pub(crate) struct SseParser {
    line: Vec<u8>,
    /// The last byte fed was `\r`: a `\n` next belongs to the same line end.
    after_cr: bool,
    /// No line has ended yet: a byte-order mark may still lead.
    first_line: bool,
    event: Option<String>,
    id: Option<String>,
    /// Each `data` line followed by `\n`, as the standard buffers them.
    data: String,
    ready: VecDeque<SseEvent>,
    limit: usize,
}

impl SseParser {
    pub(crate) fn new(limit: usize) -> Self {
        Self {
            line: Vec::new(),
            after_cr: false,
            first_line: true,
            event: None,
            id: None,
            data: String::new(),
            ready: VecDeque::new(),
            limit,
        }
    }

    /// Reads `bytes`, dispatching every event a blank line completes.
    pub(crate) fn feed(&mut self, bytes: &[u8]) -> Result<(), SseError> {
        for &byte in bytes {
            if std::mem::take(&mut self.after_cr) && byte == b'\n' {
                continue;
            }
            match byte {
                b'\r' => {
                    self.after_cr = true;
                    self.end_line()?;
                }
                b'\n' => self.end_line()?,
                _ => {
                    if self.line.len() >= self.limit {
                        return Err(SseError::LineTooLong { limit: self.limit });
                    }
                    self.line.push(byte);
                }
            }
        }
        Ok(())
    }

    /// The next dispatched event, oldest first.
    pub(crate) fn next_event(&mut self) -> Option<SseEvent> {
        self.ready.pop_front()
    }

    fn end_line(&mut self) -> Result<(), SseError> {
        let bytes = std::mem::take(&mut self.line);
        let mut line = String::from_utf8(bytes).map_err(|_| SseError::NotUtf8)?;
        if std::mem::take(&mut self.first_line) && line.starts_with('\u{feff}') {
            line.drain(..'\u{feff}'.len_utf8());
        }
        if line.is_empty() {
            self.dispatch();
            return Ok(());
        }
        if line.starts_with(':') {
            return Ok(());
        }
        let (field, value) = match line.split_once(':') {
            Some((field, value)) => (field, value.strip_prefix(' ').unwrap_or(value)),
            None => (line.as_str(), ""),
        };
        match field {
            "event" => self.event = Some(value.to_owned()),
            "data" => {
                if self.data.len() + value.len() >= self.limit {
                    return Err(SseError::LineTooLong { limit: self.limit });
                }
                self.data.push_str(value);
                self.data.push('\n');
            }
            "id" if !value.contains('\0') => self.id = Some(value.to_owned()),
            _ => {}
        }
        Ok(())
    }

    fn dispatch(&mut self) {
        let event = self.event.take();
        let id = self.id.take();
        let mut data = std::mem::take(&mut self.data);
        // An event with no `data` line is not dispatched; otherwise the
        // buffer's last `\n` is dropped.
        if data.pop().is_some() {
            self.ready.push_back(SseEvent {
                name: event.unwrap_or_else(|| "message".to_owned()),
                id,
                data,
            });
        }
    }
}
