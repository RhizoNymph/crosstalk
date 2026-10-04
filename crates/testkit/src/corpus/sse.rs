//! Server-sent events, kept byte-exact.
//!
//! A stream is split into frames, each the bytes of one event up to and
//! including the blank line that ends it, so the frames concatenate back to
//! the stream exactly. The fake upstream sends one frame per chunk, and
//! tests compare what arrived with what was recorded byte for byte.
//!
//! Lines end with LF or CRLF (the Anthropic API writes LF). Fields follow
//! the WHATWG event-stream rules: `event`, `data` (several lines join with
//! LF), `id` and `retry`; a line starting with `:` is a comment; one space
//! after the colon is dropped; unknown fields are ignored.

use bytes::Bytes;

/// One event and the exact bytes it was framed in.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SseEvent {
    /// The frame: every byte of the event including its terminating blank
    /// line.
    pub raw: Bytes,
    /// The `event:` field; `None` means the default type, `message`.
    pub event: Option<String>,
    /// The `data:` lines, joined with LF; `None` when the frame has none,
    /// which per the WHATWG rules dispatches no event (a keep-alive comment).
    pub data: Option<String>,
    pub id: Option<String>,
    pub retry: Option<u64>,
    /// The frame's comment lines (`:` lines), without the colon.
    pub comments: Vec<String>,
}

impl SseEvent {
    /// The event's type: its `event:` field, or `message`.
    pub fn kind(&self) -> &str {
        self.event.as_deref().unwrap_or("message")
    }

    /// Whether a client dispatches this frame as an event: it has data.
    pub fn dispatches(&self) -> bool {
        self.data.is_some()
    }

    /// The data as JSON (an empty frame's data is the empty string).
    pub fn json(&self) -> Result<serde_json::Value, serde_json::Error> {
        serde_json::from_str(self.data.as_deref().unwrap_or_default())
    }
}

/// A whole event stream: its events and any bytes after the last complete
/// frame (a stream cut mid-event).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventStream {
    raw: Bytes,
    events: Vec<SseEvent>,
    trailing: Bytes,
}

/// Why bytes are not an event stream.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SseError {
    #[error("the event stream is not UTF-8 at byte {offset}")]
    NotUtf8 { offset: usize },
}

impl EventStream {
    /// Split `raw` into frames and parse each.
    pub fn parse(raw: Bytes) -> Result<Self, SseError> {
        let (frames, trailing) = split_frames(&raw);
        let mut events = Vec::with_capacity(frames.len());
        for (start, end) in frames {
            events.push(parse_frame(raw.slice(start..end), start)?);
        }
        let trailing = raw.slice(trailing..);
        Ok(Self {
            raw,
            events,
            trailing,
        })
    }

    /// The stream's bytes, exactly as recorded.
    pub fn raw(&self) -> &Bytes {
        &self.raw
    }

    /// Every frame, comment-only ones included, in order.
    pub fn events(&self) -> &[SseEvent] {
        &self.events
    }

    /// The frames a client dispatches as events.
    pub fn dispatched(&self) -> impl Iterator<Item = &SseEvent> {
        self.events.iter().filter(|event| event.dispatches())
    }

    /// Bytes after the last complete frame; empty for a well-ended stream.
    pub fn trailing(&self) -> &Bytes {
        &self.trailing
    }

    /// The frames, then the trailing bytes if any: chunks that concatenate
    /// to [`EventStream::raw`].
    pub fn chunks(&self) -> Vec<Bytes> {
        let mut chunks: Vec<Bytes> = self.events.iter().map(|event| event.raw.clone()).collect();
        if !self.trailing.is_empty() {
            chunks.push(self.trailing.clone());
        }
        chunks
    }
}

/// Frame boundaries as `(start, end)` byte offsets, and where the trailing
/// partial frame starts. A frame ends after an empty line: LF LF, LF CRLF,
/// CRLF LF or CRLF CRLF.
fn split_frames(raw: &[u8]) -> (Vec<(usize, usize)>, usize) {
    let mut frames = Vec::new();
    let mut start = 0;
    let mut line_start = 0;
    let mut has_fields = false;
    for (index, byte) in raw.iter().enumerate() {
        if *byte != b'\n' {
            continue;
        }
        let line = &raw[line_start..index];
        line_start = index + 1;
        if line.is_empty() || line == b"\r" {
            // A blank line ends the frame if it holds anything; blank lines
            // before any field belong to the frame that follows.
            if has_fields {
                frames.push((start, line_start));
                start = line_start;
                has_fields = false;
            }
        } else {
            has_fields = true;
        }
    }
    (frames, start)
}

/// Parse one frame's fields. `offset` is where the frame starts in the
/// stream, for error positions.
fn parse_frame(raw: Bytes, offset: usize) -> Result<SseEvent, SseError> {
    let text = std::str::from_utf8(&raw).map_err(|error| SseError::NotUtf8 {
        offset: offset + error.valid_up_to(),
    })?;
    let mut event = None;
    let mut data: Option<String> = None;
    let mut id = None;
    let mut retry = None;
    let mut comments = Vec::new();
    for line in text.split('\n') {
        let line = line.strip_suffix('\r').unwrap_or(line);
        if let Some(comment) = line.strip_prefix(':') {
            comments.push(comment.strip_prefix(' ').unwrap_or(comment).to_owned());
            continue;
        }
        if line.is_empty() {
            continue;
        }
        let (field, value) = match line.split_once(':') {
            Some((field, value)) => (field, value.strip_prefix(' ').unwrap_or(value)),
            None => (line, ""),
        };
        match field {
            "event" => event = Some(value.to_owned()),
            "data" => match &mut data {
                Some(joined) => {
                    joined.push('\n');
                    joined.push_str(value);
                }
                None => data = Some(value.to_owned()),
            },
            "id" => id = Some(value.to_owned()),
            "retry" => retry = value.parse().ok(),
            _ => {}
        }
    }
    Ok(SseEvent {
        raw,
        event,
        data,
        id,
        retry,
        comments,
    })
}
