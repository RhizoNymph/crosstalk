//! Server-sent events, parsed from a whole body as the WHATWG event-stream
//! format defines.
//!
//! Lines end with CRLF, LF or CR; a blank line dispatches the event built
//! since the previous one (when it has data); a line starting with `:` is a
//! comment; `event:` names the event and each `data:` line appends to its
//! data (joined with LF); one space after the colon is dropped; other
//! fields (`id`, `retry`, unknown ones) are ignored here. An event not
//! closed by a blank line before the body ends is never dispatched.

/// One dispatched event.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SseEvent {
    /// The `event:` field, if the event had one.
    pub event: Option<String>,
    /// The `data:` lines, joined with LF.
    pub data: String,
}

/// A parsed body: the events dispatched, in order, and whether the body
/// stopped being UTF-8 (the events are those of the valid prefix).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SseBody {
    pub events: Vec<SseEvent>,
    /// The offset of the first byte that is not UTF-8, if any.
    pub not_utf8_at: Option<usize>,
}

/// The events `body` holds.
pub fn parse(body: &[u8]) -> SseBody {
    let (text, not_utf8_at) = match std::str::from_utf8(body) {
        Ok(text) => (text, None),
        Err(error) => {
            let valid = error.valid_up_to();
            // `valid_up_to` is a char boundary by definition, so the prefix
            // is always UTF-8.
            let prefix = std::str::from_utf8(&body[..valid]).unwrap_or_default();
            (prefix, Some(valid))
        }
    };
    let text = text.strip_prefix('\u{feff}').unwrap_or(text);
    let mut events = Vec::new();
    let mut event: Option<String> = None;
    let mut data = String::new();
    let mut has_data = false;
    let mut rest = text;
    while let Some((line, after)) = next_line(rest) {
        rest = after;
        if line.is_empty() {
            if has_data {
                events.push(SseEvent {
                    event: event.take(),
                    data: std::mem::take(&mut data),
                });
            }
            event = None;
            data.clear();
            has_data = false;
            continue;
        }
        if line.starts_with(':') {
            continue;
        }
        let (field, value) = match line.split_once(':') {
            Some((field, value)) => (field, value.strip_prefix(' ').unwrap_or(value)),
            None => (line, ""),
        };
        match field {
            "event" => event = Some(value.to_owned()),
            "data" => {
                if has_data {
                    data.push('\n');
                }
                data.push_str(value);
                has_data = true;
            }
            _ => {}
        }
    }
    SseBody {
        events,
        not_utf8_at,
    }
}

/// The next complete line of `text` and the text after its terminator;
/// `None` when no terminator is left (an unterminated line is never part of
/// a dispatched event).
fn next_line(text: &str) -> Option<(&str, &str)> {
    let end = text.find(['\n', '\r'])?;
    let line = &text[..end];
    let after = &text[end..];
    // `after` starts with the ASCII terminator found above.
    let after = after
        .strip_prefix("\r\n")
        .unwrap_or_else(|| after.get(1..).unwrap_or_default());
    Some((line, after))
}
