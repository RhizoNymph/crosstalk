//! Opaque blobs: provider signatures and encrypted payloads that look like
//! text but carry nothing an agent wrote.
//!
//! They are excluded from spans and from decoding: a thought signature
//! shared between two transcripts (or base64 that decodes to noise) must not
//! become a match. Recognised:
//!
//! - tool-call ids that embed a thought signature, as Gemini's do
//!   (`call_123__thought__<base64>`): the whole id;
//! - the string value (or array of strings) of JSON members named
//!   `thought_signature`, `thought_signatures`, `signature`,
//!   `encrypted_content` or `redacted_thinking`, as relayed logs carry them.
//!
//! Text parts never hold a canonical `Reasoning::Opaque` (it has no part
//! text), so these rules only matter where such blobs leak into text.

const THOUGHT_MARKER: &str = "__thought__";

const OPAQUE_KEYS: &[&str] = &[
    "thought_signature",
    "thought_signatures",
    "signature",
    "encrypted_content",
    "redacted_thinking",
];

fn is_base64_char(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || matches!(byte, b'+' | b'/' | b'=' | b'-' | b'_')
}

fn is_id_char(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'-'
}

/// The raw byte ranges of `text` that are opaque, sorted and disjoint.
pub fn opaque_ranges(text: &str) -> Vec<(usize, usize)> {
    let bytes = text.as_bytes();
    let mut ranges = Vec::new();
    let mut from = 0;
    while let Some(found) = text[from..].find(THOUGHT_MARKER) {
        let marker = from + found;
        let mut start = marker;
        while start > 0 && is_id_char(bytes[start - 1]) {
            start -= 1;
        }
        let mut end = marker + THOUGHT_MARKER.len();
        while end < bytes.len() && is_base64_char(bytes[end]) {
            end += 1;
        }
        ranges.push((start, end));
        from = end.max(marker + 1);
    }
    for key in OPAQUE_KEYS {
        let mut from = 0;
        while let Some(found) = text[from..].find(key) {
            let start = from + found;
            let mut after_key = start + key.len();
            from = after_key;
            // A member name: a quote right before, then (escaped) a quote.
            if start == 0 || bytes[start - 1] != b'"' {
                continue;
            }
            while after_key < bytes.len() && bytes[after_key] == b'\\' {
                after_key += 1;
            }
            if bytes.get(after_key) != Some(&b'"') {
                continue;
            }
            if let Some((value_start, value_end)) = member_value(bytes, after_key + 1) {
                ranges.push((value_start, value_end));
                from = value_end;
            }
        }
    }
    merge(ranges)
}

/// The range of a JSON string (or array of strings) value after a member
/// name ending at `at`: `: "…"` or `: ["…", …]`, escapes honoured. Relayed
/// logs escape the quotes themselves (`\"key\": \"…\"`), which the scan
/// treats the same way.
fn member_value(bytes: &[u8], at: usize) -> Option<(usize, usize)> {
    let mut pos = at;
    while pos < bytes.len() && (bytes[pos] == b'\\' || bytes[pos].is_ascii_whitespace()) {
        pos += 1;
    }
    if bytes.get(pos) != Some(&b':') {
        return None;
    }
    pos += 1;
    while pos < bytes.len() && (bytes[pos] == b'\\' || bytes[pos].is_ascii_whitespace()) {
        pos += 1;
    }
    match bytes.get(pos)? {
        b'"' => {
            let end = string_end(bytes, pos + 1)?;
            Some((pos, end))
        }
        b'[' => {
            let close = bytes[pos..].iter().position(|&b| b == b']')? + pos + 1;
            Some((pos, close))
        }
        _ => None,
    }
}

/// The index after the quote closing a string whose contents start at
/// `start`. A quote preceded by backslashes closes it unless they escape
/// it; a value inside an escaped log ends at the first `\"` instead.
fn string_end(bytes: &[u8], start: usize) -> Option<usize> {
    let escaped_context = start >= 2 && bytes[start - 2] == b'\\';
    let mut pos = start;
    while pos < bytes.len() {
        if bytes[pos] == b'"' {
            let mut slashes = 0;
            while pos > slashes && bytes[pos - 1 - slashes] == b'\\' {
                slashes += 1;
            }
            let closes = if escaped_context {
                slashes % 2 == 1
            } else {
                slashes % 2 == 0
            };
            if closes {
                return Some(pos + 1);
            }
        }
        pos += 1;
    }
    None
}

fn merge(mut ranges: Vec<(usize, usize)>) -> Vec<(usize, usize)> {
    ranges.sort_unstable();
    let mut merged: Vec<(usize, usize)> = Vec::with_capacity(ranges.len());
    for (start, end) in ranges {
        match merged.last_mut() {
            Some(last) if start <= last.1 => last.1 = last.1.max(end),
            _ => merged.push((start, end)),
        }
    }
    merged
}

/// The pieces of `text` between its opaque ranges, with their offsets.
pub fn segments(text: &str) -> Vec<(usize, &str)> {
    let mut pieces = Vec::new();
    let mut at = 0;
    for (start, end) in opaque_ranges(text) {
        if start > at
            && let Some(piece) = text.get(at..start)
        {
            pieces.push((at, piece));
        }
        at = at.max(end);
    }
    if at < text.len()
        && let Some(piece) = text.get(at..)
    {
        pieces.push((at, piece));
    }
    pieces
}
