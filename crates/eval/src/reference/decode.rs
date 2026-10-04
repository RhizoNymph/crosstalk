//! Decoding candidate tokens: base64, hex and URL encoding.
//!
//! A candidate is a run of the codec's alphabet long enough to carry a
//! span. It counts only when it decodes to UTF-8 text that is mostly
//! printable, so ordinary words and ids that happen to fit an alphabet
//! (and decode to noise) are dropped.

use base64::Engine;
use base64::engine::general_purpose::{STANDARD, STANDARD_NO_PAD, URL_SAFE, URL_SAFE_NO_PAD};

use crosstalk_spec::derived::provenance::matching::Codec;

/// One decoded token: its raw range in the text and what it decoded to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Decoded {
    pub start: usize,
    pub end: usize,
    pub codec: Codec,
    pub text: String,
}

/// Every candidate token of `text` (offset by `base`) that decodes, for
/// tokens at least `min_len` bytes long.
pub fn decode_candidates(text: &str, base: usize, min_len: usize) -> Vec<Decoded> {
    let mut out = Vec::new();
    for (start, end) in runs(text, |b| b.is_ascii_hexdigit()) {
        let token = &text[start..end];
        if token.len() >= min_len
            && token.len().is_multiple_of(2)
            && let Some(decoded) = hex(token).and_then(printable)
        {
            out.push(Decoded {
                start: base + start,
                end: base + end,
                codec: Codec::Hex,
                text: decoded,
            });
            continue;
        }
    }
    for (start, end) in runs(text, |b| {
        b.is_ascii_alphanumeric() || matches!(b, b'+' | b'/' | b'=' | b'-' | b'_')
    }) {
        let token = &text[start..end];
        if token.len() < min_len || token.bytes().all(|b| b.is_ascii_hexdigit()) {
            continue;
        }
        if let Some(decoded) = base64(token).and_then(printable) {
            out.push(Decoded {
                start: base + start,
                end: base + end,
                codec: Codec::Base64,
                text: decoded,
            });
        }
    }
    for (start, end) in runs(text, |b| !b.is_ascii_whitespace() && b != b'"') {
        let token = &text[start..end];
        if token.len() >= min_len
            && token.matches('%').count() >= 3
            && let Some(decoded) = url(token).and_then(printable)
        {
            out.push(Decoded {
                start: base + start,
                end: base + end,
                codec: Codec::UrlEncoding,
                text: decoded,
            });
        }
    }
    out.sort_by_key(|decoded| (decoded.start, decoded.end));
    out
}

/// Maximal runs of bytes satisfying `keep`, as byte ranges. Each run is
/// ASCII-only or ends at non-ASCII bytes, so the ranges fall on character
/// boundaries.
fn runs(text: &str, keep: impl Fn(u8) -> bool) -> Vec<(usize, usize)> {
    let bytes = text.as_bytes();
    let mut out = Vec::new();
    let mut start = None;
    for (at, &byte) in bytes.iter().enumerate() {
        let inside = byte.is_ascii() && keep(byte);
        match (inside, start) {
            (true, None) => start = Some(at),
            (false, Some(from)) => {
                out.push((from, at));
                start = None;
            }
            _ => {}
        }
    }
    if let Some(from) = start {
        out.push((from, bytes.len()));
    }
    out
}

fn hex(token: &str) -> Option<Vec<u8>> {
    token
        .as_bytes()
        .chunks(2)
        .map(|pair| {
            let high = char::from(pair[0]).to_digit(16)?;
            let low = char::from(*pair.get(1)?).to_digit(16)?;
            u8::try_from(high * 16 + low).ok()
        })
        .collect()
}

fn base64(token: &str) -> Option<Vec<u8>> {
    let engines = [&STANDARD, &STANDARD_NO_PAD, &URL_SAFE, &URL_SAFE_NO_PAD];
    engines.iter().find_map(|engine| engine.decode(token).ok())
}

fn url(token: &str) -> Option<Vec<u8>> {
    let bytes = token.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut at = 0;
    while at < bytes.len() {
        match bytes[at] {
            b'%' => {
                let high = char::from(*bytes.get(at + 1)?).to_digit(16)?;
                let low = char::from(*bytes.get(at + 2)?).to_digit(16)?;
                out.push(u8::try_from(high * 16 + low).ok()?);
                at += 3;
            }
            b'+' => {
                out.push(b' ');
                at += 1;
            }
            byte => {
                out.push(byte);
                at += 1;
            }
        }
    }
    Some(out)
}

/// The bytes as text when they are UTF-8 and at least 90% printable.
fn printable(bytes: Vec<u8>) -> Option<String> {
    let text = String::from_utf8(bytes).ok()?;
    let total = text.chars().count();
    if total == 0 {
        return None;
    }
    let printable = text
        .chars()
        .filter(|ch| !ch.is_control() || ch.is_whitespace())
        .count();
    (printable * 10 >= total * 9).then_some(text)
}
