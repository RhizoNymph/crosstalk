//! URL (percent) encoding: every whitespace-delimited token holding at
//! least one `%XX` escape is decoded (`%XX` to its byte, `+` to a space, as
//! form encoding writes it); a token whose decoding is not valid UTF-8 is
//! kept as it is. The whole text is one decoded payload.

use crosstalk_spec::derived::provenance::matching::Codec;
use crosstalk_spec::support::ByteRange;

use super::{DecodedText, Step, TextDecoder};
use crate::text::MappedBuilder;

/// The percent-decoding decoder.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct UrlDecoder;

fn hex(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

fn escape_at(bytes: &[u8], index: usize) -> Option<u8> {
    if bytes.get(index) != Some(&b'%') {
        return None;
    }
    let high = hex(*bytes.get(index + 1)?)?;
    let low = hex(*bytes.get(index + 2)?)?;
    Some(high << 4 | low)
}

/// The token `bytes[start..end]` decoded, with each byte's source, when it
/// holds an escape and decodes to valid UTF-8.
fn decode_token(bytes: &[u8], start: usize, end: usize) -> Option<(Vec<u8>, Vec<usize>)> {
    let token = &bytes[start..end];
    if !(0..token.len()).any(|i| escape_at(token, i).is_some()) {
        return None;
    }
    let mut out = Vec::with_capacity(token.len());
    let mut sources = Vec::with_capacity(token.len());
    let mut index = start;
    while index < end {
        if let Some(byte) = escape_at(&bytes[..end], index) {
            out.push(byte);
            sources.push(index);
            index += 3;
        } else if bytes[index] == b'+' {
            out.push(b' ');
            sources.push(index);
            index += 1;
        } else {
            out.push(bytes[index]);
            sources.push(index);
            index += 1;
        }
    }
    std::str::from_utf8(&out).ok()?;
    Some((out, sources))
}

impl TextDecoder for UrlDecoder {
    fn step(&self) -> Step {
        Step::Codec(Codec::UrlEncoding)
    }

    fn decode_mapped(&self, text: &str) -> Vec<DecodedText> {
        let bytes = text.as_bytes();
        let mut out = Vec::with_capacity(bytes.len());
        let mut sources = Vec::with_capacity(bytes.len());
        let mut changed = false;
        let mut index = 0;
        while index < bytes.len() {
            if bytes[index].is_ascii_whitespace() {
                out.push(bytes[index]);
                sources.push(index);
                index += 1;
                continue;
            }
            let start = index;
            while index < bytes.len() && !bytes[index].is_ascii_whitespace() {
                index += 1;
            }
            match decode_token(bytes, start, index) {
                Some((decoded, token_sources)) => {
                    changed = true;
                    out.extend(decoded);
                    sources.extend(token_sources);
                }
                None => {
                    out.extend_from_slice(&bytes[start..index]);
                    sources.extend(start..index);
                }
            }
        }
        if !changed || out == bytes {
            return Vec::new();
        }
        let Ok(end) = u32::try_from(bytes.len()) else {
            return Vec::new();
        };
        let sources: Vec<u32> = sources
            .into_iter()
            .map(|s| u32::try_from(s).unwrap_or(end))
            .collect();
        let Some(text) = MappedBuilder::from_utf8(out, &sources, end) else {
            return Vec::new();
        };
        let Ok(source) = ByteRange::new(0, end) else {
            return Vec::new();
        };
        vec![DecodedText { source, text }]
    }
}
