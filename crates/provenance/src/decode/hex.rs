//! Hex: maximal runs of hex digits of even length, at least the configured
//! length, decoded as one payload.

use crosstalk_spec::derived::provenance::matching::Codec;
use crosstalk_spec::support::ByteRange;

use super::{DecodedText, Step, TextDecoder};
use crate::text::MappedBuilder;

/// The hex decoder.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HexDecoder {
    min_run: usize,
}

impl HexDecoder {
    /// Runs shorter than `min_run` digits are left alone.
    pub fn new(min_run: usize) -> Self {
        Self { min_run }
    }
}

fn nibble(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

impl TextDecoder for HexDecoder {
    fn step(&self) -> Step {
        Step::Codec(Codec::Hex)
    }

    fn decode_mapped(&self, text: &str) -> Vec<DecodedText> {
        let bytes = text.as_bytes();
        let mut out = Vec::new();
        let mut index = 0;
        while index < bytes.len() {
            if nibble(bytes[index]).is_none() {
                index += 1;
                continue;
            }
            let start = index;
            while index < bytes.len() && nibble(bytes[index]).is_some() {
                index += 1;
            }
            // An odd run keeps its first digits, dropping the last one.
            let end = start + (index - start) / 2 * 2;
            if end - start >= self.min_run
                && let Some(decoded) = decode_run(bytes, start, end)
            {
                out.push(decoded);
            }
        }
        out
    }
}

fn decode_run(bytes: &[u8], start: usize, end: usize) -> Option<DecodedText> {
    let mut decoded = Vec::with_capacity((end - start) / 2);
    let mut sources = Vec::with_capacity((end - start) / 2);
    let mut index = start;
    while index + 1 < end {
        let high = nibble(bytes[index])?;
        let low = nibble(bytes[index + 1])?;
        decoded.push(high << 4 | low);
        sources.push(u32::try_from(index).ok()?);
        index += 2;
    }
    let source_end = u32::try_from(end).ok()?;
    let text = MappedBuilder::from_utf8(decoded, &sources, source_end)?;
    if text.text().is_empty() {
        return None;
    }
    let source = ByteRange::new(u32::try_from(start).ok()?, source_end).ok()?;
    Some(DecodedText { source, text })
}
