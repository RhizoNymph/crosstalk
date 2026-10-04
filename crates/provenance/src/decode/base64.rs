//! Base64: maximal runs of the standard or URL-safe alphabet, at least the
//! configured length, with optional `=` padding, decoded as one payload.

use base64::Engine as _;
use base64::alphabet;
use base64::engine::{DecodePaddingMode, GeneralPurpose, GeneralPurposeConfig};
use crosstalk_spec::derived::provenance::matching::Codec;
use crosstalk_spec::support::ByteRange;

use super::{DecodedText, Step, TextDecoder};
use crate::text::MappedBuilder;

const fn config() -> GeneralPurposeConfig {
    GeneralPurposeConfig::new()
        .with_decode_padding_mode(DecodePaddingMode::Indifferent)
        .with_decode_allow_trailing_bits(true)
}

const STANDARD: GeneralPurpose = GeneralPurpose::new(&alphabet::STANDARD, config());
const URL_SAFE: GeneralPurpose = GeneralPurpose::new(&alphabet::URL_SAFE, config());

/// The base64 decoder.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Base64Decoder {
    min_run: usize,
}

impl Base64Decoder {
    /// Runs shorter than `min_run` characters (padding excluded) are left
    /// alone: short words are valid base64 too.
    pub fn new(min_run: usize) -> Self {
        Self { min_run }
    }
}

fn standard_only(byte: u8) -> bool {
    byte == b'+' || byte == b'/'
}

fn url_only(byte: u8) -> bool {
    byte == b'-' || byte == b'_'
}

fn in_alphabet(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || standard_only(byte) || url_only(byte)
}

impl TextDecoder for Base64Decoder {
    fn step(&self) -> Step {
        Step::Codec(Codec::Base64)
    }

    fn decode_mapped(&self, text: &str) -> Vec<DecodedText> {
        let bytes = text.as_bytes();
        let mut out = Vec::new();
        let mut index = 0;
        while index < bytes.len() {
            if !in_alphabet(bytes[index]) {
                index += 1;
                continue;
            }
            let start = index;
            while index < bytes.len() && in_alphabet(bytes[index]) {
                index += 1;
            }
            let body_end = index;
            while index < bytes.len() && bytes[index] == b'=' && index - body_end < 2 {
                index += 1;
            }
            if let Some(decoded) = decode_run(bytes, start, body_end, index, self.min_run) {
                out.push(decoded);
            }
        }
        out
    }
}

fn decode_run(
    bytes: &[u8],
    start: usize,
    body_end: usize,
    end: usize,
    min_run: usize,
) -> Option<DecodedText> {
    let body = &bytes[start..body_end];
    if body.len() < min_run || body.len() % 4 == 1 {
        return None;
    }
    let standard = body.iter().copied().any(standard_only);
    let url = body.iter().copied().any(url_only);
    let engine = match (standard, url) {
        (true, true) => return None,
        (false, true) => &URL_SAFE,
        _ => &STANDARD,
    };
    let decoded = engine.decode(body).ok()?;
    if decoded.is_empty() {
        return None;
    }
    let source_start = u32::try_from(start).ok()?;
    let source_end = u32::try_from(end).ok()?;
    // Decoded byte i comes from the quantum of four characters at 4 * (i / 3).
    let sources: Vec<u32> = (0..decoded.len())
        .map(|i| source_start + u32::try_from(4 * (i / 3)).unwrap_or(u32::MAX))
        .map(|offset| offset.min(source_end))
        .collect();
    let text = MappedBuilder::from_utf8(decoded, &sources, source_end)?;
    if text.text().as_bytes() == &bytes[start..end] {
        return None;
    }
    let source = ByteRange::new(source_start, source_end).ok()?;
    Some(DecodedText { source, text })
}
