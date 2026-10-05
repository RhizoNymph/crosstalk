//! Decoding one encoded token through a chain of codecs.
//!
//! A payload carries a token in one or more nested encodings (base64 inside
//! an `atob(...)`, hex, URL-encoding, `\x..` byte escapes). This decodes a
//! token layer by layer until no further codec applies, returning the chain
//! it peeled and the final text. The eval uses the chain to label a
//! Decoded-class transmission and to verify, by actually decoding, that the
//! chain holds.
//!
//! Only base64, hex and URL-encoding map to the spec's [`Codec`]; a `\x..`
//! escape layer is reported as [`ByteEscape`](Layer::ByteEscape), which the
//! reference matcher's decoder does not handle, so a chain that needs it is a
//! known miss.

use base64::Engine;
use base64::engine::general_purpose::{STANDARD, STANDARD_NO_PAD, URL_SAFE, URL_SAFE_NO_PAD};

use crosstalk_spec::derived::provenance::matching::Codec;

/// One peeled layer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Layer {
    Base64,
    Hex,
    Url,
    /// A `\xNN` byte-escape run; no spec `Codec` and not something the
    /// reference matcher decodes.
    ByteEscape,
}

impl Layer {
    /// The spec codec this layer corresponds to, if any.
    pub fn codec(self) -> Option<Codec> {
        match self {
            Self::Base64 => Some(Codec::Base64),
            Self::Hex => Some(Codec::Hex),
            Self::Url => Some(Codec::UrlEncoding),
            Self::ByteEscape => None,
        }
    }

    /// A short name for reports.
    pub fn name(self) -> &'static str {
        match self {
            Self::Base64 => "base64",
            Self::Hex => "hex",
            Self::Url => "url",
            Self::ByteEscape => "byte_escape",
        }
    }
}

/// A decoded token: the layers peeled (outermost first) and the final text.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Decoded {
    pub layers: Vec<Layer>,
    pub text: String,
}

impl Decoded {
    /// The chain as spec codecs; `None` when a layer has no spec codec.
    pub fn codecs(&self) -> Option<Vec<Codec>> {
        self.layers.iter().map(|layer| layer.codec()).collect()
    }

    /// The chain as a dotted name, for report keys (`base64.hex`).
    pub fn chain_name(&self) -> String {
        self.layers
            .iter()
            .map(|layer| layer.name())
            .collect::<Vec<_>>()
            .join(".")
    }
}

const MAX_DEPTH: usize = 6;

/// Decodes `token` through as many codec layers as apply, bounded in depth.
/// Returns `None` when `token` is not encoded at all.
pub fn decode(token: &str) -> Option<Decoded> {
    let mut layers = Vec::new();
    let mut current = token.trim().to_owned();
    for _ in 0..MAX_DEPTH {
        let Some((layer, next)) = peel(&current) else {
            break;
        };
        layers.push(layer);
        current = next.trim().to_owned();
    }
    if layers.is_empty() {
        None
    } else {
        Some(Decoded {
            layers,
            text: current,
        })
    }
}

/// Peels one layer off `text`, if exactly one codec applies to the whole
/// token and yields printable text.
fn peel(text: &str) -> Option<(Layer, String)> {
    if let Some(inner) = byte_escape(text) {
        return Some((Layer::ByteEscape, inner));
    }
    if is_url_encoded(text)
        && let Some(inner) = url(text)
    {
        return Some((Layer::Url, inner));
    }
    if is_hex(text)
        && let Some(inner) = hex(text)
    {
        return Some((Layer::Hex, inner));
    }
    if is_base64(text)
        && let Some(inner) = base64(text)
    {
        return Some((Layer::Base64, inner));
    }
    None
}

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

fn is_hex(token: &str) -> bool {
    token.len() >= 16
        && token.len().is_multiple_of(2)
        && token.bytes().all(|b| b.is_ascii_hexdigit())
}

fn hex(token: &str) -> Option<String> {
    let bytes: Option<Vec<u8>> = token
        .as_bytes()
        .chunks(2)
        .map(|pair| {
            let high = char::from(pair[0]).to_digit(16)?;
            let low = char::from(*pair.get(1)?).to_digit(16)?;
            u8::try_from(high * 16 + low).ok()
        })
        .collect();
    printable(bytes?)
}

fn is_base64(token: &str) -> bool {
    token.len() >= 24
        && token
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'+' | b'/' | b'=' | b'-' | b'_'))
        && !token.bytes().all(|b| b.is_ascii_hexdigit())
}

fn base64(token: &str) -> Option<String> {
    let engines = [&STANDARD, &STANDARD_NO_PAD, &URL_SAFE, &URL_SAFE_NO_PAD];
    engines
        .iter()
        .find_map(|engine| engine.decode(token).ok())
        .and_then(printable)
}

fn is_url_encoded(token: &str) -> bool {
    token.matches('%').count() >= 3 && token.bytes().all(|b| !b.is_ascii_whitespace() && b != b'"')
}

fn url(token: &str) -> Option<String> {
    let bytes = token.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut at = 0;
    let mut decoded_any = false;
    while at < bytes.len() {
        match bytes[at] {
            b'%' => {
                let high = char::from(*bytes.get(at + 1)?).to_digit(16)?;
                let low = char::from(*bytes.get(at + 2)?).to_digit(16)?;
                out.push(u8::try_from(high * 16 + low).ok()?);
                at += 3;
                decoded_any = true;
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
    decoded_any.then(|| printable(out)).flatten()
}

/// Decodes a run made entirely of `\xNN` escapes.
fn byte_escape(token: &str) -> Option<String> {
    if token.len() < 8 || !token.starts_with("\\x") {
        return None;
    }
    let mut bytes = Vec::new();
    let mut rest = token;
    while let Some(tail) = rest.strip_prefix("\\x") {
        if tail.len() < 2 {
            return None;
        }
        let (pair, next) = tail.split_at(2);
        let value = u8::from_str_radix(pair, 16).ok()?;
        bytes.push(value);
        rest = next;
    }
    if rest.is_empty() {
        printable(bytes)
    } else {
        None
    }
}
