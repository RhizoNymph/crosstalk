//! String unescapes: how tools serialize text into JSON, Python or
//! JavaScript string literals, and into YAML scalars. Each runs over the
//! whole text and yields it once when at least one escape was undone.
//!
//! These are decoders, not normalization: a match that needed one is
//! reported through its decode chain. The spec names them once it gains
//! `Codec::JsonString` and `Codec::YamlString` (see [`super::Step::codec`]).

use crosstalk_spec::support::ByteRange;

use super::{DecodedText, Step, TextDecoder};
use crate::text::{MappedBuilder, MappedText};

/// JSON string escapes, plus the Python and JavaScript ones tool output
/// carries (`\'`, `\xNN`): `\"`, `\\`, `\/`, `\b`, `\f`, `\n`, `\r`, `\t`,
/// `\uXXXX` (surrogate pairs joined; a lone surrogate stays escaped). Any
/// other backslash stays as it is.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct JsonStringDecoder;

/// YAML scalar escapes: `''` (a quote inside a single-quoted scalar), an
/// escaped line break (`\` at the end of a line in a double-quoted scalar,
/// which joins the lines without a space) and an escaped space (`\ `, how
/// a dumper keeps the space that starts a continuation line). Line folding
/// is whitespace, which the fingerprinter folds already; the other
/// double-quoted escapes are JSON's.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct YamlStringDecoder;

fn hex_value(digits: &[u8]) -> Option<u32> {
    let text = std::str::from_utf8(digits).ok()?;
    if !text.bytes().all(|b| b.is_ascii_hexdigit()) {
        return None;
    }
    u32::from_str_radix(text, 16).ok()
}

/// `\uXXXX` at `index` (pointing at the backslash): the code unit.
fn unicode_unit(bytes: &[u8], index: usize) -> Option<u32> {
    if bytes.get(index) != Some(&b'\\') || bytes.get(index + 1) != Some(&b'u') {
        return None;
    }
    hex_value(bytes.get(index + 2..index + 6)?)
}

/// The escape at `index` (a backslash): the decoded character and the
/// escape's length in bytes.
fn json_escape(bytes: &[u8], index: usize) -> Option<(char, usize)> {
    let simple = match *bytes.get(index + 1)? {
        b'"' => Some('"'),
        b'\\' => Some('\\'),
        b'/' => Some('/'),
        b'\'' => Some('\''),
        b'b' => Some('\u{8}'),
        b'f' => Some('\u{c}'),
        b'n' => Some('\n'),
        b'r' => Some('\r'),
        b't' => Some('\t'),
        _ => None,
    };
    if let Some(ch) = simple {
        return Some((ch, 2));
    }
    match *bytes.get(index + 1)? {
        b'x' => {
            let value = hex_value(bytes.get(index + 2..index + 4)?)?;
            Some((char::from_u32(value)?, 4))
        }
        b'u' => {
            let unit = unicode_unit(bytes, index)?;
            if (0xD800..0xDC00).contains(&unit) {
                let low = unicode_unit(bytes, index + 6)?;
                if !(0xDC00..0xE000).contains(&low) {
                    return None;
                }
                let value = 0x10000 + ((unit - 0xD800) << 10) + (low - 0xDC00);
                Some((char::from_u32(value)?, 12))
            } else {
                Some((char::from_u32(unit)?, 6))
            }
        }
        _ => None,
    }
}

/// Walk `text`, letting `escape` decode at each backslash or quote; the
/// result when anything was decoded.
fn unescape(
    text: &str,
    escape: impl Fn(&[u8], usize) -> Option<(Option<char>, usize)>,
) -> Option<MappedText> {
    let end = u32::try_from(text.len()).ok()?;
    let bytes = text.as_bytes();
    let mut builder = MappedBuilder::new();
    let mut changed = false;
    let mut index = 0;
    while index < bytes.len() {
        let source = u32::try_from(index).ok()?;
        if let Some((decoded, length)) = escape(bytes, index) {
            if let Some(ch) = decoded {
                builder.push(ch, source);
            }
            changed = true;
            index += length;
            continue;
        }
        let ch = text[index..].chars().next()?;
        builder.push(ch, source);
        index += ch.len_utf8();
    }
    changed.then(|| builder.finish(end))
}

fn whole(text: &str, decoded: Option<MappedText>) -> Vec<DecodedText> {
    let Some(decoded) = decoded else {
        return Vec::new();
    };
    if decoded.text() == text {
        return Vec::new();
    }
    let Some(source) = u32::try_from(text.len())
        .ok()
        .and_then(|end| ByteRange::new(0, end).ok())
    else {
        return Vec::new();
    };
    vec![DecodedText {
        source,
        text: decoded,
    }]
}

impl TextDecoder for JsonStringDecoder {
    fn step(&self) -> Step {
        Step::JsonString
    }

    fn decode_mapped(&self, text: &str) -> Vec<DecodedText> {
        let decoded = unescape(text, |bytes, index| {
            if bytes[index] != b'\\' {
                return None;
            }
            json_escape(bytes, index).map(|(ch, length)| (Some(ch), length))
        });
        whole(text, decoded)
    }
}

impl TextDecoder for YamlStringDecoder {
    fn step(&self) -> Step {
        Step::YamlString
    }

    fn decode_mapped(&self, text: &str) -> Vec<DecodedText> {
        let decoded = unescape(text, |bytes, index| match bytes[index] {
            b'\'' if bytes.get(index + 1) == Some(&b'\'') => Some((Some('\''), 2)),
            b'\\' if bytes.get(index + 1) == Some(&b' ') => Some((Some(' '), 2)),
            b'\\' => {
                let mut next = index + 1;
                if bytes.get(next) == Some(&b'\r') {
                    next += 1;
                }
                if bytes.get(next) != Some(&b'\n') {
                    return None;
                }
                next += 1;
                while matches!(bytes.get(next), Some(b' ' | b'\t')) {
                    next += 1;
                }
                Some((None, next - index))
            }
            _ => None,
        });
        whole(text, decoded)
    }
}
