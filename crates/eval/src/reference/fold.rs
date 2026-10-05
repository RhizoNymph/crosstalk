//! Folding: what the reference matcher compares text under.
//!
//! [`fold`] is the matching fold. In one pass over the raw text:
//!
//! 1. **String escapes are unfolded**, whatever their nesting depth: a run of
//!    backslashes before `n`, `t`, `r`, `b` or `f` becomes whitespace, before
//!    `uXXXX` the character it names, before a line break nothing (a YAML
//!    line continuation, which also drops the next line's indentation), and
//!    before anything else is dropped (`\"` is `"`, `\\` is `\`). Content one
//!    agent wrote inside JSON tool arguments therefore folds to the same text
//!    as the same content delivered raw, escaped twice in a relayed log, or
//!    wrapped in YAML.
//! 2. **Case is folded** (`char::to_lowercase`).
//! 3. **Whitespace is collapsed**: every run becomes one space; leading
//!    whitespace is dropped.
//!
//! Each folded byte remembers the raw byte range it came from, so a folded
//! match maps back to a raw location on character boundaries.
//!
//! Step 1 is decoding, not normalization: the spec's `Normalized` is case
//! and whitespace only, and one level of string escaping undone is
//! `Decoded([JsonString])` or `Decoded([YamlString])`. So matching folds
//! all three, and a hit is then classified ([`super::classify`]) with
//! [`fold_plain`] (steps 2 and 3 only) and [`string_codec`].

use crosstalk_spec::derived::provenance::matching::Codec;

/// Folded text and, per folded byte, the raw range `[start, end)` that
/// produced it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Folded {
    pub text: String,
    pub raw_start: Vec<u32>,
    pub raw_end: Vec<u32>,
}

impl Folded {
    /// The raw byte range a folded range `[start, end)` came from.
    pub fn raw_range(&self, start: usize, end: usize) -> Option<(u32, u32)> {
        if start >= end {
            return None;
        }
        Some((*self.raw_start.get(start)?, *self.raw_end.get(end - 1)?))
    }
}

struct Writer {
    folded: Folded,
    /// The last thing written was whitespace (or nothing yet).
    in_space: bool,
}

impl Writer {
    fn push(&mut self, ch: char, start: usize, end: usize) {
        let (start, end) = (to_u32(start), to_u32(end));
        if ch.is_whitespace() {
            if !self.in_space {
                self.emit(' ', start, end);
                self.in_space = true;
            }
            return;
        }
        self.in_space = false;
        for lower in ch.to_lowercase() {
            self.emit(lower, start, end);
        }
    }

    fn emit(&mut self, ch: char, start: u32, end: u32) {
        let mut buffer = [0u8; 4];
        let encoded = ch.encode_utf8(&mut buffer);
        self.folded.text.push_str(encoded);
        for _ in 0..encoded.len() {
            self.folded.raw_start.push(start);
            self.folded.raw_end.push(end);
        }
    }
}

fn to_u32(n: usize) -> u32 {
    u32::try_from(n).unwrap_or(u32::MAX)
}

/// Case and whitespace folding only, escapes left as they are: what the
/// spec's `Normalized` compares under.
pub fn fold_plain(raw: &str) -> String {
    let mut writer = Writer {
        folded: Folded {
            text: String::with_capacity(raw.len()),
            raw_start: Vec::new(),
            raw_end: Vec::new(),
        },
        in_space: true,
    };
    for (start, ch) in raw.char_indices() {
        writer.push(ch, start, start + ch.len_utf8());
    }
    writer.folded.text
}

/// The string codec whose escapes `raw` holds: `YamlString` when it holds
/// an escape only YAML double-quoted scalars have (an escaped line break or
/// space, `\x`, `\0`, `\a`, `\e`, `\v`, `\N`, `\_`, `\L`, `\P`,
/// `\U`), otherwise `JsonString`.
pub fn string_codec(raw: &str) -> Codec {
    let mut chars = raw.chars();
    while let Some(ch) = chars.next() {
        if ch != '\\' {
            continue;
        }
        // A `\\` escapes a backslash: `next` consumes it whole.
        if let Some('\n' | '\r' | ' ' | 'x' | '0' | 'a' | 'e' | 'v' | 'N' | '_' | 'L' | 'P' | 'U') =
            chars.next()
        {
            return Codec::YamlString;
        }
    }
    Codec::JsonString
}

/// `raw` with exactly one level of string escapes undone: each backslash
/// escape (JSON's, or a YAML double-quoted scalar's) becomes what it
/// names, so `\\n` becomes `\n` (a backslash and an `n`), never a line
/// break. An escaped line break drops the break and the next line's
/// indentation. An escape that names nothing (`\q`, a short `\u`) and a
/// trailing backslash are kept as written. This is the one string level
/// the spec's `Codec::JsonString` and `Codec::YamlString` undo
/// (`provenance.decode.one-string-level`), applied leniently to a cut of a
/// literal rather than a whole one.
pub fn unescape_once(raw: &str) -> String {
    let chars: Vec<char> = raw.chars().collect();
    let mut out = String::with_capacity(raw.len());
    let mut at = 0;
    while at < chars.len() {
        let ch = chars[at];
        if ch != '\\' {
            out.push(ch);
            at += 1;
            continue;
        }
        let Some(&escaped) = chars.get(at + 1) else {
            out.push(ch);
            break;
        };
        let simple = match escaped {
            'n' => Some('\n'),
            't' | '\t' => Some('\t'),
            'r' => Some('\r'),
            'b' => Some('\u{8}'),
            'f' => Some('\u{c}'),
            '0' => Some('\0'),
            'a' => Some('\u{7}'),
            'e' => Some('\u{1b}'),
            'v' => Some('\u{b}'),
            'N' => Some('\u{85}'),
            '_' => Some('\u{a0}'),
            'L' => Some('\u{2028}'),
            'P' => Some('\u{2029}'),
            '"' | '\\' | '/' | ' ' | '\'' => Some(escaped),
            _ => None,
        };
        if let Some(decoded) = simple {
            out.push(decoded);
            at += 2;
            continue;
        }
        match escaped {
            '\n' | '\r' => {
                let mut after = at + 2;
                if escaped == '\r' && chars.get(after) == Some(&'\n') {
                    after += 1;
                }
                while matches!(chars.get(after), Some(' ' | '\t')) {
                    after += 1;
                }
                at = after;
            }
            'u' => match utf16_escape(&chars, at + 2) {
                Some((decoded, after)) => {
                    out.push(decoded);
                    at = after;
                }
                None => {
                    out.push(ch);
                    at += 1;
                }
            },
            'x' | 'U' => {
                let digits = if escaped == 'x' { 2 } else { 8 };
                match hex_n(&chars, at + 2, digits).and_then(char::from_u32) {
                    Some(decoded) => {
                        out.push(decoded);
                        at += 2 + digits;
                    }
                    None => {
                        out.push(ch);
                        at += 1;
                    }
                }
            }
            _ => {
                out.push(ch);
                at += 1;
            }
        }
    }
    out
}

/// A `uXXXX` escape's character (with `XXXX` at `at`), a surrogate pair
/// written as two single-backslash escapes combined, and the index after
/// it.
fn utf16_escape(chars: &[char], at: usize) -> Option<(char, usize)> {
    let high = hex_n(chars, at, 4)?;
    if (0xD800..0xDC00).contains(&high) {
        if chars.get(at + 4) == Some(&'\\') && chars.get(at + 5) == Some(&'u') {
            let low = hex_n(chars, at + 6, 4)?;
            if (0xDC00..0xE000).contains(&low) {
                let combined = 0x10000 + ((high - 0xD800) << 10) + (low - 0xDC00);
                return char::from_u32(combined).map(|ch| (ch, at + 10));
            }
        }
        return None;
    }
    char::from_u32(high).map(|ch| (ch, at + 4))
}

fn hex_n(chars: &[char], at: usize, n: usize) -> Option<u32> {
    chars
        .get(at..at + n)?
        .iter()
        .try_fold(0u32, |value, c| Some(value * 16 + c.to_digit(16)?))
}

/// Folds `raw`; ranges are offset by `base` (the position of `raw` in the
/// part text it was cut from).
pub fn fold(raw: &str, base: usize) -> Folded {
    let mut writer = Writer {
        folded: Folded {
            text: String::with_capacity(raw.len()),
            raw_start: Vec::with_capacity(raw.len()),
            raw_end: Vec::with_capacity(raw.len()),
        },
        in_space: true,
    };
    let chars: Vec<(usize, char)> = raw.char_indices().collect();
    let end_of = |at: usize| chars.get(at).map_or(raw.len(), |(offset, _)| *offset);
    let mut at = 0;
    while at < chars.len() {
        let (start, ch) = chars[at];
        if ch != '\\' {
            writer.push(ch, base + start, base + end_of(at + 1));
            at += 1;
            continue;
        }
        let mut next = at;
        while next < chars.len() && chars[next].1 == '\\' {
            next += 1;
        }
        let Some(&(_, escaped)) = chars.get(next) else {
            // Trailing backslashes: nothing to unfold.
            break;
        };
        match escaped {
            'n' | 't' | 'r' | 'b' | 'f' => {
                writer.push(' ', base + start, base + end_of(next + 1));
                at = next + 1;
            }
            'u' => match unicode_escape(&chars, next + 1) {
                Some((decoded, after)) => {
                    writer.push(decoded, base + start, base + end_of(after));
                    at = after;
                }
                None => at = next,
            },
            '\n' | '\r' => {
                // YAML line continuation: drop the break and the indentation.
                let mut after = next + 1;
                if escaped == '\r' && chars.get(after).is_some_and(|(_, c)| *c == '\n') {
                    after += 1;
                }
                while chars
                    .get(after)
                    .is_some_and(|(_, c)| *c == ' ' || *c == '\t')
                {
                    after += 1;
                }
                at = after;
            }
            _ => {
                // `\"`, `\\` collapsed into the run, `\/`, anything else: the
                // backslashes go, the character stays.
                writer.push(escaped, base + start, base + end_of(next + 1));
                at = next + 1;
            }
        }
    }
    writer.folded
}

/// The character a `uXXXX` escape (with `XXXX` at `at`) names, and the index
/// after it; a surrogate pair written as two escapes is combined.
fn unicode_escape(chars: &[(usize, char)], at: usize) -> Option<(char, usize)> {
    let high = hex4(chars, at)?;
    if (0xD800..0xDC00).contains(&high) {
        // Expect `\uDCxx` next (any number of backslashes).
        let mut next = at + 4;
        let mut slashes = 0;
        while chars.get(next).is_some_and(|(_, c)| *c == '\\') {
            next += 1;
            slashes += 1;
        }
        if slashes > 0 && chars.get(next).is_some_and(|(_, c)| *c == 'u') {
            let low = hex4(chars, next + 1)?;
            if (0xDC00..0xE000).contains(&low) {
                let combined = 0x10000 + ((high - 0xD800) << 10) + (low - 0xDC00);
                return char::from_u32(combined).map(|ch| (ch, next + 5));
            }
        }
        return Some(('\u{FFFD}', at + 4));
    }
    char::from_u32(high).map(|ch| (ch, at + 4))
}

fn hex4(chars: &[(usize, char)], at: usize) -> Option<u32> {
    let digits = chars.get(at..at + 4)?;
    digits
        .iter()
        .try_fold(0u32, |value, (_, c)| Some(value * 16 + c.to_digit(16)?))
}
