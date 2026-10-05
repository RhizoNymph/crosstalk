//! How an injection arrived in a tool output, and where.
//!
//! AgentDojo places each injection string into the environment; tool
//! outputs are then `yaml.dump`-ed (or JSON-encoded) before the model reads
//! them. The converter tests the injection against the actual output under
//! successively stronger decodings and labels each copy with the weakest one
//! that finds it:
//!
//! | [`Arrival`] | the output holds the injection … |
//! | --- | --- |
//! | `Exact` | byte for byte |
//! | `Whitespace` | with whitespace re-wrapped (a YAML plain or folded scalar) |
//! | `JsonString` | inside a JSON string (`\n`, `\"`, `\uXXXX`, …) |
//! | `YamlString` | inside a YAML double- or single-quoted scalar (`\`-newline continuations, `\ `, `\xXX`, `''`, …) |
//!
//! Every decoding is followed by whitespace folding (runs collapse to one
//! space). The injection is trimmed first; its surrounding newlines are
//! layout, not content. Each found copy maps back to the raw byte range it
//! came from, on character boundaries.

use std::ops::Range;

use serde::{Deserialize, Serialize};

use crate::truth::MatchNeed;

/// How a copy of an injection reached the tool output, weakest first.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Arrival {
    Exact,
    Whitespace,
    JsonString,
    YamlString,
}

impl Arrival {
    pub const ALL: [Arrival; 4] = [
        Arrival::Exact,
        Arrival::Whitespace,
        Arrival::JsonString,
        Arrival::YamlString,
    ];

    /// The label's match need: whitespace re-wrapping is `Normalized`; one
    /// level of string escaping is `Decoded([JsonString])` or
    /// `Decoded([YamlString])` (`provenance.match.string-serialised-decoded`).
    pub fn need(self) -> MatchNeed {
        match self {
            Self::Exact => MatchNeed::Exact,
            Self::Whitespace => MatchNeed::Normalized,
            Self::JsonString => MatchNeed::json_string(),
            Self::YamlString => MatchNeed::yaml_string(),
        }
    }
}

/// One copy of an injection: raw bytes `start..end` of the output.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Occurrence {
    pub start: usize,
    pub end: usize,
    pub arrival: Arrival,
}

/// Every non-overlapping copy of `injection` in `output`, in output order,
/// each with the weakest arrival that finds it.
pub fn occurrences(injection: &str, output: &str) -> Vec<Occurrence> {
    Output::new(output).occurrences(injection)
}

/// A tool output with its decodings, built once and searched for every
/// injection.
pub struct Output<'a> {
    raw: &'a str,
    whitespace: Folded,
    json: Folded,
    yaml: Folded,
}

impl<'a> Output<'a> {
    pub fn new(raw: &'a str) -> Self {
        Self {
            raw,
            whitespace: fold(plain(raw)),
            json: fold(unescape(raw, Dialect::Json)),
            yaml: fold(unescape(raw, Dialect::Yaml)),
        }
    }

    /// Every non-overlapping copy of `injection`, in output order, each with
    /// the weakest arrival that finds it.
    pub fn occurrences(&self, injection: &str) -> Vec<Occurrence> {
        let trimmed = injection.trim();
        let needle = fold_plain(trimmed);
        if needle.is_empty() {
            return Vec::new();
        }
        let mut found: Vec<Occurrence> = Vec::new();
        for arrival in Arrival::ALL {
            let candidates = match arrival {
                Arrival::Exact => self
                    .raw
                    .match_indices(trimmed)
                    .map(|(at, text)| at..at + text.len())
                    .collect(),
                Arrival::Whitespace => search(&self.whitespace, &needle),
                Arrival::JsonString => search(&self.json, &needle),
                Arrival::YamlString => search(&self.yaml, &needle),
            };
            for range in candidates {
                let overlaps = found
                    .iter()
                    .any(|seen| seen.start < range.end && range.start < seen.end);
                if !overlaps {
                    found.push(Occurrence {
                        start: range.start,
                        end: range.end,
                        arrival,
                    });
                }
            }
        }
        found.sort_by_key(|occurrence| occurrence.start);
        found
    }
}

/// A decoded character and the raw byte range it came from.
type Piece = (char, usize, usize);

/// Decoded, whitespace-folded text with each byte's raw range.
struct Folded {
    text: String,
    starts: Vec<usize>,
    ends: Vec<usize>,
}

fn search(folded: &Folded, needle: &str) -> Vec<Range<usize>> {
    folded
        .text
        .match_indices(needle)
        .filter_map(|(at, text)| {
            let start = *folded.starts.get(at)?;
            let end = *folded.ends.get(at + text.len() - 1)?;
            Some(start..end)
        })
        .collect()
}

/// `text`'s characters as they are.
fn plain(text: &str) -> Vec<Piece> {
    text.char_indices()
        .map(|(at, ch)| (ch, at, at + ch.len_utf8()))
        .collect()
}

/// Whitespace runs collapsed to one space, leading and trailing whitespace
/// dropped.
fn fold(pieces: Vec<Piece>) -> Folded {
    let mut out = Folded {
        text: String::with_capacity(pieces.len()),
        starts: Vec::with_capacity(pieces.len()),
        ends: Vec::with_capacity(pieces.len()),
    };
    let mut pending: Option<(usize, usize)> = None;
    for (ch, start, end) in pieces {
        if ch.is_whitespace() {
            pending = Some(match pending {
                Some((first, _)) => (first, end),
                None => (start, end),
            });
            continue;
        }
        if let Some((first, last)) = pending.take()
            && !out.text.is_empty()
        {
            push(&mut out, ' ', first, last);
        }
        push(&mut out, ch, start, end);
    }
    out
}

fn push(out: &mut Folded, ch: char, start: usize, end: usize) {
    out.text.push(ch);
    for _ in 0..ch.len_utf8() {
        out.starts.push(start);
        out.ends.push(end);
    }
}

/// The folded text of an injection (no decoding: it is raw).
fn fold_plain(text: &str) -> String {
    fold(plain(text)).text
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Dialect {
    Json,
    Yaml,
}

/// `text` with string escapes decoded. JSON knows `\n \t \r \b \f \/ \\ \"
/// \uXXXX`; YAML adds `\`-newline continuations, `\ `, `\_`, `\0`, `\a`,
/// `\e`, `\N`, `\L`, `\P`, `\xXX`, `\UXXXXXXXX`, `\'`, a backslash before a
/// tab, and `''` (single-quoted scalars). An escape that does not decode is
/// kept as it is.
fn unescape(text: &str, dialect: Dialect) -> Vec<Piece> {
    let chars: Vec<(usize, char)> = text.char_indices().collect();
    let end_of = |at: usize| chars.get(at).map_or(text.len(), |(offset, _)| *offset);
    let mut out = Vec::with_capacity(chars.len());
    let mut at = 0;
    while at < chars.len() {
        let (start, ch) = chars[at];
        if dialect == Dialect::Yaml && ch == '\'' && chars.get(at + 1).is_some_and(|c| c.1 == '\'')
        {
            out.push(('\'', start, end_of(at + 2)));
            at += 2;
            continue;
        }
        if ch != '\\' {
            out.push((ch, start, end_of(at + 1)));
            at += 1;
            continue;
        }
        let Some(&(_, escaped)) = chars.get(at + 1) else {
            out.push((ch, start, end_of(at + 1)));
            break;
        };
        let simple = match escaped {
            'n' => Some('\n'),
            't' => Some('\t'),
            'r' => Some('\r'),
            'b' => Some('\u{8}'),
            'f' => Some('\u{c}'),
            '/' => Some('/'),
            '\\' => Some('\\'),
            '"' => Some('"'),
            _ => None,
        };
        let yaml = dialect == Dialect::Yaml;
        let decoded: Option<(char, usize)> = match (simple, escaped) {
            (Some(decoded), _) => Some((decoded, at + 2)),
            (None, 'u') => hex(&chars, at + 2, 4).map(|c| (c, at + 6)),
            (None, 'x') if yaml => hex(&chars, at + 2, 2).map(|c| (c, at + 4)),
            (None, 'U') if yaml => hex(&chars, at + 2, 8).map(|c| (c, at + 10)),
            (None, ' ') if yaml => Some((' ', at + 2)),
            (None, '\t') if yaml => Some(('\t', at + 2)),
            (None, '_') if yaml => Some(('\u{a0}', at + 2)),
            (None, '0') if yaml => Some(('\0', at + 2)),
            (None, 'a') if yaml => Some(('\u{7}', at + 2)),
            (None, 'e') if yaml => Some(('\u{1b}', at + 2)),
            (None, 'N') if yaml => Some(('\u{85}', at + 2)),
            (None, 'L') if yaml => Some(('\u{2028}', at + 2)),
            (None, 'P') if yaml => Some(('\u{2029}', at + 2)),
            (None, '\'') if yaml => Some(('\'', at + 2)),
            _ => None,
        };
        if let Some((decoded, after)) = decoded {
            out.push((decoded, start, end_of(after)));
            at = after;
            continue;
        }
        if yaml && (escaped == '\n' || escaped == '\r') {
            // A line continuation: the break and the next line's indentation
            // vanish.
            let mut after = at + 2;
            if escaped == '\r' && chars.get(after).is_some_and(|c| c.1 == '\n') {
                after += 1;
            }
            while chars.get(after).is_some_and(|c| c.1 == ' ' || c.1 == '\t') {
                after += 1;
            }
            at = after;
            continue;
        }
        out.push((ch, start, end_of(at + 1)));
        at += 1;
    }
    out
}

/// The character named by `digits` hex digits at `at`.
fn hex(chars: &[(usize, char)], at: usize, digits: usize) -> Option<char> {
    let slice = chars.get(at..at + digits)?;
    let value = slice
        .iter()
        .try_fold(0u32, |value, (_, c)| Some(value * 16 + c.to_digit(16)?))?;
    char::from_u32(value)
}
