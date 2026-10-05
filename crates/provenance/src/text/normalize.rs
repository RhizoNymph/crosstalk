//! Whitespace and case normalization, the only normalization fingerprints
//! see (`provenance.fingerprint.normalization-invariant`).
//!
//! Every maximal run of whitespace (Unicode `White_Space`) becomes one
//! space, and every other character becomes its lowercase mapping
//! (`char::to_lowercase`, which is context-free: it never looks at the
//! neighbouring characters). Nothing is trimmed, so the normalization of a
//! substring cut on character boundaries is a substring of the
//! normalization of the whole text, apart from a whitespace run cut in two,
//! which still folds to one space.
//!
//! Each normalized character keeps the source byte range it came from: a
//! folded run's whole extent, or the one source character a lowercase
//! mapping expanded (two normalized characters can share one source
//! character, as `İ` lowercases to `i̇`). Both ends are character
//! boundaries of the source.

/// One normalized character and the source bytes it stands for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NormChar {
    pub ch: char,
    /// Source byte offset of the first byte it stands for.
    pub start: u32,
    /// Source byte offset just past the last byte it stands for.
    pub end: u32,
}

/// `text` normalized, character by character. Text past `u32::MAX` bytes
/// is left out (offsets are `u32`).
pub fn normalize(text: &str) -> Vec<NormChar> {
    let mut out: Vec<NormChar> = Vec::with_capacity(text.len());
    let mut in_space = false;
    for (index, ch) in text.char_indices() {
        let Ok(start) = u32::try_from(index) else {
            break;
        };
        let Ok(end) = u32::try_from(index + ch.len_utf8()) else {
            break;
        };
        if ch.is_whitespace() {
            match out.last_mut() {
                Some(last) if in_space => last.end = end,
                _ => out.push(NormChar {
                    ch: ' ',
                    start,
                    end,
                }),
            }
            in_space = true;
        } else {
            in_space = false;
            for lower in ch.to_lowercase() {
                out.push(NormChar {
                    ch: lower,
                    start,
                    end,
                });
            }
        }
    }
    out
}

/// The normalized text as a string.
pub fn normalized_string(text: &str) -> String {
    normalize(text).iter().map(|c| c.ch).collect()
}

/// How many normalized characters are left without leading and trailing
/// spaces.
pub fn trimmed_len(normalized: &[NormChar]) -> usize {
    let first = normalized.iter().position(|c| c.ch != ' ');
    let last = normalized.iter().rposition(|c| c.ch != ' ');
    match (first, last) {
        (Some(first), Some(last)) => last + 1 - first,
        _ => 0,
    }
}
