//! The match class of one hit: the weakest transformation under which the
//! reader's text is the span's, or that none the spec allows is.
//!
//! The matching [`fold`](super::fold::fold) undoes string escapes at any
//! depth, so it finds a candidate wherever the texts agree once every
//! escape is gone. The spec undoes at most one string level
//! (`provenance.decode.one-string-level`), so every hit is classified here,
//! in this order:
//!
//! - `Exact`: the span holds the read bytes verbatim.
//! - `Normalized`: equal after case and whitespace folding alone
//!   ([`fold_plain`]), the spec's `Normalized`.
//! - `Decoded([JsonString])` or `Decoded([YamlString])`: equal once one
//!   level of string escapes is undone ([`unescape_once`]) on the read
//!   side, or on the span side (a span written inside JSON tool arguments
//!   and delivered raw), then case and whitespace folded. The codec is the
//!   one whose escapes the undone text holds ([`string_codec`]).
//!   A YAML single-quoted `''` undone to `'` is `Decoded([YamlString])`.
//! - [`Classified::TwoStringLevels`]: equal once escapes are undone exactly
//!   twice on one side, which no spec decoder does. Such a hit is out of
//!   reach: the matcher reports no match for it, and a label for such text
//!   is `Tier::OutOfReach`.
//! - Otherwise the fold's range bridged a character neither side shares
//!   (a range is the union of adjacent matching shingle windows); it is
//!   classed by its escapes as one string level, as before.
//!
//! Decoded hits from base64, hex or URL encoding are classified where they
//! are found (`decode`), not here.

use crosstalk_spec::derived::provenance::matching::{Codec, MatchKind};
use crosstalk_spec::support::NonEmpty;

use super::fold::{fold_plain, string_codec, unescape_once};

/// A hit's class.
#[derive(Debug, Clone, PartialEq)]
pub enum Classified {
    /// A match the spec can make, of this kind.
    Match(MatchKind),
    /// Only two or more string levels undone make the texts equal: out of
    /// the spec's reach.
    TwoStringLevels,
}

/// The forms of an indexed span that classification compares against.
#[derive(Debug, Clone, Copy)]
pub struct SpanForms<'a> {
    /// As written.
    pub raw: &'a str,
    /// [`fold_plain`] of `raw`.
    pub plain: &'a str,
    /// [`fold_plain`] of [`unescape_once`] of `raw`, when `raw` holds a
    /// backslash; `None` otherwise (it would equal `plain`).
    pub unescaped: Option<&'a str>,
}

/// [`SpanForms::unescaped`] for `raw`.
pub fn unescaped_plain(raw: &str) -> Option<String> {
    raw.contains('\\').then(|| fold_plain(&unescape_once(raw)))
}

/// The class of a hit reading `read` from a span whose raw text is
/// `span_raw` and whose [`fold_plain`] is `span_plain`. `read` matched the
/// span under the matching fold.
pub fn classify(span_raw: &str, span_plain: &str, read: &str) -> Classified {
    let unescaped = unescaped_plain(span_raw);
    classify_forms(
        SpanForms {
            raw: span_raw,
            plain: span_plain,
            unescaped: unescaped.as_deref(),
        },
        read,
    )
}

/// [`classify`] with the span's forms computed once, as the matcher keeps
/// them.
pub fn classify_forms(span: SpanForms<'_>, read: &str) -> Classified {
    if span.raw.contains(read) {
        return Classified::Match(MatchKind::Exact);
    }
    let read_plain = fold_plain(read);
    let read_plain = read_plain.trim_end();
    if !read_plain.is_empty() && span.plain.contains(read_plain) {
        return Classified::Match(MatchKind::Normalized);
    }
    let decoded =
        |text: &str| Classified::Match(MatchKind::Decoded(NonEmpty::new(string_codec(text))));
    let in_span = |needle: &str| {
        !needle.is_empty()
            && (span.plain.contains(needle) || span.unescaped.is_some_and(|s| s.contains(needle)))
    };
    if read.contains('\\') {
        let once = fold_plain(&unescape_once(read));
        if in_span(once.trim_end()) {
            return decoded(read);
        }
    }
    if read.contains("''") {
        // A YAML single-quoted scalar: `''` is one `'`.
        let once = fold_plain(&read.replace("''", "'"));
        if in_span(once.trim_end()) {
            return Classified::Match(MatchKind::Decoded(NonEmpty::new(Codec::YamlString)));
        }
    }
    if !read_plain.is_empty() && span.unescaped.is_some_and(|s| s.contains(read_plain)) {
        return decoded(span.raw);
    }
    if two_levels(span, read, read_plain) {
        return Classified::TwoStringLevels;
    }
    // The fold matched, but no single reading explains the whole range: the
    // shingle runs it joins bridge a character neither side shares (a
    // range is the union of adjacent matching windows). Classed by its
    // escapes, as one string level.
    decoded(if read.contains('\\') { read } else { span.raw })
}

/// Whether undoing exactly two string levels, on the read side or on the
/// span side, makes the texts equal.
fn two_levels(span: SpanForms<'_>, read: &str, read_plain: &str) -> bool {
    if read.contains('\\') {
        let twice = fold_plain(&unescape_once(&unescape_once(read)));
        let twice = twice.trim_end();
        if !twice.is_empty() && span.plain.contains(twice) {
            return true;
        }
    }
    span.unescaped.is_some()
        && !read_plain.is_empty()
        && fold_plain(&unescape_once(&unescape_once(span.raw))).contains(read_plain)
}
