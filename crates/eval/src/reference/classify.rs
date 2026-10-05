//! The match class of one hit: the weakest transformation under which the
//! reader's text is the span's.
//!
//! - `Exact`: the span holds the read bytes verbatim.
//! - `Normalized`: equal after case and whitespace folding alone
//!   ([`fold_plain`]), the spec's `Normalized`.
//! - `Decoded([JsonString])` or `Decoded([YamlString])`: equal only once one
//!   side's string escapes are undone (the matching [`fold`](super::fold::fold)
//!   does that), with the codec whose escapes the text holds
//!   ([`string_codec`]): the read text's when it has any, else the span's
//!   (a span written inside JSON tool arguments and delivered raw).
//!
//! Decoded hits from base64, hex or URL encoding are classified where they
//! are found (`decode`), not here.

use crosstalk_spec::derived::provenance::matching::MatchKind;
use crosstalk_spec::support::NonEmpty;

use super::fold::{fold_plain, string_codec};

/// The class of a hit reading `read` from a span whose raw text is
/// `span_raw` and whose [`fold_plain`] is `span_plain`. `read` matched the
/// span under the matching fold.
pub fn classify(span_raw: &str, span_plain: &str, read: &str) -> MatchKind {
    if span_raw.contains(read) {
        return MatchKind::Exact;
    }
    let plain = fold_plain(read);
    let plain = plain.trim_end();
    if !plain.is_empty() && span_plain.contains(plain) {
        return MatchKind::Normalized;
    }
    let escaped = if read.contains('\\') { read } else { span_raw };
    MatchKind::Decoded(NonEmpty::new(string_codec(escaped)))
}
