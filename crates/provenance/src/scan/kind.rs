//! How a read had to be transformed before it matched: the `MatchKind`.
//!
//! - No decode step and the read bytes occur verbatim in the origin span's
//!   text (or the span's text in them): `Exact`.
//! - No decode step otherwise: `Normalized` (whitespace and case folding).
//! - Decoded through spec codecs: `Decoded`, listing the codecs in the order
//!   they were undone (`provenance.decode.codecs-in-decode-order`).
//! - String unescapes count as spec codecs (`Codec::JsonString`,
//!   `Codec::YamlString`, through [`Step::codec`]).

use crosstalk_spec::derived::provenance::matching::{Codec, MatchKind};
use crosstalk_spec::support::NonEmpty;

use crate::decode::Step;

/// The kind of a match found in a layer reached through `chain`; `exact`
/// says whether the read bytes occur verbatim in the origin text.
pub fn match_kind(chain: &[Step], exact: bool) -> MatchKind {
    let codecs: Vec<Codec> = chain.iter().filter_map(|step| step.codec()).collect();
    match NonEmpty::from_vec(codecs) {
        Some(codecs) => MatchKind::Decoded(codecs),
        None if chain.is_empty() && exact => MatchKind::Exact,
        None => MatchKind::Normalized,
    }
}

/// Whether `read` occurs verbatim in one of `origins`, or one of them in
/// `read`.
pub fn is_exact(read: &str, origins: &[&str]) -> bool {
    !read.is_empty()
        && origins
            .iter()
            .any(|origin| !origin.is_empty() && (origin.contains(read) || read.contains(origin)))
}
