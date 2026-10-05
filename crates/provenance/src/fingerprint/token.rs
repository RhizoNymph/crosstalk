//! Tokens for world-wide rarity (`provenance.match.cross-agent-spread`).
//!
//! A short fragment many agents hold is boilerplate only when it is not
//! distinctive: it holds no token that is (nearly) never seen outside the
//! fragment's own occurrences. A token is a maximal run of alphanumeric
//! characters of the normalized text that has at least
//! [`MIN_TOKEN_CHARS`] characters or contains a digit, so a key such as
//! `7f3a` or an id is a token, and so is any longer word. Each scanned text
//! observes its distinct tokens' hashes ([`hash::token`]) once, as one
//! observation of its own, so a token's frequency is the number of live
//! texts holding it.

use std::collections::BTreeSet;

use crosstalk_spec::derived::provenance::fingerprint::Fingerprint;

use super::hash;
use crate::text::normalize;

/// The fewest characters a token without a digit has.
pub const MIN_TOKEN_CHARS: usize = 4;

fn counted(chars: &[char]) -> bool {
    chars.len() >= MIN_TOKEN_CHARS || chars.iter().any(char::is_ascii_digit)
}

fn fingerprint(chars: &[char]) -> Fingerprint {
    Fingerprint(hash::token(hash::kgram(chars)))
}

/// The tokens of `text` as (start, end) character spans of its
/// normalization and their hashes, in order.
fn spans(chars: &[char]) -> Vec<(usize, usize, Fingerprint)> {
    let mut out = Vec::new();
    let mut at = 0;
    while at < chars.len() {
        if !chars[at].is_alphanumeric() {
            at += 1;
            continue;
        }
        let start = at;
        while at < chars.len() && chars[at].is_alphanumeric() {
            at += 1;
        }
        let token = &chars[start..at];
        if counted(token) {
            out.push((start, at, fingerprint(token)));
        }
    }
    out
}

/// The distinct token hashes of `text`, at most `cap` of them (the first
/// distinct ones in text order): what a scanned text observes. A capped
/// text undercounts its later tokens, which makes them look rarer, so the
/// cap errs toward keeping matches, never toward hiding them.
pub fn observed(text: &str, cap: usize) -> BTreeSet<Fingerprint> {
    let chars: Vec<char> = normalize(text).iter().map(|c| c.ch).collect();
    let mut seen = BTreeSet::new();
    for (_, _, token) in spans(&chars) {
        if seen.len() >= cap {
            break;
        }
        seen.insert(token);
    }
    seen
}

/// Every token of `text`, in order, repeats kept: the sequence a run of
/// another text is looked for in (`provenance.match.inherited-fragment-dropped`).
pub fn sequence(text: &str) -> Vec<Fingerprint> {
    let chars: Vec<char> = normalize(text).iter().map(|c| c.ch).collect();
    spans(&chars)
        .into_iter()
        .map(|(_, _, token)| token)
        .collect()
}

/// The hashes of the whole tokens inside `text[start..end]` (byte
/// offsets): tokens cut by either end, where the character outside is
/// alphanumeric too, are left out, since a cut word was never observed.
pub fn whole_tokens_in(text: &str, start: usize, end: usize) -> Vec<Fingerprint> {
    let Some(window) = text.get(start..end) else {
        return Vec::new();
    };
    let cut_before = text[..start]
        .chars()
        .next_back()
        .is_some_and(char::is_alphanumeric);
    let cut_after = text[end..]
        .chars()
        .next()
        .is_some_and(char::is_alphanumeric);
    let chars: Vec<char> = normalize(window).iter().map(|c| c.ch).collect();
    spans(&chars)
        .into_iter()
        .filter(|(first, last, _)| {
            !(cut_before && *first == 0) && !(cut_after && *last == chars.len())
        })
        .map(|(_, _, token)| token)
        .collect()
}
