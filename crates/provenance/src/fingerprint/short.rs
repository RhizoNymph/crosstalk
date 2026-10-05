//! The short-span exact path (`provenance.match.short-span-exact`).
//!
//! Winnowing finds nothing in a value shorter than `k` and guarantees a
//! match only for `k + w - 1` normalized characters. A whole originated
//! value of `ShortSpans::min_chars..=max_chars` normalized characters (a
//! text part, or one string value of a tool call's arguments) is also
//! indexed by one exact hash of its whole normalized text ([`whole`]), and
//! a read is looked up by the hashes of its normalized token runs of those
//! lengths ([`token_runs`]): a read matches when it holds the value as a
//! run of whole tokens, never as part of a longer word.
//!
//! A token boundary sits at either end of the text, next to a space, and
//! between a word character (alphanumeric or `_`) and any other character.
//! A token run starts and ends on boundaries and neither starts nor ends
//! with a space. The hash is [`hash::short`] of the run's characters, so it
//! never equals a k-gram fingerprint.

use crosstalk_spec::derived::provenance::fingerprint::Fingerprint;

use super::KGram;
use super::hash::{self, Prefix};
use crate::config::ShortSpans;
use crate::text::NormChar;

fn word(ch: char) -> bool {
    ch.is_alphanumeric() || ch == '_'
}

/// The short-span hash of a whole value given as its normalized
/// characters (surrounding spaces ignored), when its length is admitted.
/// The k-gram's extent is the value's source bytes, its position the first
/// kept character.
pub fn whole(normalized: &[NormChar], spans: ShortSpans) -> Option<KGram> {
    let first = normalized.iter().position(|c| c.ch != ' ')?;
    let last = normalized.iter().rposition(|c| c.ch != ' ')?;
    let kept = &normalized[first..=last];
    if !spans.admits(kept.len()) {
        return None;
    }
    let chars: Vec<char> = kept.iter().map(|c| c.ch).collect();
    Some(KGram {
        fingerprint: Fingerprint(hash::short(hash::kgram(&chars))),
        start: kept.first()?.start,
        end: kept.last()?.end,
        position: first,
    })
}

/// Whether a token run may start at `index`.
fn starts_run(chars: &[char], index: usize) -> bool {
    let Some(&ch) = chars.get(index) else {
        return false;
    };
    if ch == ' ' {
        return false;
    }
    match index.checked_sub(1).and_then(|before| chars.get(before)) {
        None => true,
        Some(&before) => before == ' ' || word(before) != word(ch),
    }
}

/// Whether a token run may end just before `index`.
fn ends_run(chars: &[char], index: usize) -> bool {
    let Some(&last) = index.checked_sub(1).and_then(|at| chars.get(at)) else {
        return false;
    };
    if last == ' ' {
        return false;
    }
    match chars.get(index) {
        None => true,
        Some(&next) => next == ' ' || word(last) != word(next),
    }
}

/// The short-span hash of every token run of the normalized text whose
/// length is admitted, each with its source extent, in start order.
pub fn token_runs(normalized: &[NormChar], spans: ShortSpans) -> Vec<KGram> {
    let chars: Vec<char> = normalized.iter().map(|c| c.ch).collect();
    if chars.len() < spans.min_chars() {
        return Vec::new();
    }
    let ends: Vec<usize> = (1..=chars.len())
        .filter(|end| ends_run(&chars, *end))
        .collect();
    let prefix = Prefix::new(&chars);
    let mut runs = Vec::new();
    for start in (0..chars.len()).filter(|start| starts_run(&chars, *start)) {
        let low = ends.partition_point(|end| *end < start + spans.min_chars());
        for &end in ends[low..]
            .iter()
            .take_while(|end| **end <= start + spans.max_chars())
        {
            let Some(window) = prefix.window(start, end) else {
                continue;
            };
            runs.push(KGram {
                fingerprint: Fingerprint(hash::short(window)),
                start: normalized[start].start,
                end: normalized[end - 1].end,
                position: start,
            });
        }
    }
    runs
}
