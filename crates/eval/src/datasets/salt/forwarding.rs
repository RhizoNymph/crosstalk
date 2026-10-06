//! Which SALT deliveries are forwarding: content the sender relayed from its
//! own tool output rather than wrote.
//!
//! **Rule.** A delivery is forwarding when at least half of its content,
//! after the reference matcher's fold (string escapes undone at any depth,
//! case folded, whitespace collapsed; [`crate::reference::fold`]), is
//! covered by [`FORWARD_K`]-byte shingles that occur in a tool result the
//! sender received before the call that sent it (any tool, any earlier
//! message of the sender's list). Content shorter than one shingle is never
//! forwarding. A pasted `get_log` chunk or `inspect_database` schema is
//! forwarding; a message that quotes a line of a tool result inside its own
//! prose is not.
//!
//! This is the converter's own knowledge (it holds the sender's messages,
//! so it knows which text came back to the sender from its tools), measured
//! the way the reference matcher decides what is seen input rather than
//! originated text.

use std::collections::HashSet;

use crate::reference::fold::fold;
use crate::reference::shingle::{covered, shingles};

/// Shingle length, in folded bytes: the reference matcher's `k`.
pub const FORWARD_K: usize = 24;

/// The share of the content's folded bytes that must be covered:
/// `numerator / denominator`.
pub const FORWARDED_SHARE: (usize, usize) = (1, 2);

/// The shingles of each tool result in one agent's message list, by the
/// result's index in the list.
#[derive(Debug, Default, Clone)]
pub struct ToolOutput {
    results: Vec<(usize, HashSet<u64>)>,
}

impl ToolOutput {
    /// Adds the tool result at `index` of the list.
    pub fn add(&mut self, index: usize, text: &str) {
        let folded = fold(text, 0);
        let hashes = shingles(folded.text.as_bytes(), FORWARD_K)
            .into_iter()
            .map(|(hash, _)| hash)
            .collect();
        self.results.push((index, hashes));
    }

    fn seen_before(&self, hash: u64, before: usize) -> bool {
        self.results
            .iter()
            .any(|(index, hashes)| *index < before && hashes.contains(&hash))
    }

    /// Whether `content`, sent by the call in message `before`, is
    /// forwarded from the tool results before it (see the module doc).
    pub fn forwards(&self, content: &str, before: usize) -> bool {
        let folded = fold(content, 0);
        let len = folded.text.len();
        if len < FORWARD_K {
            return false;
        }
        let hits: Vec<usize> = shingles(folded.text.as_bytes(), FORWARD_K)
            .into_iter()
            .filter(|&(hash, _)| self.seen_before(hash, before))
            .map(|(_, offset)| offset)
            .collect();
        let covered: usize = covered(&hits, FORWARD_K)
            .into_iter()
            .map(|(start, end)| end - start)
            .sum();
        let (numerator, denominator) = FORWARDED_SHARE;
        covered * denominator >= len * numerator
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const LOG: &str = r#"[{"seq":1,"tool":"read_code","args":{"path":"src/ledger.py"},"ok":true},{"seq":2,"tool":"query_database","args":{"sql":"SELECT * FROM orders"},"ok":true}]"#;

    /// Tool results at indexes 0, 1, …; the content is sent after all.
    fn output(texts: &[&str]) -> ToolOutput {
        let mut out = ToolOutput::default();
        for (index, text) in texts.iter().enumerate() {
            out.add(index, text);
        }
        out
    }

    const AFTER: usize = usize::MAX;

    #[test]
    fn a_pasted_tool_result_is_forwarding() {
        let content = format!("Chunk 1 of 6 of my raw log: {LOG}");
        assert!(output(&[LOG]).forwards(&content, AFTER));
    }

    #[test]
    fn prose_quoting_a_short_line_is_not() {
        let content = "I looked at the ledger and the totals look off for March; \
                       the read_code step went fine, so I will accept your verdict.";
        assert!(!output(&[LOG]).forwards(content, AFTER));
    }

    #[test]
    fn half_covered_is_forwarding_and_less_is_not() {
        let pasted = &LOG[..80];
        let own = "x".repeat(70);
        let half = format!("{pasted} {}", "y".repeat(78));
        assert!(output(&[LOG]).forwards(&half, AFTER));
        let less = format!("{pasted} {own} {own}");
        assert!(!output(&[LOG]).forwards(&less, AFTER));
    }

    #[test]
    fn escapes_and_case_are_folded() {
        let escaped = LOG.replace('"', "\\\"").to_uppercase();
        assert!(output(&[LOG]).forwards(&escaped, AFTER));
    }

    #[test]
    fn only_results_before_the_send_count() {
        let mut out = ToolOutput::default();
        out.add(5, LOG);
        assert!(!out.forwards(LOG, 5));
        assert!(out.forwards(LOG, 6));
    }

    #[test]
    fn nothing_seen_or_too_short_is_not_forwarding() {
        assert!(!output(&[]).forwards(LOG, AFTER));
        assert!(!output(&[LOG]).forwards(&LOG[..FORWARD_K - 1], AFTER));
    }
}
