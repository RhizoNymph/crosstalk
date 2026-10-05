//! Where negative-control false positives came from: the shared text each
//! one fell on, tallied over every false positive (not just the cited
//! examples), so a background run can name its top boilerplate.
//!
//! A source is the first line of the reader's text at the prediction,
//! whitespace collapsed and cut to [`SOURCE_CHARS`] characters: harness
//! banners, test-runner headers and interpreter footers collapse to one
//! source each however many readers saw them.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::truth::NegativeReason;

/// How many sources a score keeps.
pub const TOP_SOURCES: usize = 20;

/// How much of a source's first line is kept, in characters.
pub const SOURCE_CHARS: usize = 80;

/// One shared text and how many false positives violating a control of
/// `reason` fell on it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceCount {
    pub reason: NegativeReason,
    pub text: String,
    pub count: u64,
}

/// The source key of a reader's text: its first non-blank line, whitespace
/// collapsed, at most [`SOURCE_CHARS`] characters.
pub fn source_key(text: &str) -> String {
    let line = text
        .lines()
        .find(|line| !line.trim().is_empty())
        .unwrap_or_default();
    let collapsed = line.split_whitespace().collect::<Vec<_>>().join(" ");
    collapsed.chars().take(SOURCE_CHARS).collect()
}

/// Counts false positives by (reason, source).
#[derive(Debug, Default)]
pub struct SourceTally {
    counts: BTreeMap<(NegativeReason, String), u64>,
}

impl SourceTally {
    pub fn add(&mut self, reason: NegativeReason, text: &str) {
        *self.counts.entry((reason, source_key(text))).or_default() += 1;
    }

    /// The [`TOP_SOURCES`] largest, by count descending, then reason and
    /// text ascending.
    pub fn top(self) -> Vec<SourceCount> {
        let mut all: Vec<SourceCount> = self
            .counts
            .into_iter()
            .map(|((reason, text), count)| SourceCount {
                reason,
                text,
                count,
            })
            .collect();
        all.sort_by(|a, b| {
            b.count
                .cmp(&a.count)
                .then_with(|| a.reason.cmp(&b.reason))
                .then_with(|| a.text.cmp(&b.text))
        });
        all.truncate(TOP_SOURCES);
        all
    }
}
