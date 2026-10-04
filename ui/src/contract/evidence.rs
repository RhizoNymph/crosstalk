//! The text behind a transmission (item 22). Needs `Content`.

use crosstalk_spec::derived::flow::access::Access;
use crosstalk_spec::derived::flow::resource::Resource;
use crosstalk_spec::derived::flow::transmission::Transmission;
use crosstalk_spec::derived::provenance::matching::ContentMatch;

use super::verdict::TransmissionVerdict;

/// A slice of message text around a matched range. Built only through
/// [`Excerpt::new`]: the highlight lies inside the text on character
/// boundaries.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Excerpt {
    text: String,
    highlight: std::ops::Range<usize>,
    /// Bytes of the full part cut before and after `text`.
    elided: (u32, u32),
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum InvalidExcerpt {
    #[error("highlight {start}..{end} is empty or outside {len} bytes of text")]
    OutOfRange {
        start: usize,
        end: usize,
        len: usize,
    },
    #[error("highlight boundary {0} splits a character")]
    NotCharBoundary(usize),
}

impl Excerpt {
    pub fn new(
        text: String,
        highlight: std::ops::Range<usize>,
        elided: (u32, u32),
    ) -> Result<Self, InvalidExcerpt> {
        if highlight.start >= highlight.end || highlight.end > text.len() {
            return Err(InvalidExcerpt::OutOfRange {
                start: highlight.start,
                end: highlight.end,
                len: text.len(),
            });
        }
        for at in [highlight.start, highlight.end] {
            if !text.is_char_boundary(at) {
                return Err(InvalidExcerpt::NotCharBoundary(at));
            }
        }
        Ok(Self {
            text,
            highlight,
            elided,
        })
    }

    pub fn before(&self) -> &str {
        &self.text[..self.highlight.start]
    }

    pub fn matched(&self) -> &str {
        &self.text[self.highlight.clone()]
    }

    pub fn after(&self) -> &str {
        &self.text[self.highlight.end..]
    }

    pub fn elided(&self) -> (u32, u32) {
        self.elided
    }
}

/// One content match with the sender's text and the reader's text.
#[derive(Debug, Clone, PartialEq)]
pub struct MatchEvidence {
    pub content_match: ContentMatch,
    /// Around the sender's originated span.
    pub origin: Excerpt,
    /// Around the range the reader read, as it arrived (before decoding).
    pub read: Excerpt,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AccessDetail {
    pub access: Access,
    pub resource: Resource,
}

/// Everything the evidence page shows.
#[derive(Debug, Clone, PartialEq)]
pub struct TransmissionEvidence {
    pub transmission: Transmission,
    /// Empty until confirmed.
    pub matches: Vec<MatchEvidence>,
    /// The accesses named by the transmission's co-access records.
    pub accesses: Vec<AccessDetail>,
    /// The verdict log, oldest first.
    pub verdicts: Vec<TransmissionVerdict>,
}
