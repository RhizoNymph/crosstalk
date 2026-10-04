//! Excerpts: the text around a matched range, cut from a stored message
//! body for the evidence page.
//!
//! **Where the text comes from.** A span or a content match locates its
//! text by a [`SpanLocation`]: a part of a message, named by
//! [`MessageHash`], and a byte range in that part's text
//! ([`Message::part_text`]). Message bodies live in the blob store under
//! their hash (`BlobStore::get`). [`Excerpted::of`] takes the location and
//! the body the blob store returned and cuts the excerpt with
//! [`Excerpt::cut`].
//!
//! **The window.** An [`ExcerptWindow`] is how many bytes of context to
//! show on each side of the matched range, at most
//! [`ExcerptWindow::MAX_CONTEXT`]. [`Excerpt::cut`] moves each window edge
//! inward to the nearest character boundary, so it never splits a
//! character and never shows more than the window. The matched range itself
//! is shown whole up to [`Excerpt::MAX_HIGHLIGHT`] bytes; a longer range is
//! cut at the last character boundary within that bound, the bytes cut are
//! recorded ([`Excerpt::highlight_cut`]) and no context is shown after it.
//! The bytes of the part not shown before and after the excerpt are
//! recorded too, so `elided_before + text + highlight_cut + elided_after`
//! is the length of the part text.
//!
//! **Retention.** A message body the blob store no longer holds was
//! dropped by content retention: L1 writes every body before publishing
//! `ExchangeCaptured`, so every body a span or a match names was stored
//! once. That is a normal outcome, [`Excerpted::BodyDropped`], not an
//! error; the match and the rest of the evidence are still returned. A
//! location that does not fit the body that is stored ([`ExcerptError`]) is
//! a fault in the stored records.

use std::ops::Range;

use serde::{Deserialize, Serialize};

use crate::derived::provenance::span::SpanLocation;
use crate::ids::MessageHash;
use crate::observed::message::Message;
use crate::observed::message::text::NoPartText;
use crate::support::ByteRange;
use crate::wire::{Rejected, WireRequest};

/// How many bytes of context an excerpt shows on each side of the matched
/// range: `0..=MAX_CONTEXT`. A request: `{"context": 256}`, decoded through
/// [`ExcerptWindow::new`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", try_from = "RawExcerptWindow")]
pub struct ExcerptWindow {
    context: u16,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InvalidWindow {
    pub max: u16,
    pub got: u16,
}

/// [`ExcerptWindow`]'s field, decoded without the check.
#[derive(Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
struct RawExcerptWindow {
    context: u16,
}

impl TryFrom<RawExcerptWindow> for ExcerptWindow {
    type Error = Rejected<InvalidWindow>;

    fn try_from(raw: RawExcerptWindow) -> Result<Self, Self::Error> {
        Self::new(raw.context).map_err(|error| Rejected::new("excerpt window", error))
    }
}

/// A client picks how much context the evidence page shows.
impl WireRequest for ExcerptWindow {}

impl ExcerptWindow {
    pub const MAX_CONTEXT: u16 = 2048;

    /// 256 bytes either side: a few lines of prose.
    pub const DEFAULT: Self = Self { context: 256 };

    /// No context: the matched range alone. An export quotes matches with
    /// it (`export::rows::MatchText`).
    pub const MATCH_ONLY: Self = Self { context: 0 };

    pub fn new(context: u16) -> Result<Self, InvalidWindow> {
        if context > Self::MAX_CONTEXT {
            return Err(InvalidWindow {
                max: Self::MAX_CONTEXT,
                got: context,
            });
        }
        Ok(Self { context })
    }

    pub fn context(self) -> u16 {
        self.context
    }
}

impl Default for ExcerptWindow {
    fn default() -> Self {
        Self::DEFAULT
    }
}

/// A window of one part's text around a matched range, with the range
/// marked.
///
/// Built only through [`Excerpt::new`] (checked) and [`Excerpt::cut`]:
/// - the highlight is non-empty, lies inside `text` and starts and ends on
///   character boundaries, so `before`, `matched` and `after` are valid
///   UTF-8;
/// - at most [`ExcerptWindow::MAX_CONTEXT`] bytes of context on each side,
///   and at most [`Excerpt::MAX_HIGHLIGHT`] bytes highlighted;
/// - when the matched range was cut, the highlight ends the text;
/// - the byte counts add up to a part: the matched range ends within
///   `u32::MAX` bytes of the part's start (where a [`ByteRange`] can name
///   it), and the part's length fits a `u64`.
///
/// On the wire, its fields, the highlight as `{"start": .., "end": ..}`
/// (serde's form of a `Range`); decoding goes through [`Excerpt::new`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", try_from = "RawExcerpt")]
pub struct Excerpt {
    text: String,
    highlight: Range<u32>,
    elided_before: u64,
    elided_after: u64,
    highlight_cut: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InvalidExcerpt {
    /// `start >= end`.
    EmptyHighlight {
        start: u32,
        end: u32,
    },
    /// The highlight ends past the text.
    OutsideText {
        end: u32,
        len: usize,
    },
    /// A highlight boundary splits a character.
    NotCharBoundary {
        at: u32,
    },
    HighlightTooLong {
        len: u32,
        max: u32,
    },
    /// More context on one side than any window allows.
    ContextTooLong {
        len: usize,
        max: u16,
    },
    /// The highlight was cut, but text follows it.
    ContextAfterCut,
    /// The byte counts do not add up to a part: the matched range would
    /// end past `u32::MAX` (`elided_before + highlight.end + highlight_cut`),
    /// or the part's length overflows a `u64`.
    CountsOverflow,
}

/// [`Excerpt`]'s fields, decoded without the checks.
#[derive(Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
struct RawExcerpt {
    text: String,
    highlight: Range<u32>,
    elided_before: u64,
    elided_after: u64,
    highlight_cut: u64,
}

impl TryFrom<RawExcerpt> for Excerpt {
    type Error = Rejected<InvalidExcerpt>;

    fn try_from(raw: RawExcerpt) -> Result<Self, Self::Error> {
        Self::new(
            raw.text,
            raw.highlight,
            raw.elided_before,
            raw.elided_after,
            raw.highlight_cut,
        )
        .map_err(|error| Rejected::new("excerpt", error))
    }
}

/// Why a range cannot be cut from a part's text: the stored location does
/// not fit the stored text.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CutError {
    OutsideText { end: u32, len: usize },
    NotCharBoundary { at: u32 },
}

impl Excerpt {
    /// At most this many bytes of the matched range are shown.
    pub const MAX_HIGHLIGHT: u32 = 8192;

    pub fn new(
        text: String,
        highlight: Range<u32>,
        elided_before: u64,
        elided_after: u64,
        highlight_cut: u64,
    ) -> Result<Self, InvalidExcerpt> {
        let (start, end) = (highlight.start, highlight.end);
        if start >= end {
            return Err(InvalidExcerpt::EmptyHighlight { start, end });
        }
        let len = text.len();
        let end_at = usize::try_from(end).unwrap_or(usize::MAX);
        if end_at > len {
            return Err(InvalidExcerpt::OutsideText { end, len });
        }
        for at in [start, end] {
            let index = usize::try_from(at).unwrap_or(usize::MAX);
            if !text.is_char_boundary(index) {
                return Err(InvalidExcerpt::NotCharBoundary { at });
            }
        }
        if end - start > Self::MAX_HIGHLIGHT {
            return Err(InvalidExcerpt::HighlightTooLong {
                len: end - start,
                max: Self::MAX_HIGHLIGHT,
            });
        }
        let max = usize::from(ExcerptWindow::MAX_CONTEXT);
        for side in [usize::try_from(start).unwrap_or(usize::MAX), len - end_at] {
            if side > max {
                return Err(InvalidExcerpt::ContextTooLong {
                    len: side,
                    max: ExcerptWindow::MAX_CONTEXT,
                });
            }
        }
        if highlight_cut > 0 && end_at != len {
            return Err(InvalidExcerpt::ContextAfterCut);
        }
        let range_end = elided_before
            .checked_add(u64::from(end))
            .and_then(|at| at.checked_add(highlight_cut));
        let part_len = range_end
            .and_then(|at| at.checked_add(count(len - end_at)))
            .and_then(|at| at.checked_add(elided_after));
        if range_end.is_none_or(|at| at > u64::from(u32::MAX)) || part_len.is_none() {
            return Err(InvalidExcerpt::CountsOverflow);
        }
        Ok(Self {
            text,
            highlight,
            elided_before,
            elided_after,
            highlight_cut,
        })
    }

    /// The excerpt of `part` around `range`, as the module docs define it:
    /// `window.context()` bytes of context on each side at most, each edge
    /// moved inward to a character boundary; the range shown whole up to
    /// [`Excerpt::MAX_HIGHLIGHT`] bytes, a longer one cut at the last
    /// character boundary within it, with no context after it.
    pub fn cut(part: &str, range: ByteRange, window: ExcerptWindow) -> Result<Self, CutError> {
        let len = part.len();
        let (start, end) = (index(range.start()), index(range.end()));
        if end > len {
            return Err(CutError::OutsideText {
                end: range.end(),
                len,
            });
        }
        for (at, offset) in [(start, range.start()), (end, range.end())] {
            if !part.is_char_boundary(at) {
                return Err(CutError::NotCharBoundary { at: offset });
            }
        }
        let shown_end = if end - start > index(Self::MAX_HIGHLIGHT) {
            floor_boundary(part, start + index(Self::MAX_HIGHLIGHT))
        } else {
            end
        };
        let context = usize::from(window.context());
        let from = ceil_boundary(part, start.saturating_sub(context));
        let to = if shown_end < end {
            shown_end
        } else {
            floor_boundary(part, end.saturating_add(context).min(len))
        };
        Ok(Self {
            text: part[from..to].to_owned(),
            highlight: offset(start - from)..offset(shown_end - from),
            elided_before: count(from),
            elided_after: count(len - to.max(end)),
            highlight_cut: count(end - shown_end),
        })
    }

    /// The whole excerpt: context, highlight, context.
    pub fn text(&self) -> &str {
        &self.text
    }

    /// The highlight's byte range in [`Excerpt::text`].
    pub fn highlight(&self) -> Range<u32> {
        self.highlight.clone()
    }

    /// The context shown before the matched range.
    pub fn before(&self) -> &str {
        &self.text[..index(self.highlight.start)]
    }

    /// The matched range, or its first [`Excerpt::MAX_HIGHLIGHT`] bytes.
    pub fn matched(&self) -> &str {
        &self.text[index(self.highlight.start)..index(self.highlight.end)]
    }

    /// The context shown after the matched range; empty when it was cut.
    pub fn after(&self) -> &str {
        &self.text[index(self.highlight.end)..]
    }

    /// Bytes of the part before the excerpt.
    pub fn elided_before(&self) -> u64 {
        self.elided_before
    }

    /// Bytes of the part after the excerpt, beyond the matched range.
    pub fn elided_after(&self) -> u64 {
        self.elided_after
    }

    /// Bytes of the matched range not shown; 0 unless it was longer than
    /// [`Excerpt::MAX_HIGHLIGHT`].
    pub fn highlight_cut(&self) -> u64 {
        self.highlight_cut
    }

    /// The length of the part text the excerpt was cut from.
    pub fn part_len(&self) -> u64 {
        self.elided_before + count(self.text.len()) + self.highlight_cut + self.elided_after
    }
}

/// One side of a match on the evidence page: its excerpt, or why there is
/// none to show.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "type",
    content = "data",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum Excerpted {
    Shown(Excerpt),
    /// The blob store no longer holds `message`'s body: content retention
    /// dropped it. The match, its ids and its byte counts are still known.
    BodyDropped {
        message: MessageHash,
    },
}

/// A stored location that does not fit the stored body it names.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExcerptError {
    /// The body passed in is not the message the location names.
    WrongMessage {
        expected: MessageHash,
        got: MessageHash,
    },
    /// The location names a part that does not exist or has no text.
    Part(NoPartText),
    Cut(CutError),
}

impl Excerpted {
    /// The excerpt at `location`, given `body`: what `BlobStore::get`
    /// returned for `location.part.message`, decoded (`None` when it
    /// returned no body).
    pub fn of(
        location: SpanLocation,
        body: Option<&Message>,
        window: ExcerptWindow,
    ) -> Result<Self, ExcerptError> {
        let expected = location.part.message;
        let Some(body) = body else {
            return Ok(Self::BodyDropped { message: expected });
        };
        if body.hash != expected {
            return Err(ExcerptError::WrongMessage {
                expected,
                got: body.hash,
            });
        }
        let text = body
            .part_text(location.part.index)
            .map_err(ExcerptError::Part)?;
        Excerpt::cut(&text, location.range, window)
            .map(Self::Shown)
            .map_err(ExcerptError::Cut)
    }
}

/// The largest character boundary at or before `at` (`at <= text.len()`).
fn floor_boundary(text: &str, at: usize) -> usize {
    (0..=at)
        .rev()
        .find(|&i| text.is_char_boundary(i))
        .unwrap_or(0)
}

/// The smallest character boundary at or after `at` (`at <= text.len()`).
fn ceil_boundary(text: &str, at: usize) -> usize {
    (at..=text.len())
        .find(|&i| text.is_char_boundary(i))
        .unwrap_or(text.len())
}

/// A `u32` offset as an index. Lossless wherever the spec runs (at least
/// 32-bit `usize`).
fn index(offset: u32) -> usize {
    usize::try_from(offset).unwrap_or(usize::MAX)
}

/// An offset within an excerpt, which is at most
/// `MAX_HIGHLIGHT + 2 * MAX_CONTEXT` bytes long.
fn offset(at: usize) -> u32 {
    u32::try_from(at).unwrap_or(u32::MAX)
}

fn count(bytes: usize) -> u64 {
    u64::try_from(bytes).unwrap_or(u64::MAX)
}
