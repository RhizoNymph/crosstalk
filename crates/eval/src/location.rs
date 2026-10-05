//! Where text sits: helpers over the spec's `SpanLocation`.
//!
//! A location names one part of one message (by hash, so it is the same in
//! every exchange that carries the message) and a byte range into that
//! part's text ([`Message::part_text`]). Truth and predictions both locate
//! content this way, and the scorer's alignment rule compares locations.

use crosstalk_spec::derived::provenance::span::SpanLocation;
use crosstalk_spec::ids::MessageHash;
use crosstalk_spec::observed::message::{Message, PartRef};
use crosstalk_spec::support::ByteRange;

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum LocationError {
    #[error("empty range {start}..{end}")]
    Empty { start: u32, end: u32 },
    #[error("part {part} of the message has no text")]
    NoText { part: u16 },
    #[error("range {start}..{end} is outside the part's {len} bytes or splits a character")]
    OutOfText { start: u32, end: u32, len: usize },
    #[error("the location names another message")]
    OtherMessage,
}

/// `[start, end)` in part `part` of `message`; refuses an empty range.
pub fn location(
    message: MessageHash,
    part: u16,
    start: u32,
    end: u32,
) -> Result<SpanLocation, LocationError> {
    let range = ByteRange::new(start, end).map_err(|_| LocationError::Empty { start, end })?;
    Ok(SpanLocation {
        part: PartRef {
            message,
            index: part,
        },
        range,
    })
}

/// A location checked against the message it names: the part has text and
/// the range falls inside it on character boundaries.
pub fn in_message(
    message: &Message,
    part: u16,
    start: u32,
    end: u32,
) -> Result<SpanLocation, LocationError> {
    let at = location(message.hash, part, start, end)?;
    at.text(message)?;
    Ok(at)
}

/// The whole text of part `part` of `message`.
pub fn whole_part(message: &Message, part: u16) -> Result<SpanLocation, LocationError> {
    let text = message
        .part_text(part)
        .map_err(|_| LocationError::NoText { part })?;
    let len = u32::try_from(text.len()).map_err(|_| LocationError::OutOfText {
        start: 0,
        end: u32::MAX,
        len: text.len(),
    })?;
    location(message.hash, part, 0, len)
}

/// A total order for locations (the spec type has none), for sorting.
pub fn sort_key(at: &SpanLocation) -> (MessageHash, u16, u32, u32) {
    (
        at.part.message,
        at.part.index,
        at.range.start(),
        at.range.end(),
    )
}

/// What the eval asks of a location.
pub trait SpanLocationExt {
    fn message(&self) -> MessageHash;

    /// Its length in bytes; never zero.
    fn len(&self) -> u32;

    /// Always false: a location is never empty.
    fn is_empty(&self) -> bool;

    /// Whether the two share at least one byte of one part.
    fn overlaps(&self, other: &SpanLocation) -> bool;

    /// The text it cuts from `message`.
    fn text(&self, message: &Message) -> Result<String, LocationError>;
}

impl SpanLocationExt for SpanLocation {
    fn message(&self) -> MessageHash {
        self.part.message
    }

    fn len(&self) -> u32 {
        self.range.len().get()
    }

    fn is_empty(&self) -> bool {
        false
    }

    fn overlaps(&self, other: &SpanLocation) -> bool {
        self.part == other.part
            && self.range.start() < other.range.end()
            && other.range.start() < self.range.end()
    }

    fn text(&self, message: &Message) -> Result<String, LocationError> {
        if message.hash != self.part.message {
            return Err(LocationError::OtherMessage);
        }
        let part = self.part.index;
        let text = message
            .part_text(part)
            .map_err(|_| LocationError::NoText { part })?;
        let (start, end) = (self.range.start(), self.range.end());
        text.get(start as usize..end as usize)
            .map(str::to_owned)
            .ok_or(LocationError::OutOfText {
                start,
                end,
                len: text.len(),
            })
    }
}
