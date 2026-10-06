//! The text of a conversation's turns (Content): [`ConversationText`],
//! aligned with the structure `conversation_turns` returns for the same
//! window, and one slice of one part's text ([`PartText`]), the "show
//! more" of a clipped part.
//!
//! Text is `Message::part_text`, the bytes every mark's range indexes, cut
//! on character boundaries, so a mark's range applies to a slice at offset
//! `range.start - slice.from()`.

use std::num::NonZeroU32;

use serde::{Deserialize, Serialize};

use crate::ids::{ConversationId, MessageHash};
use crate::observed::message::text::NoPartText;
use crate::wire::{Rejected, WireRequest};

use super::TurnIndex;

/// How many bytes of a part's text one read returns: `1..=MAX`. On the
/// wire a number.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "u32", into = "u32")]
pub struct TextLimit(NonZeroU32);

/// A text limit outside `1..=TextLimit::MAX`. The surface reports it as
/// `InvalidInput(TextLimitOutOfRange)`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InvalidTextLimit {
    pub max: u32,
    pub got: u32,
}

impl TextLimit {
    pub const MAX: u32 = 65_536;
    /// 8 KiB: what the conversation page asks for.
    pub const DEFAULT: Self = Self(NonZeroU32::new(8192).expect("8192 is not zero"));

    pub fn new(bytes: u32) -> Result<Self, InvalidTextLimit> {
        match NonZeroU32::new(bytes) {
            Some(bytes) if bytes.get() <= Self::MAX => Ok(Self(bytes)),
            Some(_) | None => Err(InvalidTextLimit {
                max: Self::MAX,
                got: bytes,
            }),
        }
    }

    pub fn get(self) -> u32 {
        self.0.get()
    }
}

impl TryFrom<u32> for TextLimit {
    type Error = Rejected<InvalidTextLimit>;

    fn try_from(bytes: u32) -> Result<Self, Self::Error> {
        Self::new(bytes).map_err(|error| Rejected::new("text limit", error))
    }
}

impl From<TextLimit> for u32 {
    fn from(limit: TextLimit) -> Self {
        limit.get()
    }
}

/// A text limit is a request on its own (`conversation_text`'s `limit`).
impl WireRequest for TextLimit {}

/// Bytes `from .. from + limit` of one part's text. A request:
/// `{"from": 8192, "limit": 8192}`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct TextSlice {
    pub from: u32,
    pub limit: TextLimit,
}

impl WireRequest for TextSlice {}

/// The text of the turns one window names.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct ConversationText {
    pub conversation: ConversationId,
    /// The same turns, in the same order, as `conversation_turns` returns
    /// for the window (`surface.conversation.text-aligns`).
    pub turns: Vec<TurnText>,
}

/// One turn's text, aligned with `Turn`: `inputs` then `output`, the same
/// messages in the same order.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct TurnText {
    pub index: TurnIndex,
    pub inputs: Vec<MessageText>,
    pub output: Option<MessageText>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct MessageText {
    pub hash: MessageHash,
    pub body: BodyText,
}

/// One message's text.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "type",
    content = "data",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum BodyText {
    /// One per part, in part order: the part's text from its start, clipped
    /// to the limit, or `None` for a part with no text.
    Shown(Vec<Option<PartText>>),
    /// Content retention dropped the body (as `Excerpted::BodyDropped`);
    /// the rest of the read is returned (`surface.conversation.body-dropped`).
    BodyDropped,
}

/// A slice of one part's text: bytes `from .. from + text.len()` of
/// `Message::part_text`, cut on character boundaries, and the whole part's
/// length (`surface.conversation.text-slice`).
///
/// Built only through [`PartText::cut`]; decoding checks what a slice knows
/// about itself (it lies within `part_len`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", try_from = "RawPartText")]
pub struct PartText {
    from: u32,
    text: String,
    part_len: u32,
}

#[derive(Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
struct RawPartText {
    from: u32,
    text: String,
    part_len: u32,
}

/// A decoded [`PartText`] that does not lie within its part.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SliceOutsidePart {
    pub from: u32,
    pub len: usize,
    pub part_len: u32,
}

impl TryFrom<RawPartText> for PartText {
    type Error = Rejected<SliceOutsidePart>;

    fn try_from(raw: RawPartText) -> Result<Self, Self::Error> {
        let end = u64::from(raw.from) + raw.text.len() as u64;
        if end > u64::from(raw.part_len) {
            return Err(Rejected::new(
                "part text",
                SliceOutsidePart {
                    from: raw.from,
                    len: raw.text.len(),
                    part_len: raw.part_len,
                },
            ));
        }
        Ok(Self {
            from: raw.from,
            text: raw.text,
            part_len: raw.part_len,
        })
    }
}

/// Why a part's text could not be cut.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TextError {
    /// The part has no text, or the message has no such part.
    Part(NoPartText),
    /// `from` is past the end of the part's text or not on a character
    /// boundary.
    Slice { from: u32, part_len: u32 },
}

impl PartText {
    /// `part[from..]` clipped to at most `limit` bytes, ending on a
    /// character boundary; at least one character when any remain, even
    /// when that character is longer than `limit`. `from == part.len()` is
    /// an empty slice. A part longer than `u32::MAX` bytes cannot be cut.
    pub fn cut(part: &str, from: u32, limit: TextLimit) -> Result<Self, TextError> {
        let part_len = u32::try_from(part.len()).map_err(|_| TextError::Slice {
            from,
            part_len: u32::MAX,
        })?;
        let start = from as usize;
        if start > part.len() || !part.is_char_boundary(start) {
            return Err(TextError::Slice { from, part_len });
        }
        let mut end = start.saturating_add(limit.get() as usize).min(part.len());
        while !part.is_char_boundary(end) {
            end -= 1;
        }
        if end == start && start < part.len() {
            end = part[start..]
                .chars()
                .next()
                .map_or(start, |first| start + first.len_utf8());
        }
        Ok(Self {
            from,
            text: part[start..end].to_owned(),
            part_len,
        })
    }

    /// The offset of `text` in the part's text.
    pub fn from(&self) -> u32 {
        self.from
    }

    pub fn text(&self) -> &str {
        &self.text
    }

    /// The whole part text's length in bytes.
    pub fn part_len(&self) -> u32 {
        self.part_len
    }

    /// Bytes of the part after this slice: `part_len - from - text.len()`.
    pub fn remaining(&self) -> u32 {
        let cut = u32::try_from(self.text.len()).unwrap_or(u32::MAX);
        self.part_len.saturating_sub(self.from).saturating_sub(cut)
    }
}
