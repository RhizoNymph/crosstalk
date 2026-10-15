//! Where text sits: the whole text of one part of one message, as the
//! spec's `SpanLocation` (by message hash, so it is the same in every
//! exchange that carries the message, and a byte range into that part's
//! text, [`Message::part_text`]). A co-access's read and write are located
//! this way.

use crosstalk_spec::derived::provenance::span::SpanLocation;
use crosstalk_spec::observed::message::{Message, PartRef};
use crosstalk_spec::support::ByteRange;

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum LocationError {
    #[error("part {part} of the message has no text")]
    NoText { part: u16 },
    #[error("part {part}'s text is empty or longer than a location can hold ({len} bytes)")]
    Unlocatable { part: u16, len: usize },
}

/// The whole text of part `part` of `message`.
pub fn whole_part(message: &Message, part: u16) -> Result<SpanLocation, LocationError> {
    let text = message
        .part_text(part)
        .map_err(|_| LocationError::NoText { part })?;
    let unlocatable = || LocationError::Unlocatable {
        part,
        len: text.len(),
    };
    let len = u32::try_from(text.len()).map_err(|_| unlocatable())?;
    let range = ByteRange::new(0, len).map_err(|_| unlocatable())?;
    Ok(SpanLocation {
        part: PartRef {
            message: message.hash,
            index: part,
        },
        range,
    })
}
