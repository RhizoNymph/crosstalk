//! Text as provenance reads it: whitespace and case normalization with a
//! map back to source bytes ([`normalize`]), and decoded text that remembers
//! where each of its bytes came from ([`mapped`]).

pub mod mapped;
pub mod normalize;

pub use mapped::{MappedBuilder, MappedText};
pub use normalize::{NormChar, normalize};

/// `text`'s length as a byte offset, or `None` past `u32::MAX` bytes (spans
/// and ranges are `u32` offsets).
pub fn offset_len(text: &str) -> Option<u32> {
    u32::try_from(text.len()).ok()
}

/// `[start, end)` of `text` without leading and trailing whitespace; `None`
/// when only whitespace is left. Both ends stay on character boundaries.
pub fn trim_range(text: &str, start: u32, end: u32) -> Option<(u32, u32)> {
    let from = usize::try_from(start).ok()?;
    let to = usize::try_from(end).ok()?;
    let slice = text.get(from..to)?;
    let leading = slice.len() - slice.trim_start().len();
    let trailing = slice.len() - slice.trim_end().len();
    if leading + trailing >= slice.len() {
        return None;
    }
    let start = start + u32::try_from(leading).ok()?;
    let end = end - u32::try_from(trailing).ok()?;
    Some((start, end))
}
