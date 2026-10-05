//! The text-bearing parts of a message, and the view of each that
//! provenance fingerprints.
//!
//! A part's text is `Message::part_text` (spans and matches index it). A
//! tool call's arguments are canonical JSON, so text an agent writes into a
//! tool call is JSON-escaped there while the reader of the delivered value
//! sees it unescaped; the view of an argument part is its JSON-unescaped
//! text, mapped back to the argument bytes, so both sides fingerprint the
//! same characters. Every other part is viewed as it is.

use std::borrow::Cow;

use crosstalk_spec::observed::message::{AssistantPart, Message, MessageBody, Role};

use crate::decode::{JsonStringDecoder, TextDecoder};
use crate::text::MappedText;

/// What kind of text a part holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PartKind {
    /// A text part, or visible reasoning: the author's own words.
    Text,
    /// A tool call's arguments (canonical JSON, or invalid text).
    ToolArguments,
    /// A tool result: a tool message's result, or a server tool's result
    /// inside an assistant message.
    ToolResult,
}

/// One text-bearing part.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TextPart<'a> {
    pub index: u16,
    pub kind: PartKind,
    pub text: Cow<'a, str>,
}

/// Every part of `message` with text, in order. Parts past `u16::MAX`
/// cannot be named by a `PartRef` and are left out.
pub fn text_parts(message: &Message) -> Vec<TextPart<'_>> {
    let count = message.part_count().min(usize::from(u16::MAX) + 1);
    (0..count)
        .filter_map(|at| {
            let index = u16::try_from(at).ok()?;
            let text = message.part_text(index).ok()?;
            Some(TextPart {
                index,
                kind: kind(message, at),
                text,
            })
        })
        .collect()
}

fn kind(message: &Message, at: usize) -> PartKind {
    match &message.body {
        MessageBody::Assistant(parts) => match parts.get(at) {
            Some(AssistantPart::ToolCall(_)) => PartKind::ToolArguments,
            Some(AssistantPart::ServerToolResult(_)) => PartKind::ToolResult,
            _ => PartKind::Text,
        },
        MessageBody::Tool(_) => PartKind::ToolResult,
        MessageBody::System(_) | MessageBody::User(_) => PartKind::Text,
    }
}

/// The role of `message`.
pub fn role(message: &Message) -> Role {
    message.body.role()
}

/// The view of a part's text that is fingerprinted, mapped into the text.
pub fn view(text: &str, kind: PartKind) -> MappedText {
    if kind == PartKind::ToolArguments
        && let Some(decoded) = JsonStringDecoder.decode_mapped(text).into_iter().next()
    {
        return decoded.text;
    }
    MappedText::identity(text)
}

/// A string value's key (`None` at the top level) and byte range.
pub type KeyedValue = (Option<String>, (u32, u32));

/// The byte ranges of every string value in a tool call's arguments
/// (between its quotes, escapes included), in order, with the key it sits
/// under (the nearest key before it: an array's members take the array's
/// key; a top-level string has none). Object keys and non-string values
/// are left out. `None` when `text` is not JSON, so the caller treats the
/// whole part as one text.
///
/// Originated spans in tool-call arguments are cut from these ranges, so a
/// span's view (its JSON-unescaped text) is text the tool received: the
/// decoded string value, never the keys or the structure around it
/// (`provenance.span.tool-arguments-per-value`).
pub fn keyed_string_values(text: &str) -> Option<Vec<KeyedValue>> {
    serde_json::from_str::<serde::de::IgnoredAny>(text).ok()?;
    let bytes = text.as_bytes();
    let mut values = Vec::new();
    let mut key: Option<String> = None;
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] != b'"' {
            index += 1;
            continue;
        }
        let start = index + 1;
        let mut end = start;
        while end < bytes.len() && bytes[end] != b'"' {
            end += if bytes[end] == b'\\' { 2 } else { 1 };
        }
        let mut next = end + 1;
        while next < bytes.len() && bytes[next].is_ascii_whitespace() {
            next += 1;
        }
        let raw = text.get(start..end.min(bytes.len()))?;
        if bytes.get(next) == Some(&b':') {
            key = Some(serde_json::from_str::<String>(&format!("\"{raw}\"")).ok()?);
        } else if end > start {
            values.push((
                key.clone(),
                (u32::try_from(start).ok()?, u32::try_from(end).ok()?),
            ));
        }
        index = end + 1;
    }
    Some(values)
}

/// [`keyed_string_values`] without the keys.
pub fn string_values(text: &str) -> Option<Vec<(u32, u32)>> {
    keyed_string_values(text).map(|values| values.into_iter().map(|(_, range)| range).collect())
}
