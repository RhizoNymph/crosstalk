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
