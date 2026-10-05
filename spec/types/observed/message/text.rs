//! The text of one message part: what a [`SpanLocation`]'s byte range
//! indexes.
//!
//! Provenance segments, fingerprints and matches this text, and the
//! evidence page cuts its excerpts from it, so both read the same bytes for
//! the same `(PartRef, ByteRange)`. Per part:
//!
//! | Part | Text |
//! | --- | --- |
//! | `Text` (any role) | the text |
//! | `Reasoning::Visible` | the text (not its signature) |
//! | `ToolCall` | its arguments: the canonical JSON, or the invalid text kept verbatim (not its id or signature) |
//! | `ToolResult`, `ServerToolResult` | its `Text` contents in order, joined with [`TOOL_RESULT_SEPARATOR`] |
//! | `Reasoning::Opaque`, `Media`, `Unknown`, a tool result with no `Text` content | none |
//!
//! A part with no text holds no span and no match, so a location naming one
//! is a fault in the stored records, as is a location past the last part.
//!
//! Opaque provider material is never part text: tool-call ids
//! (`ToolCall::id`, `ToolResult::call_id`), reasoning and tool-call
//! signatures, and `Reasoning::Opaque` payloads. They are long, provider-
//! shaped strings (base64 signatures, Gemini ids that embed one after
//! `__thought__`) that agents of one provider share in structure, and no
//! agent wrote them; provenance segments, decodes and fingerprints part text
//! only (`provenance.decode.part-text-input`), so they never become a span
//! or a match.
//!
//! [`SpanLocation`]: crate::derived::provenance::span::SpanLocation

use std::borrow::Cow;

use super::{
    AssistantPart, Message, MessageBody, Reasoning, SystemPart, ToolArguments, ToolResult,
    ToolResultContent, UserPart,
};

/// Joins the `Text` contents of one tool result into its part text.
pub const TOOL_RESULT_SEPARATOR: &str = "\n";

/// Why a part has no text to index.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NoPartText {
    /// The message has `parts` parts, none at `index`.
    NoSuchPart { index: u16, parts: usize },
    /// The part is media, opaque reasoning, an unknown block, or a tool
    /// result without text.
    NotText { index: u16 },
}

impl Message {
    /// How many parts the body holds: the range of a [`PartRef::index`].
    ///
    /// [`PartRef::index`]: super::PartRef::index
    pub fn part_count(&self) -> usize {
        match &self.body {
            MessageBody::System(parts) => parts.len(),
            MessageBody::User(parts) => parts.len(),
            MessageBody::Assistant(parts) => parts.len(),
            MessageBody::Tool(results) => results.iter().count(),
        }
    }

    /// The text of part `index`, as the module table defines it.
    pub fn part_text(&self, index: u16) -> Result<Cow<'_, str>, NoPartText> {
        let at = usize::from(index);
        let missing = NoPartText::NoSuchPart {
            index,
            parts: self.part_count(),
        };
        let not_text = NoPartText::NotText { index };
        match &self.body {
            MessageBody::System(parts) => match parts.get(at).ok_or(missing)? {
                SystemPart::Text(text) => Ok(Cow::Borrowed(text.0.as_str())),
                SystemPart::Unknown(_) => Err(not_text),
            },
            MessageBody::User(parts) => match parts.get(at).ok_or(missing)? {
                UserPart::Text(text) => Ok(Cow::Borrowed(text.0.as_str())),
                UserPart::Media(_) | UserPart::Unknown(_) => Err(not_text),
            },
            MessageBody::Assistant(parts) => match parts.get(at).ok_or(missing)? {
                AssistantPart::Text(text)
                | AssistantPart::Reasoning(Reasoning::Visible { text, .. }) => {
                    Ok(Cow::Borrowed(text.0.as_str()))
                }
                AssistantPart::ToolCall(call) => Ok(Cow::Borrowed(match &call.arguments {
                    ToolArguments::Json(json) => json.0.as_str(),
                    ToolArguments::Invalid(raw) => raw.as_str(),
                })),
                AssistantPart::ServerToolResult(result) => result_text(result).ok_or(not_text),
                AssistantPart::Reasoning(Reasoning::Opaque { .. }) | AssistantPart::Unknown(_) => {
                    Err(not_text)
                }
            },
            MessageBody::Tool(results) => {
                result_text(results.iter().nth(at).ok_or(missing)?).ok_or(not_text)
            }
        }
    }
}

/// A tool result's `Text` contents joined with [`TOOL_RESULT_SEPARATOR`];
/// `None` when it has none. A single text content is borrowed.
fn result_text(result: &ToolResult) -> Option<Cow<'_, str>> {
    let mut texts = result.content.iter().filter_map(|content| match content {
        ToolResultContent::Text(text) => Some(text.0.as_str()),
        ToolResultContent::Media(_) | ToolResultContent::Unknown(_) => None,
    });
    let first = texts.next()?;
    let rest: Vec<&str> = texts.collect();
    if rest.is_empty() {
        return Some(Cow::Borrowed(first));
    }
    let mut joined = first.to_owned();
    for text in rest {
        joined.push_str(TOOL_RESULT_SEPARATOR);
        joined.push_str(text);
    }
    Some(Cow::Owned(joined))
}
