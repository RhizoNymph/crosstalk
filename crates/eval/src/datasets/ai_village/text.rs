//! Locating labelled text and deciding the match it needs.

use crosstalk_spec::observed::message::{Message, MessageBody};

use crosstalk_spec::derived::provenance::matching::Codec;

use crate::reference::fold::fold;
use crate::truth::MatchNeed;

/// `text` as the contents of a JSON string (no quotes), escaped the way
/// both `JSON.stringify` and serde_json escape: `"`, `\`, `\b`, `\f`, `\n`,
/// `\r`, `\t`, and other control characters as `\u00xx`.
pub fn json_escape(text: &str) -> String {
    let quoted = serde_json::Value::String(text.to_owned()).to_string();
    quoted[1..quoted.len() - 1].to_owned()
}

/// The value of `inner` read as the inside of a JSON string literal; `None`
/// when it is not one (a bare `"`, a raw control character, a bad escape).
pub fn json_unescape(inner: &str) -> Option<String> {
    serde_json::from_str(&format!("\"{inner}\"")).ok()
}

/// Whether escaping changes `text`.
pub fn escapes(text: &str) -> bool {
    text.chars()
        .any(|ch| matches!(ch, '"' | '\\' | '\u{0}'..='\u{1f}'))
}

/// The byte range of `needle` in `haystack`: the last occurrence starting
/// before `before` when given and there is one, else the first.
pub fn find(haystack: &str, needle: &str, before: Option<usize>) -> Option<(usize, usize)> {
    if needle.is_empty() {
        return None;
    }
    let start = match before {
        Some(limit) => haystack[..limit.min(haystack.len())]
            .rfind(needle)
            .or_else(|| haystack.find(needle)),
        None => haystack.find(needle),
    }?;
    Some((start, start + needle.len()))
}

/// The texts of every part of `message` that has one.
pub fn part_texts(message: &Message) -> Vec<String> {
    (0..message.part_count())
        .filter_map(|part| u16::try_from(part).ok())
        .filter_map(|part| message.part_text(part).ok().map(|t| t.into_owned()))
        .collect()
}

/// The weakest match a detector needs to tie `read` (the reader's bytes) to
/// the sender's `response`: `Exact` when the bytes occur verbatim in one of
/// its parts; `Decoded([JsonString])` when `read` is the inside of a JSON
/// string whose value (one level of unescaping, the spec's
/// `Codec::JsonString`) occurs verbatim; `Normalized` when they do after
/// folding (escapes, case, whitespace); else `Semantic`.
pub fn need(response: &Message, read: &str) -> MatchNeed {
    let texts = part_texts(response);
    if texts.iter().any(|text| text.contains(read)) {
        return MatchNeed::Exact;
    }
    if let Some(value) = json_unescape(read)
        && value != read
        && !value.trim().is_empty()
        && texts.iter().any(|text| text.contains(&value))
    {
        return MatchNeed::Decoded {
            codecs: vec![Codec::JsonString],
        };
    }
    let folded = fold(read, 0).text;
    if !folded.trim().is_empty()
        && texts
            .iter()
            .any(|text| fold(text, 0).text.contains(&folded))
    {
        return MatchNeed::Normalized;
    }
    MatchNeed::Semantic
}

/// The text of an assistant body's text and reasoning parts, joined, for
/// keyword checks.
pub fn visible_text(body: &MessageBody) -> String {
    match body {
        MessageBody::Assistant(parts) => parts
            .iter()
            .filter_map(|part| match part {
                crosstalk_spec::observed::message::AssistantPart::Text(text) => {
                    Some(text.0.as_str())
                }
                crosstalk_spec::observed::message::AssistantPart::Reasoning(
                    crosstalk_spec::observed::message::Reasoning::Visible { text, .. },
                ) => Some(text.0.as_str()),
                _ => None,
            })
            .collect::<Vec<_>>()
            .join("\n"),
        _ => String::new(),
    }
}
