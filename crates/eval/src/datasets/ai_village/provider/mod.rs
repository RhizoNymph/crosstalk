//! Provider-shaped model responses as canonical assistant messages.
//!
//! `computer_use_turns.agent_messages` and `events.data.output` hold the raw
//! response of whichever provider the agent ran on. [`response`] tells the
//! shape apart and converts it:
//!
//! | Shape | Detected by | Module |
//! | --- | --- | --- |
//! | Anthropic Messages object | an object with a `content` array of typed blocks | [`anthropic`] |
//! | OpenAI Responses item list | an array | [`openai::responses`] |
//! | OpenAI chat completion message | an object with `role` (and no block array) | [`openai::chat`] |
//! | Gemini `generateContent` | an object with `candidates` | [`gemini`] |
//!
//! Scrub markers are not data: a `[BLOB_REMOVED]` signature is dropped
//! (no `Reasoning::Opaque`, no `signature` on visible reasoning), and an
//! image block is dropped. Encrypted reasoning that survived the scrub is
//! `Reasoning::Opaque`, which has no part text. Empty text parts are
//! dropped. Unknown blocks are kept as `Unknown` with their canonical JSON.

pub mod anthropic;
pub mod gemini;
pub mod openai;

use crosstalk_spec::observed::exchange::{StopReason, WireProtocol};
use crosstalk_spec::observed::message::json::canonicalize;
use crosstalk_spec::observed::message::{
    AssistantPart, CanonicalJson, MessageBody, Reasoning, Text, ToolArguments, ToolCallId, Unknown,
};
use serde_json::Value;

/// The scrub marker for removed blobs (signatures, long base64).
pub const BLOB_REMOVED: &str = "[BLOB_REMOVED]";

/// One converted response.
#[derive(Debug, Clone, PartialEq)]
pub struct Response {
    pub protocol: WireProtocol,
    pub parts: Vec<AssistantPart>,
    pub stop: StopReason,
}

impl Response {
    pub fn body(&self) -> MessageBody {
        MessageBody::Assistant(self.parts.clone())
    }

    pub fn into_body(self) -> MessageBody {
        MessageBody::Assistant(self.parts)
    }

    /// The ids of the client tool calls it makes, in order.
    pub fn call_ids(&self) -> Vec<ToolCallId> {
        self.parts
            .iter()
            .filter_map(|part| match part {
                AssistantPart::ToolCall(call) => Some(call.id.clone()),
                _ => None,
            })
            .collect()
    }
}

/// Converts a raw response. `fallback_id` names tool calls the provider gave
/// no id (Gemini): the `n`th becomes `<fallback_id>-<n>`.
pub fn response(raw: &Value, fallback_id: &str) -> Response {
    match raw {
        Value::Array(items) => openai::responses::convert(items),
        Value::Object(members) if members.contains_key("candidates") => {
            gemini::convert(raw, fallback_id)
        }
        Value::Object(members)
            if members
                .get("content")
                .and_then(Value::as_array)
                .is_some_and(|blocks| blocks.iter().all(|block| block.get("type").is_some())) =>
        {
            anthropic::convert(raw)
        }
        Value::Object(members) if members.contains_key("role") => openai::chat::convert(raw),
        Value::Null => Response {
            protocol: WireProtocol::OpenAiChat,
            parts: Vec::new(),
            stop: StopReason::Other,
        },
        other => Response {
            protocol: WireProtocol::OpenAiChat,
            parts: vec![unknown("response", other)],
            stop: StopReason::Other,
        },
    }
}

/// Tool-call arguments: a JSON string or a JSON value, as canonical JSON
/// when they parse, else kept verbatim.
pub fn arguments(raw: &Value) -> ToolArguments {
    match raw {
        Value::String(text) => match canonicalize(text) {
            Ok(json) => ToolArguments::Json(json),
            Err(_) => ToolArguments::Invalid(text.clone()),
        },
        Value::Null => ToolArguments::Json(CanonicalJson("{}".to_owned())),
        other => {
            let text = other.to_string();
            match canonicalize(&text) {
                Ok(json) => ToolArguments::Json(json),
                Err(_) => ToolArguments::Invalid(text),
            }
        }
    }
}

/// A block the converter does not model, kept as canonical JSON.
pub fn unknown(kind: &str, raw: &Value) -> AssistantPart {
    AssistantPart::Unknown(unknown_block(kind, raw))
}

pub fn unknown_block(kind: &str, raw: &Value) -> Unknown {
    let text = raw.to_string();
    Unknown {
        kind: kind.to_owned(),
        raw: canonicalize(&text).unwrap_or(CanonicalJson(text)),
    }
}

/// Visible reasoning, dropped when empty.
pub fn visible(text: &str, signature: Option<&str>) -> Option<AssistantPart> {
    if text.is_empty() {
        return None;
    }
    Some(AssistantPart::Reasoning(Reasoning::Visible {
        text: Text(text.to_owned()),
        signature: signature
            .filter(|signature| !signature.is_empty() && *signature != BLOB_REMOVED)
            .map(str::to_owned),
    }))
}

/// Opaque reasoning, dropped when empty or scrubbed.
pub fn opaque(payload: &str) -> Option<AssistantPart> {
    if payload.is_empty() || payload == BLOB_REMOVED {
        return None;
    }
    Some(AssistantPart::Reasoning(Reasoning::Opaque {
        signature: payload.to_owned(),
    }))
}

/// A text part, dropped when empty.
pub fn text(text: &str) -> Option<AssistantPart> {
    (!text.is_empty()).then(|| AssistantPart::Text(Text(text.to_owned())))
}

/// `ToolUse` when the parts call a tool, else `fallback`.
pub fn stop_for(parts: &[AssistantPart], fallback: StopReason) -> StopReason {
    if parts
        .iter()
        .any(|part| matches!(part, AssistantPart::ToolCall(_)))
    {
        StopReason::ToolUse
    } else {
        fallback
    }
}
