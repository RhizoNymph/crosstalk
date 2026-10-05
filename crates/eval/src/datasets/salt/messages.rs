//! SALT's OpenAI-chat messages as canonical messages.
//!
//! | SALT | Canonical |
//! | --- | --- |
//! | `system` | `System([Text])` |
//! | `user` | `User([Text])` |
//! | `assistant` | `Assistant`: reasoning parts, then the text (when not empty), then tool calls |
//! | `tool` | `Tool([ToolResult])`, `Error` when the result is JSON with `"success": false` |
//!
//! Assistant reasoning: each `thinking_blocks` entry is `Reasoning::Visible`
//! plus, when signed, `Reasoning::Opaque` with its signature; otherwise
//! `reasoning_content` (or `reasoning`) is `Visible`. `reasoning_items`'
//! `encrypted_content` and `provider_specific_fields.thought_signatures` are
//! `Opaque`. Opaque parts have no part text, so no span can hold them. Tool
//! call ids are kept whole, Gemini's embedded thought signatures included.

use crosstalk_spec::observed::message::{
    AssistantPart, MessageBody, Reasoning, SystemPart, Text, ToolArguments, ToolCall, ToolCallId,
    ToolExecution, ToolName, ToolOutcome, ToolResult, ToolResultContent, UserPart,
};
use crosstalk_spec::support::NonEmpty;
use serde_json::Value;

use super::SaltError;
use super::schema::RawMessage;
use crosstalk_spec::observed::message::json::canonicalize;

/// The text of a message's `content`: a string, `null`, or a list of parts
/// whose `text` members are joined.
pub fn content_text(content: &Value) -> String {
    match content {
        Value::String(text) => text.clone(),
        Value::Array(parts) => parts
            .iter()
            .filter_map(|part| part.get("text").and_then(Value::as_str))
            .collect::<Vec<_>>()
            .join("\n"),
        _ => String::new(),
    }
}

/// One SALT message as a canonical body.
pub fn convert(raw: &RawMessage) -> Result<MessageBody, SaltError> {
    let text = content_text(&raw.content);
    match raw.role.as_str() {
        "system" => Ok(MessageBody::System(vec![SystemPart::Text(Text(text))])),
        "user" => Ok(MessageBody::User(vec![UserPart::Text(Text(text))])),
        "assistant" => Ok(MessageBody::Assistant(assistant_parts(raw, text))),
        "tool" => {
            let outcome = match serde_json::from_str::<Value>(&text) {
                Ok(Value::Object(members))
                    if members.get("success") == Some(&Value::Bool(false)) =>
                {
                    ToolOutcome::Error
                }
                _ => ToolOutcome::Success,
            };
            Ok(MessageBody::Tool(NonEmpty::new(ToolResult {
                call_id: ToolCallId(raw.tool_call_id.clone().unwrap_or_default()),
                content: vec![ToolResultContent::Text(Text(text))],
                outcome,
            })))
        }
        other => Err(SaltError::UnknownRole(other.to_owned())),
    }
}

fn assistant_parts(raw: &RawMessage, text: String) -> Vec<AssistantPart> {
    let mut parts = Vec::new();
    match &raw.thinking_blocks {
        Some(blocks) if !blocks.is_empty() => {
            for block in blocks {
                let signature = block
                    .get("signature")
                    .and_then(Value::as_str)
                    .filter(|signature| !signature.is_empty())
                    .map(str::to_owned);
                match block.get("thinking").and_then(Value::as_str) {
                    Some(thinking) if !thinking.is_empty() => {
                        parts.push(AssistantPart::Reasoning(Reasoning::Visible {
                            text: Text(thinking.into()),
                            signature,
                        }));
                    }
                    _ => {
                        // Redacted thinking: only the opaque payload.
                        if let Some(data) = block
                            .get("data")
                            .and_then(Value::as_str)
                            .map(str::to_owned)
                            .or(signature)
                        {
                            parts.push(opaque(&data));
                        }
                    }
                }
            }
        }
        _ => {
            let visible = raw
                .reasoning_content
                .as_ref()
                .or(raw.reasoning.as_ref())
                .and_then(Value::as_str)
                .filter(|text| !text.is_empty());
            if let Some(visible) = visible {
                parts.push(AssistantPart::Reasoning(Reasoning::Visible {
                    text: Text(visible.into()),
                    signature: None,
                }));
            }
        }
    }
    for item in raw.reasoning_items.iter().flatten() {
        if let Some(encrypted) = item.get("encrypted_content").and_then(Value::as_str) {
            parts.push(opaque(encrypted));
        }
    }
    if let Some(Value::Array(signatures)) = raw
        .provider_specific_fields
        .as_ref()
        .and_then(|fields| fields.get("thought_signatures"))
    {
        parts.extend(signatures.iter().filter_map(Value::as_str).map(opaque));
    }
    if !text.is_empty() {
        parts.push(AssistantPart::Text(Text(text)));
    }
    for call in raw.tool_calls.iter().flatten() {
        parts.push(AssistantPart::ToolCall(ToolCall {
            id: ToolCallId(call.id.clone()),
            name: ToolName(call.function.name.clone()),
            arguments: arguments(&call.function.arguments),
            execution: ToolExecution::Client,
            signature: None,
        }));
    }
    parts
}

fn opaque(signature: &str) -> AssistantPart {
    AssistantPart::Reasoning(Reasoning::Opaque {
        signature: signature.into(),
    })
}

/// Arguments as canonical JSON when they parse, else kept verbatim.
pub fn arguments(raw: &Value) -> ToolArguments {
    match raw {
        Value::String(text) => match canonicalize(text) {
            Ok(json) => ToolArguments::Json(json),
            Err(_) => ToolArguments::Invalid(text.clone()),
        },
        Value::Null => ToolArguments::Invalid(String::new()),
        other => match canonicalize(&other.to_string()) {
            Ok(json) => ToolArguments::Json(json),
            Err(_) => ToolArguments::Invalid(other.to_string()),
        },
    }
}

/// The string member `name` of a tool call's arguments.
pub fn argument(raw: &Value, name: &str) -> Option<String> {
    let parsed = match raw {
        Value::String(text) => serde_json::from_str::<Value>(text).ok()?,
        other => other.clone(),
    };
    parsed.get(name).and_then(Value::as_str).map(str::to_owned)
}
