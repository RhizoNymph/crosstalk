//! Anthropic Messages content blocks: model responses (standard scaffolding
//! and Claude Code alike) and Claude Code's tool results.
//!
//! | Block | Canonical |
//! | --- | --- |
//! | `text` | `Text` (dropped when empty) |
//! | `thinking` | `Reasoning::Visible` with its signature (unless scrubbed) |
//! | `redacted_thinking` | `Reasoning::Opaque` (unless scrubbed) |
//! | `tool_use` | `ToolCall`, client-executed |
//! | `server_tool_use` | `ToolCall`, server-executed |
//! | `*_tool_result` | `ServerToolResult` with the result as canonical JSON text |
//! | `tool_result` (in a user turn) | `ToolResult`: text contents kept, images dropped, `is_error` → `Error` |
//! | anything else | `Unknown` |

use crosstalk_spec::observed::exchange::{StopReason, WireProtocol};
use crosstalk_spec::observed::message::{
    AssistantPart, Text, ToolCall, ToolCallId, ToolExecution, ToolName, ToolOutcome, ToolResult,
    ToolResultContent,
};
use serde_json::Value;

use super::{Response, arguments, opaque, text, unknown, visible};

/// A whole Anthropic message object.
pub fn convert(raw: &Value) -> Response {
    let parts: Vec<AssistantPart> = raw
        .get("content")
        .and_then(Value::as_array)
        .map(|blocks| blocks.iter().filter_map(block).collect())
        .unwrap_or_default();
    let stop = stop_reason(raw.get("stop_reason").and_then(Value::as_str), &parts);
    Response {
        protocol: WireProtocol::AnthropicMessages,
        parts,
        stop,
    }
}

/// The stop reason, from `stop_reason` or, when it is missing, from whether
/// the message calls a tool.
pub fn stop_reason(reason: Option<&str>, parts: &[AssistantPart]) -> StopReason {
    match reason {
        Some("end_turn") => StopReason::EndTurn,
        Some("tool_use") => StopReason::ToolUse,
        Some("max_tokens") => StopReason::MaxTokens,
        Some("stop_sequence") => StopReason::StopSequence,
        Some("refusal") => StopReason::Refusal,
        Some(_) => StopReason::Other,
        None => super::stop_for(parts, StopReason::EndTurn),
    }
}

/// One assistant content block.
pub fn block(raw: &Value) -> Option<AssistantPart> {
    let kind = raw.get("type").and_then(Value::as_str).unwrap_or("");
    let string = |name: &str| raw.get(name).and_then(Value::as_str).unwrap_or("");
    match kind {
        "text" => text(string("text")),
        "thinking" => visible(
            string("thinking"),
            raw.get("signature").and_then(Value::as_str),
        ),
        "redacted_thinking" => opaque(string("data")),
        "tool_use" | "server_tool_use" => Some(AssistantPart::ToolCall(ToolCall {
            id: ToolCallId(string("id").to_owned()),
            name: ToolName(string("name").to_owned()),
            arguments: arguments(raw.get("input").unwrap_or(&Value::Null)),
            execution: if kind == "tool_use" {
                ToolExecution::Client
            } else {
                ToolExecution::Server
            },
            signature: None,
        })),
        kind if kind.ends_with("_tool_result") => {
            let content = raw.get("content").unwrap_or(&Value::Null).to_string();
            Some(AssistantPart::ServerToolResult(ToolResult {
                call_id: ToolCallId(string("tool_use_id").to_owned()),
                content: vec![ToolResultContent::Text(Text(content))],
                outcome: ToolOutcome::Success,
            }))
        }
        "image" => None,
        other => Some(unknown(other, raw)),
    }
}

/// A `tool_result` block of a user turn.
pub fn tool_result(raw: &Value) -> ToolResult {
    let call_id = ToolCallId(
        raw.get("tool_use_id")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_owned(),
    );
    let content = match raw.get("content") {
        Some(Value::String(text)) => vec![ToolResultContent::Text(Text(text.clone()))],
        Some(Value::Array(blocks)) => blocks
            .iter()
            .filter_map(|block| {
                match block.get("type").and_then(Value::as_str) {
                    Some("text") => block
                        .get("text")
                        .and_then(Value::as_str)
                        .map(|text| ToolResultContent::Text(Text(text.to_owned()))),
                    // Images are scrubbed or held elsewhere: dropped.
                    Some("image") => None,
                    Some(kind) => Some(ToolResultContent::Unknown(super::unknown_block(
                        kind, block,
                    ))),
                    None => None,
                }
            })
            .collect(),
        _ => Vec::new(),
    };
    let outcome = if raw.get("is_error").and_then(Value::as_bool) == Some(true) {
        ToolOutcome::Error
    } else {
        ToolOutcome::Success
    };
    ToolResult {
        call_id,
        content,
        outcome,
    }
}
