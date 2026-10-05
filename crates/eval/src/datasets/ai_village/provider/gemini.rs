//! Gemini `generateContent` responses: the first candidate's parts.
//!
//! | Part | Canonical |
//! | --- | --- |
//! | `text` with `"thought": true` | `Reasoning::Visible` |
//! | `text` | `Text` |
//! | `functionCall` | `ToolCall` (its `id`, or `<fallback>-<n>` when it has none) |
//! | `thoughtSignature` | `Reasoning::Opaque`, unless scrubbed |
//!
//! `finishReason` `MAX_TOKENS` is `MaxTokens`, `SAFETY` / `RECITATION`
//! `Refusal`; otherwise the stop is `ToolUse` when a function is called.

use crosstalk_spec::observed::exchange::{StopReason, WireProtocol};
use crosstalk_spec::observed::message::{
    AssistantPart, ToolCall, ToolCallId, ToolExecution, ToolName,
};
use serde_json::Value;

use super::{Response, arguments, opaque, stop_for, text, visible};

pub fn convert(raw: &Value, fallback_id: &str) -> Response {
    let candidate = raw
        .get("candidates")
        .and_then(Value::as_array)
        .and_then(|candidates| candidates.first())
        .unwrap_or(&Value::Null);
    let mut parts = Vec::new();
    let mut unnamed = 0usize;
    for part in candidate
        .pointer("/content/parts")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
    {
        if let Some(call) = part.get("functionCall") {
            let id = match call.get("id").and_then(Value::as_str) {
                Some(id) if !id.is_empty() => id.to_owned(),
                _ => {
                    unnamed += 1;
                    format!("{fallback_id}-{unnamed}")
                }
            };
            parts.push(AssistantPart::ToolCall(ToolCall {
                id: ToolCallId(id),
                name: ToolName(
                    call.get("name")
                        .and_then(Value::as_str)
                        .unwrap_or("")
                        .to_owned(),
                ),
                arguments: arguments(call.get("args").unwrap_or(&Value::Null)),
                execution: ToolExecution::Client,
            }));
        } else if let Some(content) = part.get("text").and_then(Value::as_str) {
            if part.get("thought").and_then(Value::as_bool) == Some(true) {
                parts.extend(visible(content, None));
            } else {
                parts.extend(text(content));
            }
        }
        if let Some(signature) = part.get("thoughtSignature").and_then(Value::as_str) {
            parts.extend(opaque(signature));
        }
    }
    let stop = match candidate.get("finishReason").and_then(Value::as_str) {
        Some("MAX_TOKENS") => StopReason::MaxTokens,
        Some("SAFETY") | Some("RECITATION") => StopReason::Refusal,
        _ => stop_for(&parts, StopReason::EndTurn),
    };
    Response {
        protocol: WireProtocol::GeminiGenerate,
        parts,
        stop,
    }
}
