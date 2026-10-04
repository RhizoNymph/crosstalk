//! Anthropic content blocks to canonical parts.
//!
//! One mapping for every place a block appears: the request's `system`
//! array and messages, and the response (whole or reassembled from a
//! stream, which is why a response block arrives as an [`Assembled`]).
//!
//! | Block | Part |
//! | --- | --- |
//! | `text` | `Text` (`citations` and `cache_control` dropped) |
//! | `image`, `document` with a `base64` source | `Media` (the decoded bytes are their own blob) |
//! | `tool_result` (user turn) | a `ToolResult` (`is_error: true` is `Error`); `content` a string, or `text`, `image` and `document` blocks |
//! | `thinking` | `Reasoning::Visible`, with its `signature` verbatim (an empty or missing one is `None`) |
//! | `redacted_thinking` | `Reasoning::Opaque` holding `data` verbatim |
//! | `tool_use` | `ToolCall`, `Client` |
//! | `server_tool_use`, `mcp_tool_use` | `ToolCall`, `Server` |
//! | `*_tool_result`, `mcp_tool_result` (assistant turn) after its server call | `ServerToolResult` |
//! | anything else, or a known block missing a field it needs | `Unknown`: its `type` and canonical JSON |
//!
//! A `cache_control` marker says where the harness wants the prompt cache
//! split, not what the model saw, and moves between turns (Claude Code
//! marks the newest message), so it is dropped from every block, an
//! `Unknown` one included: the same message hashes the same wherever the
//! marker sits (`canonical.normalize.echo-stable`,
//! `canonical.normalize.request-is-concatenation`). An `Unknown` part's raw
//! JSON is otherwise the whole block.

use std::collections::BTreeSet;

use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use crosstalk_spec::observed::message::{
    AssistantPart, Media, MediaKind, Reasoning, SystemPart, Text, ToolArguments, ToolCall,
    ToolCallId, ToolExecution, ToolName, ToolOutcome, ToolResult, ToolResultContent, Unknown,
    UserPart,
};

pub(crate) use crate::assemble::MediaSink;
use crosstalk_spec::observed::message::json::Json;

/// A response block as the response delivered it: whole (a non-streamed
/// body, or a stream's block with its deltas applied), or a tool call whose
/// streamed argument text is not JSON.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Assembled {
    Block(Json),
    /// A `tool_use`-like block whose `input_json_delta`s concatenate to
    /// text that does not parse; `block` is its `content_block_start`.
    InvalidInput {
        block: Json,
        text: String,
    },
    /// A block whose deltas this normalizer cannot apply: kept as
    /// `Unknown` with its `content_block_start`.
    Unassembled(Json),
}

/// A block of a user turn, by the canonical role it belongs to.
pub(crate) enum UserItem {
    Part(UserPart),
    Result(ToolResult),
}

/// The block as an `Unknown` part: its `type` (empty when it has none) and
/// its canonical JSON, without its `cache_control` marker (module docs).
pub(crate) fn unknown(block: &Json) -> Unknown {
    let raw = match block {
        Json::Object(members) => Json::Object(
            members
                .iter()
                .filter(|(name, _)| name != CACHE_CONTROL)
                .cloned()
                .collect(),
        )
        .canonical(),
        other => other.canonical(),
    };
    Unknown {
        kind: block.kind().unwrap_or_default().to_owned(),
        raw,
    }
}

/// The prompt-cache marker a harness puts on any block.
const CACHE_CONTROL: &str = "cache_control";

fn text_of(block: &Json, field: &str) -> Option<Text> {
    block
        .get(field)
        .and_then(Json::as_str)
        .map(|text| Text(text.to_owned()))
}

pub(crate) fn system_part(block: &Json) -> SystemPart {
    match (block.kind(), text_of(block, "text")) {
        (Some("text"), Some(text)) => SystemPart::Text(text),
        _ => SystemPart::Unknown(unknown(block)),
    }
}

/// An `image` or `document` block with a `base64` source, as media.
fn media(block: &Json, sink: &mut MediaSink) -> Option<Media> {
    let kind = match block.kind()? {
        "image" => MediaKind::Image,
        "document" => MediaKind::Document,
        _ => return None,
    };
    let source = block.get("source")?;
    if source.kind()? != "base64" {
        return None;
    }
    let bytes = STANDARD.decode(source.get("data")?.as_str()?).ok()?;
    Some(Media {
        kind,
        blob: sink.add(bytes),
    })
}

pub(crate) fn user_item(block: &Json, sink: &mut MediaSink) -> UserItem {
    match block.kind() {
        Some("text") => match text_of(block, "text") {
            Some(text) => UserItem::Part(UserPart::Text(text)),
            None => UserItem::Part(UserPart::Unknown(unknown(block))),
        },
        Some("image" | "document") => match media(block, sink) {
            Some(media) => UserItem::Part(UserPart::Media(media)),
            None => UserItem::Part(UserPart::Unknown(unknown(block))),
        },
        Some("tool_result") => match tool_result(block, sink) {
            Some(result) => UserItem::Result(result),
            None => UserItem::Part(UserPart::Unknown(unknown(block))),
        },
        _ => UserItem::Part(UserPart::Unknown(unknown(block))),
    }
}

/// One item of a tool result's `content` array.
fn result_content(item: &Json, sink: &mut MediaSink) -> ToolResultContent {
    match item.kind() {
        Some("text") => match text_of(item, "text") {
            Some(text) => ToolResultContent::Text(text),
            None => ToolResultContent::Unknown(unknown(item)),
        },
        Some("image" | "document") => match media(item, sink) {
            Some(media) => ToolResultContent::Media(media),
            None => ToolResultContent::Unknown(unknown(item)),
        },
        _ => ToolResultContent::Unknown(unknown(item)),
    }
}

/// A result's `content`: absent or null is empty, a string is one text, an
/// array is its items, and an object (a server tool's single result or
/// error) is one item. `None` for any other value.
fn result_contents(block: &Json, sink: &mut MediaSink) -> Option<Vec<ToolResultContent>> {
    match block.get("content") {
        None | Some(Json::Null) => Some(Vec::new()),
        Some(Json::String(text)) => Some(vec![ToolResultContent::Text(Text(text.clone()))]),
        Some(Json::Array(items)) => Some(
            items
                .iter()
                .map(|item| result_content(item, sink))
                .collect(),
        ),
        Some(object @ Json::Object(_)) => Some(vec![ToolResultContent::Unknown(unknown(object))]),
        Some(_) => None,
    }
}

/// `Error` when the block says `is_error: true`, or its content is a single
/// object whose type ends in `_error` (a server tool's error result).
fn outcome(block: &Json) -> ToolOutcome {
    let flagged = matches!(block.get("is_error"), Some(Json::Bool(true)));
    let error_content = block
        .get("content")
        .and_then(Json::kind)
        .is_some_and(|kind| kind.ends_with("_error"));
    if flagged || error_content {
        ToolOutcome::Error
    } else {
        ToolOutcome::Success
    }
}

fn tool_result(block: &Json, sink: &mut MediaSink) -> Option<ToolResult> {
    let call_id = block.get("tool_use_id")?.as_str()?;
    Some(ToolResult {
        call_id: ToolCallId(call_id.to_owned()),
        content: result_contents(block, sink)?,
        outcome: outcome(block),
    })
}

/// Whether a block type is a server tool's result, returned inside the
/// response (`web_search_tool_result`, `web_fetch_tool_result`,
/// `code_execution_tool_result`, `mcp_tool_result`, ...). A client
/// `tool_result` is not.
fn is_server_result(kind: &str) -> bool {
    kind.ends_with("_tool_result")
}

fn execution_of(kind: &str) -> Option<ToolExecution> {
    match kind {
        "tool_use" => Some(ToolExecution::Client),
        "server_tool_use" | "mcp_tool_use" => Some(ToolExecution::Server),
        _ => None,
    }
}

/// A tool call from its block and arguments; `None` when the block is not a
/// tool call or lacks its id or name.
fn tool_call(block: &Json, arguments: ToolArguments) -> Option<ToolCall> {
    let execution = execution_of(block.kind()?)?;
    Some(ToolCall {
        id: ToolCallId(block.get("id")?.as_str()?.to_owned()),
        name: ToolName(block.get("name")?.as_str()?.to_owned()),
        arguments,
        execution,
    })
}

/// The assistant parts of a turn's blocks, in order. A server tool result
/// becomes a `ServerToolResult` only after a server call with its id in the
/// same turn; otherwise it is kept as `Unknown`.
pub(crate) fn assistant_parts(blocks: Vec<Assembled>, sink: &mut MediaSink) -> Vec<AssistantPart> {
    let mut server_calls: BTreeSet<String> = BTreeSet::new();
    let mut parts = Vec::with_capacity(blocks.len());
    for block in blocks {
        let part = match block {
            Assembled::Block(json) => assistant_part(&json, &server_calls, sink),
            Assembled::InvalidInput { block, text } => {
                match tool_call(&block, ToolArguments::Invalid(text)) {
                    Some(call) => AssistantPart::ToolCall(call),
                    None => AssistantPart::Unknown(unknown(&block)),
                }
            }
            Assembled::Unassembled(json) => AssistantPart::Unknown(unknown(&json)),
        };
        if let AssistantPart::ToolCall(call) = &part
            && call.execution == ToolExecution::Server
        {
            server_calls.insert(call.id.0.clone());
        }
        parts.push(part);
    }
    parts
}

fn assistant_part(
    block: &Json,
    server_calls: &BTreeSet<String>,
    sink: &mut MediaSink,
) -> AssistantPart {
    let known = match block.kind() {
        Some("text") => text_of(block, "text").map(AssistantPart::Text),
        Some("thinking") => text_of(block, "thinking").map(|text| {
            AssistantPart::Reasoning(Reasoning::Visible {
                text,
                signature: block
                    .get("signature")
                    .and_then(Json::as_str)
                    .filter(|signature| !signature.is_empty())
                    .map(str::to_owned),
            })
        }),
        Some("redacted_thinking") => block.get("data").and_then(Json::as_str).map(|data| {
            AssistantPart::Reasoning(Reasoning::Opaque {
                signature: data.to_owned(),
            })
        }),
        Some("tool_use" | "server_tool_use" | "mcp_tool_use") => block
            .get("input")
            .map(|input| ToolArguments::Json(input.canonical()))
            .and_then(|arguments| tool_call(block, arguments))
            .map(AssistantPart::ToolCall),
        Some(kind) if is_server_result(kind) => block
            .get("tool_use_id")
            .and_then(Json::as_str)
            .filter(|id| server_calls.contains(*id))
            .and_then(|_| tool_result(block, sink))
            .map(AssistantPart::ServerToolResult),
        _ => None,
    };
    known.unwrap_or_else(|| AssistantPart::Unknown(unknown(block)))
}
