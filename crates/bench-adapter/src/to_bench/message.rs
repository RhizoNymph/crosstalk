//! Spec messages as bench messages.
//!
//! The bench keeps the spec's message boundaries and part order, so a part
//! index means the same part on both sides and a spec `PartRef` becomes a
//! bench `(MessageId, part)` mechanically. What the bench does not store
//! is dropped: reasoning and tool-call signatures, opaque reasoning's
//! payload, media bytes (a media part keeps its kind), unknown blocks'
//! kind and raw JSON. A tool call's arguments
//! are the spec's canonical JSON text, which the bench's canonical form
//! must reproduce byte for byte ([`ToolArguments::Json`]); a difference is
//! an error, never a silent re-canonicalisation.

use a2a_bench_format as bench;
use bench::json::CanonicalJson;
use bench::message::{
    AssistantPart, Body, ResultContent, SystemPart, ToolArguments, ToolCall, ToolExecution,
    ToolOutcome, ToolPart, ToolResult, UserPart,
};
use crosstalk_spec::observed::message::{
    self as spec, MediaKind, Message, MessageBody, Reasoning, ToolResultContent,
};

use super::{Lossy, ToBenchError};

/// The bench message of `message`. Counts what the conversion dropped in
/// `lossy`.
pub fn convert(
    message: &Message,
    lossy: &mut Lossy,
) -> Result<bench::message::Message, ToBenchError> {
    let body = match &message.body {
        MessageBody::System(parts) => Body::System(
            parts
                .iter()
                .map(|part| match part {
                    spec::SystemPart::Text(text) => SystemPart::Text {
                        text: text.0.clone(),
                    },
                    spec::SystemPart::Unknown(_) => {
                        lossy.unknown_blocks += 1;
                        SystemPart::Unknown
                    }
                })
                .collect(),
        ),
        MessageBody::User(parts) => Body::User(
            parts
                .iter()
                .map(|part| match part {
                    spec::UserPart::Text(text) => UserPart::Text {
                        text: text.0.clone(),
                    },
                    spec::UserPart::Media(media) => UserPart::Media {
                        kind: media_kind(media.kind),
                    },
                    spec::UserPart::Unknown(_) => {
                        lossy.unknown_blocks += 1;
                        UserPart::Unknown
                    }
                })
                .collect(),
        ),
        MessageBody::Assistant(parts) => {
            let mut out = Vec::with_capacity(parts.len());
            for (index, part) in parts.iter().enumerate() {
                out.push(match part {
                    spec::AssistantPart::Text(text) => AssistantPart::Text {
                        text: text.0.clone(),
                    },
                    spec::AssistantPart::Reasoning(Reasoning::Visible { text, .. }) => {
                        AssistantPart::Reasoning {
                            text: text.0.clone(),
                        }
                    }
                    spec::AssistantPart::Reasoning(Reasoning::Opaque { .. }) => {
                        AssistantPart::ReasoningOpaque
                    }
                    spec::AssistantPart::ToolCall(call) => {
                        AssistantPart::ToolCall(tool_call(message, index, call)?)
                    }
                    spec::AssistantPart::ServerToolResult(result) => {
                        AssistantPart::ServerToolResult(tool_result(result, lossy))
                    }
                    spec::AssistantPart::Unknown(_) => {
                        lossy.unknown_blocks += 1;
                        AssistantPart::Unknown
                    }
                });
            }
            Body::Assistant(out)
        }
        MessageBody::Tool(results) => Body::Tool(
            results
                .iter()
                .map(|result| ToolPart::ToolResult(tool_result(result, lossy)))
                .collect(),
        ),
    };
    bench::message::Message::new(body).map_err(|source| ToBenchError::Message {
        hash: message.hash,
        source,
    })
}

/// A media part's kind; its bytes are not stored.
pub fn media_kind(kind: MediaKind) -> bench::message::MediaKind {
    match kind {
        MediaKind::Image => bench::message::MediaKind::Image,
        MediaKind::Audio => bench::message::MediaKind::Audio,
        MediaKind::Document => bench::message::MediaKind::Document,
    }
}

fn tool_call(
    message: &Message,
    index: usize,
    call: &spec::ToolCall,
) -> Result<ToolCall, ToBenchError> {
    let arguments = match &call.arguments {
        spec::ToolArguments::Json(json) => {
            ToolArguments::Json(CanonicalJson::from_canonical(json.0.clone()).map_err(
                |source| ToBenchError::Arguments {
                    hash: message.hash,
                    part: index,
                    source,
                },
            )?)
        }
        spec::ToolArguments::Invalid(raw) => ToolArguments::Invalid(raw.clone()),
    };
    Ok(ToolCall {
        call_id: call.id.0.clone(),
        name: call.name.0.clone(),
        arguments,
        execution: match call.execution {
            spec::ToolExecution::Client => ToolExecution::Client,
            spec::ToolExecution::Server => ToolExecution::Server,
        },
    })
}

fn tool_result(result: &spec::ToolResult, lossy: &mut Lossy) -> ToolResult {
    ToolResult {
        call_id: result.call_id.0.clone(),
        content: result
            .content
            .iter()
            .map(|content| match content {
                ToolResultContent::Text(text) => ResultContent::Text {
                    text: text.0.clone(),
                },
                ToolResultContent::Media(media) => ResultContent::Media {
                    kind: media_kind(media.kind),
                },
                ToolResultContent::Unknown(_) => {
                    lossy.unknown_blocks += 1;
                    ResultContent::Unknown
                }
            })
            .collect(),
        outcome: match result.outcome {
            spec::ToolOutcome::Success => ToolOutcome::Success,
            spec::ToolOutcome::Error => ToolOutcome::Error,
            spec::ToolOutcome::Unknown => ToolOutcome::Unknown,
        },
    }
}
