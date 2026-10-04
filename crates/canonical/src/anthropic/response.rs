//! An Anthropic Messages response to the exchange's outcome and response
//! message.
//!
//! | Raw response | Outcome |
//! | --- | --- |
//! | complete, non-2xx | `Failed`, `Upstream { status }`, no partial response (`canonical.normalize.error-status-is-failed`) |
//! | complete 2xx `Http` body: a `message` | `Completed` |
//! | complete 2xx `Http` body: an `error` object | `Failed`, `UpstreamErrorEvent`, no partial response |
//! | complete 2xx `Sse` body ending in `message_stop` | `Completed` |
//! | complete 2xx `Sse` body with an `error` event | `Failed`, `UpstreamErrorEvent`, the blocks before it as the partial response |
//! | complete 2xx `Sse` body that started but has no `message_stop` | `Failed`, `StreamTruncated`, the blocks so far |
//! | complete 2xx body that does not parse (or never starts a message) | `Failed`, `UnparseableResponse`, no partial response (`canonical.normalize.unparseable-response-failed`) |
//! | failed (the proxy's failure) | `Failed` with that failure; the blocks its partial bytes started, read as far as they parse |
//!
//! A partial response exists when at least one content block started. A
//! completed response's stop reason comes from `stop_reason`, except that a
//! response holding a client tool call stops with `ToolUse` unless it hit a
//! token limit (`canonical.normalize.tool-use-stop-reason`). Usage maps as
//! [`super::usage`] says.

use crosstalk_spec::interfaces::l0_ingress::{RawExchange, RawResponse};
use crosstalk_spec::observed::exchange::{ExchangeFailure, StopReason, Transport};
use crosstalk_spec::observed::message::{AssistantPart, ToolExecution};

use super::blocks::{self, Assembled, MediaSink};
use super::stream::{self, StreamEnd};
use super::usage::Usage;
use crate::assemble::ResponseRead;
use crate::json::Json;

pub(crate) fn read(raw: &RawExchange, sink: &mut MediaSink) -> ResponseRead {
    let streamed = raw.meta.transport != Transport::Http;
    match &raw.response {
        RawResponse::Complete { status, .. } if !(200..300).contains(status) => {
            ResponseRead::Failed {
                partial: None,
                failure: ExchangeFailure::Upstream { status: *status },
            }
        }
        RawResponse::Complete { body, .. } if streamed => complete_stream(body, sink),
        RawResponse::Complete { body, .. } => complete_whole(body, sink),
        RawResponse::Failed {
            failure,
            partial_body,
        } => {
            let partial = if streamed {
                let read = stream::read(partial_body);
                read.has_blocks()
                    .then(|| blocks::assistant_parts(read.blocks(), sink))
            } else {
                match whole(partial_body) {
                    Whole::Message(message) if !message.blocks.is_empty() => {
                        Some(blocks::assistant_parts(message.blocks, sink))
                    }
                    _ => None,
                }
            };
            ResponseRead::Failed {
                partial,
                failure: *failure,
            }
        }
    }
}

fn unparseable() -> ResponseRead {
    ResponseRead::Failed {
        partial: None,
        failure: ExchangeFailure::UnparseableResponse,
    }
}

fn complete_stream(body: &[u8], sink: &mut MediaSink) -> ResponseRead {
    let read = stream::read(body);
    if !read.started || read.end == StreamEnd::Malformed {
        return match read.end {
            StreamEnd::ErrorEvent => ResponseRead::Failed {
                partial: None,
                failure: ExchangeFailure::UpstreamErrorEvent,
            },
            _ => unparseable(),
        };
    }
    let partial = |failure, sink: &mut MediaSink| ResponseRead::Failed {
        partial: read
            .has_blocks()
            .then(|| blocks::assistant_parts(read.blocks(), sink)),
        failure,
    };
    match read.end {
        StreamEnd::Stopped => {
            let parts = blocks::assistant_parts(read.blocks(), sink);
            ResponseRead::Completed {
                stop: stop_reason(read.stop_reason.as_deref(), &parts),
                parts,
                response_id: read.id.clone(),
                usage: read.usage.token_usage(),
            }
        }
        StreamEnd::ErrorEvent => partial(ExchangeFailure::UpstreamErrorEvent, sink),
        StreamEnd::Truncated | StreamEnd::Malformed => {
            partial(ExchangeFailure::StreamTruncated, sink)
        }
    }
}

fn complete_whole(body: &[u8], sink: &mut MediaSink) -> ResponseRead {
    match whole(body) {
        Whole::Message(message) => {
            let parts = blocks::assistant_parts(message.blocks, sink);
            ResponseRead::Completed {
                stop: stop_reason(message.stop_reason.as_deref(), &parts),
                parts,
                response_id: message.id,
                usage: message.usage.token_usage(),
            }
        }
        Whole::Error => ResponseRead::Failed {
            partial: None,
            failure: ExchangeFailure::UpstreamErrorEvent,
        },
        Whole::Unparseable => unparseable(),
    }
}

/// A whole (non-streamed) response body.
enum Whole {
    Message(WholeMessage),
    Error,
    Unparseable,
}

struct WholeMessage {
    id: Option<String>,
    blocks: Vec<Assembled>,
    stop_reason: Option<String>,
    usage: Usage,
}

/// A `message` object (its `type`, when present, `message`; its `role`,
/// when present, `assistant`; `content` an array), or an `error` object.
fn whole(body: &[u8]) -> Whole {
    let Ok(value) = Json::parse_bytes(body) else {
        return Whole::Unparseable;
    };
    if !value.is_object() {
        return Whole::Unparseable;
    }
    match value.kind() {
        Some("error") => return Whole::Error,
        None | Some("message") => {}
        Some(_) => return Whole::Unparseable,
    }
    if value
        .get("role")
        .is_some_and(|role| role.as_str() != Some("assistant"))
    {
        return Whole::Unparseable;
    }
    let Some(content) = value.get("content").and_then(Json::as_array) else {
        return Whole::Unparseable;
    };
    let stop_reason = match value.get("stop_reason") {
        Some(Json::String(reason)) => Some(reason.clone()),
        None | Some(Json::Null) => None,
        Some(_) => return Whole::Unparseable,
    };
    let mut usage = Usage::default();
    if let Some(counts) = value.get("usage") {
        usage.merge(counts);
    }
    Whole::Message(WholeMessage {
        id: value.get("id").and_then(Json::as_str).map(str::to_owned),
        blocks: content.iter().cloned().map(Assembled::Block).collect(),
        stop_reason,
        usage,
    })
}

/// The canonical stop reason of a completed response.
///
/// `end_turn`, `tool_use`, `max_tokens`, `stop_sequence` and `refusal` map
/// to their variants, `model_context_window_exceeded` (a token limit) to
/// `MaxTokens`, and anything else (`pause_turn`, an unknown or missing
/// reason) to `Other`. A response with a client tool call stops with
/// `ToolUse` unless it stopped at a token limit.
pub(crate) fn stop_reason(reason: Option<&str>, parts: &[AssistantPart]) -> StopReason {
    let stop = match reason {
        Some("end_turn") => StopReason::EndTurn,
        Some("tool_use") => StopReason::ToolUse,
        Some("max_tokens" | "model_context_window_exceeded") => StopReason::MaxTokens,
        Some("stop_sequence") => StopReason::StopSequence,
        Some("refusal") => StopReason::Refusal,
        _ => StopReason::Other,
    };
    let client_call = parts.iter().any(|part| {
        matches!(part, AssistantPart::ToolCall(call) if call.execution == ToolExecution::Client)
    });
    if client_call && stop != StopReason::MaxTokens {
        StopReason::ToolUse
    } else {
        stop
    }
}
