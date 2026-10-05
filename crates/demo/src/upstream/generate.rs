//! The fake model: from a request body to an assistant message and its
//! timing, deterministically. The same seed and the same body bytes always
//! give the same message, the same ids and the same timing.
//!
//! It reads only what it needs, tolerantly: `model`, `stream`,
//! `max_tokens`, the declared tool names and the last user turn (system
//! turns inside `messages` are skipped). A user turn ending in a task
//! marker ([`Task`]) gets the matching `http_request` call against the
//! wiki URL the marker names (a GET to read, a PUT with generated page text
//! to write) when the tool is declared; a turn of tool results gets a
//! closing answer; anything else gets prose.

use std::time::Duration;

use serde_json::Value;

use crate::anthropic::{AssistantMessage, ResponseBlock, StopReason, Usage};
use crate::knobs::{Rng, Span};
use crate::protocol::{HTTP_TOOL, Task, Topic, read_input, write_input};

use super::text::paragraph;

/// What the fake model is configured with.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GenConfig {
    pub seed: u64,
    /// Words of prose per answer or page.
    pub words: Span,
    /// Milliseconds before the first body byte.
    pub first_byte_ms: Span,
    /// Milliseconds from the first event to the last.
    pub stream_ms: Span,
}

impl Default for GenConfig {
    fn default() -> Self {
        Self {
            seed: 7,
            words: Span::ordered(40, 160),
            first_byte_ms: Span::ordered(300, 1500),
            stream_ms: Span::ordered(1000, 10000),
        }
    }
}

/// Why a body is not a Messages request (a 400 `invalid_request_error`).
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum RequestError {
    #[error("the body is not JSON: {0}")]
    Json(String),
    #[error("the body is not a JSON object")]
    NotObject,
    #[error("{0}: field required or malformed")]
    Field(&'static str),
}

/// The last user turn, as the model reads it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LastTurn {
    Task(Task),
    ToolResults { errors: usize, total: usize },
    Other,
}

/// What the model reads from a request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Request {
    pub model: String,
    pub stream: bool,
    pub max_tokens: u64,
    pub tools: Vec<String>,
    pub last: LastTurn,
}

/// A generated answer and when to send it.
#[derive(Debug, Clone, PartialEq)]
pub struct Reply {
    pub message: AssistantMessage,
    pub stream: bool,
    pub first_byte: Duration,
    pub stream_time: Duration,
}

/// Reads a request body.
pub fn parse_request(body: &[u8]) -> Result<Request, RequestError> {
    let value: Value =
        serde_json::from_slice(body).map_err(|e| RequestError::Json(e.to_string()))?;
    let object = value.as_object().ok_or(RequestError::NotObject)?;
    let model = object
        .get("model")
        .and_then(Value::as_str)
        .filter(|m| !m.is_empty())
        .ok_or(RequestError::Field("model"))?
        .to_owned();
    let max_tokens = object
        .get("max_tokens")
        .and_then(Value::as_u64)
        .filter(|n| *n > 0)
        .ok_or(RequestError::Field("max_tokens"))?;
    let messages = object
        .get("messages")
        .and_then(Value::as_array)
        .filter(|m| !m.is_empty())
        .ok_or(RequestError::Field("messages"))?;
    let stream = match object.get("stream") {
        None | Some(Value::Null) => false,
        Some(Value::Bool(stream)) => *stream,
        Some(_) => return Err(RequestError::Field("stream")),
    };
    let tools = object
        .get("tools")
        .and_then(Value::as_array)
        .map(|tools| {
            tools
                .iter()
                .filter_map(|tool| tool.get("name").and_then(Value::as_str))
                .map(str::to_owned)
                .collect()
        })
        .unwrap_or_default();
    let last = messages
        .iter()
        .rev()
        .find(|m| m.get("role").and_then(Value::as_str) == Some("user"))
        .map_or(LastTurn::Other, read_turn);
    Ok(Request {
        model,
        stream,
        max_tokens,
        tools,
        last,
    })
}

fn read_turn(message: &Value) -> LastTurn {
    match message.get("content") {
        Some(Value::String(text)) => Task::find(text).map_or(LastTurn::Other, LastTurn::Task),
        Some(Value::Array(blocks)) => {
            let results: Vec<&Value> = blocks
                .iter()
                .filter(|b| b.get("type").and_then(Value::as_str) == Some("tool_result"))
                .collect();
            if !results.is_empty() {
                return LastTurn::ToolResults {
                    errors: results
                        .iter()
                        .filter(|r| r.get("is_error").and_then(Value::as_bool) == Some(true))
                        .count(),
                    total: results.len(),
                };
            }
            let text: Vec<&str> = blocks
                .iter()
                .filter(|b| b.get("type").and_then(Value::as_str) == Some("text"))
                .filter_map(|b| b.get("text").and_then(Value::as_str))
                .collect();
            Task::find(&text.join("\n")).map_or(LastTurn::Other, LastTurn::Task)
        }
        _ => LastTurn::Other,
    }
}

/// The answer to `request`, whose body was `body`.
pub fn generate(config: &GenConfig, request: &Request, body: &[u8]) -> Reply {
    let mut rng = Rng::derive(config.seed, &[body]);
    let id = format!("msg_01{}", rng.base62(22));
    let first_byte = config.first_byte_ms.draw_ms(&mut rng);
    let stream_time = config.stream_ms.draw_ms(&mut rng);
    // About three words per four tokens; never more words than max_tokens
    // allows.
    let cap = usize::try_from((request.max_tokens * 3 / 4).max(1)).unwrap_or(usize::MAX);
    let words = usize::try_from(config.words.draw(&mut rng))
        .unwrap_or(usize::MAX)
        .clamp(1, cap);
    let declared = |tool: &str| request.tools.iter().any(|t| t == tool);
    let random_topic = |rng: &mut Rng| Topic::of(u32::try_from(rng.below(64)).unwrap_or(0));

    let (content, stop_reason) = match &request.last {
        LastTurn::Task(Task::Write { page, topic, base }) if declared(HTTP_TOOL) => {
            let topic = Topic::of(*topic);
            let page_text = paragraph(&mut rng, &topic, words);
            (
                vec![
                    text(format!(
                        "I'll update the wiki page `{page}` with my notes on {}.",
                        topic.label
                    )),
                    tool_use(&mut rng, HTTP_TOOL, write_input(base, page, &page_text)),
                ],
                StopReason::ToolUse,
            )
        }
        LastTurn::Task(Task::Read { page, base }) if declared(HTTP_TOOL) => (
            vec![
                text(format!("Let me check `{page}` on the wiki first.")),
                tool_use(&mut rng, HTTP_TOOL, read_input(base, page)),
            ],
            StopReason::ToolUse,
        ),
        LastTurn::Task(Task::Chat { topic }) => {
            let topic = Topic::of(*topic);
            (
                vec![text(paragraph(&mut rng, &topic, words))],
                StopReason::EndTurn,
            )
        }
        LastTurn::ToolResults { errors, total } if errors == total => {
            let topic = random_topic(&mut rng);
            let answer = format!(
                "That did not work, so I'll continue from what I know. {}",
                paragraph(&mut rng, &topic, words)
            );
            (vec![text(answer)], StopReason::EndTurn)
        }
        LastTurn::Task(_) | LastTurn::ToolResults { .. } | LastTurn::Other => {
            let topic = random_topic(&mut rng);
            (
                vec![text(paragraph(&mut rng, &topic, words))],
                StopReason::EndTurn,
            )
        }
    };
    let output_tokens = content.iter().map(tokens_of).sum::<u64>().max(1);
    Reply {
        message: AssistantMessage {
            id,
            model: request.model.clone(),
            content,
            stop_reason,
            usage: Usage {
                input_tokens: (body.len() as u64 / 4).max(1),
                output_tokens,
            },
        },
        stream: request.stream,
        first_byte,
        stream_time,
    }
}

fn text(text: String) -> ResponseBlock {
    ResponseBlock::Text { text }
}

fn tool_use(rng: &mut Rng, name: &str, input: Value) -> ResponseBlock {
    ResponseBlock::ToolUse {
        id: format!("toolu_01{}", rng.base62(22)),
        name: name.to_owned(),
        input,
    }
}

/// Roughly four tokens per three words.
fn tokens_of(block: &ResponseBlock) -> u64 {
    let words = match block {
        ResponseBlock::Text { text } => text.split_whitespace().count(),
        ResponseBlock::ToolUse { input, .. } => input.to_string().split_whitespace().count() + 8,
    };
    (words as u64 * 4).div_ceil(3)
}
