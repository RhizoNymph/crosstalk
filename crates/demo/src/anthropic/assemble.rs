//! Reassembling an assistant message from its event stream, as a harness
//! does: blocks by index, text deltas appended, tool input JSON
//! concatenated and parsed at the block's stop, the stop reason and output
//! tokens from `message_delta`, done at `message_stop`. `ping` and unknown
//! events are skipped; an `error` event fails the message.

use crosstalk_testkit::corpus::sse::{EventStream, SseError};
use serde_json::Value;

use super::{AssistantMessage, ResponseBlock, StopReason, Usage};

/// Why an event stream is not a whole assistant message.
#[derive(Debug, thiserror::Error)]
pub enum AssembleError {
    #[error("the body is not an event stream: {0}")]
    Sse(#[from] SseError),
    #[error("event {event} has data that is not JSON: {source}")]
    Json {
        event: String,
        source: serde_json::Error,
    },
    #[error("{0} before message_start")]
    BeforeStart(String),
    #[error("message_start twice")]
    StartedTwice,
    #[error("event {event} is missing or has a malformed {field}")]
    Field { event: String, field: &'static str },
    #[error("event for block {index}, which was never started or already stopped")]
    BadIndex { index: usize },
    #[error("a {delta} delta for a block of another kind (index {index})")]
    DeltaKind { index: usize, delta: String },
    #[error("tool input of block {index} is not JSON: {source}")]
    ToolInput {
        index: usize,
        source: serde_json::Error,
    },
    #[error("the stream ended without message_stop")]
    Unfinished,
    #[error("message_stop without a stop reason")]
    NoStopReason,
    #[error("upstream error event: {kind}: {message}")]
    ErrorEvent { kind: String, message: String },
}

/// A block being received.
#[derive(Debug)]
enum Open {
    Text(String),
    Tool {
        id: String,
        name: String,
        json: String,
    },
}

/// A message being received.
#[derive(Debug)]
struct Partial {
    id: String,
    model: String,
    input_tokens: u64,
    output_tokens: u64,
    blocks: Vec<Option<ResponseBlock>>,
    open: Option<(usize, Open)>,
    stop_reason: Option<StopReason>,
}

/// Every dispatched event of `stream`, reassembled.
pub fn assemble_stream(stream: &EventStream) -> Result<AssistantMessage, AssembleError> {
    let mut events = Vec::new();
    for event in stream.dispatched() {
        let data = event.json().map_err(|source| AssembleError::Json {
            event: event.kind().to_owned(),
            source,
        })?;
        events.push((event.kind().to_owned(), data));
    }
    assemble(events)
}

/// `(event name, data)` pairs, reassembled.
pub fn assemble(
    events: impl IntoIterator<Item = (String, Value)>,
) -> Result<AssistantMessage, AssembleError> {
    let mut partial: Option<Partial> = None;
    for (event, data) in events {
        match event.as_str() {
            "ping" => {}
            "error" => {
                let text = |field: &str| {
                    data.pointer(&format!("/error/{field}"))
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_owned()
                };
                return Err(AssembleError::ErrorEvent {
                    kind: text("type"),
                    message: text("message"),
                });
            }
            "message_start" => {
                if partial.is_some() {
                    return Err(AssembleError::StartedTwice);
                }
                let field = |field: &'static str| AssembleError::Field {
                    event: event.clone(),
                    field,
                };
                let message = data.get("message").ok_or_else(|| field("message"))?;
                partial = Some(Partial {
                    id: str_at(message, "/id").ok_or_else(|| field("message.id"))?,
                    model: str_at(message, "/model").ok_or_else(|| field("message.model"))?,
                    input_tokens: message
                        .pointer("/usage/input_tokens")
                        .and_then(Value::as_u64)
                        .unwrap_or(0),
                    output_tokens: 0,
                    blocks: Vec::new(),
                    open: None,
                    stop_reason: None,
                });
            }
            "message_stop" => {
                let Some(partial) = partial else {
                    return Err(AssembleError::BeforeStart(event));
                };
                return partial.finish();
            }
            _ => {
                let Some(partial) = partial.as_mut() else {
                    return Err(AssembleError::BeforeStart(event));
                };
                partial.apply(&event, &data)?;
            }
        }
    }
    Err(AssembleError::Unfinished)
}

fn str_at(value: &Value, pointer: &str) -> Option<String> {
    value
        .pointer(pointer)
        .and_then(Value::as_str)
        .map(str::to_owned)
}

impl Partial {
    fn apply(&mut self, event: &str, data: &Value) -> Result<(), AssembleError> {
        let field = |field: &'static str| AssembleError::Field {
            event: event.to_owned(),
            field,
        };
        let index = || {
            data.get("index")
                .and_then(Value::as_u64)
                .and_then(|i| usize::try_from(i).ok())
                .ok_or_else(|| field("index"))
        };
        match event {
            "content_block_start" => {
                let index = index()?;
                if self.open.is_some() || index != self.blocks.len() {
                    return Err(AssembleError::BadIndex { index });
                }
                let block = data
                    .get("content_block")
                    .ok_or_else(|| field("content_block"))?;
                let open = match block.get("type").and_then(Value::as_str) {
                    Some("text") => Open::Text(str_at(block, "/text").unwrap_or_default()),
                    Some("tool_use") => Open::Tool {
                        id: str_at(block, "/id").ok_or_else(|| field("content_block.id"))?,
                        name: str_at(block, "/name").ok_or_else(|| field("content_block.name"))?,
                        json: String::new(),
                    },
                    _ => return Err(field("content_block.type")),
                };
                self.blocks.push(None);
                self.open = Some((index, open));
            }
            "content_block_delta" => {
                let index = index()?;
                let Some((open_index, open)) = self.open.as_mut() else {
                    return Err(AssembleError::BadIndex { index });
                };
                if *open_index != index {
                    return Err(AssembleError::BadIndex { index });
                }
                let delta = data.get("delta").ok_or_else(|| field("delta"))?;
                let kind = delta
                    .get("type")
                    .and_then(Value::as_str)
                    .ok_or_else(|| field("delta.type"))?;
                match (kind, open) {
                    ("text_delta", Open::Text(text)) => {
                        text.push_str(&str_at(delta, "/text").ok_or_else(|| field("delta.text"))?);
                    }
                    ("input_json_delta", Open::Tool { json, .. }) => json.push_str(
                        &str_at(delta, "/partial_json")
                            .ok_or_else(|| field("delta.partial_json"))?,
                    ),
                    (other, _) => {
                        return Err(AssembleError::DeltaKind {
                            index,
                            delta: other.to_owned(),
                        });
                    }
                }
            }
            "content_block_stop" => {
                let index = index()?;
                match self.open.take() {
                    Some((open_index, open)) if open_index == index => {
                        let block = match open {
                            Open::Text(text) => ResponseBlock::Text { text },
                            Open::Tool { id, name, json } => {
                                let input = if json.is_empty() {
                                    Value::Object(serde_json::Map::new())
                                } else {
                                    serde_json::from_str(&json).map_err(|source| {
                                        AssembleError::ToolInput { index, source }
                                    })?
                                };
                                ResponseBlock::ToolUse { id, name, input }
                            }
                        };
                        if let Some(slot) = self.blocks.get_mut(index) {
                            *slot = Some(block);
                        }
                    }
                    _ => return Err(AssembleError::BadIndex { index }),
                }
            }
            "message_delta" => {
                if let Some(reason) = data.pointer("/delta/stop_reason")
                    && !reason.is_null()
                {
                    self.stop_reason = Some(
                        serde_json::from_value(reason.clone())
                            .map_err(|_| field("delta.stop_reason"))?,
                    );
                }
                if let Some(tokens) = data.pointer("/usage/output_tokens").and_then(Value::as_u64) {
                    self.output_tokens = tokens;
                }
            }
            _ => {}
        }
        Ok(())
    }

    fn finish(self) -> Result<AssistantMessage, AssembleError> {
        if let Some((index, _)) = self.open {
            return Err(AssembleError::BadIndex { index });
        }
        let stop_reason = self.stop_reason.ok_or(AssembleError::NoStopReason)?;
        let mut content = Vec::with_capacity(self.blocks.len());
        for (index, block) in self.blocks.into_iter().enumerate() {
            content.push(block.ok_or(AssembleError::BadIndex { index })?);
        }
        Ok(AssistantMessage {
            id: self.id,
            model: self.model,
            content,
            stop_reason,
            usage: Usage {
                input_tokens: self.input_tokens,
                output_tokens: self.output_tokens,
            },
        })
    }
}
