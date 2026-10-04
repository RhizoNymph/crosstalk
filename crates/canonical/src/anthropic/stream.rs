//! Reassembling a streamed Anthropic Messages response from its events.
//!
//! Each event's JSON `data` carries its `type` (the SSE `event:` name only
//! repeats it):
//!
//! | Event | Effect |
//! | --- | --- |
//! | `message_start` | the response id, the first usage, and any blocks already in `message.content` |
//! | `content_block_start` | a block at `index`, as its start shape |
//! | `content_block_delta` | `text_delta`, `input_json_delta`, `thinking_delta` and `signature_delta` append to their block; `citations_delta` is dropped (citations are not part of a canonical text); any other delta leaves its block `Unassembled` |
//! | `content_block_stop` | nothing to do: a block is complete when the response is |
//! | `message_delta` | the stop reason, and usage fields laid over the earlier ones |
//! | `message_stop` | the end: the response completed |
//! | `ping`, unknown event types | nothing |
//! | `error` | the upstream reported an error inside the stream |
//!
//! A block keeps its stream index, and the response's blocks are in index
//! order, however their deltas interleave. When the stream ends, each block
//! is written back in its whole-body shape (`text` holding the joined text
//! deltas, `input` the parsed JSON of the joined `partial_json`, `thinking`
//! and `signature` theirs), so a streamed response and the same response
//! sent whole go through one block mapping and give the same message
//! (`canonical.normalize.stream-independent`). An `input_json_delta` text
//! that does not parse is kept verbatim as the call's invalid arguments.
//!
//! The stream is malformed when an event's data is not a JSON object with a
//! string `type`, content arrives before `message_start` or names an index
//! with no block (or starts one twice), a delta of a known type lacks its
//! field or does not fit its block, or the body stops being UTF-8.

use std::collections::BTreeMap;

use super::blocks::Assembled;
use super::usage::Usage;
use crate::sse;
use crosstalk_spec::observed::message::json::Json;

/// How the events ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum StreamEnd {
    /// `message_stop` arrived.
    Stopped,
    /// An `error` event arrived.
    ErrorEvent,
    /// The events ran out with neither.
    Truncated,
    /// An event could not be applied; the read stopped before it.
    Malformed,
}

/// What a stream said, up to its end.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StreamRead {
    /// Whether `message_start` arrived.
    pub(crate) started: bool,
    pub(crate) id: Option<String>,
    blocks: BTreeMap<u64, BlockState>,
    pub(crate) stop_reason: Option<String>,
    pub(crate) usage: Usage,
    pub(crate) end: StreamEnd,
}

impl StreamRead {
    /// Whether any content block started.
    pub(crate) fn has_blocks(&self) -> bool {
        !self.blocks.is_empty()
    }

    /// The blocks in index order, each in its whole-body shape.
    pub(crate) fn blocks(&self) -> Vec<Assembled> {
        self.blocks.values().map(BlockState::assemble).collect()
    }
}

/// One block while its deltas arrive. `start` is its
/// `content_block_start` shape.
#[derive(Debug, Clone, PartialEq, Eq)]
enum BlockState {
    Text {
        start: Json,
        text: String,
    },
    ToolInput {
        start: Json,
        json: String,
    },
    Thinking {
        start: Json,
        thinking: String,
        signature: String,
    },
    /// A block that takes no deltas (redacted thinking, server tool
    /// results, unknown types): complete at its start. Deltas to it are
    /// ignored.
    Whole(Json),
    Unassembled(Json),
}

fn string_field(block: &Json, field: &str) -> String {
    block
        .get(field)
        .and_then(Json::as_str)
        .unwrap_or_default()
        .to_owned()
}

impl BlockState {
    fn start(block: Json) -> Self {
        match block.kind() {
            Some("text") => Self::Text {
                text: string_field(&block, "text"),
                start: block,
            },
            Some("tool_use" | "server_tool_use" | "mcp_tool_use") => Self::ToolInput {
                start: block,
                json: String::new(),
            },
            Some("thinking") => Self::Thinking {
                thinking: string_field(&block, "thinking"),
                signature: string_field(&block, "signature"),
                start: block,
            },
            _ => Self::Whole(block),
        }
    }

    /// Applies one delta; `Err` when the stream is malformed.
    fn apply(&mut self, delta: &Json) -> Result<(), Malformed> {
        let kind = delta.kind().ok_or(Malformed)?;
        let field = |name: &str| delta.get(name).and_then(Json::as_str).ok_or(Malformed);
        match (&mut *self, kind) {
            (Self::Whole(_) | Self::Unassembled(_), _) => {}
            (Self::Text { text, .. }, "text_delta") => text.push_str(field("text")?),
            (Self::Text { .. }, "citations_delta") => {}
            (Self::ToolInput { json, .. }, "input_json_delta") => {
                json.push_str(field("partial_json")?);
            }
            (Self::Thinking { thinking, .. }, "thinking_delta") => {
                thinking.push_str(field("thinking")?);
            }
            (Self::Thinking { signature, .. }, "signature_delta") => {
                signature.push_str(field("signature")?);
            }
            (
                _,
                "text_delta" | "citations_delta" | "input_json_delta" | "thinking_delta"
                | "signature_delta",
            ) => return Err(Malformed),
            (
                Self::Text { start, .. }
                | Self::ToolInput { start, .. }
                | Self::Thinking { start, .. },
                _,
            ) => {
                *self = Self::Unassembled(start.clone());
            }
        }
        Ok(())
    }

    fn assemble(&self) -> Assembled {
        match self {
            Self::Text { start, text } => {
                Assembled::Block(start.clone().with("text", Json::String(text.clone())))
            }
            Self::ToolInput { start, json } if json.is_empty() => Assembled::Block(start.clone()),
            Self::ToolInput { start, json } => match Json::parse(json) {
                Ok(input) => Assembled::Block(start.clone().with("input", input)),
                Err(_) => Assembled::InvalidInput {
                    block: start.clone(),
                    text: json.clone(),
                },
            },
            Self::Thinking {
                start,
                thinking,
                signature,
            } => Assembled::Block(
                start
                    .clone()
                    .with("thinking", Json::String(thinking.clone()))
                    .with("signature", Json::String(signature.clone())),
            ),
            Self::Whole(block) => Assembled::Block(block.clone()),
            Self::Unassembled(block) => Assembled::Unassembled(block.clone()),
        }
    }
}

/// An event that cannot be applied.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Malformed;

/// Reads `body`'s events until `message_stop`, an `error` event, a
/// malformed event or the end.
pub(crate) fn read(body: &[u8]) -> StreamRead {
    let parsed = sse::parse(body);
    let mut read = StreamRead {
        started: false,
        id: None,
        blocks: BTreeMap::new(),
        stop_reason: None,
        usage: Usage::default(),
        end: StreamEnd::Truncated,
    };
    for event in &parsed.events {
        match apply(&mut read, event) {
            Ok(None) => {}
            Ok(Some(end)) => {
                read.end = end;
                return read;
            }
            Err(Malformed) => {
                read.end = StreamEnd::Malformed;
                return read;
            }
        }
    }
    if parsed.not_utf8_at.is_some() {
        read.end = StreamEnd::Malformed;
    }
    read
}

/// Applies one event; `Some` when it ends the stream.
fn apply(read: &mut StreamRead, event: &sse::SseEvent) -> Result<Option<StreamEnd>, Malformed> {
    let data = Json::parse(&event.data).map_err(|_| Malformed)?;
    let kind = data.kind().ok_or(Malformed)?;
    match kind {
        "message_start" => {
            if read.started {
                return Err(Malformed);
            }
            let message = data
                .get("message")
                .filter(|m| m.is_object())
                .ok_or(Malformed)?;
            read.started = true;
            read.id = message.get("id").and_then(Json::as_str).map(str::to_owned);
            if let Some(usage) = message.get("usage") {
                read.usage.merge(usage);
            }
            if let Some(content) = message.get("content").and_then(Json::as_array) {
                for (index, block) in (0u64..).zip(content) {
                    read.blocks.insert(index, BlockState::Whole(block.clone()));
                }
            }
        }
        "content_block_start" => {
            let index = started_index(read, &data)?;
            let block = data
                .get("content_block")
                .filter(|block| block.is_object())
                .ok_or(Malformed)?;
            if read.blocks.contains_key(&index) {
                return Err(Malformed);
            }
            read.blocks.insert(index, BlockState::start(block.clone()));
        }
        "content_block_delta" => {
            let index = started_index(read, &data)?;
            let delta = data.get("delta").ok_or(Malformed)?;
            read.blocks.get_mut(&index).ok_or(Malformed)?.apply(delta)?;
        }
        "content_block_stop" => {
            let index = started_index(read, &data)?;
            if !read.blocks.contains_key(&index) {
                return Err(Malformed);
            }
        }
        "message_delta" => {
            if !read.started {
                return Err(Malformed);
            }
            if let Some(delta) = data.get("delta") {
                match delta.get("stop_reason") {
                    Some(Json::String(reason)) => read.stop_reason = Some(reason.clone()),
                    None | Some(Json::Null) => {}
                    Some(_) => return Err(Malformed),
                }
            }
            if let Some(usage) = data.get("usage") {
                read.usage.merge(usage);
            }
        }
        "message_stop" => {
            if !read.started {
                return Err(Malformed);
            }
            return Ok(Some(StreamEnd::Stopped));
        }
        "error" => return Ok(Some(StreamEnd::ErrorEvent)),
        _ => {}
    }
    Ok(None)
}

/// The event's block `index`, once the message has started.
fn started_index(read: &StreamRead, data: &Json) -> Result<u64, Malformed> {
    if !read.started {
        return Err(Malformed);
    }
    data.get("index").and_then(Json::as_u64).ok_or(Malformed)
}
