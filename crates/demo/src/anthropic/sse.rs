//! Encoding an assistant message as the Anthropic Messages event stream.
//!
//! The sequence is the API's: `message_start` (the message with empty
//! content and no stop reason), one `ping`, then per content block
//! `content_block_start`, its deltas (`text_delta` pieces of a few words,
//! or `input_json_delta` pieces of the tool input's JSON, the first one
//! empty as the API sends it) and `content_block_stop`, then
//! `message_delta` (stop reason and output tokens) and `message_stop`.
//! The deltas of a block concatenate to its text or its input's JSON.

use bytes::Bytes;
use serde_json::{Value, json};

use super::{AssistantMessage, ResponseBlock};

/// How a message is cut into deltas.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Split {
    /// Words per `text_delta` (at least one).
    pub words_per_delta: usize,
    /// Characters per `input_json_delta` (at least one).
    pub json_chars_per_delta: usize,
}

impl Default for Split {
    fn default() -> Self {
        Self {
            words_per_delta: 3,
            json_chars_per_delta: 24,
        }
    }
}

/// One server-sent event: its `event:` name and its `data:` JSON.
#[derive(Debug, Clone, PartialEq)]
pub struct Frame {
    pub event: &'static str,
    pub data: Value,
}

impl Frame {
    fn new(event: &'static str, data: Value) -> Self {
        Self { event, data }
    }

    /// `event: <name>\ndata: <json>\n\n`, as the API frames it.
    pub fn to_bytes(&self) -> Bytes {
        Bytes::from(format!("event: {}\ndata: {}\n\n", self.event, self.data))
    }
}

/// The event stream for `message`, cut as `split` says.
pub fn encode(message: &AssistantMessage, split: Split) -> Vec<Frame> {
    let mut frames = vec![
        Frame::new(
            "message_start",
            json!({
                "type": "message_start",
                "message": {
                    "id": message.id,
                    "type": "message",
                    "role": "assistant",
                    "model": message.model,
                    "content": [],
                    "stop_reason": null,
                    "stop_sequence": null,
                    "usage": {
                        "input_tokens": message.usage.input_tokens,
                        "output_tokens": 1,
                    },
                },
            }),
        ),
        Frame::new("ping", json!({"type": "ping"})),
    ];
    for (index, block) in message.content.iter().enumerate() {
        match block {
            ResponseBlock::Text { text } => {
                frames.push(block_start(index, json!({"type": "text", "text": ""})));
                for piece in word_pieces(text, split.words_per_delta) {
                    frames.push(delta(index, json!({"type": "text_delta", "text": piece})));
                }
            }
            ResponseBlock::ToolUse { id, name, input } => {
                frames.push(block_start(
                    index,
                    json!({"type": "tool_use", "id": id, "name": name, "input": {}}),
                ));
                let mut pieces = vec![String::new()];
                pieces.extend(char_pieces(&input.to_string(), split.json_chars_per_delta));
                for piece in pieces {
                    frames.push(delta(
                        index,
                        json!({"type": "input_json_delta", "partial_json": piece}),
                    ));
                }
            }
        }
        frames.push(Frame::new(
            "content_block_stop",
            json!({"type": "content_block_stop", "index": index}),
        ));
    }
    frames.push(Frame::new(
        "message_delta",
        json!({
            "type": "message_delta",
            "delta": {"stop_reason": message.stop_reason, "stop_sequence": null},
            "usage": {"output_tokens": message.usage.output_tokens},
        }),
    ));
    frames.push(Frame::new("message_stop", json!({"type": "message_stop"})));
    frames
}

fn block_start(index: usize, block: Value) -> Frame {
    Frame::new(
        "content_block_start",
        json!({"type": "content_block_start", "index": index, "content_block": block}),
    )
}

fn delta(index: usize, delta: Value) -> Frame {
    Frame::new(
        "content_block_delta",
        json!({"type": "content_block_delta", "index": index, "delta": delta}),
    )
}

/// `text` in pieces of `words` words, each piece keeping the whitespace
/// before its next word, so the pieces concatenate to `text`.
pub fn word_pieces(text: &str, words: usize) -> Vec<String> {
    let words = words.max(1);
    let mut pieces = Vec::new();
    let mut current = String::new();
    let mut count = 0;
    let mut in_word = false;
    for ch in text.chars() {
        let space = ch.is_whitespace();
        if !space && !in_word {
            if count == words {
                pieces.push(std::mem::take(&mut current));
                count = 0;
            }
            count += 1;
        }
        in_word = !space;
        current.push(ch);
    }
    if !current.is_empty() {
        pieces.push(current);
    }
    pieces
}

/// `text` in pieces of at most `chars` characters.
pub fn char_pieces(text: &str, chars: usize) -> Vec<String> {
    let chars = chars.max(1);
    let all: Vec<char> = text.chars().collect();
    all.chunks(chars).map(|c| c.iter().collect()).collect()
}
