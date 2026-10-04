//! The slice of the Anthropic Messages wire format the demo speaks: the
//! request messages a harness sends, the assistant message a model returns,
//! its non-streaming JSON document ([`AssistantMessage::to_document`]), its
//! event stream ([`sse::encode`]) and the reassembly of that stream on the
//! client side ([`assemble`]).

pub mod assemble;
pub mod sse;

use serde::{Deserialize, Serialize};

/// The `anthropic-version` header value harnesses send.
pub const API_VERSION: &str = "2023-06-01";

/// Who a request message is from. `System` is the turn Claude Code puts
/// inside `messages` (`--claude-code-shape`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Role {
    User,
    Assistant,
    System,
}

/// One content block of a request message.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Block {
    Text {
        text: String,
    },
    ToolUse {
        id: String,
        name: String,
        input: serde_json::Value,
    },
    ToolResult {
        tool_use_id: String,
        content: String,
        #[serde(default, skip_serializing_if = "std::ops::Not::not")]
        is_error: bool,
    },
}

/// A message's content: a bare string or a list of blocks.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Content {
    Text(String),
    Blocks(Vec<Block>),
}

/// One entry of a request's `messages`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Message {
    pub role: Role,
    pub content: Content,
}

/// A block a model can answer with. Narrower than [`Block`]: a model never
/// returns a tool result.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ResponseBlock {
    Text {
        text: String,
    },
    ToolUse {
        id: String,
        name: String,
        input: serde_json::Value,
    },
}

impl From<ResponseBlock> for Block {
    fn from(block: ResponseBlock) -> Self {
        match block {
            ResponseBlock::Text { text } => Block::Text { text },
            ResponseBlock::ToolUse { id, name, input } => Block::ToolUse { id, name, input },
        }
    }
}

/// Why the model stopped.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StopReason {
    EndTurn,
    ToolUse,
    MaxTokens,
    StopSequence,
}

/// Token counts as the API reports them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Usage {
    pub input_tokens: u64,
    pub output_tokens: u64,
}

/// A whole assistant message: what a non-streaming response carries and
/// what an event stream reassembles to.
#[derive(Debug, Clone, PartialEq)]
pub struct AssistantMessage {
    pub id: String,
    pub model: String,
    pub content: Vec<ResponseBlock>,
    pub stop_reason: StopReason,
    pub usage: Usage,
}

/// The non-streaming response document.
#[derive(Debug, Serialize, Deserialize)]
struct Document {
    id: String,
    #[serde(rename = "type")]
    kind: String,
    role: Role,
    model: String,
    content: Vec<ResponseBlock>,
    stop_reason: StopReason,
    stop_sequence: Option<String>,
    usage: Usage,
}

/// Why a non-streaming response document could not be read.
#[derive(Debug, thiserror::Error)]
pub enum DocumentError {
    #[error("not a message document: {0}")]
    Json(#[from] serde_json::Error),
    #[error("document type is {0:?}, not \"message\"")]
    NotAMessage(String),
}

impl AssistantMessage {
    /// The body of a non-streaming `POST /v1/messages` response.
    pub fn to_document(&self) -> serde_json::Value {
        serde_json::json!({
            "id": self.id,
            "type": "message",
            "role": "assistant",
            "model": self.model,
            "content": self.content,
            "stop_reason": self.stop_reason,
            "stop_sequence": null,
            "usage": self.usage,
        })
    }

    /// Reads a non-streaming response body.
    pub fn from_document(bytes: &[u8]) -> Result<Self, DocumentError> {
        let document: Document = serde_json::from_slice(bytes)?;
        if document.kind != "message" {
            return Err(DocumentError::NotAMessage(document.kind));
        }
        Ok(Self {
            id: document.id,
            model: document.model,
            content: document.content,
            stop_reason: document.stop_reason,
            usage: document.usage,
        })
    }
}

/// An Anthropic error document: `{"type":"error","error":{"type","message"}}`.
pub fn error_document(kind: &str, message: &str) -> serde_json::Value {
    serde_json::json!({
        "type": "error",
        "error": {"type": kind, "message": message},
    })
}
