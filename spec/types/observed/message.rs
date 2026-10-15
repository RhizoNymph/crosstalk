//! Canonical messages, independent of provider wire format.
//!
//! The role is encoded in the body variant, so a message can only hold parts
//! that role can produce: tool calls only in assistant messages, and tool
//! results only in tool messages or, for server-executed tools, in the
//! assistant message that made the call. Normalizers split provider messages that mix them
//! (Anthropic puts `tool_result` blocks inside user turns) into one canonical
//! message per role, in their original order.
//!
//! Blocks a normalizer does not recognize are kept as [`Unknown`] parts
//! rather than dropping the exchange, so read-side detection still sees the
//! rest of it and the exchange can be re-normalized later.
//!
//! **Part text.** Spans and content matches locate text by a [`PartRef`] and
//! a byte range. The text a range indexes is [`Message::part_text`] of the
//! part ([`text`]), so provenance and the evidence page cut the same bytes.

pub mod text;

use serde::{Deserialize, Serialize};

use crate::ids::MessageHash;
use crate::support::NonEmpty;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Message {
    pub hash: MessageHash,
    pub body: MessageBody,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MessageBody {
    System(Vec<SystemPart>),
    User(Vec<UserPart>),
    Assistant(Vec<AssistantPart>),
    Tool(NonEmpty<ToolResult>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Role {
    System,
    User,
    Assistant,
    Tool,
}

impl MessageBody {
    pub fn role(&self) -> Role {
        match self {
            Self::System(_) => Role::System,
            Self::User(_) => Role::User,
            Self::Assistant(_) => Role::Assistant,
            Self::Tool(_) => Role::Tool,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Text(pub String);

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SystemPart {
    Text(Text),
    Unknown(Unknown),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UserPart {
    Text(Text),
    Media(Media),
    Unknown(Unknown),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AssistantPart {
    Text(Text),
    Reasoning(Reasoning),
    ToolCall(ToolCall),
    /// The result of a server-executed tool call (provider-hosted web
    /// search, web fetch, code execution), returned inside the response. Its
    /// call is an earlier `ToolCall` with `ToolExecution::Server` in the same
    /// message.
    ServerToolResult(ToolResult),
    Unknown(Unknown),
}

/// A block the normalizer did not recognize, kept verbatim.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Unknown {
    /// The provider's block type (`"type"` field, or equivalent).
    pub kind: String,
    /// The block as canonical JSON.
    pub raw: CanonicalJson,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Reasoning {
    Visible(Text),
    /// The provider returned an encrypted or redacted block. Kept so the
    /// message hash matches what the provider will see echoed back.
    Opaque {
        signature: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Media {
    pub kind: MediaKind,
    /// The media bytes, stored as their own blob.
    pub blob: MessageHash,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MediaKind {
    Image,
    Audio,
    Document,
}

/// The id a provider assigned to a tool call. Unique within one conversation.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ToolCallId(pub String);

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ToolName(pub String);

/// JSON text in canonical form: RFC 8785 (sorted keys, no insignificant
/// whitespace, canonical escapes), except that a number is written as its
/// exact decimal value rather than through an IEEE double, so integers beyond
/// 2^53 (ids in tool arguments) keep every digit. Two semantically equal JSON
/// values have the same canonical text.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CanonicalJson(pub String);

/// Tool-call arguments.
///
/// Stored canonically, because harnesses re-serialize arguments when they
/// echo a response back in the next request (Anthropic `tool_use.input` is a
/// JSON object, not text), and the echoed message must hash the same as the
/// original.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ToolArguments {
    Json(CanonicalJson),
    /// The model produced text that is not valid JSON (possible where the
    /// protocol carries arguments as a string). Kept exactly.
    Invalid(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToolCall {
    pub id: ToolCallId,
    pub name: ToolName,
    pub arguments: ToolArguments,
    /// Server-side tools (provider-hosted web fetch, code execution) run
    /// upstream. Their results arrive in the response, not in a later
    /// request.
    pub execution: ToolExecution,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ToolExecution {
    Client,
    Server,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToolResult {
    pub call_id: ToolCallId,
    pub content: Vec<ToolResultContent>,
    pub outcome: ToolOutcome,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ToolResultContent {
    Text(Text),
    Media(Media),
    Unknown(Unknown),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ToolOutcome {
    Success,
    Error,
}

/// Points at one part of one message, by position in its part list.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct PartRef {
    pub message: MessageHash,
    pub index: u16,
}
