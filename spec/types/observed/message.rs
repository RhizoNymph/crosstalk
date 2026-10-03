//! Canonical messages, independent of provider wire format.
//!
//! The role is encoded in the body variant, so a message can only hold parts
//! that role can produce: tool calls only in assistant messages, tool results
//! only in tool messages. Normalizers split provider messages that mix them
//! (Anthropic puts `tool_result` blocks inside user turns) into one canonical
//! message per role, in their original order.

use crate::ids::MessageHash;
use crate::support::NonEmpty;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Message {
    pub hash: MessageHash,
    pub body: MessageBody,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MessageBody {
    System(Vec<Text>),
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
pub enum UserPart {
    Text(Text),
    Media(Media),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AssistantPart {
    Text(Text),
    Reasoning(Reasoning),
    ToolCall(ToolCall),
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
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ToolCallId(pub String);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ToolName(pub String);

/// Tool arguments exactly as the model produced them (JSON text). Kept raw:
/// extraction parses it, but hashing and matching need the original bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToolArguments(pub String);

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
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ToolOutcome {
    Success,
    Error,
}

/// Points at one part of one message, by position in its part list.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PartRef {
    pub message: MessageHash,
    pub index: u16,
}
