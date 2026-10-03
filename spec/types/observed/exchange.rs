//! One request/response round trip through the proxy.
//!
//! An exchange exists in two forms. While it is in flight it lives only in
//! the proxy node's memory, and its position is an [`ExchangeStage`]. Once
//! normalized it becomes an [`Exchange`] record: immutable, with the request
//! and response referenced by message hash.

use crate::ids::{AgentId, ConversationId, ExchangeId, KeyHash, MessageHash};
use crate::support::Timestamp;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Provider {
    AnthropicMessages,
    OpenAiChat,
    OpenAiResponses,
    GeminiGenerate,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ModelName(pub String);

/// The value of an `x-agent-id` style header the harness may send.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct AgentHeader(pub String);

/// What the proxy knows about an exchange before reading any bodies.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExchangeMeta {
    pub id: ExchangeId,
    pub provider: Provider,
    pub model: ModelName,
    pub key: KeyHash,
    pub agent_header: Option<AgentHeader>,
    pub started_at: Timestamp,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Exchange {
    pub meta: ExchangeMeta,
    /// The full message history sent, in order. Most of it repeats earlier
    /// exchanges; threading finds the new part.
    pub request: Vec<MessageHash>,
    pub outcome: ExchangeOutcome,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExchangeOutcome {
    Completed {
        response: MessageHash,
        finished_at: Timestamp,
        stop: StopReason,
        usage: Option<TokenUsage>,
    },
    /// Failed exchanges are still captured: the request side carries the
    /// tool results that read-side detection needs.
    Failed {
        partial_response: Option<MessageHash>,
        failed_at: Timestamp,
        failure: ExchangeFailure,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StopReason {
    EndTurn,
    ToolUse,
    MaxTokens,
    StopSequence,
    Refusal,
    Other,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TokenUsage {
    pub input: u32,
    pub output: u32,
    pub cache_read: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExchangeFailure {
    Upstream { status: u16 },
    UpstreamUnreachable,
    ClientDisconnected,
    Timeout,
}

/// Where an exchange is in the pipeline.
///
/// `Forwarded` through `Failed` are proxy-side and in memory only.
/// `Captured` onward are pipeline states recorded against the exchange id.
///
/// ```text
/// Forwarded ─first chunk─▶ Responding ─done─▶ Completed ─┐
///     │                        │                        ├─▶ Captured ─▶ Normalized ─▶ Threaded
///     └─upstream error─▶ Failed ◀─client disconnect─┘    │
///                         └──────────────────────────────┘
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExchangeStage {
    Forwarded {
        at: Timestamp,
    },
    Responding {
        first_chunk_at: Timestamp,
    },
    Completed,
    Failed(ExchangeFailure),
    Captured,
    Normalized,
    Threaded {
        conversation: ConversationId,
        agent: AgentId,
    },
}
