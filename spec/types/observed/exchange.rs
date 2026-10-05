//! One request/response round trip through the proxy.
//!
//! An exchange exists in two forms. While it is in flight it lives only in
//! the proxy node's memory, and its position is an [`ExchangeStage`]. Once
//! normalized it becomes an [`Exchange`] record: immutable, with the request
//! and response referenced by message hash.
//!
//! On a WebSocket transport (Codex, pi and oh-my-pi's Codex mode) one
//! connection carries many exchanges, one per `response.create` turn, and each
//! turn sends only the input added since the previous response. Such an
//! exchange's `request` holds only that increment, and its [`Continuation`]
//! names the response it continues. Reconstruction resolves the full history.

use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::ids::{self, AgentId, ConversationId, ExchangeId, InvalidUlidText, MessageHash};
use crate::observed::client::ClientContext;
use crate::support::Timestamp;
use crate::wire::{Rejected, decode_text};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WireProtocol {
    AnthropicMessages,
    OpenAiChat,
    OpenAiResponses,
    GeminiGenerate,
    /// Google's Code Assist wrapping of generateContent
    /// (`v1internal:streamGenerateContent`), used by Gemini CLI and
    /// Antigravity subscriptions.
    GeminiCodeAssist,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Transport {
    /// One request, one complete response body.
    Http,
    /// One request, a server-sent-event stream.
    Sse,
    /// A long-lived connection carrying many turns.
    WebSocket,
}

/// A WebSocket connection through the proxy: a ULID the proxy node mints
/// when it accepts the upgrade, like every entity id ([`crate::ids`]).
///
/// On the wire, its ULID text (`"01J9Z3K8M4Q7R2T5V6W8X9Y0ZA"`), never the
/// bare `u128`: a JSON number cannot carry 128 bits exactly, and the id is
/// an entity the proxy creates (stored in [`Continuation::Increment`],
/// compared across nodes), not a digest of content, so it takes the entity
/// ids' text rather than hex. Not a request: no client names a connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ConnectionId(pub u128);

impl ConnectionId {
    /// The id as ULID text, as [`crate::ids::AgentId::ulid_text`] writes it.
    pub fn ulid_text(self) -> String {
        ids::ulid_text(self.0)
    }

    /// The id `text` names, accepting exactly the text [`Self::ulid_text`]
    /// writes.
    pub fn from_ulid_text(text: &str) -> Result<Self, InvalidUlidText> {
        ids::parse_ulid_text(text).map(Self)
    }
}

impl ids::EntityId for ConnectionId {
    fn from_ulid(raw: u128) -> Self {
        Self(raw)
    }

    fn as_ulid(self) -> u128 {
        self.0
    }
}

impl Serialize for ConnectionId {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.ulid_text())
    }
}

impl<'de> Deserialize<'de> for ConnectionId {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        decode_text(deserializer, "ULID text", |text| {
            Self::from_ulid_text(&text)
        })
    }
}

/// The id a provider assigned to a response (`resp_…`, `msg_…`).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ResponseId(pub String);

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ModelName(pub String);

/// What the proxy knows about an exchange before its body is normalized.
/// Assembled when the exchange is handed to capture, once its request has
/// decoded.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct ExchangeMeta {
    pub id: ExchangeId,
    pub protocol: WireProtocol,
    /// `Http` or `Sse` from the response head's content type, `WebSocket`
    /// for a turn on a connection.
    pub transport: Transport,
    /// Read from the request body by the adapter, off the hot path.
    pub model: ModelName,
    pub client: ClientContext,
    pub started_at: Timestamp,
}

/// Whether `request` is the whole history or an increment.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "type",
    content = "data",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum Continuation {
    /// The request carries the full message history.
    FullHistory,
    /// The request carries only input added after `previous`
    /// (`previous_response_id`).
    Increment {
        previous: ResponseId,
        connection: Option<ConnectionId>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct Exchange {
    pub meta: ExchangeMeta,
    pub continuation: Continuation,
    /// The messages sent, in order: the full history, or the increment.
    pub request: Vec<MessageHash>,
    pub outcome: ExchangeOutcome,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "type",
    content = "data",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum ExchangeOutcome {
    Completed {
        response: MessageHash,
        response_id: Option<ResponseId>,
        first_chunk_at: Timestamp,
        finished_at: Timestamp,
        stop: StopReason,
        usage: Option<TokenUsage>,
    },
    /// Failed exchanges are still captured: the request side carries the
    /// tool results that read-side detection needs.
    Failed {
        partial_response: Option<MessageHash>,
        first_chunk_at: Option<Timestamp>,
        failed_at: Timestamp,
        failure: ExchangeFailure,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StopReason {
    EndTurn,
    ToolUse,
    MaxTokens,
    StopSequence,
    Refusal,
    /// The server aborted generation (SGLang `abort`).
    Aborted,
    Other,
}

/// The token counts a provider reported for one exchange, in one meaning
/// across protocols:
///
/// | Field | Counts |
/// | --- | --- |
/// | `input` | every prompt token: read from the cache, written to it, or neither (OpenAI's `prompt_tokens`) |
/// | `cache_read` | the part of `input` served from the prompt cache |
/// | `cache_write` | the part of `input` written to the prompt cache; `None` when the protocol does not report cache writes (OpenAI, Gemini cache implicitly) |
/// | `output` | every generated token, reasoning included |
/// | `reasoning` | the part of `output` spent on reasoning; `None` when the protocol does not report it apart |
///
/// So `input - cache_read - cache_write` ([`TokenUsage::uncached_input`])
/// is the prompt processed without the cache. Anthropic reports three
/// disjoint prompt counts, which map as `input = input_tokens +
/// cache_creation_input_tokens + cache_read_input_tokens`, `cache_read =
/// cache_read_input_tokens` and `cache_write =
/// Some(cache_creation_input_tokens)`.
///
/// Checked: the cache counts never exceed `input`, nor `reasoning`
/// `output`, so the parts always sum within their whole. On the wire, the
/// fields of [`TokenCounts`], decoded through [`TokenUsage::new`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", try_from = "TokenCounts")]
pub struct TokenUsage {
    input: u32,
    output: u32,
    cache_read: u32,
    cache_write: Option<u32>,
    reasoning: Option<u32>,
}

/// [`TokenUsage`]'s counts, unchecked: what a normalizer fills in from a
/// provider's usage, and what decoding reads before the check.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct TokenCounts {
    pub input: u32,
    pub output: u32,
    pub cache_read: u32,
    pub cache_write: Option<u32>,
    pub reasoning: Option<u32>,
}

/// Why counts are not a [`TokenUsage`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InvalidTokenUsage {
    /// `cache_read + cache_write` is more than `input`.
    CachedBeyondInput { input: u32, cached: u64 },
    /// `reasoning` is more than `output`.
    ReasoningBeyondOutput { output: u32, reasoning: u32 },
}

impl TryFrom<TokenCounts> for TokenUsage {
    type Error = Rejected<InvalidTokenUsage>;

    fn try_from(counts: TokenCounts) -> Result<Self, Self::Error> {
        Self::new(counts).map_err(|error| Rejected::new("token usage", error))
    }
}

impl TokenUsage {
    pub fn new(counts: TokenCounts) -> Result<Self, InvalidTokenUsage> {
        let TokenCounts {
            input,
            output,
            cache_read,
            cache_write,
            reasoning,
        } = counts;
        let cached = u64::from(cache_read) + u64::from(cache_write.unwrap_or(0));
        if cached > u64::from(input) {
            return Err(InvalidTokenUsage::CachedBeyondInput { input, cached });
        }
        if let Some(reasoning) = reasoning.filter(|reasoning| *reasoning > output) {
            return Err(InvalidTokenUsage::ReasoningBeyondOutput { output, reasoning });
        }
        Ok(Self {
            input,
            output,
            cache_read,
            cache_write,
            reasoning,
        })
    }

    pub const fn input(&self) -> u32 {
        self.input
    }

    pub const fn output(&self) -> u32 {
        self.output
    }

    pub const fn cache_read(&self) -> u32 {
        self.cache_read
    }

    pub const fn cache_write(&self) -> Option<u32> {
        self.cache_write
    }

    pub const fn reasoning(&self) -> Option<u32> {
        self.reasoning
    }

    /// The prompt tokens neither read from nor written to the cache.
    pub fn uncached_input(&self) -> u32 {
        // The check keeps the cache counts within `input`, so this never
        // saturates.
        self.input
            .saturating_sub(self.cache_read)
            .saturating_sub(self.cache_write.unwrap_or(0))
    }

    /// The counts, as [`TokenUsage::new`] took them.
    pub const fn counts(&self) -> TokenCounts {
        TokenCounts {
            input: self.input,
            output: self.output,
            cache_read: self.cache_read,
            cache_write: self.cache_write,
            reasoning: self.reasoning,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "type",
    content = "data",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum ExchangeFailure {
    /// A non-2xx status before any content.
    Upstream {
        status: u16,
    },
    UpstreamUnreachable,
    /// The stream ended before its terminal frame.
    StreamTruncated,
    /// The stream contained bytes the framer could not parse.
    MalformedStream {
        offset: u64,
    },
    /// The upstream sent an error event inside a 2xx stream.
    UpstreamErrorEvent,
    /// A complete 2xx response whose body the normalizer could not parse.
    UnparseableResponse,
    ClientDisconnected,
    Timeout,
}

/// Where an exchange is in the pipeline.
///
/// `Forwarded` through `Failed` are proxy-side and in memory only.
/// `Captured` onward are pipeline states recorded against the exchange id.
/// A response whose first content and terminal frame arrive in one chunk
/// passes through `Responding` within that chunk. The request body decodes
/// concurrently with these stages and never holds them up; an exchange
/// reaches `Captured` only once its request has decoded. If decoding fails,
/// the exchange still runs to `Completed` or `Failed`, then leaves the
/// pipeline counted as uncaptured.
///
/// ```text
/// Forwarded ─first content─▶ Responding ─terminal frame─▶ Completed ─┐
///     │                          │                                  ├─▶ Captured ─▶ Normalized ─▶ Threaded
///     └──────── failure ─────────┴──────▶ Failed ───────────────────┘
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
