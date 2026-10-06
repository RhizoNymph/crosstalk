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
//!
//! **Encoding.** A body's canonical encoding ([`encoding`]) is the canonical
//! JSON ([`json`]) of its JSON shape: the wire contract's conventions
//! applied to the types here, which is also the body's serde form. Its
//! BLAKE3 is the [`MessageHash`], the blob store's key for the encoding,
//! and [`encoding::decode`] reads exactly the bytes [`encoding::encode`]
//! writes. Bodies cross between layers as those bytes in the blob store;
//! their JSON appears on its own only in a [`NormalizedExchange`], which
//! stays in process (its serde pins goldens).
//!
//! [`NormalizedExchange`]: crate::interfaces::l1_canonical::NormalizedExchange

pub mod encoding;
pub mod json;
pub mod text;

use std::fmt;

use serde::{Deserialize, Serialize};

use crate::ids::MessageHash;
use crate::support::{InvalidHex, NonEmpty, from_hex, hex};
use crate::wire::Rejected;

/// A message body and its content id: the BLAKE3 of the body's canonical
/// encoding ([`encoding::hash`]).
///
/// The fields are public, so a value in memory can pair a body with
/// another hash; [`Message::new`] cannot, and decoding refuses it
/// (`canonical.message.hash-is-blake3-of-encoding`). On the wire
/// `{"hash": "<hex>", "body": <body>}`, the body in its encoding's shape.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", try_from = "RawMessage")]
pub struct Message {
    pub hash: MessageHash,
    pub body: MessageBody,
}

impl Message {
    /// `body` with its hash.
    pub fn new(body: MessageBody) -> Self {
        encoding::message(body)
    }
}

/// [`Message`]'s fields, decoded before the hash is checked.
#[derive(Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
struct RawMessage {
    hash: MessageHash,
    body: MessageBody,
}

/// A message whose hash is not its body's.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MessageHashMismatch {
    pub stated: MessageHash,
    pub encoded: MessageHash,
}

impl TryFrom<RawMessage> for Message {
    type Error = Rejected<MessageHashMismatch>;

    fn try_from(raw: RawMessage) -> Result<Self, Self::Error> {
        let encoded = encoding::hash(&raw.body);
        if encoded != raw.hash {
            return Err(Rejected::new(
                "message",
                MessageHashMismatch {
                    stated: raw.hash,
                    encoded,
                },
            ));
        }
        Ok(Self {
            hash: raw.hash,
            body: raw.body,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MessageBody {
    System(Vec<SystemPart>),
    User(Vec<UserPart>),
    Assistant(Vec<AssistantPart>),
    Tool(NonEmpty<ToolResult>),
}

/// On the wire, snake_case strings (`"system"`, `"user"`, ...).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
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
    /// Reasoning the provider showed, as text.
    Visible {
        text: Text,
        /// The provider's signature over the reasoning (Anthropic's
        /// `thinking.signature`), verbatim, which the harness must echo back
        /// unchanged for the provider to accept the block. `None` when the
        /// provider sent none, or an empty one (a stream cut before its
        /// `signature_delta`, a dialect without signatures).
        ///
        /// Part of the encoding, so of the hash: a body round-trips through
        /// the blob store only if every field is encoded
        /// (`canonical.encoding.round-trips`), and an echo carries the same
        /// signature its response did, so it still hashes like the response
        /// (`canonical.normalize.echo-stable`). Not part of the part's text.
        signature: Option<String>,
    },
    /// The provider returned an encrypted or redacted block. Kept so the
    /// message hash matches what the provider will see echoed back.
    Opaque { signature: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Media {
    pub kind: MediaKind,
    /// The media bytes, stored as their own blob: a [`MediaBlob`]'s hash.
    pub blob: MessageHash,
}

/// Media bytes (decoded from the provider's base64 or data URL) and their
/// content id, the BLAKE3 of the bytes
/// (`canonical.media.hash-of-decoded-bytes`): what a [`Media`] part's
/// `blob` names and what L1 stores under it.
///
/// Checked: the hash is always computed from the bytes. On the wire
/// `{"hash": "<hex>", "bytes": "<hex>"}` (raw bytes in lower-case hex, as
/// digests are); decoding recomputes the hash and refuses a mismatch.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", try_from = "RawMediaBlob")]
pub struct MediaBlob {
    hash: MessageHash,
    #[serde(serialize_with = "bytes_as_hex")]
    bytes: Vec<u8>,
}

impl MediaBlob {
    pub fn new(bytes: Vec<u8>) -> Self {
        Self {
            hash: encoding::hash_bytes(&bytes),
            bytes,
        }
    }

    pub const fn hash(&self) -> MessageHash {
        self.hash
    }

    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    pub fn into_bytes(self) -> Vec<u8> {
        self.bytes
    }
}

/// The hash and the length; the bytes are not printed.
impl fmt::Debug for MediaBlob {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MediaBlob")
            .field("hash", &self.hash)
            .field("len", &self.bytes.len())
            .finish()
    }
}

fn bytes_as_hex<S: serde::Serializer>(bytes: &[u8], serializer: S) -> Result<S::Ok, S::Error> {
    serializer.serialize_str(&hex(bytes))
}

/// [`MediaBlob`]'s fields, decoded before the hash is checked.
#[derive(Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
struct RawMediaBlob {
    hash: MessageHash,
    bytes: String,
}

/// Why JSON is not a [`MediaBlob`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InvalidMediaBlob {
    /// `bytes` is not lower-case hex.
    Bytes(InvalidHex),
    /// The hash is not the bytes' BLAKE3.
    HashMismatch {
        stated: MessageHash,
        encoded: MessageHash,
    },
}

impl TryFrom<RawMediaBlob> for MediaBlob {
    type Error = Rejected<InvalidMediaBlob>;

    fn try_from(raw: RawMediaBlob) -> Result<Self, Self::Error> {
        let reject = |error| Rejected::new("media blob", error);
        let bytes = from_hex(&raw.bytes).map_err(|error| reject(InvalidMediaBlob::Bytes(error)))?;
        let blob = Self::new(bytes);
        if blob.hash != raw.hash {
            return Err(reject(InvalidMediaBlob::HashMismatch {
                stated: raw.hash,
                encoded: blob.hash,
            }));
        }
        Ok(blob)
    }
}

/// On the wire, snake_case strings.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
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
    /// The provider's opaque signature over the call (Gemini's
    /// `thoughtSignature` on a `functionCall` part), verbatim, which the
    /// harness must echo back unchanged. `None` when the provider sent none
    /// or an empty one (Anthropic and OpenAI send none).
    ///
    /// Treated as [`Reasoning::Visible`]'s signature is: part of the
    /// encoding, so of the hash (an echo carries the same signature, so it
    /// still hashes like its response), and never part of the part's text.
    /// `None` is omitted from the encoding rather than written as `null`,
    /// so a call without a signature encodes and hashes as it did before
    /// the field existed
    /// (`canonical.tool-call.signature-verbatim`,
    /// `canonical.opaque.outside-part-text`).
    pub signature: Option<String>,
}

/// On the wire, snake_case strings.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
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

/// Whether a tool call succeeded, as the wire protocol flags it.
///
/// A normalizer sets `Success` or `Error` only from a failure flag the
/// protocol carries (Anthropic's `is_error`: `true` is `Error`, absent or
/// `false` is `Success`), and `Unknown` for a protocol whose tool results
/// carry no such flag (an OpenAI Chat `tool` message), whatever the
/// result's text says (`canonical.tool-outcome.unknown-without-flag`).
/// Reading failure out of a result's text is L5's, per known tool
/// (`WriteOutcome`).
///
/// [`WriteOutcome`]: crate::derived::flow::access::WriteOutcome
/// On the wire, snake_case strings.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolOutcome {
    Success,
    Error,
    /// The protocol has no failure flag for this result.
    Unknown,
}

/// Points at one part of one message, by position in its part list.
/// Ordered by message hash, then index.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct PartRef {
    pub message: MessageHash,
    pub index: u16,
}
