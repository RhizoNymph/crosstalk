//! The canonical encoding of a message body, and the message hash.
//!
//! A body's encoding is the canonical JSON text ([`super::json`]: sorted
//! keys, no whitespace, fixed escapes) of its JSON shape:
//!
//! | Body or part | JSON |
//! | --- | --- |
//! | `MessageBody` | `{"type": "system" \| "user" \| "assistant" \| "tool", "data": [parts]}` (a tool body's parts are its results) |
//! | `Text` part | `{"type": "text", "data": "<text>"}` |
//! | `Reasoning::Visible` | `{"type": "reasoning", "data": {"type": "visible", "data": {"signature": "<signature>" \| null, "text": "<text>"}}}` |
//! | `Reasoning::Opaque` | `{"type": "reasoning", "data": {"type": "opaque", "data": {"signature": "<payload>"}}}` |
//! | `Media` | `{"type": "media", "data": {"blob": "<hex>", "kind": "image" \| "audio" \| "document"}}` |
//! | `Unknown` | `{"type": "unknown", "data": {"kind": "<block type>", "raw": "<canonical JSON text>"}}` |
//! | `ToolCall` | `{"type": "tool_call", "data": {"arguments": {"type": "json" \| "invalid", "data": "<text>"}, "execution": "client" \| "server", "id": "..", "name": ".."}}` |
//! | `ToolResult` (a tool body's item, or `server_tool_result`'s data) | `{"call_id": "..", "content": [{"type": "text" \| "media" \| "unknown", "data": ..}], "outcome": "success" \| "error"}` |
//!
//! Canonical JSON inside a body (arguments, an unknown block) is carried as
//! a JSON string holding its text, so its exact numbers survive.
//!
//! The [`MessageHash`] of a body is the BLAKE3 digest of its encoding, the
//! same digest the blob store keys the encoding by
//! (`canonical.message.hash-is-blake3-of-encoding`). [`decode`] accepts only
//! bytes [`encode`] writes: canonical text of a valid body.

mod mirror;

use std::fmt;

use super::json::{self, JsonError};
use super::{Message, MessageBody};
use crate::ids::MessageHash;
use crate::support::Blake3;

/// Why bytes are not the canonical encoding of a message body.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DecodeError {
    /// Not JSON the canonical parser accepts.
    NotJson(JsonError),
    /// JSON, but not the canonical text of its value, or not the text
    /// [`encode`] writes for the body it holds.
    NotCanonical,
    /// Canonical JSON that is not a message body's shape.
    Shape { reason: String },
    /// A tool message with no result.
    EmptyTool,
    /// Canonical JSON inside the body (arguments, an unknown block's raw
    /// JSON) that is not canonical.
    NonCanonicalJson,
}

impl fmt::Display for DecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotJson(error) => write!(f, "not JSON: {error}"),
            Self::NotCanonical => f.write_str("not in canonical form"),
            Self::Shape { reason } => write!(f, "not a message body: {reason}"),
            Self::EmptyTool => f.write_str("a tool message with no result"),
            Self::NonCanonicalJson => {
                f.write_str("canonical JSON inside the body is not canonical")
            }
        }
    }
}

impl std::error::Error for DecodeError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::NotJson(error) => Some(error),
            _ => None,
        }
    }
}

impl From<JsonError> for DecodeError {
    fn from(error: JsonError) -> Self {
        Self::NotJson(error)
    }
}

/// The canonical encoding of `body`.
pub fn encode(body: &MessageBody) -> Vec<u8> {
    let mirror = mirror::Body::from(body);
    // Infallible: the mirror is structs, adjacently tagged enums, strings,
    // string vectors and `MessageHash` (which writes its hex string), with
    // no maps and no numbers. serde_json fails only on non-string map keys
    // or a `Serialize` impl that reports an error, and none of these does.
    let text = serde_json::to_string(&mirror).expect("a message body always serializes");
    // Infallible: serde_json writes valid JSON, nested at most six levels
    // and without numbers, which the canonical parser always accepts.
    let canonical = json::canonicalize(&text).expect("serde_json output is valid JSON");
    canonical.0.into_bytes()
}

/// The body `bytes` encode, accepting only what [`encode`] writes: `Ok(body)`
/// exactly when `encode(&body) == bytes`
/// (`canonical.encoding.decode-inverts-encode`).
pub fn decode(bytes: &[u8]) -> Result<MessageBody, DecodeError> {
    let parsed = json::Json::parse_bytes(bytes)?;
    if parsed.canonical_text().as_bytes() != bytes {
        return Err(DecodeError::NotCanonical);
    }
    let mirror: mirror::Body =
        serde_json::from_slice(bytes).map_err(|error| DecodeError::Shape {
            reason: error.to_string(),
        })?;
    let body = MessageBody::try_from(mirror).map_err(DecodeError::from)?;
    // Canonical text of a valid body's shape that `encode` would still not
    // write (an optional field left out, which serde reads as `None`) is
    // refused: one body, one encoding.
    if encode(&body) != bytes {
        return Err(DecodeError::NotCanonical);
    }
    Ok(body)
}

/// The BLAKE3 digest of `bytes` as a message hash: what the blob store keys
/// `bytes` by.
pub fn hash_bytes(bytes: &[u8]) -> MessageHash {
    MessageHash::from_digest(Blake3::of(bytes))
}

/// The hash of `body`: the BLAKE3 digest of its encoding.
pub fn hash(body: &MessageBody) -> MessageHash {
    hash_bytes(&encode(body))
}

/// `body` as a message, with its hash.
pub fn message(body: MessageBody) -> Message {
    Message {
        hash: hash(&body),
        body,
    }
}
