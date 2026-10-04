//! The canonical encoding of a message body, and the message hash.
//!
//! A body's encoding is the canonical JSON text ([`crate::json`]: sorted
//! keys, no whitespace, fixed escapes) of its JSON shape:
//!
//! | Body or part | JSON |
//! | --- | --- |
//! | `MessageBody` | `{"type": "system" \| "user" \| "assistant" \| "tool", "data": [parts]}` (a tool body's parts are its results) |
//! | `Text` part, `Reasoning::Visible` | `{"type": "text", "data": "<text>"}`, `{"type": "reasoning", "data": {"type": "visible", "data": "<text>"}}` |
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

mod wire;

use crosstalk_spec::ids::MessageHash;
use crosstalk_spec::observed::message::{Message, MessageBody};
use crosstalk_spec::support::Blake3;

use crate::json;

/// Why bytes are not the canonical encoding of a message body.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum DecodeError {
    #[error("not JSON: {0}")]
    NotJson(#[from] json::JsonError),
    #[error("not in canonical form")]
    NotCanonical,
    #[error("not a message body: {reason}")]
    Shape { reason: String },
    #[error("a tool message with no result")]
    EmptyTool,
    #[error("canonical JSON inside the body is not canonical")]
    NonCanonicalJson,
}

/// The canonical encoding of `body`.
pub fn encode(body: &MessageBody) -> Vec<u8> {
    let mirror = wire::Body::from(body);
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

/// The body `bytes` encode, accepting only what [`encode`] writes.
pub fn decode(bytes: &[u8]) -> Result<MessageBody, DecodeError> {
    let parsed = json::Json::parse_bytes(bytes)?;
    if parsed.canonical_text().as_bytes() != bytes {
        return Err(DecodeError::NotCanonical);
    }
    let mirror: wire::Body = serde_json::from_slice(bytes).map_err(|error| DecodeError::Shape {
        reason: error.to_string(),
    })?;
    MessageBody::try_from(mirror).map_err(|invalid| match invalid {
        wire::Invalid::EmptyTool => DecodeError::EmptyTool,
        wire::Invalid::NonCanonicalJson => DecodeError::NonCanonicalJson,
    })
}

/// The BLAKE3 digest of `bytes` as a message hash: what the blob store keys
/// `bytes` by.
pub fn hash_bytes(bytes: &[u8]) -> MessageHash {
    MessageHash::from_digest(Blake3::from_bytes(*blake3::hash(bytes).as_bytes()))
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
