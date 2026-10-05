//! How L3's values are stored in Postgres columns.
//!
//! - Entity ids are their ULID text (`text`), which sorts like the id.
//! - Spec values with a wire form (agent states, evidence, merge records,
//!   vetoes, harness claims, bus events) are their wire JSON in a `text`
//!   column, decoded strictly through the spec's serde (and so through its
//!   checked constructors). The workspace's sqlx has no `json` feature.
//! - Timestamps are microseconds since the epoch (`bigint`).
//! - Digests are their 32 bytes (`bytea`).
//!
//! Every decode failure is a [`CodecError`]: a stored value the spec would
//! refuse, which only a bug or a hand edit can produce.

use crosstalk_spec::ids::{EntityId, InvalidUlidText, MessageHash};
use crosstalk_spec::observed::exchange::ConnectionId;
use crosstalk_spec::support::{Blake3, Timestamp};
use serde::Serialize;
use serde::de::DeserializeOwned;

/// A stored value that does not decode.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum CodecError {
    #[error("{column} holds an invalid id: {reason}")]
    Id {
        column: &'static str,
        reason: String,
    },
    #[error("{column} holds JSON the spec refuses: {reason}")]
    Json {
        column: &'static str,
        reason: String,
    },
    #[error("{column} holds a negative or oversized number: {value}")]
    Number { column: &'static str, value: i64 },
    #[error("{column} holds {len} bytes, not a 32-byte digest")]
    Digest { column: &'static str, len: usize },
    #[error("a value could not be encoded: {reason}")]
    Encode { reason: String },
}

/// The id as stored: its ULID text, as every entity id writes it on the
/// wire. (`ConnectionId` is the spec's public conversion between a bare
/// 128-bit ULID and its text.)
pub(crate) fn id_text<I: EntityId>(id: I) -> String {
    ConnectionId(id.as_ulid()).ulid_text()
}

/// The id a stored text names.
pub(crate) fn id_of<I: EntityId>(column: &'static str, text: &str) -> Result<I, CodecError> {
    ConnectionId::from_ulid_text(text)
        .map(|raw| I::from_ulid(raw.0))
        .map_err(|error: InvalidUlidText| CodecError::Id {
            column,
            reason: format!("{error:?}"),
        })
}

/// A value's wire JSON.
pub(crate) fn json<T: Serialize>(value: &T) -> Result<String, CodecError> {
    serde_json::to_string(value).map_err(|error| CodecError::Encode {
        reason: error.to_string(),
    })
}

/// The value stored wire JSON decodes to.
pub(crate) fn from_json<T: DeserializeOwned>(
    column: &'static str,
    text: &str,
) -> Result<T, CodecError> {
    serde_json::from_str(text).map_err(|error| CodecError::Json {
        column,
        reason: error.to_string(),
    })
}

/// A timestamp as stored. Times past `i64::MAX` microseconds (year 294247)
/// are refused rather than wrapped.
pub(crate) fn micros(at: Timestamp) -> Result<i64, CodecError> {
    i64::try_from(at.as_micros()).map_err(|_| CodecError::Encode {
        reason: format!("timestamp {} is beyond the stored range", at.as_micros()),
    })
}

/// The timestamp a stored number names.
pub(crate) fn timestamp(column: &'static str, value: i64) -> Result<Timestamp, CodecError> {
    u64::try_from(value)
        .map(Timestamp::from_micros)
        .map_err(|_| CodecError::Number { column, value })
}

/// A count as stored.
pub(crate) fn count(value: usize) -> Result<i32, CodecError> {
    i32::try_from(value).map_err(|_| CodecError::Encode {
        reason: format!("count {value} is beyond the stored range"),
    })
}

/// The count a stored number names.
pub(crate) fn count_of(column: &'static str, value: i32) -> Result<u32, CodecError> {
    u32::try_from(value).map_err(|_| CodecError::Number {
        column,
        value: i64::from(value),
    })
}

/// A digest's bytes.
pub(crate) fn digest_bytes(digest: &Blake3) -> Vec<u8> {
    digest.as_bytes().to_vec()
}

/// The digest stored bytes hold.
pub(crate) fn digest(column: &'static str, bytes: &[u8]) -> Result<Blake3, CodecError> {
    let array: [u8; 32] = bytes.try_into().map_err(|_| CodecError::Digest {
        column,
        len: bytes.len(),
    })?;
    Ok(Blake3::from_bytes(array))
}

/// A message hash's bytes.
pub(crate) fn hash_bytes(hash: &MessageHash) -> Vec<u8> {
    digest_bytes(hash.digest())
}

/// The message hash stored bytes hold.
pub(crate) fn message_hash(column: &'static str, bytes: &[u8]) -> Result<MessageHash, CodecError> {
    digest(column, bytes).map(MessageHash::from_digest)
}
