//! The content key of a blob: the BLAKE3 digest of its bytes, as the spec's
//! [`MessageHash`].

use crosstalk_spec::ids::MessageHash;
use crosstalk_spec::support::Blake3;

/// The key `bytes` are stored under: the unkeyed BLAKE3 hash of exactly
/// those bytes. Its hex ([`Blake3::to_hex`]) is the same text
/// `blake3::Hash::to_hex` writes.
pub fn message_hash(bytes: &[u8]) -> MessageHash {
    MessageHash::from_digest(Blake3::from_bytes(*blake3::hash(bytes).as_bytes()))
}

/// Whether `bytes` are the body `hash` names.
pub(super) fn matches(hash: MessageHash, bytes: &[u8]) -> bool {
    message_hash(bytes) == hash
}
