//! Fingerprints: winnowed k-gram hashes of normalized text.

use std::num::NonZeroU16;

use crate::ids::SpanId;

/// A 64-bit hash of one k-gram of normalized text.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Fingerprint(pub u64);

impl Fingerprint {
    /// The index shard that owns this fingerprint. Sharding by fingerprint
    /// means a match never needs to look on more than one shard.
    pub fn shard(self, shards: NonZeroU16) -> u16 {
        let shards = u64::from(shards.get());
        u16::try_from(self.0 % shards).unwrap_or(0)
    }
}

/// Winnowing parameters. Any match at least `k + w - 1` characters long is
/// guaranteed to share a fingerprint; matches shorter than `k` never do.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WinnowParams {
    pub k: NonZeroU16,
    pub w: NonZeroU16,
}

/// A fingerprint and the byte offset in the text where its k-gram starts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PositionedFingerprint {
    pub fingerprint: Fingerprint,
    pub offset: u32,
}

/// One index lookup result: an indexed span that contains `fingerprint`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FingerprintHit {
    pub fingerprint: Fingerprint,
    pub span: SpanId,
    /// Offset of the k-gram in the span's text.
    pub span_offset: u32,
    /// Offset of the k-gram in the text being looked up.
    pub query_offset: u32,
}
