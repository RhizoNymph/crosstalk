//! The fixture's row hasher: a stand-in for the spec's digest function.
//!
//! The spec defines an export's digest as
//! `BLAKE3-derive_key(ROW_DIGEST_CONTEXT, …)` over the framed canonical
//! row encoding (`export::digest`), and leaves the hash behind the
//! [`RowHasher`] trait. The UI crate has no BLAKE3 dependency, so the
//! fixture hashes with four FNV-1a 64-bit lanes (each with its own offset
//! basis, finished with the SplitMix64 mixer) keyed by the same context
//! string. The framing, the row encoding, the order and the count are the
//! spec's; only the hash function differs, so a fixture digest never
//! equals a gateway digest of the same rows. It is deterministic: the same
//! rows always give the same digest.

use crosstalk_spec::interfaces::l8_surface::export::{ROW_DIGEST_CONTEXT, RowHasher};
use crosstalk_spec::support::Blake3;

const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

/// The FNV-1a 64-bit offset basis, and three others derived from it, one
/// per lane.
const LANE_BASES: [u64; 4] = [
    0xcbf2_9ce4_8422_2325,
    0x8422_2325_cbf2_9ce4,
    0x9e37_79b9_7f4a_7c15,
    0xbf58_476d_1ce4_e5b9,
];

/// A [`RowHasher`] standing in for BLAKE3 in derive-key mode.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RowDigest {
    lanes: [u64; 4],
}

impl RowDigest {
    /// A fresh hasher keyed by [`ROW_DIGEST_CONTEXT`], as the spec's
    /// `blake3::Hasher::new_derive_key(ROW_DIGEST_CONTEXT)` is.
    pub fn new() -> Self {
        let mut digest = Self { lanes: LANE_BASES };
        let context = ROW_DIGEST_CONTEXT.as_bytes();
        let len = u64::try_from(context.len()).unwrap_or(u64::MAX);
        digest.update(&len.to_le_bytes());
        digest.update(context);
        digest
    }
}

impl Default for RowDigest {
    fn default() -> Self {
        Self::new()
    }
}

/// SplitMix64's output mixer: spreads every input bit over the word.
fn mix(mut z: u64) -> u64 {
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}

impl RowHasher for RowDigest {
    fn update(&mut self, bytes: &[u8]) {
        for (index, lane) in (0u8..).zip(self.lanes.iter_mut()) {
            for byte in bytes {
                *lane ^= u64::from(byte ^ index);
                *lane = lane.wrapping_mul(FNV_PRIME);
            }
        }
    }

    fn finalize(&self) -> Blake3 {
        let mut out = [0u8; 32];
        for (chunk, lane) in out.as_chunks_mut::<8>().0.iter_mut().zip(self.lanes) {
            *chunk = mix(lane).to_le_bytes();
        }
        Blake3::from_bytes(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn digest(parts: &[&[u8]]) -> Blake3 {
        let mut hasher = RowDigest::new();
        for part in parts {
            hasher.update(part);
        }
        hasher.finalize()
    }

    #[test]
    fn the_same_bytes_give_the_same_digest_however_they_are_fed() {
        assert_eq!(digest(&[b"abc", b"def"]), digest(&[b"abcdef"]));
        assert_eq!(digest(&[]), digest(&[b""]));
    }

    #[test]
    fn different_bytes_give_different_digests() {
        let all = [
            digest(&[]),
            digest(&[b"a"]),
            digest(&[b"b"]),
            digest(&[b"ab"]),
            digest(&[b"ba"]),
            digest(&[&[0]]),
        ];
        for (i, a) in all.iter().enumerate() {
            for b in &all[i + 1..] {
                assert_ne!(a, b);
            }
        }
    }

    #[test]
    fn every_lane_contributes() {
        let bytes = *digest(&[b"row"]).as_bytes();
        let lanes: Vec<&[u8]> = bytes.chunks(8).collect();
        for (i, a) in lanes.iter().enumerate() {
            for b in &lanes[i + 1..] {
                assert_ne!(a, b);
            }
        }
    }
}
