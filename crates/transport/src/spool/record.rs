//! The spool's on-disk record format.
//!
//! A segment file starts with [`SEGMENT_HEADER`] and holds records back to
//! back:
//!
//! ```text
//! u32 LE  payload length (at most MAX_PAYLOAD)
//! [u8;16] first 16 bytes of BLAKE3(record number LE ‖ payload)
//! u64 LE  record number (dense from 1 within a spool, never reused)
//! payload Envelope wire JSON (the bus codec's bytes, unchanged)
//! ```
//!
//! The checksum covers the number, so a record moved or duplicated within
//! the spool fails it as surely as a flipped bit.

/// The first bytes of every segment file.
pub(crate) const SEGMENT_HEADER: &[u8; 8] = b"CTSPOOL1";

/// Bytes before a record's payload: length, checksum, number.
pub(crate) const RECORD_HEADER_LEN: usize = 4 + 16 + 8;

/// The largest payload a record holds: 16 MiB.
pub(crate) const MAX_PAYLOAD: usize = 16 * 1024 * 1024;

/// The record checksum: the first 16 bytes of BLAKE3 over the number
/// (little-endian) and the payload.
pub(crate) fn checksum(number: u64, payload: &[u8]) -> [u8; 16] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(&number.to_le_bytes());
    hasher.update(payload);
    let digest = hasher.finalize();
    let mut out = [0u8; 16];
    out.copy_from_slice(&digest.as_bytes()[..16]);
    out
}

/// A whole record's bytes. The caller checks `payload.len() <= MAX_PAYLOAD`.
pub(crate) fn encode(number: u64, payload: &[u8]) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(RECORD_HEADER_LEN + payload.len());
    // The caller bounds the payload at MAX_PAYLOAD, far below u32::MAX.
    let len = u32::try_from(payload.len()).unwrap_or(u32::MAX);
    bytes.extend_from_slice(&len.to_le_bytes());
    bytes.extend_from_slice(&checksum(number, payload));
    bytes.extend_from_slice(&number.to_le_bytes());
    bytes.extend_from_slice(payload);
    bytes
}

/// What the bytes at a position hold.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Parsed<'a> {
    /// A whole, intact record of `size` bytes (header and payload).
    Record {
        number: u64,
        payload: &'a [u8],
        size: usize,
    },
    /// Fewer bytes than the record header, or than the length it claims:
    /// an append cut short, if this is the end of the last segment.
    Short,
    /// A record that ends at `size` bytes but fails its checksum, or
    /// claims a payload above [`MAX_PAYLOAD`].
    Bad { size: usize },
}

/// Parse the record at the start of `bytes`.
pub(crate) fn parse(bytes: &[u8]) -> Parsed<'_> {
    let Some(header) = bytes.get(..RECORD_HEADER_LEN) else {
        return Parsed::Short;
    };
    let mut len = [0u8; 4];
    len.copy_from_slice(&header[..4]);
    let len = u32::from_le_bytes(len) as usize;
    let size = RECORD_HEADER_LEN.saturating_add(len);
    let Some(payload) = bytes.get(RECORD_HEADER_LEN..size) else {
        return Parsed::Short;
    };
    if len > MAX_PAYLOAD {
        return Parsed::Bad { size };
    }
    let mut sum = [0u8; 16];
    sum.copy_from_slice(&header[4..20]);
    let mut number = [0u8; 8];
    number.copy_from_slice(&header[20..28]);
    let number = u64::from_le_bytes(number);
    if checksum(number, payload) != sum {
        return Parsed::Bad { size };
    }
    Parsed::Record {
        number,
        payload,
        size,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        /// The record format round-trips.
        #[test]
        fn records_round_trip(number in any::<u64>(), payload in proptest::collection::vec(any::<u8>(), 0..512)) {
            let bytes = encode(number, &payload);
            prop_assert_eq!(bytes.len(), RECORD_HEADER_LEN + payload.len());
            prop_assert_eq!(
                parse(&bytes),
                Parsed::Record { number, payload: &payload, size: bytes.len() }
            );
            // Trailing bytes belong to the next record.
            let mut longer = bytes.clone();
            longer.extend_from_slice(b"next");
            prop_assert_eq!(
                parse(&longer),
                Parsed::Record { number, payload: &payload, size: bytes.len() }
            );
        }

        /// Every proper prefix is short; every single flipped bit is bad.
        #[test]
        fn prefixes_are_short_and_flips_are_bad(number in any::<u64>(), payload in proptest::collection::vec(any::<u8>(), 1..64), bit in any::<prop::sample::Index>()) {
            let bytes = encode(number, &payload);
            for cut in 0..bytes.len() {
                prop_assert_eq!(parse(&bytes[..cut]), Parsed::Short);
            }
            // Flip a bit outside the length field (a changed length moves
            // the record's end instead).
            let index = 4 * 8 + bit.index(bytes.len() * 8 - 4 * 8);
            let mut flipped = bytes.clone();
            flipped[index / 8] ^= 1 << (index % 8);
            prop_assert_eq!(parse(&flipped), Parsed::Bad { size: bytes.len() });
        }
    }

    #[test]
    fn an_oversized_length_is_short_until_its_claimed_end_then_bad() {
        let mut bytes = encode(1, b"x");
        let huge = u32::try_from(MAX_PAYLOAD + 1).expect("fits");
        bytes[..4].copy_from_slice(&huge.to_le_bytes());
        assert_eq!(parse(&bytes), Parsed::Short);
        bytes.resize(RECORD_HEADER_LEN + MAX_PAYLOAD + 1, 0);
        assert_eq!(
            parse(&bytes),
            Parsed::Bad {
                size: RECORD_HEADER_LEN + MAX_PAYLOAD + 1
            }
        );
    }
}
