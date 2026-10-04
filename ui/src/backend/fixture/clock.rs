//! The fixture's fixed clock and its id mint.
//!
//! The world covers [`DAYS`] days ending at [`NOW`]. Every id is a ULID whose
//! time part is the entity's creation time, so ids sort by time like the
//! gateway's.

use std::num::NonZeroU64;

use crosstalk_spec::aggregates::series::BucketWidth;
use crosstalk_spec::support::Timestamp;

use super::rng::Rng;

/// 2026-10-03T00:00:00Z: the end of the generated data.
pub const NOW: Timestamp = Timestamp::from_micros(1_790_985_600_000_000);

pub const SECOND: u64 = 1_000_000;
pub const MINUTE: u64 = 60 * SECOND;
pub const HOUR: u64 = 60 * MINUTE;
pub const DAY: u64 = 24 * HOUR;

/// How many days of traffic the world holds.
pub const DAYS: u64 = 7;

/// The first instant of generated traffic.
pub const START: Timestamp = Timestamp::from_micros(NOW.as_micros() - DAYS * DAY);

/// The width of every aggregate bucket: windows start and end on its
/// multiples.
pub const BUCKET: BucketWidth = BucketWidth::from_micros(match NonZeroU64::new(5 * MINUTE) {
    Some(width) => width,
    None => NonZeroU64::MIN,
});

/// Every bucket before this is final. A bucket boundary.
pub const WATERMARK: Timestamp = Timestamp::from_micros(NOW.as_micros() - 10 * MINUTE);

/// The correlation window: a read further than this after a write is not a
/// co-access.
pub const CORRELATION_WINDOW: std::time::Duration = std::time::Duration::from_secs(24 * 3600);

/// `NOW` minus `micros`.
pub const fn ago(micros: u64) -> Timestamp {
    Timestamp::from_micros(NOW.as_micros() - micros)
}

pub const fn plus(at: Timestamp, micros: u64) -> Timestamp {
    Timestamp::from_micros(at.as_micros().saturating_add(micros))
}

pub const fn minus(at: Timestamp, micros: u64) -> Timestamp {
    Timestamp::from_micros(at.as_micros().saturating_sub(micros))
}

/// Mints unique ULIDs: 48 bits of milliseconds, 56 random bits and a 24-bit
/// counter, so two ids minted by one mint never collide.
#[derive(Debug, Clone)]
pub struct Mint {
    rng: Rng,
    counter: u32,
}

impl Mint {
    pub fn new(seed: u64) -> Self {
        Self {
            rng: Rng::fork(seed, "ids"),
            counter: 0,
        }
    }

    pub fn ulid(&mut self, at: Timestamp) -> u128 {
        let ms = u128::from(at.as_micros() / 1000) & ((1u128 << 48) - 1);
        let random = u128::from(self.rng.next_u64() >> 8);
        self.counter = self.counter.wrapping_add(1) & 0x00ff_ffff;
        (ms << 80) | (random << 24) | u128::from(self.counter)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn now_is_2026_10_03() {
        assert_eq!(NOW.as_micros(), 1_790_985_600_000_000);
        assert_eq!(NOW.as_micros() - START.as_micros(), 7 * DAY);
    }

    #[test]
    fn the_clock_sits_on_bucket_boundaries() {
        for at in [NOW, START, WATERMARK] {
            assert!(BUCKET.is_boundary(at), "{at:?}");
        }
    }

    #[test]
    fn minted_ids_are_unique_and_carry_time() {
        let mut mint = Mint::new(1);
        let at = ago(HOUR);
        let a = mint.ulid(at);
        let b = mint.ulid(at);
        assert_ne!(a, b);
        let ms = u64::try_from(a >> 80).expect("48-bit milliseconds");
        assert_eq!(Timestamp::from_micros(ms * 1000), at);
    }
}
