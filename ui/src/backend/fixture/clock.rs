//! The fixture's fixed clock and its id mint.
//!
//! The world covers [`DAYS`] days ending at [`NOW`]. Every id is a ULID whose
//! time part is the entity's creation time, so ids sort by time like the
//! gateway's.

use std::num::NonZeroU64;

use crosstalk_spec::aggregates::series::BucketWidth;
use crosstalk_spec::support::{EmptyWindow, TimeWindow, Timestamp};

use super::rng::Rng;
use crate::config::ReplayConfig;

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

/// A bucket-aligned window holding every generated access and
/// confirmation (`[START, NOW + BUCKET)`): what a read "over all time"
/// counts in.
pub fn all_time() -> Result<TimeWindow, EmptyWindow> {
    TimeWindow::new(START, plus(NOW, BUCKET.as_micros().get()))
}

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

/// The fixture's present. The generated data always ends at [`NOW`]; a
/// live clock moves on from there in real time, so what an operator does
/// while the UI runs is stamped after the data and shows in a fresh default
/// view, whose window ends on the bucket boundary after the present.
#[derive(Debug, Clone, Copy)]
pub enum Clock {
    /// Always [`NOW`]: for tests and anything that must be reproducible.
    Fixed,
    /// [`NOW`] plus the real time since `started`.
    Live { started: std::time::Instant },
    /// A replay of the data's last stretch: `from` plus the real time
    /// since `started` times `speed`, stopping at [`NOW`] (it does not
    /// loop). Everything stamped after it is invisible.
    Replay {
        started: std::time::Instant,
        from: Timestamp,
        speed: u32,
    },
}

impl Clock {
    pub fn live() -> Self {
        Self::Live {
            started: std::time::Instant::now(),
        }
    }

    /// A replay of `config` starting now.
    pub fn replay(config: ReplayConfig) -> Self {
        Self::Replay {
            started: std::time::Instant::now(),
            from: ago(config.window_minutes.saturating_mul(MINUTE).min(DAYS * DAY)),
            speed: config.speed.max(1),
        }
    }

    pub fn now(&self) -> Timestamp {
        match self {
            Self::Fixed => NOW,
            Self::Live { started } => {
                let elapsed = u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX);
                plus(NOW, elapsed)
            }
            Self::Replay {
                started,
                from,
                speed,
            } => {
                let elapsed = u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX);
                replay_at(*from, elapsed, *speed)
            }
        }
    }

    /// The replay's present: data stamped after it is invisible. `None`
    /// when nothing is hidden.
    pub fn cutoff(&self) -> Option<Timestamp> {
        match self {
            Self::Replay { .. } => Some(self.now()),
            Self::Fixed | Self::Live { .. } => None,
        }
    }

    /// The watermark: ten minutes before the replay's present on a bucket
    /// boundary, or [`WATERMARK`] without a replay.
    pub fn watermark(&self) -> Timestamp {
        match self.cutoff() {
            Some(now) => replay_watermark(now),
            None => WATERMARK,
        }
    }

    /// Where a default view ends: the end of the data, so a replay fills
    /// the default window in.
    pub fn view_end(&self) -> Timestamp {
        match self {
            Self::Replay { .. } => NOW,
            Self::Fixed | Self::Live { .. } => self.now(),
        }
    }
}

/// `min(NOW, from + elapsed × speed)`.
pub fn replay_at(from: Timestamp, elapsed_micros: u64, speed: u32) -> Timestamp {
    plus(from, elapsed_micros.saturating_mul(u64::from(speed))).min(NOW)
}

/// Ten minutes before `now`, aligned down to a bucket boundary.
pub fn replay_watermark(now: Timestamp) -> Timestamp {
    let width = BUCKET.as_micros().get();
    let at = now.as_micros().saturating_sub(10 * MINUTE);
    Timestamp::from_micros(at - at % width)
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
    fn a_fixed_clock_stays_at_now() {
        assert_eq!(Clock::Fixed.now(), NOW);
    }

    #[test]
    fn a_live_clock_moves_on_from_now() {
        let started = std::time::Instant::now()
            .checked_sub(std::time::Duration::from_secs(60))
            .expect("a minute ago");
        let clock = Clock::Live { started };
        let first = clock.now();
        assert!(first >= plus(NOW, MINUTE));
        assert!(clock.now() >= first);
    }

    #[test]
    fn a_replay_runs_at_speed_from_the_window_start_and_stops_at_now() {
        let from = ago(2 * HOUR);
        assert_eq!(replay_at(from, 0, 10), from);
        assert_eq!(replay_at(from, MINUTE, 10), plus(from, 10 * MINUTE));
        assert_eq!(replay_at(from, 12 * MINUTE, 10), NOW);
        assert_eq!(replay_at(from, DAY, 10), NOW);
        let clock = Clock::replay(ReplayConfig {
            window_minutes: 120,
            speed: 10,
        });
        let now = clock.now();
        assert!(now >= from && now < plus(from, MINUTE), "{now:?}");
        assert_eq!(clock.cutoff().map(|c| c >= now), Some(true));
        assert_eq!(clock.view_end(), NOW);
        assert_eq!(Clock::Fixed.cutoff(), None);
    }

    #[test]
    fn a_replay_watermark_trails_by_ten_minutes_on_a_bucket() {
        let w = replay_watermark(plus(ago(HOUR), 3 * MINUTE));
        assert_eq!(w, ago(HOUR + 10 * MINUTE));
        assert!(BUCKET.is_boundary(w));
        assert_eq!(replay_watermark(NOW), WATERMARK);
    }

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
