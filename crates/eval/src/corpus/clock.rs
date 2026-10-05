//! The virtual clock for datasets without times.
//!
//! A time is composed from up to three ordered components, so a converter can
//! order exchanges by whatever its dataset does record (an episode, an event
//! id, a position in a list):
//!
//! ```text
//! EPOCH + pace(major) + minor × 1 ms + sub × 1 µs
//! ```
//!
//! `major` counts calls: consecutive majors are one call apart, a step the
//! [`Pace`] draws deterministically between its minimum and maximum (1 to
//! 5 s by default, about what a model call and a tool run take). `minor`
//! and `sub` order what happens within one step, with `minor <
//! MINOR_LIMIT` and `sub < SUB_LIMIT`, so they stay below a pace's
//! smallest step (1 s) and the order of `(major, minor, sub)` is the order
//! of the times. Every world starts at the same [`EPOCH_MICROS`]; worlds
//! never interact, so their times may coincide.
//!
//! A realistic step matters to the live detector: its correlation window
//! (60 s in `LiveSettings::short`) pairs a write and a read only when they
//! are that close, as a real swarm's are. A step of 1,000 s put every pair
//! two calls apart out of reach.
//!
//! Datasets that record times (τ²-bench, AI Village, LMCache's elapsed
//! seconds) keep theirs and never use this clock.

use std::time::Duration;

use crosstalk_spec::support::Timestamp;

/// 2026-01-01T00:00:00Z, in microseconds.
pub const EPOCH_MICROS: u64 = 1_767_225_600_000_000;

/// `minor` is below this: 1,000 one-millisecond slots, under 1 s.
pub const MINOR_LIMIT: u64 = 1_000;
/// `sub` is below this: 1,000 one-microsecond slots, under 1 ms.
pub const SUB_LIMIT: u64 = 1_000;

/// The smallest step a pace may take: `minor` and `sub` must fit under it.
pub const MIN_STEP: Duration = Duration::from_micros(MINOR_LIMIT * SUB_LIMIT);

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum ClockError {
    #[error("minor component {0} is not below {MINOR_LIMIT}")]
    Minor(u64),
    #[error("sub component {0} is not below {SUB_LIMIT}")]
    Sub(u64),
    #[error("major component {0} overflows the clock")]
    Major(u64),
    #[error("a pace's steps must be at least {MIN_STEP:?} and its maximum at least its minimum")]
    Pace,
}

/// How far apart consecutive calls are: each step is drawn, from `seed`
/// and the step's index, between `min` and `max` inclusive. The same pace
/// gives the same times on every run.
///
/// Built only through [`Pace::new`] (or [`Pace::DEFAULT`]), so a step is
/// never shorter than [`MIN_STEP`] and the components never overlap.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Pace {
    /// The mean step, in microseconds: `(min + max) / 2`.
    mean: u64,
    /// The largest jitter of one call's time around `major × mean`, in
    /// microseconds: `(max - min) / 4`, so one step (the difference of two
    /// jitters) stays within `[min, max]`.
    jitter: u64,
    seed: u64,
}

impl Pace {
    /// 1 to 5 s per call, seed 0.
    pub const DEFAULT: Self = Self {
        mean: 3_000_000,
        jitter: 1_000_000,
        seed: 0,
    };

    /// Steps between `min` and `max`, drawn from `seed`.
    pub fn new(min: Duration, max: Duration, seed: u64) -> Result<Self, ClockError> {
        if min < MIN_STEP || max < min {
            return Err(ClockError::Pace);
        }
        let min = u64::try_from(min.as_micros()).map_err(|_| ClockError::Pace)?;
        let max = u64::try_from(max.as_micros()).map_err(|_| ClockError::Pace)?;
        Ok(Self {
            mean: min + (max - min) / 2,
            jitter: (max - min) / 4,
            seed,
        })
    }

    /// The smallest step.
    pub fn min(&self) -> Duration {
        Duration::from_micros(self.mean - 2 * self.jitter)
    }

    /// The largest step.
    pub fn max(&self) -> Duration {
        Duration::from_micros(self.mean + 2 * self.jitter)
    }

    /// Microseconds from the epoch to call `major`: `major × mean` moved by
    /// a jitter in `[-jitter, jitter]`, none for call 0.
    fn offset(&self, major: u64) -> Option<u64> {
        let base = major.checked_mul(self.mean)?;
        if major == 0 || self.jitter == 0 {
            return Some(base);
        }
        let draw =
            mix(self.seed ^ major.wrapping_mul(0x9E37_79B9_7F4A_7C15)) % (2 * self.jitter + 1);
        (base + draw).checked_sub(self.jitter)
    }

    /// The time of `(major, minor, sub)` under this pace.
    pub fn at(&self, major: u64, minor: u64, sub: u64) -> Result<Timestamp, ClockError> {
        if minor >= MINOR_LIMIT {
            return Err(ClockError::Minor(minor));
        }
        if sub >= SUB_LIMIT {
            return Err(ClockError::Sub(sub));
        }
        let micros = self
            .offset(major)
            .and_then(|offset| offset.checked_add(EPOCH_MICROS))
            .and_then(|micros| micros.checked_add(minor * SUB_LIMIT + sub))
            .ok_or(ClockError::Major(major))?;
        Ok(Timestamp::from_micros(micros))
    }
}

impl Default for Pace {
    fn default() -> Self {
        Self::DEFAULT
    }
}

/// SplitMix64's output function: a fixed, platform-independent mix.
fn mix(value: u64) -> u64 {
    let mut z = value.wrapping_add(0x9E37_79B9_7F4A_7C15);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

/// The time of `(major, minor, sub)` under [`Pace::DEFAULT`].
pub fn compose(major: u64, minor: u64, sub: u64) -> Result<Timestamp, ClockError> {
    Pace::DEFAULT.at(major, minor, sub)
}

/// A time that only orders: the `ordinal`-th instant, one millisecond
/// apart. For fixtures and tests whose spacing does not matter; a
/// converter paces its calls with [`Pace::at`] instead.
pub fn ordinal(ordinal: u64) -> Result<Timestamp, ClockError> {
    ordinal
        .checked_mul(SUB_LIMIT)
        .and_then(|micros| micros.checked_add(EPOCH_MICROS))
        .map(Timestamp::from_micros)
        .ok_or(ClockError::Major(ordinal))
}
