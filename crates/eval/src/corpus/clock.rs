//! The virtual clock for datasets without times.
//!
//! A time is composed from up to three ordered components, so a converter can
//! order exchanges by whatever its dataset does record (an episode, an event
//! id, a position in a list):
//!
//! ```text
//! EPOCH + major × 1,000 s + minor × 1 ms + sub × 1 µs
//! ```
//!
//! with `minor < 1,000,000` and `sub < 1,000`, so the components never
//! overflow into each other and the order of `(major, minor, sub)` is the
//! order of the times. Every world starts at the same [`EPOCH_MICROS`]; worlds never
//! interact, so their times may coincide.

use crosstalk_spec::support::Timestamp;

/// 2026-01-01T00:00:00Z, in microseconds.
pub const EPOCH_MICROS: u64 = 1_767_225_600_000_000;

pub const MINOR_LIMIT: u64 = 1_000_000;
pub const SUB_LIMIT: u64 = 1_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum ClockError {
    #[error("minor component {0} is not below {MINOR_LIMIT}")]
    Minor(u64),
    #[error("sub component {0} is not below {SUB_LIMIT}")]
    Sub(u64),
    #[error("major component {0} overflows the clock")]
    Major(u64),
}

/// The time of `(major, minor, sub)`.
pub fn compose(major: u64, minor: u64, sub: u64) -> Result<Timestamp, ClockError> {
    if minor >= MINOR_LIMIT {
        return Err(ClockError::Minor(minor));
    }
    if sub >= SUB_LIMIT {
        return Err(ClockError::Sub(sub));
    }
    let major_micros = major
        .checked_mul(MINOR_LIMIT * SUB_LIMIT)
        .and_then(|micros| micros.checked_add(EPOCH_MICROS))
        .ok_or(ClockError::Major(major))?;
    Ok(Timestamp::from_micros(
        major_micros + minor * SUB_LIMIT + sub,
    ))
}

/// A synthetic time for the `ordinal`-th record of a dataset that records
/// only an order: one millisecond apart.
pub fn ordinal(ordinal: u64) -> Result<Timestamp, ClockError> {
    compose(ordinal / MINOR_LIMIT, ordinal % MINOR_LIMIT, 0)
}
