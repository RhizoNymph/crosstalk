//! Test time: a fixed epoch and offsets from it.
//!
//! Builders default every timestamp to [`T0`] or a fixed offset after it, so
//! values are reproducible. Durations are added in whole microseconds,
//! saturating at the end of the representable range.

use std::time::Duration;

use crosstalk_spec::support::Timestamp;

/// 2026-10-01T00:00:00Z: the testkit epoch. The ULID time part of every
/// generated id ([`crate::ids::Ids::TIME_MS`]).
pub const T0: Timestamp = Timestamp::from_micros(1_790_812_800_000_000);

/// `t` plus `by`, in whole microseconds, saturating.
pub fn after(t: Timestamp, by: Duration) -> Timestamp {
    let micros = u64::try_from(by.as_micros()).unwrap_or(u64::MAX);
    Timestamp::from_micros(t.as_micros().saturating_add(micros))
}

/// [`T0`] plus `offset`.
pub fn at(offset: Duration) -> Timestamp {
    after(T0, offset)
}

/// [`T0`] plus `seconds`.
pub fn secs(seconds: u64) -> Timestamp {
    at(Duration::from_secs(seconds))
}

/// [`T0`] plus `millis`.
pub fn millis(millis: u64) -> Timestamp {
    at(Duration::from_millis(millis))
}
