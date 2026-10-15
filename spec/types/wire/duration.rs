//! Durations on the wire: whole microseconds as a JSON number, in a field
//! whose name ends in `_micros`.
//!
//! ```json
//! {"write": "01J9Z3K8M4Q7R2T5V6W8X9Y0ZA", "read": "01J9Z3M2C5D6E7F8G9H0J1K2M3", "lag_micros": 30000000}
//! ```
//!
//! Microseconds are the crate's time unit: a [`Timestamp`] counts them, so a
//! duration between two timestamps is always a whole number of them, and a
//! number keeps the field sortable and summable by any reader without a
//! duration parser. The unit lives in the field's name, so the JSON reads
//! unambiguously without the schema; a field holding a [`Duration`] is
//! therefore named `<what>_micros` in Rust as well (the wire's keys are the
//! Rust field names), and its accessor keeps the plain name.
//!
//! Every `Duration` field on the wire uses this module:
//!
//! ```ignore
//! #[serde(with = "crate::wire::duration")]
//! lag_micros: Duration,
//! ```
//!
//! Encoding refuses a duration that does not fit ([`UnfitDuration`]): one
//! with a fraction of a microsecond, which the number cannot hold exactly,
//! and one longer than `u64::MAX` microseconds (about 584,000 years).
//! Decoding accepts exactly the non-negative integers up to `u64::MAX`;
//! a negative number, a fraction, a string or serde's own
//! `{"secs": .., "nanos": ..}` form is refused.
//!
//! [`Timestamp`]: crate::support::Timestamp

use std::time::Duration;

use serde::ser::Error as _;
use serde::{Deserialize, Deserializer, Serializer};

use super::Rejected;

const NANOS_PER_MICRO: u32 = 1_000;

/// A duration with no exact text as whole microseconds in a `u64`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnfitDuration {
    /// Not a whole number of microseconds.
    SubMicrosecond(Duration),
    /// More than `u64::MAX` microseconds.
    TooLong(Duration),
}

/// `duration` in whole microseconds, when it is exactly that many.
pub fn micros(duration: Duration) -> Result<u64, UnfitDuration> {
    if !duration.subsec_nanos().is_multiple_of(NANOS_PER_MICRO) {
        return Err(UnfitDuration::SubMicrosecond(duration));
    }
    u64::try_from(duration.as_micros()).map_err(|_| UnfitDuration::TooLong(duration))
}

/// `#[serde(with = "crate::wire::duration")]`: write the microseconds.
pub fn serialize<S: Serializer>(duration: &Duration, serializer: S) -> Result<S::Ok, S::Error> {
    let micros =
        micros(*duration).map_err(|error| S::Error::custom(Rejected::new("duration", error)))?;
    serializer.serialize_u64(micros)
}

/// `#[serde(with = "crate::wire::duration")]`: read the microseconds.
pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Duration, D::Error> {
    u64::deserialize(deserializer).map(Duration::from_micros)
}
