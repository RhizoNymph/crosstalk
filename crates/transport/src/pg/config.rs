//! Structured configuration for [`PgBus`](super::PgBus).
//!
//! Read from the gateway's config like [`BusConfig`](crate::BusConfig):
//! every field has a default, unknown fields are refused, and durations are
//! whole microseconds in fields named `<what>_micros`.
//!
//! ```json
//! {
//!   "group_capacity": 1024,
//!   "ack_timeout_micros": 30000000,
//!   "poll_micros": 250000,
//!   "publish_timeout_micros": 5000000,
//!   "retention_micros": 604800000000
//! }
//! ```

use std::num::{NonZeroU64, NonZeroUsize};
use std::time::Duration;

use serde::{Deserialize, Deserializer};

use crate::config::NonZeroDuration;

/// How [`PgBus`](super::PgBus) admits, times out, polls and retains.
///
/// Every value is valid by construction: non-zero sizes and durations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PgBusConfig {
    /// Envelopes a group tracks at once (ready, delayed or held). `next`
    /// admits new log entries into a group only while it has room;
    /// `publish` never waits for it (the log is the queue).
    pub group_capacity: NonZeroUsize,
    /// How long a consumer may hold a delivery before the bus takes it back.
    pub ack_timeout: NonZeroDuration,
    /// How often a waiting `next` re-reads the database when no
    /// notification arrives (the `LISTEN`/`NOTIFY` fallback).
    pub poll: NonZeroDuration,
    /// How long `publish` may take, connecting included, before it reports
    /// [`BusError::Disconnected`](crosstalk_spec::interfaces::l2_transport::BusError).
    /// Bounds the wait of the first publish of a database outage before a
    /// [`SpoolingBus`](crate::SpoolingBus) spools.
    pub publish_timeout: NonZeroDuration,
    /// How long the log keeps an envelope every group has acked (decision
    /// Q6: 7 days); [`PgBus::prune`](super::PgBus::prune) takes it.
    pub retention: NonZeroDuration,
}

impl PgBusConfig {
    // `unwrap` on literal non-zero constants, evaluated at compile time.
    pub const DEFAULT_GROUP_CAPACITY: NonZeroUsize = NonZeroUsize::new(1024).unwrap();
    pub const DEFAULT_ACK_TIMEOUT: Duration = Duration::from_secs(30);
    pub const DEFAULT_POLL: Duration = Duration::from_millis(250);
    pub const DEFAULT_PUBLISH_TIMEOUT: Duration = Duration::from_secs(5);
    pub const DEFAULT_RETENTION: Duration = Duration::from_secs(7 * 24 * 60 * 60);
}

const fn constant(duration: Duration) -> NonZeroDuration {
    match NonZeroDuration::new(duration) {
        Some(duration) => duration,
        // Every caller passes a non-zero constant; evaluated at compile time.
        None => panic!("zero default duration"),
    }
}

impl Default for PgBusConfig {
    fn default() -> Self {
        Self {
            group_capacity: Self::DEFAULT_GROUP_CAPACITY,
            ack_timeout: const { constant(Self::DEFAULT_ACK_TIMEOUT) },
            poll: const { constant(Self::DEFAULT_POLL) },
            publish_timeout: const { constant(Self::DEFAULT_PUBLISH_TIMEOUT) },
            retention: const { constant(Self::DEFAULT_RETENTION) },
        }
    }
}

/// Why a decoded config is refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum InvalidPgBusConfig {
    #[error("{field} must be greater than zero")]
    ZeroDuration { field: &'static str },
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields, default)]
struct RawPgBusConfig {
    group_capacity: NonZeroUsize,
    ack_timeout_micros: u64,
    poll_micros: u64,
    publish_timeout_micros: u64,
    retention_micros: u64,
}

fn micros(duration: NonZeroDuration) -> u64 {
    u64::try_from(duration.get().as_micros()).unwrap_or(u64::MAX)
}

impl Default for RawPgBusConfig {
    fn default() -> Self {
        let config = PgBusConfig::default();
        Self {
            group_capacity: config.group_capacity,
            ack_timeout_micros: micros(config.ack_timeout),
            poll_micros: micros(config.poll),
            publish_timeout_micros: micros(config.publish_timeout),
            retention_micros: micros(config.retention),
        }
    }
}

fn non_zero(field: &'static str, micros: u64) -> Result<NonZeroDuration, InvalidPgBusConfig> {
    NonZeroU64::new(micros)
        .map(NonZeroDuration::from_micros)
        .ok_or(InvalidPgBusConfig::ZeroDuration { field })
}

impl TryFrom<RawPgBusConfig> for PgBusConfig {
    type Error = InvalidPgBusConfig;

    fn try_from(raw: RawPgBusConfig) -> Result<Self, Self::Error> {
        Ok(Self {
            group_capacity: raw.group_capacity,
            ack_timeout: non_zero("ack_timeout_micros", raw.ack_timeout_micros)?,
            poll: non_zero("poll_micros", raw.poll_micros)?,
            publish_timeout: non_zero("publish_timeout_micros", raw.publish_timeout_micros)?,
            retention: non_zero("retention_micros", raw.retention_micros)?,
        })
    }
}

impl<'de> Deserialize<'de> for PgBusConfig {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let raw = RawPgBusConfig::deserialize(deserializer)?;
        Self::try_from(raw).map_err(serde::de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_object_gives_the_defaults() {
        let config: PgBusConfig = serde_json::from_str("{}").expect("decodes");
        assert_eq!(config, PgBusConfig::default());
        assert_eq!(config.retention.get(), Duration::from_secs(604_800));
        assert_eq!(config.poll.get(), Duration::from_millis(250));
    }

    #[test]
    fn fields_decode_and_zero_durations_are_refused() {
        let config: PgBusConfig =
            serde_json::from_str(r#"{"group_capacity": 8, "poll_micros": 1000}"#).expect("decodes");
        assert_eq!(config.group_capacity.get(), 8);
        assert_eq!(config.poll.get(), Duration::from_millis(1));
        for field in [
            "ack_timeout_micros",
            "poll_micros",
            "publish_timeout_micros",
            "retention_micros",
        ] {
            let json = format!(r#"{{"{field}": 0}}"#);
            assert!(
                serde_json::from_str::<PgBusConfig>(&json).is_err(),
                "{field}"
            );
        }
        assert!(serde_json::from_str::<PgBusConfig>(r#"{"other": 1}"#).is_err());
    }
}
