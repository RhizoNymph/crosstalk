//! Structured configuration for [`MpscBus`](crate::MpscBus).
//!
//! Read from the gateway's JSON or YAML config. Every field has a default
//! ([`BusConfig::default`]), so a config may name only what it changes;
//! unknown fields are refused. Durations follow the wire contract's
//! convention: whole microseconds in a field named `<what>_micros`.
//!
//! ```json
//! {
//!   "group_capacity": 1024,
//!   "command_buffer": 256,
//!   "ack_timeout_micros": 30000000,
//!   "dead_letter_retry_micros": 1000000,
//!   "order": {"type": "fifo"},
//!   "retry": {"max_attempts": 5, "initial_backoff_micros": 100000, "max_backoff_micros": 30000000}
//! }
//! ```
//!
//! Config is read, never written back, so it only decodes.

use std::num::{NonZeroU32, NonZeroU64, NonZeroUsize};
use std::time::Duration;

use crosstalk_spec::interfaces::l2_transport::{InvalidRetryPolicy, RetryPolicy};
use serde::{Deserialize, Deserializer};

/// A duration greater than zero.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct NonZeroDuration(Duration);

impl NonZeroDuration {
    /// `None` for a zero duration.
    pub const fn new(duration: Duration) -> Option<Self> {
        if duration.is_zero() {
            None
        } else {
            Some(Self(duration))
        }
    }

    pub const fn from_micros(micros: NonZeroU64) -> Self {
        Self(Duration::from_micros(micros.get()))
    }

    pub const fn get(self) -> Duration {
        self.0
    }
}

/// In which order a consumer group's ready deliveries are handed out.
///
/// No order is promised either way (`transport.ordering.unconstrained`).
/// `Fifo` is what production uses; `Shuffled` picks a ready delivery with a
/// seeded generator, so simulation tests reach every order and a consumer
/// that depends on publish order fails them. The same seed and the same
/// call sequence give the same order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Deserialize)]
#[serde(
    tag = "type",
    content = "data",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum DeliveryOrder {
    /// Ready deliveries go out in the order they became ready.
    #[default]
    Fifo,
    /// Each hand-out picks a ready delivery uniformly at random.
    Shuffled { seed: u64 },
}

/// How the in-process bus is sized and how it retries.
///
/// Every value is valid by construction: capacities are non-zero, durations
/// are non-zero, and `retry` is a checked [`RetryPolicy`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BusConfig {
    /// Envelopes a consumer group may hold at once: queued, waiting for a
    /// retry, held by a consumer, or waiting for its dead letter to be
    /// stored. `publish` waits while any target group is full
    /// (`transport.backpressure.bounded-queue`, `publish-waits`).
    pub group_capacity: NonZeroUsize,
    /// The bus task's command queue. Callers wait when it is full.
    pub command_buffer: NonZeroUsize,
    /// How long a consumer may hold a delivery before the bus takes it back
    /// and schedules a redelivery.
    pub ack_timeout: NonZeroDuration,
    /// How long the bus waits before retrying a dead letter it failed to
    /// store.
    pub dead_letter_retry: NonZeroDuration,
    pub order: DeliveryOrder,
    /// The policy consumers subscribe with unless they choose their own.
    /// The bus does not apply it itself: every `subscribe` passes a policy,
    /// and the gateway passes this one.
    pub retry: RetryPolicy,
}

impl BusConfig {
    // `unwrap` on literal non-zero constants, evaluated at compile time: a
    // zero would fail the build, never panic at run time.
    pub const DEFAULT_GROUP_CAPACITY: NonZeroUsize = NonZeroUsize::new(1024).unwrap();
    pub const DEFAULT_COMMAND_BUFFER: NonZeroUsize = NonZeroUsize::new(256).unwrap();
    pub const DEFAULT_ACK_TIMEOUT: Duration = Duration::from_secs(30);
    pub const DEFAULT_DEAD_LETTER_RETRY: Duration = Duration::from_secs(1);
    pub const DEFAULT_MAX_ATTEMPTS: NonZeroU32 = NonZeroU32::new(5).unwrap();
    pub const DEFAULT_INITIAL_BACKOFF: Duration = Duration::from_millis(100);
    pub const DEFAULT_MAX_BACKOFF: Duration = Duration::from_secs(30);
}

impl Default for BusConfig {
    fn default() -> Self {
        let raw = RawBusConfig::default();
        // The defaults are non-zero constants with initial <= max backoff,
        // so the conversion cannot fail; `from_raw_defaults` states that.
        Self::from_raw_defaults(raw)
    }
}

/// Why a decoded config is refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum InvalidBusConfig {
    #[error("{field} must be greater than zero")]
    ZeroDuration { field: &'static str },
    #[error("invalid retry policy: {0:?}")]
    Retry(InvalidRetryPolicy),
}

/// The JSON shape, decoded without the checks.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields, default)]
struct RawBusConfig {
    group_capacity: NonZeroUsize,
    command_buffer: NonZeroUsize,
    ack_timeout_micros: u64,
    dead_letter_retry_micros: u64,
    order: DeliveryOrder,
    retry: RawRetry,
}

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields, default)]
struct RawRetry {
    max_attempts: NonZeroU32,
    initial_backoff_micros: u64,
    max_backoff_micros: u64,
}

fn micros(duration: Duration) -> u64 {
    // The defaults are a few seconds, far below u64::MAX microseconds.
    u64::try_from(duration.as_micros()).unwrap_or(u64::MAX)
}

impl Default for RawBusConfig {
    fn default() -> Self {
        Self {
            group_capacity: BusConfig::DEFAULT_GROUP_CAPACITY,
            command_buffer: BusConfig::DEFAULT_COMMAND_BUFFER,
            ack_timeout_micros: micros(BusConfig::DEFAULT_ACK_TIMEOUT),
            dead_letter_retry_micros: micros(BusConfig::DEFAULT_DEAD_LETTER_RETRY),
            order: DeliveryOrder::default(),
            retry: RawRetry::default(),
        }
    }
}

impl Default for RawRetry {
    fn default() -> Self {
        Self {
            max_attempts: BusConfig::DEFAULT_MAX_ATTEMPTS,
            initial_backoff_micros: micros(BusConfig::DEFAULT_INITIAL_BACKOFF),
            max_backoff_micros: micros(BusConfig::DEFAULT_MAX_BACKOFF),
        }
    }
}

fn non_zero(field: &'static str, micros: u64) -> Result<NonZeroDuration, InvalidBusConfig> {
    NonZeroU64::new(micros)
        .map(NonZeroDuration::from_micros)
        .ok_or(InvalidBusConfig::ZeroDuration { field })
}

impl TryFrom<RawBusConfig> for BusConfig {
    type Error = InvalidBusConfig;

    fn try_from(raw: RawBusConfig) -> Result<Self, Self::Error> {
        let retry = RetryPolicy::new(
            raw.retry.max_attempts,
            Duration::from_micros(raw.retry.initial_backoff_micros),
            Duration::from_micros(raw.retry.max_backoff_micros),
        )
        .map_err(InvalidBusConfig::Retry)?;
        Ok(Self {
            group_capacity: raw.group_capacity,
            command_buffer: raw.command_buffer,
            ack_timeout: non_zero("ack_timeout_micros", raw.ack_timeout_micros)?,
            dead_letter_retry: non_zero("dead_letter_retry_micros", raw.dead_letter_retry_micros)?,
            order: raw.order,
            retry,
        })
    }
}

impl BusConfig {
    fn from_raw_defaults(raw: RawBusConfig) -> Self {
        match Self::try_from(raw) {
            Ok(config) => config,
            // Unreachable: every default is a non-zero constant and the
            // default initial backoff is below the default maximum
            // (`tests::config::defaults_are_valid`).
            Err(error) => unreachable!("default bus config is invalid: {error}"),
        }
    }
}

/// Decodes through [`InvalidBusConfig`]'s checks; a refused value is a
/// decode error.
impl<'de> Deserialize<'de> for BusConfig {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let raw = RawBusConfig::deserialize(deserializer)?;
        Self::try_from(raw).map_err(serde::de::Error::custom)
    }
}
