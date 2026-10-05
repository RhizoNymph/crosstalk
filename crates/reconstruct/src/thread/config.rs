//! Typed threading configuration: how long a conversation store remembers
//! the messages each agent has seen (`reconstruct.delta.excludes-seen-elsewhere`).
//!
//! Every value exists only in a valid state: the constructor checks, and
//! JSON decodes through it (unknown fields refused, every field
//! defaulted). On the wire:
//!
//! ```json
//! {"seen_retention_secs": 2592000}
//! ```

use std::time::Duration;

use crosstalk_spec::support::Timestamp;
use serde::Deserialize;

/// The default seen-message retention: 30 days, L4's default index
/// retention.
pub const DEFAULT_SEEN_RETENTION: Duration = Duration::from_secs(30 * 24 * 60 * 60);

/// Why a threading configuration was refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum ThreadConfigError {
    #[error("the seen-message retention must be non-zero")]
    ZeroRetention,
    #[error("the seen-message retention must fit in u64 microseconds")]
    RetentionTooLong,
}

/// How long a message an agent saw (received in a request, or produced as
/// an output) keeps counting as seen: non-zero, at most `u64::MAX`
/// microseconds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SeenRetention {
    micros: u64,
}

impl SeenRetention {
    pub fn new(retention: Duration) -> Result<Self, ThreadConfigError> {
        let micros = u64::try_from(retention.as_micros())
            .map_err(|_| ThreadConfigError::RetentionTooLong)?;
        if micros == 0 {
            return Err(ThreadConfigError::ZeroRetention);
        }
        Ok(Self { micros })
    }

    pub fn as_duration(self) -> Duration {
        Duration::from_micros(self.micros)
    }

    /// The earliest time a sighting still counts at `at`: `at` less the
    /// retention, or the epoch.
    pub fn cutoff(self, at: Timestamp) -> Timestamp {
        Timestamp::from_micros(at.as_micros().saturating_sub(self.micros))
    }
}

impl Default for SeenRetention {
    fn default() -> Self {
        Self {
            micros: DEFAULT_SEEN_RETENTION.as_secs() * 1_000_000,
        }
    }
}

/// A conversation store's settings.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(try_from = "RawThreadConfig")]
pub struct ThreadConfig {
    /// How long a seen message withholds the same message from a later
    /// delta's `new_inputs` in another conversation of the agent's cluster.
    pub seen_retention: SeenRetention,
}

/// [`ThreadConfig`]'s fields, decoded without the checks.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawThreadConfig {
    #[serde(default = "default_retention_secs")]
    seen_retention_secs: u64,
}

fn default_retention_secs() -> u64 {
    DEFAULT_SEEN_RETENTION.as_secs()
}

impl TryFrom<RawThreadConfig> for ThreadConfig {
    type Error = ThreadConfigError;

    fn try_from(raw: RawThreadConfig) -> Result<Self, Self::Error> {
        Ok(Self {
            seen_retention: SeenRetention::new(Duration::from_secs(raw.seen_retention_secs))?,
        })
    }
}
