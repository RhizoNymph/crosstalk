//! The flow consumer's configuration: the correlator's timing (whose
//! `settle_after` is the settle window), how long a write can still be
//! confirmed by content, the number of correlator shards, and how often it
//! ticks.

use std::num::NonZeroUsize;
use std::time::Duration;

use crosstalk_spec::derived::flow::timing::{CorrelationTiming, InvalidTiming};
use serde::Deserialize;

use crate::correlate::{ContentRetention, InvalidRetention};

/// Checked settings.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Settings {
    pub timing: CorrelationTiming,
    /// The longest write-to-read lag a content-confirmed channel
    /// transmission may have (`flow.correlator.content-confirms-past-window`).
    pub content_retention: ContentRetention,
    pub shards: NonZeroUsize,
    /// How often the consumer ticks, in elapsed (tokio) time. Each tick
    /// reads the injected clock, which a replay drives, so windows close on
    /// the clock's time, never on wall time.
    pub tick_every: Duration,
    /// How often a durable consumer checkpoints its shards (and then acks
    /// the deliveries the checkpoint covers), in elapsed time. The flow
    /// group's ack timeout must exceed it.
    pub checkpoint_every: Duration,
    /// A durable consumer also checkpoints once this many deliveries wait
    /// for their ack.
    pub max_unacked: NonZeroUsize,
}

/// The `flow` section of a config document. Durations in milliseconds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct FlowConfig {
    #[serde(default = "defaults::correlation_window_ms")]
    pub correlation_window_ms: u64,
    #[serde(default = "defaults::evidence_window_ms")]
    pub evidence_window_ms: u64,
    #[serde(default = "defaults::suspected_ttl_ms")]
    pub suspected_ttl_ms: u64,
    #[serde(default = "defaults::content_retention_ms")]
    pub content_retention_ms: u64,
    #[serde(default = "defaults::shards")]
    pub shards: usize,
    #[serde(default = "defaults::tick_ms")]
    pub tick_ms: u64,
    #[serde(default = "defaults::checkpoint_ms")]
    pub checkpoint_ms: u64,
    #[serde(default = "defaults::checkpoint_unacked")]
    pub checkpoint_unacked: usize,
}

mod defaults {
    /// Ten minutes from a write to a read.
    pub fn correlation_window_ms() -> u64 {
        600_000
    }

    /// Two minutes after a read for its content match.
    pub fn evidence_window_ms() -> u64 {
        120_000
    }

    /// Thirty minutes for a late match once suspected.
    pub fn suspected_ttl_ms() -> u64 {
        1_800_000
    }

    /// L4's span index retention, 30 days: content confirms a write read
    /// up to this long after it.
    pub fn content_retention_ms() -> u64 {
        2_592_000_000
    }

    pub fn shards() -> usize {
        1
    }

    pub fn tick_ms() -> u64 {
        1_000
    }

    /// Ten seconds between checkpoints: the longest a delivery waits for
    /// its ack, against a settle window of minutes.
    pub fn checkpoint_ms() -> u64 {
        10_000
    }

    pub fn checkpoint_unacked() -> usize {
        512
    }
}

impl Default for FlowConfig {
    fn default() -> Self {
        Self {
            correlation_window_ms: defaults::correlation_window_ms(),
            evidence_window_ms: defaults::evidence_window_ms(),
            suspected_ttl_ms: defaults::suspected_ttl_ms(),
            content_retention_ms: defaults::content_retention_ms(),
            shards: defaults::shards(),
            tick_ms: defaults::tick_ms(),
            checkpoint_ms: defaults::checkpoint_ms(),
            checkpoint_unacked: defaults::checkpoint_unacked(),
        }
    }
}

/// Why a `flow` section was refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum InvalidFlowConfig {
    #[error("correlation timing: {0:?}")]
    Timing(InvalidTiming),
    #[error("content_retention_ms: {0}")]
    ContentRetention(InvalidRetention),
    #[error("shards must be at least 1")]
    ZeroShards,
    #[error("tick_ms must be at least 1")]
    ZeroTick,
    #[error("checkpoint_ms must be at least 1")]
    ZeroCheckpoint,
    #[error("checkpoint_unacked must be at least 1")]
    ZeroUnacked,
}

impl TryFrom<FlowConfig> for Settings {
    type Error = InvalidFlowConfig;

    fn try_from(config: FlowConfig) -> Result<Self, Self::Error> {
        let timing = CorrelationTiming::new(
            Duration::from_millis(config.correlation_window_ms),
            Duration::from_millis(config.evidence_window_ms),
            Duration::from_millis(config.suspected_ttl_ms),
        )
        .map_err(InvalidFlowConfig::Timing)?;
        let content_retention =
            ContentRetention::new(Duration::from_millis(config.content_retention_ms), timing)
                .map_err(InvalidFlowConfig::ContentRetention)?;
        let shards = NonZeroUsize::new(config.shards).ok_or(InvalidFlowConfig::ZeroShards)?;
        if config.tick_ms == 0 {
            return Err(InvalidFlowConfig::ZeroTick);
        }
        if config.checkpoint_ms == 0 {
            return Err(InvalidFlowConfig::ZeroCheckpoint);
        }
        let max_unacked =
            NonZeroUsize::new(config.checkpoint_unacked).ok_or(InvalidFlowConfig::ZeroUnacked)?;
        Ok(Self {
            timing,
            content_retention,
            shards,
            tick_every: Duration::from_millis(config.tick_ms),
            checkpoint_every: Duration::from_millis(config.checkpoint_ms),
            max_unacked,
        })
    }
}
