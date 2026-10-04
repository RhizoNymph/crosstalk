//! The pipeline's counters and the blob put retry policy.

use std::num::NonZeroU32;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::config::PipelineConfig;
use crate::normalize_failure::{FailureCounts, FailureStats, NormalizeFailure};

/// The capture stage's and ingest's counters, shared with the health
/// endpoint.
#[derive(Debug, Default)]
pub struct PipelineStats {
    published: AtomicU64,
    normalize_failed: FailureStats,
    store_failed: AtomicU64,
    store_retries: AtomicU64,
    publish_failed: AtomicU64,
}

/// A reading of [`PipelineStats`]. On the health endpoint, its JSON.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct PipelineCounts {
    /// Exchanges whose bodies were stored and whose `ExchangeCaptured` was
    /// published.
    pub published: u64,
    /// Exchanges the normalizer refused (the proxy path only), whatever
    /// the reason: the sum of [`PipelineStats::normalize_failures`].
    pub normalize_failed: u64,
    /// Exchanges given up after every blob put attempt failed.
    pub store_failed: u64,
    /// Put attempts that failed and were retried.
    pub store_retries: u64,
    /// Exchanges whose bodies were stored but whose event was not
    /// published: the bus refused it, or no event id was left to mint.
    pub publish_failed: u64,
}

/// One of the plain counters, named so a caller cannot bump the wrong
/// field. Refusals are counted by reason and protocol instead, through
/// [`PipelineStats::bump_normalize_failed`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Counter {
    Published,
    StoreFailed,
    StoreRetries,
    PublishFailed,
}

impl PipelineStats {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn snapshot(&self) -> PipelineCounts {
        PipelineCounts {
            published: self.published.load(Ordering::Relaxed),
            normalize_failed: self.normalize_failed.snapshot().total(),
            store_failed: self.store_failed.load(Ordering::Relaxed),
            store_retries: self.store_retries.load(Ordering::Relaxed),
            publish_failed: self.publish_failed.load(Ordering::Relaxed),
        }
    }

    pub(crate) fn bump(&self, counter: Counter) {
        let field = match counter {
            Counter::Published => &self.published,
            Counter::StoreFailed => &self.store_failed,
            Counter::StoreRetries => &self.store_retries,
            Counter::PublishFailed => &self.publish_failed,
        };
        field.fetch_add(1, Ordering::Relaxed);
    }

    /// Count one refusal under its reason and protocol.
    pub(crate) fn bump_normalize_failed(&self, failure: NormalizeFailure) {
        self.normalize_failed.bump(failure);
    }

    /// The refusals by reason and protocol.
    pub fn normalize_failures(&self) -> FailureCounts {
        self.normalize_failed.snapshot()
    }
}

/// How blob puts are retried: the whole put set of one exchange is tried up
/// to `attempts` times, `backoff` apart. Puts are idempotent, so a retry
/// rewrites nothing that was stored.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PutRetry {
    pub attempts: NonZeroU32,
    pub backoff: Duration,
}

impl From<PipelineConfig> for PutRetry {
    fn from(config: PipelineConfig) -> Self {
        Self {
            attempts: config.blob_put_attempts,
            backoff: config.blob_put_backoff(),
        }
    }
}

impl Default for PutRetry {
    fn default() -> Self {
        Self::from(PipelineConfig::default())
    }
}
