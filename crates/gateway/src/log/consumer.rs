//! The exchange log's bus consumer: group [`GROUP`], subject
//! `exchange_captured`.
//!
//! Each delivery is appended (and synced) before it is acked; a failed
//! append is nacked, so the bus redelivers it after a backoff and
//! dead-letters it once the group's retry budget is spent. The consumer
//! ends when the bus shuts down, then closes the log.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use crosstalk_spec::interfaces::l2_transport::{ConsumerGroup, Subscription};
use serde::{Deserialize, Serialize};

use super::{Appended, ExchangeLog};

/// The consumer group the exchange log reads with.
pub const GROUP: &str = "exchange-log";

/// The group as the bus names it.
pub fn group() -> ConsumerGroup {
    ConsumerGroup(GROUP.to_owned())
}

/// How long a failed append waits before the bus redelivers it (clamped
/// to the group's retry policy by the bus).
const RETRY_AFTER: Duration = Duration::from_millis(200);

/// The consumer's counters, shared with the health endpoint.
#[derive(Debug, Default)]
pub struct LogStats {
    written: AtomicU64,
    duplicates: AtomicU64,
    write_failed: AtomicU64,
}

/// A reading of [`LogStats`]. On the health endpoint, its JSON.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct LogCounts {
    /// Envelopes appended.
    pub written: u64,
    /// Deliveries whose envelope was already in the log.
    pub duplicates: u64,
    /// Appends that failed and were nacked.
    pub write_failed: u64,
}

impl LogStats {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn snapshot(&self) -> LogCounts {
        LogCounts {
            written: self.written.load(Ordering::Relaxed),
            duplicates: self.duplicates.load(Ordering::Relaxed),
            write_failed: self.write_failed.load(Ordering::Relaxed),
        }
    }
}

/// Append every delivery of `subscription` to `log` until the bus shuts
/// down, then close the log.
pub async fn run<S: Subscription>(mut subscription: S, mut log: ExchangeLog, stats: Arc<LogStats>) {
    tracing::info!(group = GROUP, path = %log.path().display(), "exchange log consumer started");
    while let Some(next) = subscription.next().await {
        let delivery = match next {
            Ok(delivery) => delivery,
            Err(error) => {
                tracing::warn!(group = GROUP, error = ?error, "undecodable delivery skipped");
                continue;
            }
        };
        let event = delivery.envelope.id.ulid_text();
        match log.append(&delivery.envelope).await {
            Ok(appended) => {
                let counter = match appended {
                    Appended::Written => &stats.written,
                    Appended::Duplicate => &stats.duplicates,
                };
                counter.fetch_add(1, Ordering::Relaxed);
                tracing::debug!(group = GROUP, event = %event, duplicate = appended == Appended::Duplicate, "exchange logged");
                if let Err(error) = subscription.ack(delivery.id).await {
                    tracing::warn!(group = GROUP, event = %event, error = ?error, "ack failed; the bus will redeliver");
                }
            }
            Err(error) => {
                stats.write_failed.fetch_add(1, Ordering::Relaxed);
                tracing::error!(group = GROUP, event = %event, attempt = delivery.attempt.get(), error = %error, "exchange log append failed");
                let reason = "exchange log append failed".to_owned();
                if let Err(error) = subscription.nack(delivery.id, RETRY_AFTER, reason).await {
                    tracing::warn!(group = GROUP, event = %event, error = ?error, "nack failed");
                }
            }
        }
    }
    if let Err(error) = log.close().await {
        tracing::error!(group = GROUP, error = %error, "closing the exchange log failed");
    }
    tracing::info!(group = GROUP, "exchange log consumer stopped");
}
