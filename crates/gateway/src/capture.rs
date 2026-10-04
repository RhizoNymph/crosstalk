//! The capture stage: the far end of the proxy's capture channel.
//!
//! For each [`RawExchange`] the proxy hands off, in order of arrival:
//!
//! 1. **Normalize** it with L1's [`AnthropicMessages`] (pure; an exchange
//!    of another protocol, or whose request body is not a Messages request,
//!    is counted `normalize_failed`, by reason and protocol
//!    ([`crate::normalize_failure`]), and dropped; at debug level the
//!    refused body's top-level shape is logged, never its content).
//! 2. **Store** every message body and media blob through
//!    [`crosstalk_canonical::store`] into the [`BlobStore`], retrying the
//!    whole put set up to `blob_put_attempts` times (puts are idempotent).
//!    When every attempt fails the exchange is counted `store_failed` and
//!    nothing is published (`canonical.capture.blobs-before-event`).
//! 3. **Publish** `ExchangeCaptured` in an [`Envelope`] stamped with the
//!    injected clock's time and a fresh [`EventId`] minted at that time by
//!    the spec's [`UlidGenerator`], only after the store returned `Ok`.
//!
//! The stage ends when the channel closes: every sender (the proxy and its
//! per-exchange capture tasks) has been dropped and every exchange already
//! queued has been handled. It logs ids, counts and outcomes, never bodies.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use crosstalk_canonical::anthropic::RequestShape;
use crosstalk_canonical::{AnthropicMessages, StoreError};
use crosstalk_spec::events::ingest::IngestEvent;
use crosstalk_spec::events::{BusEvent, Envelope};
use crosstalk_spec::ids::{EventId, SeededRandom, UlidGenerator};
use crosstalk_spec::interfaces::l0_ingress::RawExchange;
use crosstalk_spec::interfaces::l1_canonical::{NormalizeError, NormalizedExchange, Normalizer};
use crosstalk_spec::interfaces::l2_transport::{BlobStore, EventBus};
use crosstalk_spec::observed::exchange::ExchangeOutcome;
use crosstalk_spec::support::Clock;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

use crate::config::PipelineConfig;
use crate::normalize_failure::{
    FailureCounts, FailureReason, FailureStats, NormalizeFailure, protocol_code,
};

/// The capture stage's counters, shared with the health endpoint.
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
    /// Exchanges the normalizer refused, whatever the reason: the sum of
    /// [`PipelineStats::normalize_failures`].
    pub normalize_failed: u64,
    /// Exchanges given up after every blob put attempt failed.
    pub store_failed: u64,
    /// Put attempts that failed and were retried.
    pub store_retries: u64,
    /// Exchanges whose bodies were stored but whose event was not
    /// published: the bus refused it, or no event id was left to mint.
    pub publish_failed: u64,
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

    /// The refusals by reason and protocol.
    pub fn normalize_failures(&self) -> FailureCounts {
        self.normalize_failed.snapshot()
    }

    fn bump(counter: &AtomicU64) {
        counter.fetch_add(1, Ordering::Relaxed);
    }
}

/// L1's normalization of `raw`, or why there is none: the refusal and the
/// normalizer's error (`None` when no normalizer handles the protocol).
fn normalize(
    raw: &RawExchange,
) -> Result<NormalizedExchange, (NormalizeFailure, Option<NormalizeError>)> {
    let protocol = raw.meta.protocol;
    if protocol != AnthropicMessages.protocol() {
        let failure = NormalizeFailure {
            reason: FailureReason::UnsupportedProtocol,
            protocol,
        };
        return Err((failure, None));
    }
    AnthropicMessages.normalize(raw).map_err(|error| {
        let failure = NormalizeFailure {
            reason: FailureReason::of(&error),
            protocol,
        };
        (failure, Some(error))
    })
}

/// What became of one exchange.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Captured {
    Published(EventId),
    NotNormalized,
    NotStored,
    NotPublished,
}

/// How blob puts are retried.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PutRetry {
    pub attempts: std::num::NonZeroU32,
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

/// The capture stage over any blob store and bus.
pub struct CaptureStage<B, E> {
    blobs: B,
    bus: E,
    clock: Arc<dyn Clock>,
    /// Mints envelope ids; owned by the stage's one task.
    ids: UlidGenerator<SeededRandom>,
    stats: Arc<PipelineStats>,
    retry: PutRetry,
}

impl<B, E> CaptureStage<B, E>
where
    B: BlobStore + Sync,
    E: EventBus + Sync,
{
    pub fn new(
        blobs: B,
        bus: E,
        clock: Arc<dyn Clock>,
        ids: UlidGenerator<SeededRandom>,
        stats: Arc<PipelineStats>,
        retry: PutRetry,
    ) -> Self {
        Self {
            blobs,
            bus,
            clock,
            ids,
            stats,
            retry,
        }
    }

    /// Handle every exchange `captured` yields until it closes.
    pub async fn run(mut self, mut captured: mpsc::Receiver<RawExchange>) {
        tracing::info!("capture stage started");
        while let Some(raw) = captured.recv().await {
            self.capture(&raw).await;
        }
        tracing::info!("capture stage stopped: the capture channel closed");
    }

    /// Normalize, store and publish one exchange.
    pub async fn capture(&mut self, raw: &RawExchange) -> Captured {
        let exchange = raw.meta.id.ulid_text();
        let normalization = match normalize(raw) {
            Ok(normalization) => normalization,
            Err((failure, error)) => {
                self.stats.normalize_failed.bump(failure);
                tracing::warn!(
                    exchange = %exchange,
                    reason = failure.reason.code(),
                    protocol = protocol_code(failure.protocol),
                    error = ?error,
                    "exchange not normalized; dropped"
                );
                // Nothing of a refused exchange is kept, so its body's
                // shape (keys, roles, content kinds; never a value or a
                // header) is the only record of what the normalizer saw.
                tracing::debug!(
                    exchange = %exchange,
                    request_shape = %RequestShape::of(&raw.request.body),
                    "refused request shape"
                );
                return Captured::NotNormalized;
            }
        };
        if let Err(error) = self.store(&normalization).await {
            PipelineStats::bump(&self.stats.store_failed);
            tracing::error!(
                exchange = %exchange,
                attempts = self.retry.attempts.get(),
                error = %error,
                "exchange bodies not stored; nothing published"
            );
            return Captured::NotStored;
        }
        let at = self.clock.now();
        let id = match self.ids.mint_at::<EventId>(at) {
            Ok(id) => id,
            Err(error) => {
                PipelineStats::bump(&self.stats.publish_failed);
                tracing::error!(exchange = %exchange, error = %error, "no event id left to mint; ExchangeCaptured not published");
                return Captured::NotPublished;
            }
        };
        let summary = Summary::of(&normalization);
        let envelope = Envelope {
            id,
            at,
            event: BusEvent::Ingest(IngestEvent::ExchangeCaptured(Box::new(
                normalization.exchange,
            ))),
        };
        match self.bus.publish(envelope).await {
            Ok(()) => {
                PipelineStats::bump(&self.stats.published);
                tracing::info!(
                    exchange = %exchange,
                    event = %id.ulid_text(),
                    model = %raw.meta.model.0,
                    outcome = summary.outcome,
                    messages = summary.messages,
                    media = summary.media,
                    warnings = summary.warnings,
                    "exchange captured"
                );
                Captured::Published(id)
            }
            Err(error) => {
                PipelineStats::bump(&self.stats.publish_failed);
                tracing::error!(exchange = %exchange, error = ?error, "ExchangeCaptured not published");
                Captured::NotPublished
            }
        }
    }

    async fn store(&self, normalization: &NormalizedExchange) -> Result<(), StoreError> {
        let mut attempt = 1;
        loop {
            match crosstalk_canonical::store(&self.blobs, normalization).await {
                Ok(()) => return Ok(()),
                Err(error) if attempt < self.retry.attempts.get() => {
                    PipelineStats::bump(&self.stats.store_retries);
                    tracing::warn!(
                        exchange = %normalization.exchange.meta.id.ulid_text(),
                        attempt,
                        error = %error,
                        "blob put failed; retrying"
                    );
                    attempt += 1;
                    tokio::time::sleep(self.retry.backoff).await;
                }
                Err(error) => return Err(error),
            }
        }
    }
}

/// What the capture log line says about an exchange.
struct Summary {
    outcome: &'static str,
    messages: usize,
    media: usize,
    warnings: usize,
}

impl Summary {
    fn of(normalization: &NormalizedExchange) -> Self {
        Self {
            outcome: match normalization.exchange.outcome {
                ExchangeOutcome::Completed { .. } => "completed",
                ExchangeOutcome::Failed { .. } => "failed",
            },
            messages: normalization.messages.len(),
            media: normalization.media.len(),
            warnings: normalization.warnings.len(),
        }
    }
}
