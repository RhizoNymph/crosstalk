//! The capture stage: the far end of the proxy's capture channel.
//!
//! For each [`RawExchange`] the proxy hands off, in order of arrival:
//!
//! 1. **Normalize** it with L1's [`AnthropicMessages`] (pure; an exchange
//!    of another protocol, or whose request body is not a Messages request,
//!    is counted `normalize_failed` and dropped).
//! 2. **Store** every message body and media blob through
//!    [`crosstalk_canonical::store`] into the [`BlobStore`], retrying the
//!    whole put set up to `blob_put_attempts` times (puts are idempotent).
//!    When every attempt fails the exchange is counted `store_failed` and
//!    nothing is published (`canonical.capture.blobs-before-event`).
//! 3. **Publish** `ExchangeCaptured` in an [`Envelope`] stamped with a fresh
//!    [`EventId`] and the injected clock's time, only after the store
//!    returned `Ok`.
//!
//! The stage ends when the channel closes: every sender (the proxy and its
//! per-exchange capture tasks) has been dropped and every exchange already
//! queued has been handled. It logs ids, counts and outcomes, never bodies.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use crosstalk_canonical::{AnthropicMessages, Normalization, StoreError};
use crosstalk_ingress::ids::ExchangeIds;
use crosstalk_spec::events::ingest::IngestEvent;
use crosstalk_spec::events::{BusEvent, Envelope};
use crosstalk_spec::ids::EventId;
use crosstalk_spec::interfaces::l0_ingress::RawExchange;
use crosstalk_spec::interfaces::l2_transport::{BlobStore, EventBus};
use crosstalk_spec::observed::exchange::ExchangeOutcome;
use crosstalk_spec::support::{Clock, Timestamp};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

use crate::config::PipelineConfig;

/// Mints envelope ids. The ULID generator is ingress's (the stand-in for
/// the spec's, roadmap P0.7); envelope ids and exchange ids share its
/// semantics but come from separate generators.
#[derive(Debug)]
pub struct EventIds(ExchangeIds);

impl EventIds {
    /// A generator with a random base.
    pub fn random() -> Self {
        Self(ExchangeIds::random())
    }

    /// A generator with a fixed base, for tests and simulations.
    pub fn seeded(seed: u64) -> Self {
        Self(ExchangeIds::seeded(seed))
    }

    /// The next id, stamped with `at`.
    pub fn next(&self, at: Timestamp) -> EventId {
        EventId::from_ulid(self.0.next(at).as_ulid())
    }
}

/// The capture stage's counters, shared with the health endpoint.
#[derive(Debug, Default)]
pub struct PipelineStats {
    published: AtomicU64,
    normalize_failed: AtomicU64,
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
    /// Exchanges the normalizer refused.
    pub normalize_failed: u64,
    /// Exchanges given up after every blob put attempt failed.
    pub store_failed: u64,
    /// Put attempts that failed and were retried.
    pub store_retries: u64,
    /// Exchanges whose bodies were stored but whose event the bus refused.
    pub publish_failed: u64,
}

impl PipelineStats {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn snapshot(&self) -> PipelineCounts {
        PipelineCounts {
            published: self.published.load(Ordering::Relaxed),
            normalize_failed: self.normalize_failed.load(Ordering::Relaxed),
            store_failed: self.store_failed.load(Ordering::Relaxed),
            store_retries: self.store_retries.load(Ordering::Relaxed),
            publish_failed: self.publish_failed.load(Ordering::Relaxed),
        }
    }

    fn bump(counter: &AtomicU64) {
        counter.fetch_add(1, Ordering::Relaxed);
    }
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
    ids: EventIds,
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
        ids: EventIds,
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
    pub async fn run(self, mut captured: mpsc::Receiver<RawExchange>) {
        tracing::info!("capture stage started");
        while let Some(raw) = captured.recv().await {
            self.capture(&raw).await;
        }
        tracing::info!("capture stage stopped: the capture channel closed");
    }

    /// Normalize, store and publish one exchange.
    pub async fn capture(&self, raw: &RawExchange) -> Captured {
        let exchange = raw.meta.id.ulid_text();
        let normalization = match AnthropicMessages.normalize_with_media(raw) {
            Ok(normalization) => normalization,
            Err(error) => {
                PipelineStats::bump(&self.stats.normalize_failed);
                tracing::warn!(exchange = %exchange, error = ?error, "exchange not normalized; dropped");
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
        let id = self.ids.next(at);
        let summary = Summary::of(&normalization);
        let envelope = Envelope {
            id,
            at,
            event: BusEvent::Ingest(IngestEvent::ExchangeCaptured(Box::new(
                normalization.exchange.exchange,
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

    async fn store(&self, normalization: &Normalization) -> Result<(), StoreError> {
        let mut attempt = 1;
        loop {
            match crosstalk_canonical::store(&self.blobs, normalization).await {
                Ok(()) => return Ok(()),
                Err(error) if attempt < self.retry.attempts.get() => {
                    PipelineStats::bump(&self.stats.store_retries);
                    tracing::warn!(
                        exchange = %normalization.exchange.exchange.meta.id.ulid_text(),
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
    fn of(normalization: &Normalization) -> Self {
        let exchange = &normalization.exchange;
        Self {
            outcome: match exchange.exchange.outcome {
                ExchangeOutcome::Completed { .. } => "completed",
                ExchangeOutcome::Failed { .. } => "failed",
            },
            messages: exchange.messages.len(),
            media: normalization.media.len(),
            warnings: exchange.warnings.len(),
        }
    }
}
