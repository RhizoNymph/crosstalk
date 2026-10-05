//! Ingest at L1: a normalized exchange enters the pipeline.
//!
//! [`Ingester::ingest`] is the capture path after L1 normalization, and
//! the only one: the proxy's capture stage normalizes and calls it, and a
//! caller that already holds a [`NormalizedExchange`] (the eval harness
//! replaying a dataset) calls it directly. For one exchange, in order:
//!
//! 1. **Store** every message body and media blob through
//!    [`crosstalk_canonical::store`] into the [`BlobStore`], retrying the
//!    whole put set up to [`PutRetry::attempts`] times, [`PutRetry::backoff`]
//!    apart (puts are idempotent). When every attempt fails, nothing is
//!    published (`canonical.capture.blobs-before-event`) and the error is
//!    [`IngestError::NotStored`].
//! 2. **Mint** the envelope's [`EventId`] at `at` with the spec's
//!    [`UlidGenerator`] (monotonic: an `at` in or before the last id's
//!    millisecond gets the last id plus one).
//! 3. **Publish** `ExchangeCaptured` in an [`Envelope`] stamped `at`.
//!
//! Steps 2 and 3 run under one lock, so envelopes reach the bus's
//! `publish` in strictly increasing id order however many ingests run at
//! once; storing (step 1) runs concurrently. Logs carry ids, counts and
//! outcomes, never bodies.

use std::num::NonZeroU32;
use std::sync::Arc;

use crosstalk_canonical::StoreError;
use crosstalk_spec::events::ingest::IngestEvent;
use crosstalk_spec::events::{BusEvent, Envelope};
use crosstalk_spec::ids::{EventId, SeededRandom, UlidGenerator};
use crosstalk_spec::interfaces::l1_canonical::NormalizedExchange;
use crosstalk_spec::interfaces::l2_transport::{BlobStore, BusError, EventBus};
use crosstalk_spec::observed::exchange::ExchangeOutcome;
use crosstalk_spec::support::{Clock, Timestamp};
use tokio::sync::Mutex;

use super::stats::{Counter, PipelineStats, PutRetry};

/// Why an exchange was not published. Each case is counted in
/// [`PipelineStats`] before it is returned.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum IngestError {
    /// Every put attempt failed; nothing was published. `source` is the
    /// last attempt's failure.
    #[error("the exchange's blobs were not stored after {attempts} attempts: {source}")]
    NotStored {
        attempts: NonZeroU32,
        #[source]
        source: StoreError,
    },
    /// The blobs are stored, but no event id was left to mint at `at`.
    #[error("no event id left to mint at {at:?}; ExchangeCaptured not published")]
    IdsExhausted { at: Timestamp },
    /// The blobs are stored, but the bus refused the envelope.
    #[error("the bus refused ExchangeCaptured: {0:?}")]
    NotPublished(BusError),
}

/// Why an event was not published by [`Ingester::publish`].
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum PublishError {
    #[error("no event id left to mint at {at:?}")]
    IdsExhausted { at: Timestamp },
    #[error("the bus refused the event: {0:?}")]
    NotPublished(BusError),
}

/// The ingest path over a blob store `B` and a bus `E`. Clones share the
/// stores, the id generator and the counters.
pub struct Ingester<B, E> {
    inner: Arc<Inner<B, E>>,
}

impl<B, E> Clone for Ingester<B, E> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<B, E> std::fmt::Debug for Ingester<B, E> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Ingester")
            .field("retry", &self.inner.retry)
            .finish_non_exhaustive()
    }
}

struct Inner<B, E> {
    blobs: B,
    bus: E,
    clock: Arc<dyn Clock>,
    /// Mints envelope ids. Held across the publish, so ids reach the bus in
    /// mint order.
    ids: Mutex<UlidGenerator<SeededRandom>>,
    stats: Arc<PipelineStats>,
    retry: PutRetry,
}

impl<B, E> Ingester<B, E>
where
    B: BlobStore + Sync,
    E: EventBus + Sync,
{
    /// `id_entropy` seeds the random part of envelope ids; the generator
    /// reads `clock` only through [`Ingester::now`].
    pub(crate) fn new(
        blobs: B,
        bus: E,
        clock: Arc<dyn Clock>,
        id_entropy: SeededRandom,
        retry: PutRetry,
        stats: Arc<PipelineStats>,
    ) -> Self {
        let ids = UlidGenerator::new(Arc::clone(&clock), id_entropy);
        Self {
            inner: Arc::new(Inner {
                blobs,
                bus,
                clock,
                ids: Mutex::new(ids),
                stats,
                retry,
            }),
        }
    }

    /// Store `exchange`'s blobs, then publish `ExchangeCaptured` in an
    /// envelope stamped `at` whose id is minted at `at`. Returns the
    /// envelope's id.
    pub async fn ingest(
        &self,
        exchange: NormalizedExchange,
        at: Timestamp,
    ) -> Result<EventId, IngestError> {
        let inner = &self.inner;
        let exchange_id = exchange.exchange.meta.id.ulid_text();
        if let Err(source) = self.store(&exchange).await {
            inner.stats.bump(Counter::StoreFailed);
            tracing::error!(
                exchange = %exchange_id,
                attempts = inner.retry.attempts.get(),
                error = %source,
                "exchange bodies not stored; nothing published"
            );
            return Err(IngestError::NotStored {
                attempts: inner.retry.attempts,
                source,
            });
        }
        let summary = Summary::of(&exchange);
        let model = exchange.exchange.meta.model.0.clone();
        let mut ids = inner.ids.lock().await;
        let id = match ids.mint_at::<EventId>(at) {
            Ok(id) => id,
            Err(_exhausted) => {
                inner.stats.bump(Counter::PublishFailed);
                tracing::error!(
                    exchange = %exchange_id,
                    at_micros = at.as_micros(),
                    "no event id left to mint; ExchangeCaptured not published"
                );
                return Err(IngestError::IdsExhausted { at });
            }
        };
        let envelope = Envelope {
            id,
            at,
            event: BusEvent::Ingest(IngestEvent::ExchangeCaptured(Box::new(exchange.exchange))),
        };
        let published = inner.bus.publish(envelope).await;
        drop(ids);
        match published {
            Ok(()) => {
                inner.stats.bump(Counter::Published);
                tracing::info!(
                    exchange = %exchange_id,
                    event = %id.ulid_text(),
                    model = %model,
                    outcome = summary.outcome,
                    messages = summary.messages,
                    media = summary.media,
                    warnings = summary.warnings,
                    "exchange captured"
                );
                Ok(id)
            }
            Err(error) => {
                inner.stats.bump(Counter::PublishFailed);
                tracing::error!(exchange = %exchange_id, error = ?error, "ExchangeCaptured not published");
                Err(IngestError::NotPublished(error))
            }
        }
    }

    /// Publish `event` in an envelope stamped `at`, minting its id under
    /// the same lock as [`Ingester::ingest`], so every envelope this
    /// pipeline publishes reaches the bus in id order. What the stages a
    /// composer runs over this pipeline publish through (store outboxes,
    /// layer consumers). Not counted in [`PipelineStats`], which counts
    /// exchanges.
    pub async fn publish(&self, event: BusEvent, at: Timestamp) -> Result<EventId, PublishError> {
        let inner = &self.inner;
        let subject = event.subject();
        let mut ids = inner.ids.lock().await;
        let id = ids
            .mint_at::<EventId>(at)
            .map_err(|_exhausted| PublishError::IdsExhausted { at })?;
        let published = inner.bus.publish(Envelope { id, at, event }).await;
        drop(ids);
        match published {
            Ok(()) => {
                tracing::debug!(event = %id.ulid_text(), subject = ?subject, "event published");
                Ok(id)
            }
            Err(error) => {
                tracing::warn!(subject = ?subject, error = ?error, "event not published");
                Err(PublishError::NotPublished(error))
            }
        }
    }

    /// The injected clock's reading: the `at` the proxy path ingests with.
    pub fn now(&self) -> Timestamp {
        self.inner.clock.now()
    }

    pub fn blobs(&self) -> &B {
        &self.inner.blobs
    }

    pub fn bus(&self) -> &E {
        &self.inner.bus
    }

    pub fn stats(&self) -> &Arc<PipelineStats> {
        &self.inner.stats
    }

    pub fn retry(&self) -> PutRetry {
        self.inner.retry
    }

    async fn store(&self, exchange: &NormalizedExchange) -> Result<(), StoreError> {
        let inner = &self.inner;
        let mut attempt = 1;
        loop {
            match crosstalk_canonical::store(&inner.blobs, exchange).await {
                Ok(()) => return Ok(()),
                Err(error) if attempt < inner.retry.attempts.get() => {
                    inner.stats.bump(Counter::StoreRetries);
                    tracing::warn!(
                        exchange = %exchange.exchange.meta.id.ulid_text(),
                        attempt,
                        error = %error,
                        "blob put failed; retrying"
                    );
                    attempt += 1;
                    tokio::time::sleep(inner.retry.backoff).await;
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
    fn of(exchange: &NormalizedExchange) -> Self {
        Self {
            outcome: match exchange.exchange.outcome {
                ExchangeOutcome::Completed { .. } => "completed",
                ExchangeOutcome::Failed { .. } => "failed",
            },
            messages: exchange.messages.len(),
            media: exchange.media.len(),
            warnings: exchange.warnings.len(),
        }
    }
}
