//! L4's bus consumer: group [`GROUP`], subjects `exchange_captured` and
//! `conversation_delta`, publishing `SpanOriginated`, `SpanRelayed` and
//! `ContentMatched`.
//!
//! A later wiring step adds it to `crosstalk_gateway::pipeline` as a stage,
//! like the exchange log: subscribe with [`subscribe`] while building (so
//! nothing is published before the group exists), then spawn [`run`] with
//! the subscription, a [`Provenance`] over the stores, the bus to publish
//! on and the injected clock.
//!
//! One task handles one delivery at a time:
//!
//! - `ExchangeCaptured`: record the exchange, ack.
//! - `ConversationDelta`: process it; publish every returned envelope, then
//!   ack. A delta whose exchange is not recorded yet, a transient store,
//!   index or blob failure, or a failed publish is nacked and redelivered
//!   after a backoff (the bus dead-letters it once the group's retries run
//!   out); redelivery republishes the stored outcome, so consumers see the
//!   same envelope ids. A scan that cannot succeed is acked: its status
//!   records why.
//! - Anything else is acked unread.
//!
//! Between deliveries, every eviction interval (on tokio time, so it runs
//! under paused time) the clock is read and expired spans are evicted.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use crosstalk_spec::events::ingest::IngestEvent;
use crosstalk_spec::events::{BusEvent, Subject};
use crosstalk_spec::interfaces::l2_transport::{
    BusError, ConsumerGroup, Delivery, EventBus, RetryPolicy, Subscription,
};
use crosstalk_spec::interfaces::l4_provenance::{FingerprintIndex, SemanticMatcher};
use crosstalk_spec::support::Clock;

use crate::engine::{EngineError, Processed, Provenance};
use crate::scan::messages::MessageSource;
use crate::store::ProvenanceStore;

/// The consumer group provenance reads with.
pub const GROUP: &str = "provenance";

/// The subjects it subscribes to.
pub const SUBJECTS: [Subject; 2] = [Subject::ExchangeCaptured, Subject::ConversationDelta];

/// The group as the bus names it.
pub fn group() -> ConsumerGroup {
    ConsumerGroup(GROUP.to_owned())
}

/// Subscribe provenance's group.
pub async fn subscribe<E: EventBus>(
    bus: &E,
    retry: RetryPolicy,
) -> Result<E::Subscription, BusError> {
    bus.subscribe(&SUBJECTS, group(), retry).await
}

/// The consumer's timing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConsumerSettings {
    /// How long a nacked delivery waits (clamped by the group's policy).
    pub retry_after: Duration,
    /// How often expired spans are evicted.
    pub eviction_interval: Duration,
}

/// The consumer's counters.
#[derive(Debug, Default)]
pub struct ConsumerStats {
    exchanges: AtomicU64,
    scanned: AtomicU64,
    replayed: AtomicU64,
    failed: AtomicU64,
    retried: AtomicU64,
    published: AtomicU64,
    expired: AtomicU64,
}

/// A reading of [`ConsumerStats`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ConsumerCounts {
    /// Exchanges recorded.
    pub exchanges: u64,
    /// Deltas scanned.
    pub scanned: u64,
    /// Deltas redelivered after their scan, republished.
    pub replayed: u64,
    /// Deltas whose scan failed for good.
    pub failed: u64,
    /// Deliveries nacked for a retry.
    pub retried: u64,
    /// Envelopes published.
    pub published: u64,
    /// Spans expired.
    pub expired: u64,
}

impl ConsumerStats {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn snapshot(&self) -> ConsumerCounts {
        ConsumerCounts {
            exchanges: self.exchanges.load(Ordering::Relaxed),
            scanned: self.scanned.load(Ordering::Relaxed),
            replayed: self.replayed.load(Ordering::Relaxed),
            failed: self.failed.load(Ordering::Relaxed),
            retried: self.retried.load(Ordering::Relaxed),
            published: self.published.load(Ordering::Relaxed),
            expired: self.expired.load(Ordering::Relaxed),
        }
    }
}

/// Why a delivery is retried.
#[derive(Debug, thiserror::Error)]
enum Retry {
    #[error(transparent)]
    Engine(EngineError),
    #[error("publishing: {0:?}")]
    Publish(BusError),
}

/// Consume `subscription` until the bus shuts down.
pub async fn run<Sub, E, I, S, M, L>(
    mut subscription: Sub,
    mut engine: Provenance<I, S, M, L>,
    bus: E,
    clock: Arc<dyn Clock>,
    settings: ConsumerSettings,
    stats: Arc<ConsumerStats>,
) where
    Sub: Subscription,
    E: EventBus + Sync,
    I: FingerprintIndex + Send + Sync,
    S: ProvenanceStore + Send + Sync,
    M: SemanticMatcher + Send + Sync,
    L: MessageSource + Send + Sync,
{
    tracing::info!(group = GROUP, "provenance consumer started");
    let mut eviction = tokio::time::interval(settings.eviction_interval);
    eviction.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            next = subscription.next() => {
                let Some(next) = next else { break };
                let delivery = match next {
                    Ok(delivery) => delivery,
                    Err(error) => {
                        tracing::warn!(group = GROUP, error = ?error, "undecodable delivery skipped");
                        continue;
                    }
                };
                handle(&mut subscription, &mut engine, &bus, &settings, &stats, delivery).await;
            }
            _ = eviction.tick() => {
                match engine.expire(clock.now()).await {
                    Ok(expired) => {
                        stats.expired.fetch_add(u64::try_from(expired).unwrap_or(u64::MAX), Ordering::Relaxed);
                    }
                    Err(error) => tracing::warn!(group = GROUP, error = %error, "eviction failed; retried next interval"),
                }
            }
        }
    }
    tracing::info!(group = GROUP, "provenance consumer stopped");
}

async fn handle<Sub, E, I, S, M, L>(
    subscription: &mut Sub,
    engine: &mut Provenance<I, S, M, L>,
    bus: &E,
    settings: &ConsumerSettings,
    stats: &ConsumerStats,
    delivery: Delivery,
) where
    Sub: Subscription,
    E: EventBus + Sync,
    I: FingerprintIndex + Send + Sync,
    S: ProvenanceStore + Send + Sync,
    M: SemanticMatcher + Send + Sync,
    L: MessageSource + Send + Sync,
{
    let event = delivery.envelope.id.ulid_text();
    let outcome = match &delivery.envelope.event {
        BusEvent::Ingest(IngestEvent::ExchangeCaptured(exchange)) => engine
            .record_exchange(exchange)
            .await
            .map(|()| {
                stats.exchanges.fetch_add(1, Ordering::Relaxed);
            })
            .map_err(Retry::Engine),
        BusEvent::Ingest(IngestEvent::ConversationDelta(delta)) => {
            deliver(engine, bus, stats, delta).await
        }
        _ => Ok(()),
    };
    match outcome {
        Ok(()) => {
            if let Err(error) = subscription.ack(delivery.id).await {
                tracing::warn!(group = GROUP, event = %event, error = ?error, "ack failed; the bus will redeliver");
            }
        }
        Err(Retry::Engine(error)) if !error.is_transient() => {
            tracing::error!(group = GROUP, event = %event, error = %error, "provenance cannot process the event; acked");
            if let Err(error) = subscription.ack(delivery.id).await {
                tracing::warn!(group = GROUP, event = %event, error = ?error, "ack failed; the bus will redeliver");
            }
        }
        Err(retry) => {
            stats.retried.fetch_add(1, Ordering::Relaxed);
            tracing::warn!(group = GROUP, event = %event, attempt = delivery.attempt.get(), reason = %retry, "delivery retried");
            if let Err(error) = subscription
                .nack(delivery.id, settings.retry_after, retry.to_string())
                .await
            {
                tracing::warn!(group = GROUP, event = %event, error = ?error, "nack failed");
            }
        }
    }
}

async fn deliver<E, I, S, M, L>(
    engine: &mut Provenance<I, S, M, L>,
    bus: &E,
    stats: &ConsumerStats,
    delta: &crosstalk_spec::events::ingest::ConversationDelta,
) -> Result<(), Retry>
where
    E: EventBus + Sync,
    I: FingerprintIndex + Send + Sync,
    S: ProvenanceStore + Send + Sync,
    M: SemanticMatcher + Send + Sync,
    L: MessageSource + Send + Sync,
{
    let processed = engine.process(delta).await.map_err(Retry::Engine)?;
    let counter = match &processed {
        Processed::Scanned { .. } => &stats.scanned,
        Processed::Replayed { .. } => &stats.replayed,
        Processed::Failed { .. } => &stats.failed,
    };
    counter.fetch_add(1, Ordering::Relaxed);
    for envelope in processed.events() {
        bus.publish(envelope.clone())
            .await
            .map_err(Retry::Publish)?;
        stats.published.fetch_add(1, Ordering::Relaxed);
    }
    Ok(())
}
