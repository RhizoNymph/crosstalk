//! The topology consumer: group [`GROUP`], which feeds the edge store from
//! the bus and recomputes the watermark.
//!
//! ```text
//! TransmissionClassified ─▶ EdgeStore::apply ─ Ok(key) ─▶ publish EdgeUpdated(key) ─▶ ack
//!                                     │          (Refit: then EdgeStore::activate(version))
//!                                     ├ SelfEdge, VersionNotRetained ─▶ ack (permanent)
//!                                     └ LateContribution (error), Store ─▶ nack
//! TopicVersionReady   ─▶ version_ready, then activate ─▶ ack
//! TopicVersionDropped ─▶ drop_version ─▶ ack
//! VerdictSet          ─▶ judge ─▶ ack
//! AccessRecorded      ─▶ apply_access ─▶ ack
//! every bucket width  ─▶ FrontierSource::frontier ─▶ advance_watermark
//! ```
//!
//! **Envelope ids.** The `EdgeUpdated` a delivery causes is published as
//! `Envelope { id: EventId::derive(delivery id, EDGE_UPDATED, 0), at:
//! delivery's at }`: a function of the delivery alone, so a redelivery
//! republishes the same envelope, which the bus and every consumer
//! deduplicate (`transport.consumer.derived-envelope-ids`). The events the
//! store decides (`TopicVersionActivated`, `WatermarkAdvanced`, ...) go
//! through its outbox, whose relay stamps them once
//! ([`crate::outbox`]).
//!
//! A delivery is acked only after everything it causes is done: the apply
//! committed and, for an applied contribution, `EdgeUpdated` published. A
//! failure before that is nacked, so the bus redelivers it; every store
//! write is idempotent, so a redelivery repeats only what did not happen.
//! A contribution refused as a self-edge or for a dropped version is a
//! permanent outcome and acked. A `LateContribution` signals a frontier
//! that broke its contract: it is logged at error and nacked until the
//! bus dead-letters it.
//!
//! Like the gateway's stages, [`run`] takes a subscription its caller made
//! (`subscribe(&SUBJECTS, group(), retry)`) before anything publishes, and
//! returns when the bus shuts down.

use std::num::NonZeroU64;
use std::time::Duration;

use crosstalk_spec::aggregates::edge::EdgeKey;
use crosstalk_spec::events::detect::DetectEvent;
use crosstalk_spec::events::insight::{ClassificationCause, InsightEvent};
use crosstalk_spec::events::{BusEvent, Envelope, Subject};
use crosstalk_spec::ids::EventId;
use crosstalk_spec::interfaces::l2_transport::{ConsumerGroup, Subscription};
use crosstalk_spec::interfaces::l7_topology::{
    AccessContribution, EdgeContribution, EdgeError, EdgeStore, FrontierSource,
};
use tokio::time::MissedTickBehavior;

use crate::outbox::Announce;

/// The consumer group the topology consumer reads with.
pub const GROUP: &str = "topology";

/// What the topology consumer subscribes to.
pub const SUBJECTS: [Subject; 5] = [
    Subject::TransmissionClassified,
    Subject::TopicVersionReady,
    Subject::TopicVersionDropped,
    Subject::VerdictSet,
    Subject::AccessRecorded,
];

/// The label of the `EdgeUpdated` a delivery causes, for
/// [`EventId::derive`]. Each delivery causes at most one, ordinal 0.
pub const EDGE_UPDATED: &str = "edge-updated";

/// The envelope of the `EdgeUpdated` for `key` that the delivery `cause`
/// causes: its id derived from the delivery's, its time the delivery's.
pub fn edge_updated(cause: &Envelope, key: EdgeKey) -> Envelope {
    Envelope {
        id: EventId::derive(cause.id, EDGE_UPDATED, 0),
        at: cause.at,
        event: BusEvent::Insight(InsightEvent::EdgeUpdated(key)),
    }
}

/// The group as the bus names it.
pub fn group() -> ConsumerGroup {
    ConsumerGroup(GROUP.to_owned())
}

/// How the consumer paces itself.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConsumerSettings {
    /// How often the watermark is recomputed: the store's bucket width, so
    /// at least once per bucket width.
    pub recompute_every: Duration,
    /// How long a nacked delivery waits before redelivery (clamped to the
    /// group's retry policy by the bus).
    pub retry_after: Duration,
}

impl ConsumerSettings {
    /// Recompute once per `bucket_width` microseconds; retry after 200 ms.
    pub fn for_bucket_width(bucket_width: NonZeroU64) -> Self {
        Self {
            recompute_every: Duration::from_micros(bucket_width.get()),
            retry_after: Duration::from_millis(200),
        }
    }
}

/// What a delivery came to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    Ack,
    /// Redeliver; the reason becomes the dead letter's `last_error`.
    Nack(String),
}

fn nack(what: &str, error: &impl std::fmt::Debug) -> Outcome {
    Outcome::Nack(format!("{what}: {error:?}"))
}

/// Consume `subscription` into `store` until the bus shuts down,
/// recomputing the watermark from `frontier` every
/// `settings.recompute_every`, publishing `EdgeUpdated` through
/// `announcer`.
pub async fn run<S, E, F, A>(
    mut subscription: S,
    mut store: E,
    frontier: F,
    announcer: A,
    settings: ConsumerSettings,
) where
    S: Subscription,
    E: EdgeStore,
    F: FrontierSource,
    A: Announce,
{
    tracing::info!(
        group = GROUP,
        recompute_ms = settings.recompute_every.as_millis(),
        "topology consumer started"
    );
    let mut ticker = tokio::time::interval(settings.recompute_every);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            biased;
            _ = ticker.tick() => recompute(&mut store, &frontier).await,
            next = subscription.next() => {
                let Some(next) = next else { break };
                let delivery = match next {
                    Ok(delivery) => delivery,
                    Err(error) => {
                        tracing::warn!(group = GROUP, error = ?error, "undecodable delivery skipped");
                        continue;
                    }
                };
                let event = delivery.envelope.id.ulid_text();
                match handle(&mut store, &announcer, &delivery.envelope).await {
                    Outcome::Ack => {
                        if let Err(error) = subscription.ack(delivery.id).await {
                            tracing::warn!(group = GROUP, event = %event, error = ?error, "ack failed; the bus will redeliver");
                        }
                    }
                    Outcome::Nack(reason) => {
                        tracing::warn!(group = GROUP, event = %event, attempt = delivery.attempt.get(), reason = %reason, "delivery nacked");
                        if let Err(error) = subscription.nack(delivery.id, settings.retry_after, reason).await {
                            tracing::warn!(group = GROUP, event = %event, error = ?error, "nack failed");
                        }
                    }
                }
            }
        }
    }
    tracing::info!(group = GROUP, "topology consumer stopped");
}

/// Recompute the watermark once. Failures are logged; the next tick
/// retries.
pub async fn recompute<E: EdgeStore, F: FrontierSource>(store: &mut E, frontier: &F) {
    let read = match frontier.frontier().await {
        Ok(read) => read,
        Err(error) => {
            tracing::warn!(group = GROUP, error = ?error, "frontier read failed");
            return;
        }
    };
    match store.advance_watermark(read).await {
        Ok(Some(advanced)) => {
            tracing::debug!(
                group = GROUP,
                watermark = advanced.at().as_micros(),
                "watermark advanced"
            );
        }
        Ok(None) => {}
        Err(error) => tracing::warn!(group = GROUP, error = ?error, "watermark advance failed"),
    }
}

/// Handle one delivered envelope: what the store must do for its event,
/// and whether the delivery can be acked.
pub async fn handle<E: EdgeStore, A: Announce>(
    store: &mut E,
    announcer: &A,
    envelope: &Envelope,
) -> Outcome {
    match &envelope.event {
        BusEvent::Insight(InsightEvent::TransmissionClassified {
            cause,
            transmission,
            from,
            to,
            route,
            at,
            matched_bytes,
            classification,
        }) => {
            let contribution = EdgeContribution {
                transmission: *transmission,
                from: *from,
                to: *to,
                route: route.clone(),
                at: *at,
                matched_bytes: *matched_bytes,
                classification: classification.clone(),
                cause: *cause,
            };
            classified(store, announcer, envelope, &contribution).await
        }
        BusEvent::Insight(InsightEvent::TopicVersionReady {
            version,
            transmissions,
        }) => match store.version_ready(*version, *transmissions).await {
            Ok(()) => activate(store, *version).await,
            Err(EdgeError::VersionNotRetained { .. }) => Outcome::Ack,
            Err(error) => nack("version ready", &error),
        },
        BusEvent::Insight(InsightEvent::TopicVersionDropped { version }) => {
            match store.drop_version(*version).await {
                Ok(()) => Outcome::Ack,
                Err(error) => {
                    tracing::error!(group = GROUP, version = version.0, error = ?error, "dropping a topic version failed");
                    nack("drop version", &error)
                }
            }
        }
        BusEvent::Detect(DetectEvent::VerdictSet {
            transmission,
            verdict,
            revision,
            ..
        }) => match store.judge(*transmission, *verdict, *revision).await {
            Ok(_) => Outcome::Ack,
            Err(error) => nack("judge", &error),
        },
        BusEvent::Detect(DetectEvent::AccessRecorded { access, .. }) => {
            let contribution = AccessContribution {
                access: access.id,
                agent: access.agent,
                resource: access.resource,
                op: access.op.kind(),
                at: access.at,
            };
            match store.apply_access(&contribution).await {
                Ok(_) => Outcome::Ack,
                Err(error) => nack("apply access", &error),
            }
        }
        // Not subscribed: nothing to do.
        BusEvent::Ingest(_) | BusEvent::Detect(_) | BusEvent::Insight(_) | BusEvent::Changed(_) => {
            Outcome::Ack
        }
    }
}

async fn classified<E: EdgeStore, A: Announce>(
    store: &mut E,
    announcer: &A,
    cause: &Envelope,
    contribution: &EdgeContribution,
) -> Outcome {
    let version = contribution.classification.version;
    match store.apply(contribution).await {
        Ok(key) => {
            if let Err(error) = announcer.announce(edge_updated(cause, key)).await {
                return nack("publish EdgeUpdated", &error);
            }
        }
        Err(EdgeError::SelfEdge) => {
            tracing::debug!(group = GROUP, transmission = %contribution.transmission.ulid_text(), "self-edge contribution acked");
        }
        Err(EdgeError::VersionNotRetained { .. }) => return Outcome::Ack,
        Err(error @ EdgeError::LateContribution { .. }) => {
            tracing::error!(group = GROUP, transmission = %contribution.transmission.ulid_text(), error = ?error, "contribution into a final bucket: the frontier broke its contract");
            return nack("late contribution", &error);
        }
        Err(error) => return nack("apply", &error),
    }
    if contribution.cause == ClassificationCause::Refit {
        return activate(store, version).await;
    }
    Outcome::Ack
}

async fn activate<E: EdgeStore>(
    store: &mut E,
    version: crosstalk_spec::aggregates::topic::TopicModelVersion,
) -> Outcome {
    match store.activate(version).await {
        Ok(_) | Err(EdgeError::VersionNotRetained { .. }) => Outcome::Ack,
        Err(error) => nack("activate", &error),
    }
}
