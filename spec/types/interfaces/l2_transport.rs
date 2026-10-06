//! L2 transport: the only path between components.
//!
//! Implementations:
//! - `EventBus`: `MpscBus` (single node, tokio channels, not durable;
//!   memory mode and the simulation tests), `PgBus` (single node, durable:
//!   an append-only log in the `transport` schema, with each group's
//!   deliveries and dead letters beside it) and `JetStreamBus` (multi
//!   node).
//! - `SpoolingBus<B>`: the `EventBus` decorator `serve` puts in front of
//!   `PgBus` whenever a database is configured. A publish the database
//!   cannot take ([`BusError::Disconnected`]) is appended to an fsynced
//!   spool on the data volume and sent, in order and under its own id,
//!   when the database returns; until the spool is empty every later
//!   publish queues behind it. A full spool refuses with
//!   [`BusError::SpoolFull`] and never waits.
//! - `BlobStore`: `PgBlobStore` and `ObjectStoreBlobs`.
//!
//! **Durability.** On a durable bus (`PgBus`, `JetStreamBus`, and
//! `SpoolingBus` over either), `publish` returning `Ok` means the envelope
//! is in the bus log or durably spooled, and every group subscribed then
//! receives it until the group acks it, across restarts
//! (`transport.durability.pg-publish-persisted`,
//! `transport.spool.ok-means-durable`). `PgBus` is idempotent on
//! [`Envelope::id`]: publishing an id the log already holds is `Ok` and
//! delivers nothing new (`transport.publish.idempotent-on-id`), which is
//! what makes outbox relays and consumer republishes after a crash
//! harmless. A delivery held by a process that stopped is redelivered with
//! its attempt counted (`transport.restart.held-redelivered`). `MpscBus`
//! keeps the consumer-side dedup instead, and only it bounds a group's
//! queue and makes `publish` wait for room (`transport.backpressure.*`):
//! a durable log is the queue.
//!
//! **Derived envelope ids.** Every envelope a pipeline consumer publishes
//! because of a delivery has an id that is a function of that delivery
//! ([`EventId::derive`]), so a redelivery republishes the same ids
//! (`transport.consumer.derived-envelope-ids`).

use std::num::NonZeroU32;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::events::{Envelope, Subject};
use crate::ids::{EventId, MessageHash};
use crate::paging::{DeadLetterList, Page, PageRequest};
use crate::wire::WireRequest;

/// Consumers in the same group share deliveries: each event goes to one of
/// them. Different groups each get every event. On the wire, the name.
///
/// A request: `QueryApi::dead_letters` takes the group whose dead letters
/// to list from the client. Any name decodes; a name no consumer uses lists
/// no letters.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ConsumerGroup(pub String);

/// A client names the group whose dead letters it lists.
impl WireRequest for ConsumerGroup {}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct DeliveryId(pub u64);

#[derive(Debug, Clone, PartialEq)]
pub struct Delivery {
    pub id: DeliveryId,
    /// 1 on first delivery. Higher after a nack or an ack timeout. Restarts
    /// at 1 when a dead letter is replayed.
    pub attempt: NonZeroU32,
    pub envelope: Envelope,
}

pub trait EventBus {
    type Subscription: Subscription + Send + 'static;

    fn publish(&self, envelope: Envelope) -> impl Future<Output = Result<(), BusError>> + Send;

    /// Every subscription in a group must use the same subject set and retry
    /// policy; a different one is rejected with `GroupSubjectMismatch` or
    /// `GroupRetryMismatch`.
    fn subscribe(
        &self,
        subjects: &[Subject],
        group: ConsumerGroup,
        retry: RetryPolicy,
    ) -> impl Future<Output = Result<Self::Subscription, BusError>> + Send;
}

pub trait Subscription {
    /// `None` when the bus has shut down.
    fn next(&mut self) -> impl Future<Output = Option<Result<Delivery, BusError>>> + Send;

    fn ack(&mut self, id: DeliveryId) -> impl Future<Output = Result<(), BusError>> + Send;

    /// `reason` is what the consumer reports; it becomes the dead letter's
    /// `last_error` if retries run out.
    fn nack(
        &mut self,
        id: DeliveryId,
        retry_after: Duration,
        reason: String,
    ) -> impl Future<Output = Result<(), BusError>> + Send;
}

/// How often a delivery is retried before it is dead-lettered, and how long
/// to wait between attempts. A nack's `retry_after` is clamped to
/// `initial_backoff..=max_backoff`.
///
/// Built only through [`RetryPolicy::new`]: `initial_backoff` is non-zero
/// and no greater than `max_backoff`. Every subscription in a group uses the
/// same policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetryPolicy {
    max_attempts: NonZeroU32,
    initial_backoff: Duration,
    max_backoff: Duration,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InvalidRetryPolicy {
    ZeroBackoff,
    InitialAboveMax,
}

impl RetryPolicy {
    pub fn new(
        max_attempts: NonZeroU32,
        initial_backoff: Duration,
        max_backoff: Duration,
    ) -> Result<Self, InvalidRetryPolicy> {
        if initial_backoff.is_zero() {
            return Err(InvalidRetryPolicy::ZeroBackoff);
        }
        if initial_backoff > max_backoff {
            return Err(InvalidRetryPolicy::InitialAboveMax);
        }
        Ok(Self {
            max_attempts,
            initial_backoff,
            max_backoff,
        })
    }

    pub fn max_attempts(&self) -> NonZeroU32 {
        self.max_attempts
    }

    pub fn initial_backoff(&self) -> Duration {
        self.initial_backoff
    }

    pub fn max_backoff(&self) -> Duration {
        self.max_backoff
    }
}

/// A delivery that exhausted its retries. Kept for an operator to inspect
/// and replay; never redelivered on its own. A response
/// (`QueryApi::dead_letters`), never a request: it holds an [`Envelope`],
/// which the publishing node stamped. On the wire, `{"group": "flow",
/// "envelope": {..}, "attempts": 5, "last_error": ".."}`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct DeadLetter {
    pub group: ConsumerGroup,
    pub envelope: Envelope,
    pub attempts: NonZeroU32,
    pub last_error: String,
}

pub trait DeadLetterStore {
    fn put(&self, letter: DeadLetter) -> impl Future<Output = Result<(), BusError>> + Send;

    /// Redeliver a dead letter to its group and remove it from the store.
    fn replay(
        &self,
        group: &ConsumerGroup,
        id: EventId,
    ) -> impl Future<Output = Result<(), BusError>> + Send;

    /// Stored dead letters, of one group or of all groups, newest envelope
    /// first (descending (`Envelope::id`, group)). A letter replayed during
    /// a traversal simply stops appearing; no other letter is skipped.
    fn list(
        &self,
        group: Option<&ConsumerGroup>,
        page: &PageRequest<DeadLetterList>,
    ) -> impl Future<Output = Result<Page<DeadLetter, DeadLetterList>, BusError>> + Send;
}

pub trait BlobStore {
    /// Idempotent: putting the same bytes twice returns the same hash.
    fn put(&self, bytes: &[u8]) -> impl Future<Output = Result<MessageHash, BlobError>> + Send;

    /// `None` when no body is stored under `hash`. L1 stores every body
    /// before publishing `ExchangeCaptured`, so for a hash a span, match or
    /// access names, `None` means content retention dropped it; the
    /// evidence page shows that as `Excerpted::BodyDropped`.
    fn get(
        &self,
        hash: MessageHash,
    ) -> impl Future<Output = Result<Option<Vec<u8>>, BlobError>> + Send;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BusError {
    Disconnected,
    PublishRejected {
        reason: String,
    },
    UnknownDelivery(DeliveryId),
    Encode {
        reason: String,
    },
    GroupSubjectMismatch {
        group: ConsumerGroup,
    },
    GroupRetryMismatch {
        group: ConsumerGroup,
    },
    /// No dead letter for that group and event id (never stored, or already
    /// replayed).
    UnknownDeadLetter {
        group: ConsumerGroup,
        id: EventId,
    },
    Decode {
        reason: String,
    },
    /// A cursor the store did not issue, or issued for another group.
    InvalidCursor,
    /// `SpoolingBus` could not reach its inner bus and its spool has no
    /// room for the envelope: the spool already holds `bytes` and the
    /// envelope would take it past its configured bound. Nothing was
    /// written; the publish did not wait (`transport.spool.bounded`).
    SpoolFull {
        bytes: u64,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BlobError {
    Unavailable {
        reason: String,
    },
    /// The stored bytes do not hash to the key they are stored under.
    Corrupt(MessageHash),
}
