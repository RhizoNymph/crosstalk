//! L2 transport: the only path between components.
//!
//! Implementations:
//! - `EventBus`: `MpscBus` (single node, tokio channels) and `JetStreamBus`
//!   (multi node).
//! - `BlobStore`: `PgBlobStore` and `ObjectStoreBlobs`.

use std::num::NonZeroU32;
use std::time::Duration;

use crate::events::{Envelope, Subject};
use crate::ids::MessageHash;

/// Consumers in the same group share deliveries: each event goes to one of
/// them. Different groups each get every event.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ConsumerGroup(pub String);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct DeliveryId(pub u64);

#[derive(Debug, Clone, PartialEq)]
pub struct Delivery {
    pub id: DeliveryId,
    /// 1 on first delivery. Higher after a nack or an ack timeout.
    pub attempt: NonZeroU32,
    pub envelope: Envelope,
}

pub trait EventBus {
    type Subscription: Subscription;

    async fn publish(&self, envelope: Envelope) -> Result<(), BusError>;

    /// Every subscription in a group must use the same subject set; a
    /// different one is rejected with `GroupSubjectMismatch`.
    async fn subscribe(
        &self,
        subjects: &[Subject],
        group: ConsumerGroup,
        retry: RetryPolicy,
    ) -> Result<Self::Subscription, BusError>;
}

pub trait Subscription {
    /// `None` when the bus has shut down.
    async fn next(&mut self) -> Option<Result<Delivery, BusError>>;

    async fn ack(&mut self, id: DeliveryId) -> Result<(), BusError>;

    async fn nack(&mut self, id: DeliveryId, retry_after: Duration) -> Result<(), BusError>;
}

/// How often a delivery is retried before it is dead-lettered.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetryPolicy {
    pub max_attempts: NonZeroU32,
    pub initial_backoff: Duration,
    pub max_backoff: Duration,
}

/// A delivery that exhausted its retries. Kept for an operator to inspect
/// and replay; never redelivered on its own.
#[derive(Debug, Clone, PartialEq)]
pub struct DeadLetter {
    pub group: ConsumerGroup,
    pub envelope: Envelope,
    pub attempts: NonZeroU32,
    pub last_error: String,
}

pub trait DeadLetterStore {
    async fn put(&self, letter: DeadLetter) -> Result<(), BusError>;

    async fn replay(&self, group: &ConsumerGroup, id: crate::ids::EventId) -> Result<(), BusError>;
}

pub trait BlobStore {
    /// Idempotent: putting the same bytes twice returns the same hash.
    async fn put(&self, bytes: &[u8]) -> Result<MessageHash, BlobError>;

    async fn get(&self, hash: MessageHash) -> Result<Option<Vec<u8>>, BlobError>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BusError {
    Disconnected,
    PublishRejected { reason: String },
    UnknownDelivery(DeliveryId),
    Encode { reason: String },
    GroupSubjectMismatch { group: ConsumerGroup },
    Decode { reason: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BlobError {
    Unavailable {
        reason: String,
    },
    /// The stored bytes do not hash to the key they are stored under.
    Corrupt(MessageHash),
}
