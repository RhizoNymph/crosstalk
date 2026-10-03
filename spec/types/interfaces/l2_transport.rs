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

    async fn subscribe(
        &self,
        subjects: &[Subject],
        group: ConsumerGroup,
    ) -> Result<Self::Subscription, BusError>;
}

pub trait Subscription {
    /// `None` when the bus has shut down.
    async fn next(&mut self) -> Option<Result<Delivery, BusError>>;

    async fn ack(&mut self, id: DeliveryId) -> Result<(), BusError>;

    async fn nack(&mut self, id: DeliveryId, retry_after: Duration) -> Result<(), BusError>;
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
