//! L2 transport: the only path between components.
//!
//! Implementations:
//! - `EventBus`: `MpscBus` (single node, tokio channels) and `JetStreamBus`
//!   (multi node).
//! - `BlobStore`: `PgBlobStore` and `ObjectStoreBlobs`.

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
    type Subscription: Subscription;

    async fn publish(&self, envelope: Envelope) -> Result<(), BusError>;

    /// Every subscription in a group must use the same subject set and retry
    /// policy; a different one is rejected with `GroupSubjectMismatch` or
    /// `GroupRetryMismatch`.
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

    /// `reason` is what the consumer reports; it becomes the dead letter's
    /// `last_error` if retries run out.
    async fn nack(
        &mut self,
        id: DeliveryId,
        retry_after: Duration,
        reason: String,
    ) -> Result<(), BusError>;
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
    async fn put(&self, letter: DeadLetter) -> Result<(), BusError>;

    /// Redeliver a dead letter to its group and remove it from the store.
    async fn replay(&self, group: &ConsumerGroup, id: EventId) -> Result<(), BusError>;

    /// Stored dead letters, of one group or of all groups, newest envelope
    /// first (descending (`Envelope::id`, group)). A letter replayed during
    /// a traversal simply stops appearing; no other letter is skipped.
    async fn list(
        &self,
        group: Option<&ConsumerGroup>,
        page: &PageRequest<DeadLetterList>,
    ) -> Result<Page<DeadLetter, DeadLetterList>, BusError>;
}

pub trait BlobStore {
    /// Idempotent: putting the same bytes twice returns the same hash.
    async fn put(&self, bytes: &[u8]) -> Result<MessageHash, BlobError>;

    /// `None` when no body is stored under `hash`. L1 stores every body
    /// before publishing `ExchangeCaptured`, so for a hash a span, match or
    /// access names, `None` means content retention dropped it; the
    /// evidence page shows that as `Excerpted::BodyDropped`.
    async fn get(&self, hash: MessageHash) -> Result<Option<Vec<u8>>, BlobError>;
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
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BlobError {
    Unavailable {
        reason: String,
    },
    /// The stored bytes do not hash to the key they are stored under.
    Corrupt(MessageHash),
}
