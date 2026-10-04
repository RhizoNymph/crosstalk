//! What handles send the bus task, and what it hands back.

use std::collections::HashSet;
use std::num::NonZeroU32;
use std::time::Duration;

use crosstalk_spec::events::Subject;
use crosstalk_spec::ids::EventId;
use crosstalk_spec::interfaces::l2_transport::{
    BusError, ConsumerGroup, DeadLetter, DeliveryId, RetryPolicy,
};
use crosstalk_spec::paging::{DeadLetterList, Page, PageRequest};
use tokio::sync::oneshot;

use crate::codec::Message;

/// One subscription, for the bus task's bookkeeping. Never reused.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) struct SubId(pub(crate) u64);

pub(crate) type Reply<T> = oneshot::Sender<Result<T, BusError>>;

/// A delivery as the bus task hands it to a subscription, still encoded.
/// The subscription decodes it.
#[derive(Debug)]
pub(crate) struct Handout {
    pub(crate) delivery: DeliveryId,
    pub(crate) attempt: NonZeroU32,
    pub(crate) message: Message,
}

/// How many envelopes a consumer group holds, by state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct GroupDepth {
    /// Waiting for a consumer.
    pub ready: usize,
    /// Nacked or timed out, waiting for their backoff to pass.
    pub delayed: usize,
    /// Held by a subscription, between `next` and ack, nack or timeout.
    pub held: usize,
    /// Out of attempts, waiting for their dead letter to be stored.
    pub exhausted: usize,
    /// Publishes and replays waiting for room in the group.
    pub waiting: usize,
    /// Live subscriptions of the group.
    pub subscriptions: usize,
}

impl GroupDepth {
    /// Envelopes the group holds: everything but `waiting`. Never above
    /// [`BusConfig::group_capacity`](crate::BusConfig).
    pub fn tracked(&self) -> usize {
        self.ready + self.delayed + self.held + self.exhausted
    }
}

pub(crate) enum Command {
    Publish {
        message: Message,
        reply: Reply<()>,
    },
    Subscribe {
        subjects: HashSet<Subject>,
        group: ConsumerGroup,
        retry: RetryPolicy,
        reply: Reply<SubId>,
    },
    Next {
        sub: SubId,
        reply: oneshot::Sender<Handout>,
    },
    Ack {
        sub: SubId,
        delivery: DeliveryId,
        reply: Reply<()>,
    },
    Nack {
        sub: SubId,
        delivery: DeliveryId,
        retry_after: Duration,
        reason: String,
        reply: Reply<()>,
    },
    /// The subscription could not decode the delivery: end it without a
    /// redelivery or a dead letter (`transport.codec.undecodable-not-redelivered`).
    Terminate {
        sub: SubId,
        delivery: DeliveryId,
        reason: String,
        reply: oneshot::Sender<()>,
    },
    PutLetter {
        letter: Box<DeadLetter>,
        reply: Reply<()>,
    },
    Replay {
        group: ConsumerGroup,
        id: EventId,
        reply: Reply<()>,
    },
    ListLetters {
        group: Option<ConsumerGroup>,
        page: PageRequest<DeadLetterList>,
        reply: Reply<Page<DeadLetter, DeadLetterList>>,
    },
    Depth {
        group: ConsumerGroup,
        reply: oneshot::Sender<Option<GroupDepth>>,
    },
    HandledContains {
        group: ConsumerGroup,
        id: EventId,
        reply: oneshot::Sender<bool>,
    },
    HandledRecord {
        group: ConsumerGroup,
        id: EventId,
        reply: oneshot::Sender<()>,
    },
    Shutdown,
}
