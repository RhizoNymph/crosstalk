//! [`MpscBus`]: the single-node event bus, on tokio channels.
//!
//! One task owns all bus state ([`actor`]); [`MpscBus`], [`DeadLetters`],
//! [`MemoryHandledIds`] and [`MpscSubscription`] are handles that send it
//! commands and await replies. Envelopes cross it encoded (`crate::codec`).

mod actor;
mod command;
mod group;
mod letters;
mod subscription;

use std::collections::HashSet;

use crosstalk_spec::events::{Envelope, Subject};
use crosstalk_spec::ids::EventId;
use crosstalk_spec::interfaces::l2_transport::{
    BusError, ConsumerGroup, DeadLetter, DeadLetterStore, EventBus, RetryPolicy,
};
use crosstalk_spec::paging::{DeadLetterList, Page, PageRequest};
use tokio::sync::{mpsc, oneshot};

pub use command::GroupDepth;
pub use subscription::MpscSubscription;

use self::actor::Actor;
use self::command::{Command, SubId};
use crate::codec::{self, Message};
use crate::config::BusConfig;
use crate::dedup::HandledIds;

/// Why the bus could not start.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum StartError {
    /// [`MpscBus::start`] needs a tokio runtime to spawn its task on.
    #[error("the bus must be started from within a tokio runtime")]
    NoRuntime,
}

/// The in-process [`EventBus`]: consumer groups, ack and nack, retries with
/// backoff, ack timeouts, and dead letters after the retry budget.
///
/// - Each consumer group gets every envelope published, after the group's
///   first subscribe, under one of its subjects; the subscriptions of a
///   group share them, one holder at a time.
/// - Delivery is at least once. A delivery comes back after a nack, an ack
///   timeout or the drop of the subscription holding it, until it is acked
///   or reaches the group's `RetryPolicy::max_attempts`, when it becomes a
///   [`DeadLetter`] in [`DeadLetters`].
/// - No order is promised, within a subject or across subjects
///   (`transport.ordering.unconstrained`); see [`crate::DeliveryOrder`].
/// - A group holds at most [`BusConfig::group_capacity`] envelopes;
///   `publish` waits for room rather than dropping.
/// - Groups live as long as the bus task: a group whose subscriptions are
///   all dropped keeps receiving envelopes until one resubscribes. Nothing
///   survives the process (`transport.durability.publish-persisted` is the
///   cluster bus's).
///
/// Cloning gives another handle on the same bus. The task stops when
/// [`MpscBus::shutdown`] is called or every `MpscBus`, [`DeadLetters`] and
/// [`MemoryHandledIds`] handle is dropped; subscriptions then see `None`.
#[derive(Debug, Clone)]
pub struct MpscBus {
    commands: mpsc::Sender<Command>,
    drops: mpsc::UnboundedSender<SubId>,
}

async fn request<T>(
    commands: &mpsc::Sender<Command>,
    command: impl FnOnce(oneshot::Sender<T>) -> Command,
) -> Result<T, BusError> {
    let (reply, response) = oneshot::channel();
    commands
        .send(command(reply))
        .await
        .map_err(|_| BusError::Disconnected)?;
    response.await.map_err(|_| BusError::Disconnected)
}

impl MpscBus {
    /// Start the bus task on the current tokio runtime.
    pub fn start(config: BusConfig) -> Result<Self, StartError> {
        Self::start_with(Actor::new(config.clone()), &config)
    }

    /// Start a bus whose dead-letter store refuses its first `failing_puts`
    /// puts (a store outage), for the simulation tests.
    #[cfg(test)]
    pub(crate) fn start_with_failing_puts(
        config: BusConfig,
        failing_puts: u32,
    ) -> Result<Self, StartError> {
        let mut actor = Actor::new(config.clone());
        actor.shelf.failing_puts = failing_puts;
        Self::start_with(actor, &config)
    }

    fn start_with(actor: Actor, config: &BusConfig) -> Result<Self, StartError> {
        let runtime = tokio::runtime::Handle::try_current().map_err(|_| StartError::NoRuntime)?;
        let (commands, receiver) = mpsc::channel(config.command_buffer.get());
        let (drops, dropped) = mpsc::unbounded_channel();
        runtime.spawn(actor.run(receiver, dropped));
        Ok(Self { commands, drops })
    }

    /// Publish bytes that are meant to be an encoded [`Envelope`], routed
    /// under `subject`, without checking them: the path a foreign publisher
    /// or a bridge from another bus takes. A consumer decodes them strictly;
    /// bytes that do not decode, or decode to an event of another subject,
    /// are reported to the group once as [`BusError::Decode`] and never
    /// redelivered (`transport.codec.undecodable-not-redelivered`).
    pub async fn publish_encoded(&self, subject: Subject, bytes: Vec<u8>) -> Result<(), BusError> {
        let message = Message {
            subject,
            id: None,
            bytes: bytes.into(),
        };
        self.send_publish(message).await
    }

    async fn send_publish(&self, message: Message) -> Result<(), BusError> {
        request(&self.commands, |reply| Command::Publish { message, reply }).await?
    }

    /// How many envelopes `group` holds, by state; `None` for a group no
    /// one has subscribed.
    pub async fn depth(&self, group: &ConsumerGroup) -> Result<Option<GroupDepth>, BusError> {
        let group = group.clone();
        request(&self.commands, |reply| Command::Depth { group, reply }).await
    }

    /// The bus's dead-letter store.
    pub fn dead_letters(&self) -> DeadLetters {
        DeadLetters {
            commands: self.commands.clone(),
        }
    }

    /// The handled-id record the [`Dedup`](crate::Dedup) wrapper keeps per
    /// group, held by the bus task for the life of the process.
    pub fn handled_ids(&self) -> MemoryHandledIds {
        MemoryHandledIds {
            commands: self.commands.clone(),
        }
    }

    /// Stop the bus task. Waiting `next` calls return `None`; held
    /// deliveries and stored dead letters are discarded.
    pub async fn shutdown(&self) {
        // Fails only when the task has already stopped.
        let _ = self.commands.send(Command::Shutdown).await;
    }
}

impl EventBus for MpscBus {
    type Subscription = MpscSubscription;

    /// Encode the envelope and enqueue it for every group subscribed to its
    /// subject. Returns once every such group has taken it, waiting while a
    /// group is full.
    async fn publish(&self, envelope: Envelope) -> Result<(), BusError> {
        let message = codec::encode(&envelope)?;
        self.send_publish(message).await
    }

    async fn subscribe(
        &self,
        subjects: &[Subject],
        group: ConsumerGroup,
        retry: RetryPolicy,
    ) -> Result<MpscSubscription, BusError> {
        let subjects: HashSet<Subject> = subjects.iter().copied().collect();
        let name = group.clone();
        let sub = request(&self.commands, |reply| Command::Subscribe {
            subjects,
            group: name,
            retry,
            reply,
        })
        .await??;
        Ok(MpscSubscription::new(
            sub,
            group,
            self.commands.downgrade(),
            self.drops.clone(),
        ))
    }
}

/// The [`DeadLetterStore`] of an [`MpscBus`]: letters live in the bus task,
/// so a replay re-enqueues the envelope and removes the letter in one step.
#[derive(Debug, Clone)]
pub struct DeadLetters {
    commands: mpsc::Sender<Command>,
}

impl DeadLetterStore for DeadLetters {
    async fn put(&self, letter: DeadLetter) -> Result<(), BusError> {
        request(&self.commands, |reply| Command::PutLetter {
            letter: Box::new(letter),
            reply,
        })
        .await?
    }

    /// Re-enqueue the letter for its group alone, at attempt 1, then remove
    /// it. Waits for room in the group like a publish. A group that never
    /// subscribed on this bus, or no longer takes the letter's subject,
    /// is `PublishRejected` and the letter stays.
    async fn replay(&self, group: &ConsumerGroup, id: EventId) -> Result<(), BusError> {
        let group = group.clone();
        request(&self.commands, |reply| Command::Replay { group, id, reply }).await?
    }

    async fn list(
        &self,
        group: Option<&ConsumerGroup>,
        page: &PageRequest<DeadLetterList>,
    ) -> Result<Page<DeadLetter, DeadLetterList>, BusError> {
        let group = group.cloned();
        let page = page.clone();
        request(&self.commands, |reply| Command::ListLetters {
            group,
            page,
            reply,
        })
        .await?
    }
}

/// The in-memory [`HandledIds`] of an [`MpscBus`]: per group, the envelope
/// ids consumer logic handled. Shared by every consumer of the group in the
/// process and kept for its life; it is never pruned.
#[derive(Debug, Clone)]
pub struct MemoryHandledIds {
    commands: mpsc::Sender<Command>,
}

impl HandledIds for MemoryHandledIds {
    async fn contains(&self, group: &ConsumerGroup, id: EventId) -> Result<bool, BusError> {
        let group = group.clone();
        request(&self.commands, |reply| Command::HandledContains {
            group,
            id,
            reply,
        })
        .await
    }

    async fn record(&self, group: &ConsumerGroup, id: EventId) -> Result<(), BusError> {
        let group = group.clone();
        request(&self.commands, |reply| Command::HandledRecord {
            group,
            id,
            reply,
        })
        .await
    }
}
