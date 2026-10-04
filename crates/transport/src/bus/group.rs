//! One consumer group's state inside the bus task.
//!
//! Every envelope a group holds is one [`Entry`] in exactly one
//! [`EntryState`]. The state is the single source of truth for who may
//! deliver, ack or redeliver it, so one entry can never be held by two
//! subscriptions at once (`transport.delivery.single-holder`).

use std::collections::{HashMap, HashSet, VecDeque};

use crosstalk_spec::events::Subject;
use crosstalk_spec::ids::EventId;
use crosstalk_spec::interfaces::l2_transport::{
    BusError, ConsumerGroup, DeadLetter, DeliveryId, RetryPolicy,
};
use tokio::sync::oneshot;
use tokio::time::Instant;

use super::command::{GroupDepth, Handout, Reply, SubId};
use crate::codec::Message;

/// A group-local sequence number for an entry. Logged in place of the
/// payload, and never reused within the group.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) struct EntryKey(pub(crate) u64);

#[derive(Debug)]
pub(crate) enum EntryState {
    /// In the group's ready queue.
    Ready,
    /// Nacked or timed out; ready again at `until`.
    Delayed { until: Instant },
    /// Handed to `sub` as `delivery`; taken back at `deadline`.
    Held {
        delivery: DeliveryId,
        sub: SubId,
        deadline: Instant,
    },
    /// Out of attempts; its dead letter is not stored yet, and the bus
    /// retries the put. Still counted against the group's capacity, never
    /// delivered (`transport.deadletter.stored-before-release`).
    Exhausted { letter: Box<DeadLetter> },
}

#[derive(Debug)]
pub(crate) struct Entry {
    pub(crate) message: Message,
    /// Deliveries of this envelope to this group since it was published or
    /// last replayed (`transport.delivery.attempt-counts-deliveries`).
    pub(crate) deliveries: u32,
    pub(crate) state: EntryState,
}

/// A publish or replay waiting for room in the group.
pub(crate) enum Waiting {
    Publish { publish: u64, message: Message },
    Replay { id: EventId, reply: Reply<()> },
}

pub(crate) struct Group {
    pub(crate) name: ConsumerGroup,
    pub(crate) subjects: HashSet<Subject>,
    pub(crate) retry: RetryPolicy,
    pub(crate) members: HashSet<SubId>,
    pub(crate) entries: HashMap<EntryKey, Entry>,
    /// Ready entries, in the order they became ready.
    pub(crate) ready: VecDeque<EntryKey>,
    /// Subscriptions waiting in `next`, first come first served.
    pub(crate) waiters: VecDeque<(SubId, oneshot::Sender<Handout>)>,
    pub(crate) waiting: VecDeque<Waiting>,
    next_key: u64,
}

impl Group {
    pub(crate) fn new(name: ConsumerGroup, subjects: HashSet<Subject>, retry: RetryPolicy) -> Self {
        Self {
            name,
            subjects,
            retry,
            members: HashSet::new(),
            entries: HashMap::new(),
            ready: VecDeque::new(),
            waiters: VecDeque::new(),
            waiting: VecDeque::new(),
            next_key: 0,
        }
    }

    /// Whether a subscribe with these settings may join the group.
    pub(crate) fn admits(
        &self,
        subjects: &HashSet<Subject>,
        retry: &RetryPolicy,
    ) -> Result<(), BusError> {
        if self.subjects != *subjects {
            return Err(BusError::GroupSubjectMismatch {
                group: self.name.clone(),
            });
        }
        if self.retry != *retry {
            return Err(BusError::GroupRetryMismatch {
                group: self.name.clone(),
            });
        }
        Ok(())
    }

    /// Envelopes the group holds, in every state.
    pub(crate) fn held_total(&self) -> usize {
        self.entries.len()
    }

    /// Add a fresh entry (attempt count zero) to the ready queue.
    pub(crate) fn admit(&mut self, message: Message) -> EntryKey {
        let key = EntryKey(self.next_key);
        self.next_key += 1;
        self.entries.insert(
            key,
            Entry {
                message,
                deliveries: 0,
                state: EntryState::Ready,
            },
        );
        self.ready.push_back(key);
        key
    }

    pub(crate) fn depth(&self) -> GroupDepth {
        let mut depth = GroupDepth {
            waiting: self.waiting.len(),
            subscriptions: self.members.len(),
            ..GroupDepth::default()
        };
        for entry in self.entries.values() {
            match entry.state {
                EntryState::Ready => depth.ready += 1,
                EntryState::Delayed { .. } => depth.delayed += 1,
                EntryState::Held { .. } => depth.held += 1,
                EntryState::Exhausted { .. } => depth.exhausted += 1,
            }
        }
        depth
    }
}
