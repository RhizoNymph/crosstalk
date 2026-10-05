//! The bus task: the one owner of every group, delivery and dead letter.
//!
//! Handles talk to it only through its command channel (and subscriptions
//! report their drop on a second channel), so no bus state is shared or
//! locked. The task processes one event at a time:
//!
//! 1. a due timer (an ack deadline, the end of a backoff, a dead-letter put
//!    to retry), checked first so a busy command queue cannot starve it;
//! 2. a dropped subscription, whose held deliveries are taken back;
//! 3. a command.
//!
//! After each event it admits waiting publishes into groups with room and
//! hands ready entries to waiting subscriptions. The `select!` is biased and
//! nothing iterates a hash map where order matters, so under tokio's paused
//! clock on one thread a run is a function of its inputs.

use std::cmp::Reverse;
use std::collections::{BTreeMap, BinaryHeap, HashMap, HashSet, VecDeque};
use std::num::NonZeroU32;
use std::time::Duration;

use crosstalk_spec::ids::EventId;
use crosstalk_spec::interfaces::l2_transport::{
    BusError, ConsumerGroup, DeadLetter, DeliveryId, RetryPolicy,
};
use tokio::sync::mpsc;
use tokio::time::Instant;

use super::command::{Command, Handout, Reply, SubId};
use super::group::{EntryKey, EntryState, Group, Waiting};
use super::letters::Shelf;
use crate::codec::{self, Message};
use crate::config::{BusConfig, DeliveryOrder};
use crate::rng::SplitMix64;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Timer {
    /// The delivery with this id is taken back if still held.
    AckDeadline(u64),
    /// The delayed entry becomes ready.
    Ready(usize, EntryKey),
    /// Retry storing the exhausted entry's dead letter.
    PutRetry(usize, EntryKey),
}

/// How a held delivery ended without an ack.
enum Failure {
    Nack {
        retry_after: Duration,
        reason: String,
    },
    Timeout,
    Dropped,
}

struct PendingPublish {
    remaining: usize,
    reply: Reply<()>,
}

pub(crate) struct Actor {
    config: BusConfig,
    groups: Vec<Group>,
    by_name: HashMap<ConsumerGroup, usize>,
    subs: HashMap<SubId, usize>,
    /// Every held delivery, by id: its group and entry.
    held: BTreeMap<u64, (usize, EntryKey)>,
    timers: BinaryHeap<Reverse<(Instant, Timer)>>,
    publishes: HashMap<u64, PendingPublish>,
    pub(crate) shelf: Shelf,
    handled: HashMap<ConsumerGroup, HashSet<EventId>>,
    rng: Option<SplitMix64>,
    next_delivery: u64,
    next_sub: u64,
    next_publish: u64,
}

/// Sleep until `deadline`, or forever when there is none.
async fn sleep_until(deadline: Option<Instant>) {
    match deadline {
        Some(deadline) => tokio::time::sleep_until(deadline).await,
        None => std::future::pending().await,
    }
}

/// Exponential backoff after a timeout or a dropped holder: the initial
/// backoff doubled per earlier delivery, capped at the maximum. Always within
/// `initial_backoff..=max_backoff` (`transport.retry.backoff-floor`,
/// `backoff-ceiling`).
fn backoff(retry: &RetryPolicy, deliveries: u32) -> Duration {
    let doublings = deliveries.saturating_sub(1).min(31);
    retry
        .initial_backoff()
        .checked_mul(1u32 << doublings)
        .map_or(retry.max_backoff(), |delay| delay.min(retry.max_backoff()))
}

impl Actor {
    pub(crate) fn new(config: BusConfig) -> Self {
        let rng = match config.order {
            DeliveryOrder::Fifo => None,
            DeliveryOrder::Shuffled { seed } => Some(SplitMix64::new(seed)),
        };
        Self {
            config,
            groups: Vec::new(),
            by_name: HashMap::new(),
            subs: HashMap::new(),
            held: BTreeMap::new(),
            timers: BinaryHeap::new(),
            publishes: HashMap::new(),
            shelf: Shelf::default(),
            handled: HashMap::new(),
            rng,
            next_delivery: 0,
            next_sub: 0,
            next_publish: 0,
        }
    }

    pub(crate) async fn run(
        mut self,
        mut commands: mpsc::Receiver<Command>,
        mut drops: mpsc::UnboundedReceiver<SubId>,
    ) {
        tracing::debug!("bus task started");
        loop {
            let deadline = self.timers.peek().map(|Reverse((at, _))| *at);
            tokio::select! {
                biased;
                () = sleep_until(deadline) => self.fire_due(),
                Some(sub) = drops.recv() => self.dropped(sub),
                command = commands.recv() => match command {
                    None | Some(Command::Shutdown) => break,
                    Some(command) => self.handle(command),
                },
            }
            self.pump();
        }
        tracing::debug!(
            groups = self.groups.len(),
            dead_letters = self.shelf.len(),
            "bus task stopped"
        );
    }

    fn handle(&mut self, command: Command) {
        match command {
            Command::Publish { message, reply } => self.publish(message, reply),
            Command::Subscribe {
                subjects,
                group,
                retry,
                reply,
            } => {
                let result = self.subscribe(subjects, group, retry);
                // The caller may have stopped waiting; then the subscription
                // id is never used and no subscription holds anything.
                let _ = reply.send(result);
            }
            Command::Next { sub, reply } => match self.subs.get(&sub) {
                Some(&group) => self.groups[group].waiters.push_back((sub, reply)),
                // Unknown only after the subscription was dropped; dropping
                // the reply ends its `next` with `None`.
                None => drop(reply),
            },
            Command::Ack {
                sub,
                delivery,
                reply,
            } => {
                let result = match self.take_held(sub, delivery) {
                    Some((group, key)) => {
                        self.release(group, key);
                        tracing::debug!(
                            group = %self.groups[group].name.0,
                            delivery = delivery.0,
                            entry = key.0,
                            "delivery acked"
                        );
                        Ok(())
                    }
                    None => Err(BusError::UnknownDelivery(delivery)),
                };
                let _ = reply.send(result);
            }
            Command::Nack {
                sub,
                delivery,
                retry_after,
                reason,
                reply,
            } => {
                let result = match self.take_held(sub, delivery) {
                    Some((group, key)) => {
                        self.fail(
                            group,
                            key,
                            Failure::Nack {
                                retry_after,
                                reason,
                            },
                        );
                        Ok(())
                    }
                    None => Err(BusError::UnknownDelivery(delivery)),
                };
                let _ = reply.send(result);
            }
            Command::Terminate {
                sub,
                delivery,
                reason,
                reply,
            } => {
                if let Some((group, key)) = self.take_held(sub, delivery) {
                    let subject = self.groups[group]
                        .entries
                        .get(&key)
                        .map(|entry| entry.message.subject);
                    self.release(group, key);
                    tracing::warn!(
                        group = %self.groups[group].name.0,
                        subject = ?subject,
                        entry = key.0,
                        delivery = delivery.0,
                        reason = %reason,
                        "undecodable message terminated"
                    );
                }
                let _ = reply.send(());
            }
            Command::PutLetter { letter, reply } => {
                let _ = reply.send(self.shelf.put(*letter));
            }
            Command::Replay { group, id, reply } => self.replay(group, id, reply),
            Command::ListLetters { group, page, reply } => {
                let _ = reply.send(self.shelf.list(group.as_ref(), &page));
            }
            Command::Depth { group, reply } => {
                let depth = self
                    .by_name
                    .get(&group)
                    .map(|&index| self.groups[index].depth());
                let _ = reply.send(depth);
            }
            Command::HandledContains { group, id, reply } => {
                let known = self
                    .handled
                    .get(&group)
                    .is_some_and(|ids| ids.contains(&id));
                let _ = reply.send(known);
            }
            Command::HandledRecord { group, id, reply } => {
                self.handled.entry(group).or_default().insert(id);
                let _ = reply.send(());
            }
            // Handled by `run`, which stops.
            Command::Shutdown => {}
        }
    }

    fn publish(&mut self, message: Message, reply: Reply<()>) {
        let capacity = self.config.group_capacity.get();
        let publish = self.next_publish;
        self.next_publish += 1;
        let mut remaining = 0;
        let mut groups = 0;
        for group in &mut self.groups {
            if !group.subjects.contains(&message.subject) {
                continue;
            }
            groups += 1;
            if group.waiting.is_empty() && group.held_total() < capacity {
                group.admit(message.clone());
            } else {
                group.waiting.push_back(Waiting::Publish {
                    publish,
                    message: message.clone(),
                });
                remaining += 1;
            }
        }
        tracing::debug!(
            subject = ?message.subject,
            event = ?message.id.map(EventId::ulid_text),
            groups,
            waiting = remaining,
            "published"
        );
        if remaining == 0 {
            let _ = reply.send(Ok(()));
        } else {
            self.publishes
                .insert(publish, PendingPublish { remaining, reply });
        }
    }

    fn subscribe(
        &mut self,
        subjects: HashSet<crosstalk_spec::events::Subject>,
        name: ConsumerGroup,
        retry: RetryPolicy,
    ) -> Result<SubId, BusError> {
        let index = match self.by_name.get(&name) {
            Some(&index) => {
                self.groups[index].admits(&subjects, &retry)?;
                index
            }
            None => {
                let index = self.groups.len();
                tracing::info!(group = %name.0, subjects = subjects.len(), "consumer group created");
                self.groups.push(Group::new(name.clone(), subjects, retry));
                self.by_name.insert(name, index);
                index
            }
        };
        let sub = SubId(self.next_sub);
        self.next_sub += 1;
        self.groups[index].members.insert(sub);
        self.subs.insert(sub, index);
        Ok(sub)
    }

    /// The group and entry of `delivery` if `sub` holds it now, removed
    /// from the held index. The entry stays `Held` until the caller moves it.
    fn take_held(&mut self, sub: SubId, delivery: DeliveryId) -> Option<(usize, EntryKey)> {
        let &(group, key) = self.held.get(&delivery.0)?;
        let entry = self.groups[group].entries.get(&key)?;
        match entry.state {
            EntryState::Held {
                delivery: holding,
                sub: holder,
                ..
            } if holding == delivery && holder == sub => {
                self.held.remove(&delivery.0);
                Some((group, key))
            }
            _ => None,
        }
    }

    /// The group stops tracking the entry: acked, dead-lettered or
    /// terminated.
    fn release(&mut self, group: usize, key: EntryKey) {
        self.groups[group].entries.remove(&key);
    }

    fn fail(&mut self, group: usize, key: EntryKey, failure: Failure) {
        let now = Instant::now();
        let ack_timeout = self.config.ack_timeout.get();
        let state = &mut self.groups[group];
        let retry = state.retry;
        let Some(entry) = state.entries.get_mut(&key) else {
            return;
        };
        let deliveries = entry.deliveries;
        if deliveries >= retry.max_attempts().get() {
            let last_error = match failure {
                Failure::Nack { reason, .. } => reason,
                Failure::Timeout => format!("ack timeout after {} ms", ack_timeout.as_millis()),
                Failure::Dropped => "subscription dropped while holding the delivery".to_owned(),
            };
            self.exhaust(group, key, last_error);
            return;
        }
        let (delay, cause) = match failure {
            Failure::Nack { retry_after, .. } => (
                retry_after.clamp(retry.initial_backoff(), retry.max_backoff()),
                "nack",
            ),
            Failure::Timeout => (backoff(&retry, deliveries), "ack timeout"),
            Failure::Dropped => (backoff(&retry, deliveries), "holder dropped"),
        };
        let until = now + delay;
        entry.state = EntryState::Delayed { until };
        self.timers.push(Reverse((until, Timer::Ready(group, key))));
        tracing::debug!(
            group = %state.name.0,
            entry = key.0,
            attempt = deliveries,
            cause,
            delay_ms = u64::try_from(delay.as_millis()).unwrap_or(u64::MAX),
            "redelivery scheduled"
        );
    }

    /// Build the entry's dead letter and store it.
    fn exhaust(&mut self, group: usize, key: EntryKey, last_error: String) {
        let state = &self.groups[group];
        let Some(entry) = state.entries.get(&key) else {
            return;
        };
        let envelope = match codec::decode(&entry.message) {
            Ok(envelope) => envelope,
            Err(error) => {
                // Unreachable for an entry that was delivered: a delivery
                // that fails to decode is terminated, never failed.
                tracing::error!(
                    group = %state.name.0,
                    entry = key.0,
                    error = ?error,
                    "exhausted entry does not decode; terminated"
                );
                self.release(group, key);
                return;
            }
        };
        let letter = DeadLetter {
            group: state.name.clone(),
            envelope,
            // Exhaustion follows at least one delivery.
            attempts: NonZeroU32::new(entry.deliveries).unwrap_or(NonZeroU32::MIN),
            last_error,
        };
        self.store_letter(group, key, letter);
    }

    /// Store the letter, then release the entry; on failure keep the entry,
    /// exhausted, and retry later (`transport.deadletter.stored-before-release`).
    fn store_letter(&mut self, group: usize, key: EntryKey, letter: DeadLetter) {
        let event = letter.envelope.id.ulid_text();
        let attempts = letter.attempts.get();
        match self.shelf.put(letter.clone()) {
            Ok(()) => {
                tracing::info!(
                    group = %self.groups[group].name.0,
                    event = %event,
                    attempts,
                    "dead-lettered"
                );
                self.release(group, key);
            }
            Err(error) => {
                let until = Instant::now() + self.config.dead_letter_retry.get();
                tracing::warn!(
                    group = %self.groups[group].name.0,
                    event = %event,
                    error = ?error,
                    "dead letter not stored; retrying"
                );
                if let Some(entry) = self.groups[group].entries.get_mut(&key) {
                    entry.state = EntryState::Exhausted {
                        letter: Box::new(letter),
                    };
                    self.timers
                        .push(Reverse((until, Timer::PutRetry(group, key))));
                }
            }
        }
    }

    fn replay(&mut self, name: ConsumerGroup, id: EventId, reply: Reply<()>) {
        let Some(letter) = self.shelf.get(&name, id) else {
            let _ = reply.send(Err(BusError::UnknownDeadLetter { group: name, id }));
            return;
        };
        let subject = letter.envelope.event.subject();
        let Some(&group) = self.by_name.get(&name) else {
            let _ = reply.send(Err(BusError::PublishRejected {
                reason: "no consumer group of that name has subscribed on this bus".to_owned(),
            }));
            return;
        };
        let state = &mut self.groups[group];
        if !state.subjects.contains(&subject) {
            let _ = reply.send(Err(BusError::PublishRejected {
                reason: format!("the group does not subscribe to {subject:?}"),
            }));
            return;
        }
        if state.waiting.is_empty() && state.held_total() < self.config.group_capacity.get() {
            let _ = reply.send(self.replay_now(group, id));
        } else {
            state.waiting.push_back(Waiting::Replay { id, reply });
        }
    }

    /// Re-enqueue the letter for its group with a fresh attempt count, then
    /// remove it (`transport.deadletter.replay-consumes`).
    fn replay_now(&mut self, group: usize, id: EventId) -> Result<(), BusError> {
        let state = &mut self.groups[group];
        let Some(letter) = self.shelf.get(&state.name, id) else {
            return Err(BusError::UnknownDeadLetter {
                group: state.name.clone(),
                id,
            });
        };
        let message = codec::encode(&letter.envelope)?;
        let key = state.admit(message);
        self.shelf.remove(&state.name, id);
        // Other replays of this letter waiting for room fail now rather
        // than wait for room they would not use.
        let mut kept = VecDeque::with_capacity(state.waiting.len());
        for waiting in state.waiting.drain(..) {
            match waiting {
                Waiting::Replay { id: other, reply } if other == id => {
                    let _ = reply.send(Err(BusError::UnknownDeadLetter {
                        group: state.name.clone(),
                        id,
                    }));
                }
                waiting => kept.push_back(waiting),
            }
        }
        state.waiting = kept;
        tracing::info!(
            group = %state.name.0,
            event = %id.ulid_text(),
            entry = key.0,
            "dead letter replayed"
        );
        Ok(())
    }

    fn fire_due(&mut self) {
        let now = Instant::now();
        while let Some(&Reverse((at, timer))) = self.timers.peek() {
            if at > now {
                break;
            }
            self.timers.pop();
            match timer {
                Timer::AckDeadline(delivery) => self.expire(delivery, now),
                Timer::Ready(group, key) => {
                    let state = &mut self.groups[group];
                    if let Some(entry) = state.entries.get_mut(&key)
                        && let EntryState::Delayed { until } = entry.state
                        && until <= now
                    {
                        entry.state = EntryState::Ready;
                        state.ready.push_back(key);
                    }
                }
                Timer::PutRetry(group, key) => {
                    let letter = self.groups[group].entries.get_mut(&key).and_then(|entry| {
                        match std::mem::replace(&mut entry.state, EntryState::Ready) {
                            EntryState::Exhausted { letter } => Some(letter),
                            other => {
                                entry.state = other;
                                None
                            }
                        }
                    });
                    if let Some(letter) = letter {
                        self.store_letter(group, key, *letter);
                    }
                }
            }
        }
    }

    fn expire(&mut self, delivery: u64, now: Instant) {
        let Some(&(group, key)) = self.held.get(&delivery) else {
            return;
        };
        let due = self.groups[group].entries.get(&key).is_some_and(
            |entry| matches!(entry.state, EntryState::Held { deadline, .. } if deadline <= now),
        );
        if due {
            self.held.remove(&delivery);
            tracing::debug!(
                group = %self.groups[group].name.0,
                delivery,
                entry = key.0,
                "ack timeout"
            );
            self.fail(group, key, Failure::Timeout);
        }
    }

    fn dropped(&mut self, sub: SubId) {
        let Some(group) = self.subs.remove(&sub) else {
            return;
        };
        let state = &mut self.groups[group];
        state.members.remove(&sub);
        state.waiters.retain(|(waiter, _)| *waiter != sub);
        let holding: Vec<(u64, EntryKey)> = self
            .held
            .iter()
            .filter(|(_, (g, key))| {
                *g == group
                    && state.entries.get(key).is_some_and(|entry| {
                        matches!(entry.state, EntryState::Held { sub: holder, .. } if holder == sub)
                    })
            })
            .map(|(delivery, (_, key))| (*delivery, *key))
            .collect();
        tracing::debug!(
            group = %state.name.0,
            released = holding.len(),
            "subscription dropped"
        );
        for (delivery, key) in holding {
            self.held.remove(&delivery);
            self.fail(group, key, Failure::Dropped);
        }
    }

    /// Admit waiting publishes and replays where there is room, then hand
    /// ready entries to waiting subscriptions.
    fn pump(&mut self) {
        for group in 0..self.groups.len() {
            self.admit_waiting(group);
            self.dispatch(group);
        }
    }

    fn admit_waiting(&mut self, group: usize) {
        let capacity = self.config.group_capacity.get();
        while self.groups[group].held_total() < capacity {
            let Some(waiting) = self.groups[group].waiting.pop_front() else {
                break;
            };
            match waiting {
                Waiting::Publish { publish, message } => {
                    self.groups[group].admit(message);
                    if let Some(pending) = self.publishes.get_mut(&publish) {
                        pending.remaining -= 1;
                        if pending.remaining == 0
                            && let Some(pending) = self.publishes.remove(&publish)
                        {
                            let _ = pending.reply.send(Ok(()));
                        }
                    }
                }
                Waiting::Replay { id, reply } => {
                    let result = self.replay_now(group, id);
                    let _ = reply.send(result);
                }
            }
        }
    }

    fn dispatch(&mut self, group: usize) {
        let ack_timeout = self.config.ack_timeout.get();
        let state = &mut self.groups[group];
        while !state.ready.is_empty() {
            let Some((sub, reply)) = state.waiters.pop_front() else {
                break;
            };
            if reply.is_closed() {
                continue;
            }
            let index = self
                .rng
                .as_mut()
                .map_or(0, |rng| rng.below(state.ready.len()));
            let Some(key) = state.ready.remove(index) else {
                break;
            };
            let Some(entry) = state.entries.get_mut(&key) else {
                continue;
            };
            let deliveries = entry.deliveries + 1;
            let delivery = DeliveryId(self.next_delivery);
            self.next_delivery += 1;
            let handout = Handout {
                delivery,
                // `deliveries` is at least one here.
                attempt: NonZeroU32::new(deliveries).unwrap_or(NonZeroU32::MIN),
                message: entry.message.clone(),
            };
            if reply.send(handout).is_err() {
                // The subscription stopped waiting; the entry stays ready.
                state.ready.insert(index, key);
                continue;
            }
            let deadline = Instant::now() + ack_timeout;
            entry.deliveries = deliveries;
            entry.state = EntryState::Held {
                delivery,
                sub,
                deadline,
            };
            self.held.insert(delivery.0, (group, key));
            self.timers
                .push(Reverse((deadline, Timer::AckDeadline(delivery.0))));
            tracing::debug!(
                group = %state.name.0,
                subject = ?entry.message.subject,
                entry = key.0,
                delivery = delivery.0,
                attempt = deliveries,
                "delivered"
            );
        }
    }
}
