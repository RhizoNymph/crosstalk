//! A toy in-memory [`EventBus`] for the kit's own tests: consumer groups,
//! ack, nack with `retry_after`, and an ack timeout. No dead letters, no
//! backpressure, no group checks; the real bus is crosstalk-transport's.

use std::collections::{HashMap, VecDeque};
use std::num::NonZeroU32;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use crosstalk_spec::events::changed::Changed;
use crosstalk_spec::events::insight::InsightEvent;
use crosstalk_spec::events::{BusEvent, Envelope, Subject};
use crosstalk_spec::ids::{AgentId, EventId};
use crosstalk_spec::interfaces::l2_transport::{
    BusError, ConsumerGroup, Delivery, DeliveryId, EventBus, RetryPolicy, Subscription,
};
use crosstalk_spec::support::{Timestamp, Watermark};
use tokio::sync::Notify;

#[derive(Debug, Clone)]
struct Pending {
    envelope: Envelope,
    attempt: NonZeroU32,
}

#[derive(Debug)]
struct Group {
    name: ConsumerGroup,
    subjects: Vec<Subject>,
    ready: VecDeque<Pending>,
    held: HashMap<DeliveryId, Pending>,
}

#[derive(Debug, Default)]
struct State {
    groups: Vec<Group>,
    next_delivery: u64,
}

#[derive(Debug)]
struct Shared {
    state: Mutex<State>,
    notify: Notify,
    ack_timeout: Duration,
}

impl Shared {
    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Puts a held delivery back with its attempt raised, if it is still
    /// held under `id`.
    fn requeue(&self, group: usize, id: DeliveryId) {
        let mut state = self.lock();
        if let Some(group) = state.groups.get_mut(group)
            && let Some(mut pending) = group.held.remove(&id)
        {
            pending.attempt = pending.attempt.saturating_add(1);
            group.ready.push_back(pending);
        }
        drop(state);
        self.notify.notify_waiters();
    }
}

#[derive(Debug, Clone)]
pub(super) struct ToyBus {
    shared: Arc<Shared>,
}

impl ToyBus {
    pub(super) fn new(ack_timeout: Duration) -> Self {
        Self {
            shared: Arc::new(Shared {
                state: Mutex::new(State::default()),
                notify: Notify::new(),
                ack_timeout,
            }),
        }
    }

    /// Envelopes not yet acked, over every group.
    pub(super) fn unacked(&self) -> usize {
        let state = self.shared.lock();
        state
            .groups
            .iter()
            .map(|group| group.ready.len() + group.held.len())
            .sum()
    }
}

impl EventBus for ToyBus {
    type Subscription = ToySubscription;

    async fn publish(&self, envelope: Envelope) -> Result<(), BusError> {
        let subject = envelope.event.subject();
        let mut state = self.shared.lock();
        for group in &mut state.groups {
            if group.subjects.contains(&subject) {
                group.ready.push_back(Pending {
                    envelope: envelope.clone(),
                    attempt: NonZeroU32::MIN,
                });
            }
        }
        drop(state);
        self.shared.notify.notify_waiters();
        Ok(())
    }

    async fn subscribe(
        &self,
        subjects: &[Subject],
        group: ConsumerGroup,
        _retry: RetryPolicy,
    ) -> Result<ToySubscription, BusError> {
        let mut state = self.shared.lock();
        let index = match state.groups.iter().position(|g| g.name == group) {
            Some(index) => index,
            None => {
                state.groups.push(Group {
                    name: group,
                    subjects: subjects.to_vec(),
                    ready: VecDeque::new(),
                    held: HashMap::new(),
                });
                state.groups.len() - 1
            }
        };
        Ok(ToySubscription {
            shared: Arc::clone(&self.shared),
            group: index,
        })
    }
}

#[derive(Debug)]
pub(super) struct ToySubscription {
    shared: Arc<Shared>,
    group: usize,
}

impl ToySubscription {
    fn try_take(&self) -> Option<Delivery> {
        let mut state = self.shared.lock();
        let id = DeliveryId(state.next_delivery);
        let group = state.groups.get_mut(self.group)?;
        let pending = group.ready.pop_front()?;
        group.held.insert(id, pending.clone());
        state.next_delivery += 1;
        drop(state);
        let shared = Arc::clone(&self.shared);
        let group = self.group;
        tokio::spawn(async move {
            tokio::time::sleep(shared.ack_timeout).await;
            shared.requeue(group, id);
        });
        Some(Delivery {
            id,
            attempt: pending.attempt,
            envelope: pending.envelope,
        })
    }
}

impl Subscription for ToySubscription {
    async fn next(&mut self) -> Option<Result<Delivery, BusError>> {
        loop {
            let notified = self.shared.notify.notified();
            let mut notified = std::pin::pin!(notified);
            notified.as_mut().enable();
            if let Some(delivery) = self.try_take() {
                return Some(Ok(delivery));
            }
            notified.await;
        }
    }

    async fn ack(&mut self, id: DeliveryId) -> Result<(), BusError> {
        let mut state = self.shared.lock();
        let held = state
            .groups
            .get_mut(self.group)
            .and_then(|group| group.held.remove(&id));
        held.map(|_| ()).ok_or(BusError::UnknownDelivery(id))
    }

    async fn nack(
        &mut self,
        id: DeliveryId,
        retry_after: Duration,
        _reason: String,
    ) -> Result<(), BusError> {
        let held = {
            let state = self.shared.lock();
            state
                .groups
                .get(self.group)
                .is_some_and(|group| group.held.contains_key(&id))
        };
        if !held {
            return Err(BusError::UnknownDelivery(id));
        }
        let shared = Arc::clone(&self.shared);
        let group = self.group;
        tokio::spawn(async move {
            tokio::time::sleep(retry_after).await;
            shared.requeue(group, id);
        });
        Ok(())
    }
}

/// A `Changed` envelope (subject `changed`) with id `n`.
pub(super) fn changed(n: u64) -> Envelope {
    Envelope {
        id: EventId::from_ulid(u128::from(n)),
        at: Timestamp::from_micros(n),
        event: BusEvent::Changed(Changed::Agent(AgentId::from_ulid(u128::from(n)))),
    }
}

/// A `WatermarkAdvanced` envelope (subject `watermark_advanced`) with id
/// `n`.
pub(super) fn watermark(n: u64) -> Envelope {
    Envelope {
        id: EventId::from_ulid(u128::from(n)),
        at: Timestamp::from_micros(n),
        event: BusEvent::Insight(InsightEvent::WatermarkAdvanced(Watermark(
            Timestamp::from_micros(n),
        ))),
    }
}

pub(super) fn retry() -> RetryPolicy {
    RetryPolicy::new(
        NonZeroU32::MIN.saturating_add(9),
        Duration::from_millis(10),
        Duration::from_secs(1),
    )
    .expect("a valid retry policy")
}

pub(super) fn group(name: &str) -> ConsumerGroup {
    ConsumerGroup(name.to_owned())
}

/// The ULID of an envelope's id, for compact notes.
pub(super) fn n(envelope: &Envelope) -> u128 {
    envelope.id.as_ulid()
}
