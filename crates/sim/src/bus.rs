//! [`FaultyBus`]: wraps any spec [`EventBus`] and injects the bus faults of
//! a [`BusFaults`] plan, per subject.
//!
//! Where each fault is injected:
//!
//! | Fault | Where | Effect |
//! | --- | --- | --- |
//! | duplicate | `publish`, after the inner publish returned `Ok` | the envelope is published again (same `Envelope::id`) |
//! | crash on publish | `publish`, before the inner publish | the node crashes; the envelope never reaches the bus |
//! | reorder | `next` | deliveries are pulled into a window and handed out in random order |
//! | drop | `next`, after pulling | the consumer never sees the delivery; it is nacked or left to the ack timeout, and the bus redelivers it |
//! | delay | `next`, before handing out | the delivery reaches the consumer late |
//! | crash before ack | `ack` | the node crashes; the ack never reaches the bus |
//!
//! Every fault stays within what the real bus may do: at-least-once
//! delivery, any order within a consumer group, and redelivery after a lost
//! delivery or a lost ack.
//!
//! **Cancel safety.** `FaultySubscription::next` is cancel-safe when the
//! inner subscription's `next` is: a delivery pulled from the inner bus is
//! kept in the wrapper until handed out, including across a delay. The
//! reorder window relies on this, since it pulls under a timeout.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use crosstalk_spec::events::{Envelope, Subject};
use crosstalk_spec::interfaces::l2_transport::{
    BusError, ConsumerGroup, Delivery, DeliveryId, EventBus, RetryPolicy, Subscription,
};
use tokio::time::Instant;

use crate::node::NodeHandle;
use crate::plan::{BusFaults, Redelivery, SubjectFaults};
use crate::rng::SimRng;
use crate::trace::{FaultKind, FaultSite};

/// The reason a dropped delivery is nacked with.
pub const DROPPED_DELIVERY_REASON: &str = "sim: delivery dropped in transit";

/// An [`EventBus`] that injects faults into another. Clones share the
/// inner bus, the plan and the random stream. Build one per node with
/// [`SimCtx::faulty_bus`](crate::SimCtx::faulty_bus), all over the same
/// inner bus, so crashes take down the right node.
#[derive(Debug)]
pub struct FaultyBus<B> {
    inner: Arc<B>,
    faults: Arc<BusFaults>,
    rng: Arc<Mutex<SimRng>>,
    node: NodeHandle,
}

impl<B> Clone for FaultyBus<B> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
            faults: Arc::clone(&self.faults),
            rng: Arc::clone(&self.rng),
            node: self.node.clone(),
        }
    }
}

impl<B> FaultyBus<B> {
    pub fn new(inner: Arc<B>, faults: BusFaults, rng: SimRng, node: NodeHandle) -> Self {
        Self {
            inner,
            faults: Arc::new(faults),
            rng: Arc::new(Mutex::new(rng)),
            node,
        }
    }

    /// The wrapped bus, for calls that should see no faults.
    pub fn inner(&self) -> &Arc<B> {
        &self.inner
    }

    /// Draws the publish-side faults for one envelope.
    fn draw_publish(&self, faults: &SubjectFaults) -> (bool, bool) {
        let mut rng = self.rng.lock().unwrap_or_else(PoisonError::into_inner);
        let crash = rng.chance(faults.crash_on_publish);
        let duplicate = rng.chance(faults.duplicate);
        (crash, duplicate)
    }

    fn fork_rng(&self) -> SimRng {
        self.rng
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .fork()
    }
}

fn bus_site(envelope: &Envelope) -> FaultSite {
    FaultSite::Bus {
        subject: envelope.event.subject(),
        event: envelope.id,
    }
}

impl<B> EventBus for FaultyBus<B>
where
    B: EventBus + Send + Sync + 'static,
{
    type Subscription = FaultySubscription<B::Subscription>;

    async fn publish(&self, envelope: Envelope) -> Result<(), BusError> {
        let faults = self.faults.for_subject(envelope.event.subject());
        let (crash, duplicate) = self.draw_publish(faults);
        if crash {
            match self
                .node
                .crash(FaultKind::BusCrashOnPublish, bus_site(&envelope))
                .await {}
        }
        let copy = duplicate.then(|| envelope.clone());
        self.inner.publish(envelope).await?;
        if let Some(copy) = copy {
            self.node.fault(FaultKind::BusDuplicate, bus_site(&copy));
            self.inner.publish(copy).await?;
        }
        Ok(())
    }

    async fn subscribe(
        &self,
        subjects: &[Subject],
        group: ConsumerGroup,
        retry: RetryPolicy,
    ) -> Result<Self::Subscription, BusError> {
        let inner = self.inner.subscribe(subjects, group, retry).await?;
        Ok(FaultySubscription {
            inner,
            faults: Arc::clone(&self.faults),
            rng: self.fork_rng(),
            node: self.node.clone(),
            window: Vec::new(),
            delayed: None,
            closed: false,
            outstanding: HashMap::new(),
        })
    }
}

/// A subscription that injects reorder, drop, delay and crash-before-ack.
/// Owns its random stream, forked from its bus's when it subscribed.
#[derive(Debug)]
pub struct FaultySubscription<S> {
    inner: S,
    faults: Arc<BusFaults>,
    rng: SimRng,
    node: NodeHandle,
    /// Deliveries pulled from the inner bus and not yet handed out.
    window: Vec<Delivery>,
    /// A delivery held back until its deadline.
    delayed: Option<(Delivery, Instant)>,
    /// The inner subscription reported shutdown.
    closed: bool,
    /// Handed-out deliveries not yet acked or nacked, with where they
    /// would crash.
    outstanding: HashMap<DeliveryId, FaultSite>,
}

impl<S: Subscription + Send> FaultySubscription<S> {
    /// The wrapped subscription.
    pub fn inner(&self) -> &S {
        &self.inner
    }

    /// The next delivery from the window, refilled from the inner bus; `None`
    /// once the inner bus shut down and the window is empty.
    async fn pull(&mut self) -> Option<Result<Delivery, BusError>> {
        if self.window.is_empty() {
            if self.closed {
                return None;
            }
            match self.inner.next().await {
                None => {
                    self.closed = true;
                    return None;
                }
                Some(Err(error)) => return Some(Err(error)),
                Some(Ok(delivery)) => self.window.push(delivery),
            }
        }
        self.widen().await;
        let index = self.rng.index(self.window.len()).unwrap_or(0);
        let delivery = self.window.remove(index);
        if index != 0 {
            self.node
                .fault(FaultKind::BusReorder, bus_site(&delivery.envelope));
        }
        Some(Ok(delivery))
    }

    /// Pulls more deliveries into the window while the newest one's
    /// reorder fault fires and more arrive within its wait.
    async fn widen(&mut self) {
        while !self.closed {
            let Some(newest) = self.window.last() else {
                return;
            };
            let subject = newest.envelope.event.subject();
            let Some(reorder) = self.faults.for_subject(subject).reorder else {
                return;
            };
            if self.window.len() >= reorder.window().get() || !self.rng.chance(reorder.chance()) {
                return;
            }
            match tokio::time::timeout(reorder.wait(), self.inner.next()).await {
                Err(_elapsed) => return,
                Ok(None) => self.closed = true,
                // Hand out what the window holds; the error resurfaces on
                // a later pull if it persists.
                Ok(Some(Err(error))) => {
                    tracing::debug!(error = ?error, "sim reorder window stopped by a bus error");
                    return;
                }
                Ok(Some(Ok(delivery))) => self.window.push(delivery),
            }
        }
    }

    /// Applies drop and delay to a pulled delivery. `None` when dropped.
    async fn deliver(&mut self, delivery: Delivery) -> Option<Result<Delivery, BusError>> {
        let subject = delivery.envelope.event.subject();
        let faults = self.faults.for_subject(subject).clone();
        if let Some(drop) = faults.drop
            && self.rng.chance(drop.chance)
        {
            self.node
                .fault(FaultKind::BusDrop, bus_site(&delivery.envelope));
            if let Redelivery::Nack { after } = drop.redelivery {
                let retry_after = self.rng.duration_in(after);
                if let Err(error) = self
                    .inner
                    .nack(delivery.id, retry_after, DROPPED_DELIVERY_REASON.to_owned())
                    .await
                {
                    return Some(Err(error));
                }
            }
            return None;
        }
        if let Some(delay) = faults.delay
            && self.rng.chance(delay.chance)
        {
            let wait: Duration = self.rng.duration_in(delay.within);
            self.node
                .fault(FaultKind::BusDelay, bus_site(&delivery.envelope));
            self.delayed = Some((delivery, Instant::now() + wait));
            return self.release_delayed().await.map(Ok);
        }
        self.outstanding
            .insert(delivery.id, bus_site(&delivery.envelope));
        Some(Ok(delivery))
    }

    /// Waits out the held-back delivery, if any, and hands it out.
    async fn release_delayed(&mut self) -> Option<Delivery> {
        let deadline = self.delayed.as_ref().map(|(_, deadline)| *deadline)?;
        tokio::time::sleep_until(deadline).await;
        let (delivery, _) = self.delayed.take()?;
        self.outstanding
            .insert(delivery.id, bus_site(&delivery.envelope));
        Some(delivery)
    }
}

impl<S> Subscription for FaultySubscription<S>
where
    S: Subscription + Send,
{
    async fn next(&mut self) -> Option<Result<Delivery, BusError>> {
        if let Some(delivery) = self.release_delayed().await {
            return Some(Ok(delivery));
        }
        loop {
            let delivery = match self.pull().await? {
                Ok(delivery) => delivery,
                Err(error) => return Some(Err(error)),
            };
            if let Some(outcome) = self.deliver(delivery).await {
                return Some(outcome);
            }
        }
    }

    async fn ack(&mut self, id: DeliveryId) -> Result<(), BusError> {
        if let Some(site) = self.outstanding.remove(&id)
            && let FaultSite::Bus { subject, .. } = site
            && self
                .rng
                .chance(self.faults.for_subject(subject).crash_before_ack)
        {
            match self.node.crash(FaultKind::BusCrashBeforeAck, site).await {}
        }
        self.inner.ack(id).await
    }

    async fn nack(
        &mut self,
        id: DeliveryId,
        retry_after: Duration,
        reason: String,
    ) -> Result<(), BusError> {
        self.outstanding.remove(&id);
        self.inner.nack(id, retry_after, reason).await
    }
}
