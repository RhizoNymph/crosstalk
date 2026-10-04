//! Envelope-level deduplication for consumers: [`Dedup`] wraps a
//! [`Subscription`] so consumer logic sees each [`Envelope::id`] at most
//! once per group after it handled it.
//!
//! Delivery is at least once: a lost ack, an ack timeout, or a publisher
//! that republishes the same envelope can all hand an envelope over again.
//! The wrapper keeps a per-group record of handled ids ([`HandledIds`]):
//!
//! - `ack` writes the id to the record, then acks
//!   (`transport.dedup.at-most-once`); the record is written only on the
//!   success path, so a failed handling records nothing and its redelivery
//!   reaches consumer logic (`transport.dedup.suppress-only-handled`);
//! - `next` acks and skips a delivery whose id is in the record, so a
//!   suppressed duplicate does not come back (`transport.dedup.duplicate-acked`).
//!
//! A crash between handling and the record can still hand an envelope over
//! again, which is why consumers stay idempotent on entity ids too.
//!
//! [`Envelope::id`]: crosstalk_spec::events::Envelope

use std::collections::HashMap;
use std::time::Duration;

use crosstalk_spec::ids::EventId;
use crosstalk_spec::interfaces::l2_transport::{
    BusError, ConsumerGroup, Delivery, DeliveryId, Subscription,
};

/// The handled-id record of every consumer group: shared by every consumer
/// of a group, and surviving consumer restarts.
pub trait HandledIds {
    /// Whether consumer logic of `group` handled an envelope with `id`.
    fn contains(
        &self,
        group: &ConsumerGroup,
        id: EventId,
    ) -> impl Future<Output = Result<bool, BusError>> + Send;

    /// Record that consumer logic of `group` handled `id`. `Ok` means the
    /// record is written.
    fn record(
        &self,
        group: &ConsumerGroup,
        id: EventId,
    ) -> impl Future<Output = Result<(), BusError>> + Send;
}

/// A [`Subscription`] that withholds envelopes its group already handled.
///
/// Consumer logic calls `ack` only after it handled the delivery
/// successfully, and `nack` otherwise.
///
/// `next` is cancel-safe when the inner subscription's is: a delivery it
/// took from the inner subscription is kept on the wrapper until it is
/// handed over or withheld, so a dropped `next` resumes with it.
#[derive(Debug)]
pub struct Dedup<S, H> {
    inner: S,
    group: ConsumerGroup,
    handled: H,
    /// The envelope id of each delivery handed to consumer logic and not yet
    /// acked or nacked.
    open: HashMap<DeliveryId, EventId>,
    /// A step of `next` a cancelled call left unfinished.
    pending: Option<Step>,
}

/// Where `next` stands with the delivery it took from the inner
/// subscription.
#[derive(Debug)]
enum Step {
    /// Checking the record for its id.
    Check(Box<Delivery>),
    /// A withheld duplicate, being acked.
    AckDuplicate(DeliveryId),
}

impl<S, H> Dedup<S, H> {
    pub fn new(inner: S, group: ConsumerGroup, handled: H) -> Self {
        Self {
            inner,
            group,
            handled,
            open: HashMap::new(),
            pending: None,
        }
    }

    pub fn group(&self) -> &ConsumerGroup {
        &self.group
    }

    pub fn into_inner(self) -> S {
        self.inner
    }
}

impl<S, H> Subscription for Dedup<S, H>
where
    S: Subscription + Send,
    H: HandledIds + Sync + Send,
{
    async fn next(&mut self) -> Option<Result<Delivery, BusError>> {
        loop {
            let step = match self.pending.take() {
                Some(step) => step,
                None => match self.inner.next().await? {
                    Ok(delivery) => Step::Check(Box::new(delivery)),
                    Err(error) => return Some(Err(error)),
                },
            };
            // Kept on `self` across every await below, so a cancelled call
            // loses nothing.
            let step = self.pending.insert(step);
            match step {
                Step::Check(delivery) => {
                    let id = delivery.envelope.id;
                    match self.handled.contains(&self.group, id).await {
                        Ok(false) => {
                            // Always the check just made.
                            if let Some(Step::Check(delivery)) = self.pending.take() {
                                self.open.insert(delivery.id, id);
                                return Some(Ok(*delivery));
                            }
                        }
                        Ok(true) => {
                            tracing::debug!(
                                group = %self.group.0,
                                event = %id.ulid_text(),
                                delivery = delivery.id.0,
                                "duplicate withheld"
                            );
                            self.pending = Some(Step::AckDuplicate(delivery.id));
                        }
                        // The record is unreadable: hand nothing over. The
                        // delivery comes back after its ack timeout.
                        Err(error) => {
                            self.pending = None;
                            return Some(Err(error));
                        }
                    }
                }
                Step::AckDuplicate(delivery) => {
                    let delivery = *delivery;
                    let acked = self.inner.ack(delivery).await;
                    self.pending = None;
                    match acked {
                        // Taken back already (an ack timeout): its next
                        // delivery is withheld the same way.
                        Ok(()) | Err(BusError::UnknownDelivery(_)) => {}
                        Err(error) => return Some(Err(error)),
                    }
                }
            }
        }
    }

    async fn ack(&mut self, id: DeliveryId) -> Result<(), BusError> {
        let Some(&event) = self.open.get(&id) else {
            return self.inner.ack(id).await;
        };
        self.handled.record(&self.group, event).await?;
        self.open.remove(&id);
        self.inner.ack(id).await
    }

    async fn nack(
        &mut self,
        id: DeliveryId,
        retry_after: Duration,
        reason: String,
    ) -> Result<(), BusError> {
        self.open.remove(&id);
        self.inner.nack(id, retry_after, reason).await
    }
}
