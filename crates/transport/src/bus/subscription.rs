//! [`MpscSubscription`]: one consumer's handle on its group.

use std::time::Duration;

use crosstalk_spec::interfaces::l2_transport::{
    BusError, ConsumerGroup, Delivery, DeliveryId, Subscription,
};
use tokio::sync::{mpsc, oneshot};

use super::command::{Command, Handout, SubId};
use crate::codec;

/// A subscription to an [`MpscBus`](crate::MpscBus) consumer group.
///
/// `next` waits for a delivery and decodes it. Dropping the subscription
/// is a consumer crash: every delivery it holds is taken back and
/// redelivered after its backoff, or dead-lettered on its last attempt.
///
/// `next` is cancel-safe: dropping its future before it completes loses no
/// delivery. A cancelled call leaves its request with the bus, and the
/// following call returns the delivery the bus granted it, on the same
/// attempt; an undecodable delivery whose termination was cut short is
/// terminated and reported by the following call. Wrappers that pull from
/// `next` under a timeout (the simulation kit's faulty subscription) rely
/// on this.
///
/// The subscription keeps only a weak handle on the bus: once every
/// [`MpscBus`](crate::MpscBus) handle is dropped (or the bus is shut down),
/// `next` returns `None` and acks fail with `Disconnected`.
#[derive(Debug)]
pub struct MpscSubscription {
    sub: SubId,
    group: ConsumerGroup,
    commands: mpsc::WeakSender<Command>,
    drops: mpsc::UnboundedSender<SubId>,
    pending: Option<oneshot::Receiver<Handout>>,
    /// An undecodable delivery whose termination a cancelled `next` did not
    /// finish, with the error to report.
    terminating: Option<(DeliveryId, BusError)>,
}

impl MpscSubscription {
    pub(crate) fn new(
        sub: SubId,
        group: ConsumerGroup,
        commands: mpsc::WeakSender<Command>,
        drops: mpsc::UnboundedSender<SubId>,
    ) -> Self {
        Self {
            sub,
            group,
            commands,
            drops,
            pending: None,
            terminating: None,
        }
    }

    pub fn group(&self) -> &ConsumerGroup {
        &self.group
    }

    async fn send(&self, command: Command) -> Result<(), BusError> {
        let commands = self.commands.upgrade().ok_or(BusError::Disconnected)?;
        commands
            .send(command)
            .await
            .map_err(|_| BusError::Disconnected)
    }

    async fn request<T>(
        &self,
        command: impl FnOnce(oneshot::Sender<Result<T, BusError>>) -> Command,
    ) -> Result<T, BusError> {
        let (reply, response) = oneshot::channel();
        self.send(command(reply)).await?;
        response.await.map_err(|_| BusError::Disconnected)?
    }

    /// Decode a handout; an undecodable one is terminated with the bus and
    /// reported once.
    async fn open(&mut self, handout: Handout) -> Result<Delivery, BusError> {
        match codec::decode(&handout.message) {
            Ok(envelope) => Ok(Delivery {
                id: handout.delivery,
                attempt: handout.attempt,
                envelope,
            }),
            Err(error) => {
                self.terminating = Some((handout.delivery, error));
                self.terminate().await
            }
        }
    }

    /// Finish terminating the undecodable delivery in `self.terminating`
    /// and return its error. Repeating it after a cancellation is harmless:
    /// the bus ignores a delivery it no longer tracks.
    async fn terminate(&mut self) -> Result<Delivery, BusError> {
        let Some((delivery, error)) = &self.terminating else {
            return Err(BusError::Disconnected);
        };
        let reason = match error {
            BusError::Decode { reason } => reason.clone(),
            other => format!("{other:?}"),
        };
        let (reply, done) = oneshot::channel();
        let terminate = Command::Terminate {
            sub: self.sub,
            delivery: *delivery,
            reason,
            reply,
        };
        if self.send(terminate).await.is_ok() {
            // An error here means the bus stopped; the message goes with it.
            let _ = done.await;
        }
        match self.terminating.take() {
            Some((_, error)) => Err(error),
            None => Err(BusError::Disconnected),
        }
    }
}

impl Subscription for MpscSubscription {
    async fn next(&mut self) -> Option<Result<Delivery, BusError>> {
        if self.terminating.is_some() {
            return Some(self.terminate().await);
        }
        let pending = match self.pending.take() {
            Some(pending) => pending,
            None => {
                let (reply, pending) = oneshot::channel();
                let next = Command::Next {
                    sub: self.sub,
                    reply,
                };
                self.send(next).await.ok()?;
                pending
            }
        };
        // Keep the receiver across a cancellation of this call: the bus
        // may already have granted it a delivery.
        let pending = self.pending.insert(pending);
        let handout = pending.await;
        self.pending = None;
        // A closed reply means the bus task stopped.
        let handout = handout.ok()?;
        Some(self.open(handout).await)
    }

    async fn ack(&mut self, id: DeliveryId) -> Result<(), BusError> {
        let sub = self.sub;
        self.request(|reply| Command::Ack {
            sub,
            delivery: id,
            reply,
        })
        .await
    }

    async fn nack(
        &mut self,
        id: DeliveryId,
        retry_after: Duration,
        reason: String,
    ) -> Result<(), BusError> {
        let sub = self.sub;
        self.request(|reply| Command::Nack {
            sub,
            delivery: id,
            retry_after,
            reason,
            reply,
        })
        .await
    }
}

impl Drop for MpscSubscription {
    fn drop(&mut self) {
        // Fails only when the bus task has stopped, and then there is
        // nothing left to release.
        let _ = self.drops.send(self.sub);
    }
}
