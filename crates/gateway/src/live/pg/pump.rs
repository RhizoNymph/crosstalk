//! [`Pumped`]: a `PgSubscription` behind a task, so a stage loop can race
//! its deliveries against its commands without cancelling a take.
//!
//! `PgSubscription::next` is not fully cancel-safe: a call dropped while
//! its take commits leaves the delivery held until the reaper's
//! provisional deadline (twice the ack timeout plus the publish timeout).
//! A stage loop `select!`s `next` against its commands, so every command
//! would risk that. The pump owns the subscription and fetches only when
//! the stage asks (`next` sends a pull); a stage's `next` future dropped
//! early only loses its reply channel, and the pump keeps the delivery for
//! the next pull. Acks and nacks go through the pump too.
//!
//! ```text
//! stage ── Pull(reply) ──▶ pump ── PgSubscription::next ──▶ reply (or kept for the next pull)
//! stage ── Ack / Nack ───▶ pump ── PgSubscription::ack / nack ──▶ reply
//! ```
//!
//! The one remaining cancellation: an ack or nack that arrives while the
//! pump is fetching for an earlier pull (the durable flow consumer acks at
//! its checkpoint while it waits for deliveries) stops that fetch to apply
//! it. A stage that acks each delivery before it pulls the next never
//! does that.

use std::time::Duration;

use crosstalk_spec::interfaces::l2_transport::{BusError, Delivery, DeliveryId, Subscription};
use crosstalk_transport::PgSubscription;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;

type Item = Option<Result<Delivery, BusError>>;

enum Request {
    Pull(oneshot::Sender<Item>),
    Ack(DeliveryId, oneshot::Sender<Result<(), BusError>>),
    Nack(
        DeliveryId,
        Duration,
        String,
        oneshot::Sender<Result<(), BusError>>,
    ),
}

/// A subscription served by a pump task. Dropping it stops the task,
/// which drops the `PgSubscription` (its held deliveries go back to the
/// group).
#[derive(Debug)]
pub struct Pumped {
    requests: mpsc::UnboundedSender<Request>,
    task: JoinHandle<()>,
}

impl Drop for Pumped {
    fn drop(&mut self) {
        // The task ends on its own once the requests channel closes; the
        // abort covers a fetch that never returns.
        self.task.abort();
    }
}

/// Serve `subscription` from a pump task.
pub fn pump(subscription: PgSubscription) -> Pumped {
    let (requests, received) = mpsc::unbounded_channel();
    let task = tokio::spawn(run(subscription, received));
    Pumped { requests, task }
}

/// Apply one settle request; a pull is returned to the caller.
async fn settle(
    subscription: &mut PgSubscription,
    request: Request,
) -> Option<oneshot::Sender<Item>> {
    match request {
        Request::Pull(reply) => Some(reply),
        Request::Ack(id, reply) => {
            let _ = reply.send(subscription.ack(id).await);
            None
        }
        Request::Nack(id, delay, reason, reply) => {
            let _ = reply.send(subscription.nack(id, delay, reason).await);
            None
        }
    }
}

async fn run(mut subscription: PgSubscription, mut requests: mpsc::UnboundedReceiver<Request>) {
    // A delivery fetched for a pull whose caller had gone.
    let mut kept: Option<Item> = None;
    while let Some(request) = requests.recv().await {
        let Some(mut reply) = settle(&mut subscription, request).await else {
            continue;
        };
        let item = match kept.take() {
            Some(item) => item,
            None => loop {
                // Pulls arriving while fetching only move the reply; an ack
                // or nack stops the fetch (see the module docs).
                let interrupt = {
                    let fetch = subscription.next();
                    tokio::pin!(fetch);
                    loop {
                        tokio::select! {
                            biased;
                            request = requests.recv() => match request {
                                None => return,
                                Some(Request::Pull(newer)) => reply = newer,
                                Some(other) => break Err(other),
                            },
                            item = &mut fetch => break Ok(item),
                        }
                    }
                };
                match interrupt {
                    Ok(item) => break item,
                    Err(request) => {
                        if let Some(newer) = settle(&mut subscription, request).await {
                            reply = newer;
                        }
                    }
                }
            },
        };
        let closed = item.is_none();
        if let Err(unsent) = reply.send(item)
            && !closed
        {
            kept = Some(unsent);
        }
        if closed {
            return;
        }
    }
}

impl Subscription for Pumped {
    async fn next(&mut self) -> Option<Result<Delivery, BusError>> {
        let (reply, answer) = oneshot::channel();
        self.requests.send(Request::Pull(reply)).ok()?;
        answer.await.ok().flatten()
    }

    async fn ack(&mut self, id: DeliveryId) -> Result<(), BusError> {
        let (reply, answer) = oneshot::channel();
        self.requests
            .send(Request::Ack(id, reply))
            .map_err(|_| BusError::Disconnected)?;
        answer.await.map_err(|_| BusError::Disconnected)?
    }

    async fn nack(
        &mut self,
        id: DeliveryId,
        retry_after: Duration,
        reason: String,
    ) -> Result<(), BusError> {
        let (reply, answer) = oneshot::channel();
        self.requests
            .send(Request::Nack(id, retry_after, reason, reply))
            .map_err(|_| BusError::Disconnected)?;
        answer.await.map_err(|_| BusError::Disconnected)?
    }
}
