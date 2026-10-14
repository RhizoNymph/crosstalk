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
//!
//! [`Pumped::next`] keeps its pull's reply channel in the `Pumped`, not in
//! its future: a stage loop that drops a `next` future (a command won its
//! `select!`) after the pump answered would otherwise drop the delivery
//! with the channel, held and never acked until the ack timeout, and a
//! settle that keeps sending drain commands could lose it again on every
//! redelivery (the restart e2e's settles hung on `live-surface-relay`
//! holding one delivery). The next `next` call awaits the same reply.

use std::time::Duration;

use crosstalk_spec::interfaces::l2_transport::{BusError, Delivery, DeliveryId, Subscription};
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
    /// The reply to the last pull, until a `next` call took it.
    pending: Option<oneshot::Receiver<Item>>,
}

impl Drop for Pumped {
    fn drop(&mut self) {
        // The task ends on its own once the requests channel closes; the
        // abort covers a fetch that never returns.
        self.task.abort();
    }
}

/// Serve `subscription` from a pump task. Generic so the pump's
/// cancellation behaviour is tested over a scripted subscription; the
/// Postgres set pumps `PgSubscription`s.
pub fn pump<S: Subscription + Send + 'static>(subscription: S) -> Pumped {
    let (requests, received) = mpsc::unbounded_channel();
    let task = tokio::spawn(run(subscription, received));
    Pumped {
        requests,
        task,
        pending: None,
    }
}

/// Apply one settle request; a pull is returned to the caller.
async fn settle<S: Subscription>(
    subscription: &mut S,
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

async fn run<S: Subscription>(mut subscription: S, mut requests: mpsc::UnboundedReceiver<Request>) {
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
    /// Cancel-safe: dropped before its answer came, the pull stays
    /// pending in `self`, and the next call returns its answer.
    async fn next(&mut self) -> Option<Result<Delivery, BusError>> {
        if self.pending.is_none() {
            let (reply, answer) = oneshot::channel();
            self.requests.send(Request::Pull(reply)).ok()?;
            self.pending = Some(answer);
        }
        let answer = self.pending.as_mut()?.await;
        self.pending = None;
        answer.ok().flatten()
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

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::num::NonZeroU32;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU32, Ordering};

    use crosstalk_spec::events::changed::Changed;
    use crosstalk_spec::events::{BusEvent, Envelope};
    use crosstalk_spec::ids::{AgentId, EventId};
    use crosstalk_spec::support::Timestamp;

    use super::*;

    /// A subscription that hands out its scripted deliveries once each,
    /// then waits forever (a bus with nothing new), and counts handouts
    /// and acks.
    struct Scripted {
        deliveries: VecDeque<Delivery>,
        handed: Arc<AtomicU32>,
        acked: Arc<AtomicU32>,
    }

    impl Subscription for Scripted {
        async fn next(&mut self) -> Option<Result<Delivery, BusError>> {
            match self.deliveries.pop_front() {
                Some(delivery) => {
                    self.handed.fetch_add(1, Ordering::SeqCst);
                    Some(Ok(delivery))
                }
                None => std::future::pending().await,
            }
        }

        async fn ack(&mut self, _id: DeliveryId) -> Result<(), BusError> {
            self.acked.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        async fn nack(
            &mut self,
            _id: DeliveryId,
            _retry_after: Duration,
            _reason: String,
        ) -> Result<(), BusError> {
            Ok(())
        }
    }

    fn delivery() -> Delivery {
        Delivery {
            id: DeliveryId(7),
            attempt: NonZeroU32::MIN,
            envelope: Envelope {
                id: EventId::from_ulid(0xE7),
                at: Timestamp::from_micros(1),
                event: BusEvent::Changed(Changed::Agent(AgentId::from_ulid(1))),
            },
        }
    }

    /// `gateway.stage-pump.no-dropped-delivery`: a stage loop's `next`
    /// future dropped after the pump answered it (a command won the
    /// loop's `select!` on the next poll) loses nothing. The next `next`
    /// returns that very delivery, at its attempt, at once: no ack
    /// timeout, no redelivery, and nothing fetched twice.
    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn a_next_dropped_after_its_answer_keeps_the_delivery() {
        let handed = Arc::new(AtomicU32::new(0));
        let acked = Arc::new(AtomicU32::new(0));
        let mut pumped = pump(Scripted {
            deliveries: VecDeque::from([delivery()]),
            handed: Arc::clone(&handed),
            acked: Arc::clone(&acked),
        });

        // The stage loop polls `next` once: the pull goes out.
        {
            let mut next = std::pin::pin!(pumped.next());
            let polled = std::future::poll_fn(|cx| {
                std::task::Poll::Ready(next.as_mut().poll(cx).is_ready())
            })
            .await;
            assert!(!polled, "the pump has not answered yet");
            // The pump answers: on this one thread it runs while we yield,
            // fetching and sending in one poll.
            while handed.load(Ordering::SeqCst) == 0 {
                tokio::task::yield_now().await;
            }
            tokio::task::yield_now().await;
            // A command wins the loop's next poll: the future is dropped
            // with the answer already in its reply channel.
        }

        let again = tokio::time::timeout(Duration::from_secs(1), pumped.next())
            .await
            .expect("the dropped answer is returned at once, not lost to an ack timeout")
            .expect("open")
            .expect("a delivery");
        assert_eq!(again, delivery(), "the same delivery, at the same attempt");
        assert_eq!(handed.load(Ordering::SeqCst), 1, "fetched once");
        pumped.ack(again.id).await.expect("acks");
        assert_eq!(acked.load(Ordering::SeqCst), 1);
    }
}
