//! The two hops between the stores and the bus.
//!
//! ```text
//! stores ── Outbox ──▶ forward_outbox ── Publisher ──▶ bus
//! bus ── group live-surface-relay ──▶ SurfaceRelay ──▶ InProcess relay (node facts, live feed)
//! ```
//!
//! So a store-decided event (`ChannelDiscovered`, `Changed::*`) reaches
//! every consumer on the bus, and the surface sees what the layer
//! consumers publish as well as what the stores do.

use crosstalk_spec::events::{BusEvent, Envelope, Subject};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio::sync::oneshot;

use super::stage::{Activity, DERIVED_SUBJECTS, Publisher, Stage, StageError};

/// A request to forward everything already in the outbox now, answered
/// with how many events that was.
#[derive(Debug)]
pub(crate) struct Flush(pub(crate) oneshot::Sender<u64>);

/// Publish every event the stores put in their outbox, in order, stamped
/// with the clock's reading; on a [`Flush`], forward what is queued first.
/// Ends when every outbox handle is dropped.
pub(crate) async fn forward_outbox(
    mut outbox: UnboundedReceiver<BusEvent>,
    publisher: Publisher,
    mut flushes: UnboundedReceiver<Flush>,
    activity: Activity,
) {
    loop {
        tokio::select! {
            biased;
            Some(Flush(done)) = flushes.recv() => {
                let mut forwarded = 0;
                while let Ok(event) = outbox.try_recv() {
                    forward(&publisher, event).await;
                    forwarded += 1;
                }
                activity.add(forwarded);
                let _ = done.send(forwarded);
            }
            event = outbox.recv() => match event {
                Some(event) => {
                    forward(&publisher, event).await;
                    activity.bump();
                }
                None => break,
            },
        }
    }
    tracing::debug!("outbox closed; forwarding stopped");
}

async fn forward(publisher: &Publisher, event: BusEvent) {
    let subject = event.subject();
    if let Err(error) = publisher.publish(event).await {
        tracing::warn!(subject = ?subject, error = %error, "outbox event not forwarded to the bus");
    }
}

/// The surface relay's stage: hands every derived event to the in-process
/// surface's relay.
pub(crate) struct SurfaceRelay {
    events: UnboundedSender<BusEvent>,
}

impl SurfaceRelay {
    pub(crate) fn new(events: UnboundedSender<BusEvent>) -> Self {
        Self { events }
    }
}

impl Stage for SurfaceRelay {
    fn subjects(&self) -> Vec<Subject> {
        DERIVED_SUBJECTS.to_vec()
    }

    async fn handle(&mut self, envelope: &Envelope) -> Result<(), StageError> {
        self.events
            .send(envelope.event.clone())
            .map_err(|_| StageError::Reject {
                reason: "the surface's relay has stopped".to_owned(),
            })
    }
}
