//! Where a store publishes the events its trait documents.
//!
//! A database store writes its events to an outbox table in the
//! transaction that makes the change and relays them to the bus after
//! commit. The in-memory stores send them on a channel, never before the
//! change is visible: the L3 to L5 stores send right after their critical
//! section, the L6 to L8 stores from inside it (so events of concurrent
//! writes arrive in commit order). A receiver that re-queries the store on
//! an event always sees the change. The wiring (or a test) owns the
//! receiving end and stamps each event into an `Envelope` for the bus.

use crosstalk_spec::events::BusEvent;
use crosstalk_spec::events::changed::Changed;
use crosstalk_spec::events::insight::InsightEvent;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};

/// The sending half of a store's outbox. [`Outbox::none`] discards every
/// event, for stores whose events nobody reads.
#[derive(Debug, Clone, Default)]
pub struct Outbox {
    sender: Option<UnboundedSender<BusEvent>>,
}

impl Outbox {
    /// An outbox and the receiver its events arrive on, in publish order.
    pub fn channel() -> (Self, UnboundedReceiver<BusEvent>) {
        let (sender, receiver) = unbounded_channel();
        (
            Self {
                sender: Some(sender),
            },
            receiver,
        )
    }

    /// An outbox that drops every event.
    pub fn none() -> Self {
        Self { sender: None }
    }

    /// Publish `events` in order. A closed receiver means nobody listens
    /// any more; the events are dropped and the store carries on, as a
    /// relay would retry later.
    pub fn publish(&self, events: impl IntoIterator<Item = BusEvent>) {
        let Some(sender) = &self.sender else {
            return;
        };
        for event in events {
            if sender.send(event).is_err() {
                tracing::debug!(reason = "receiver closed", "outbox event dropped");
                return;
            }
        }
    }

    /// Publish one insight event.
    pub fn insight(&self, event: InsightEvent) {
        self.publish([BusEvent::Insight(event)]);
    }

    /// Publish one live-feed notification.
    pub fn changed(&self, changed: Changed) {
        self.publish([BusEvent::Changed(changed)]);
    }
}

/// Every event waiting on `receiver`, without waiting for more.
pub fn drain(receiver: &mut UnboundedReceiver<BusEvent>) -> Vec<BusEvent> {
    let mut events = Vec::new();
    while let Ok(event) = receiver.try_recv() {
        events.push(event);
    }
    events
}
