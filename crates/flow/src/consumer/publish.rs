//! Publishing the consumer's own events: each stamped into an `Envelope`
//! with an id minted on the injected clock.

use std::sync::Arc;

use crosstalk_spec::events::{BusEvent, Envelope};
use crosstalk_spec::ids::{EventId, SeededRandom, UlidExhausted, UlidGenerator};
use crosstalk_spec::interfaces::l2_transport::{BusError, EventBus};
use crosstalk_spec::support::Clock;

/// Why an event was not published.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum PublishError {
    #[error("minting an event id: {0}")]
    Ids(UlidExhausted),
    #[error("bus: {0:?}")]
    Bus(BusError),
}

/// The consumer's publishing half.
pub struct Publisher<B> {
    bus: B,
    clock: Arc<dyn Clock>,
    ids: UlidGenerator<SeededRandom>,
}

impl<B: EventBus> Publisher<B> {
    pub fn new(bus: B, clock: Arc<dyn Clock>, entropy: SeededRandom) -> Self {
        Self {
            ids: UlidGenerator::new(Arc::clone(&clock), entropy),
            bus,
            clock,
        }
    }

    pub fn bus(&self) -> &B {
        &self.bus
    }

    pub async fn publish(&mut self, event: BusEvent) -> Result<(), PublishError> {
        let id: EventId = self.ids.mint().map_err(PublishError::Ids)?;
        let subject = event.subject();
        let envelope = Envelope {
            id,
            at: self.clock.now(),
            event,
        };
        self.bus
            .publish(envelope)
            .await
            .map_err(PublishError::Bus)?;
        tracing::debug!(event = %id.ulid_text(), ?subject, "flow event published");
        Ok(())
    }
}

impl<B> std::fmt::Debug for Publisher<B> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Publisher").finish_non_exhaustive()
    }
}
