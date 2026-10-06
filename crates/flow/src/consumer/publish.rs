//! Publishing the consumer's own events, each under an envelope id derived
//! from the event (`transport.consumer.derived-envelope-ids`, INV-1202).
//!
//! The consumer is at least once: after a redelivery, a re-fed access or
//! a restore it decides the same things again and publishes the same
//! events. Each event's envelope id is a function of the event alone,
//! stamped with the millisecond of what it is about (ids that are
//! themselves derived from the input):
//!
//! - `AccessRecorded`: the access; `TransmissionConfirmed`,
//!   `TransmissionSuspected`: the transmission. One each: the consumer
//!   decides each once, and a repeat after a restart that saw more (an
//!   access resolved to a channel discovered since, a confirmation that
//!   already holds its later extensions) lands on the id of the first,
//!   which the bus keeps.
//! - `ChannelCrossAccessed`: the co-access's read and a digest of the
//!   event's wire JSON (one per transmission opened on the read).
//!
//! A republished event lands on the id the bus log already holds
//! (`transport.publish.idempotent-on-id`). The envelope's `at` is the
//! injected clock's reading when it is published.

use std::sync::Arc;

use crosstalk_spec::events::detect::DetectEvent;
use crosstalk_spec::events::{BusEvent, Envelope};
use crosstalk_spec::ids::EventId;
use crosstalk_spec::interfaces::l2_transport::{BusError, EventBus};
use crosstalk_spec::support::{Clock, Timestamp};

use crate::correlate::Derive;

/// The bits of a ULID below its millisecond.
const RANDOM_BITS: u32 = 80;

/// Why an event was not published.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum PublishError {
    #[error("encoding the event for its id: {reason}")]
    Encode { reason: String },
    #[error("bus: {0:?}")]
    Bus(BusError),
}

/// The envelope id the consumer publishes `event` under.
pub fn envelope_id(event: &BusEvent, now: Timestamp) -> Result<EventId, PublishError> {
    let once = |label: &[u8], about: u128| {
        let millis = u64::try_from(about >> RANDOM_BITS).unwrap_or(u64::MAX);
        let derived = Derive::new("crosstalk.flow.envelope.v1")
            .bytes(label)
            .ulid(about)
            .at(Timestamp::from_micros(millis.saturating_mul(1_000)));
        EventId::from_ulid(derived)
    };
    let (label, about): (&[u8], Option<u128>) = match event {
        BusEvent::Detect(DetectEvent::AccessRecorded { access, .. }) => {
            return Ok(once(b"access-recorded", access.id.as_ulid()));
        }
        BusEvent::Detect(DetectEvent::TransmissionConfirmed { transmission, .. }) => {
            return Ok(once(b"transmission-confirmed", transmission.as_ulid()));
        }
        BusEvent::Detect(DetectEvent::TransmissionSuspected { transmission, .. }) => {
            return Ok(once(b"transmission-suspected", transmission.as_ulid()));
        }
        BusEvent::Detect(DetectEvent::ChannelCrossAccessed { co_access, .. }) => {
            (b"channel-cross-accessed", Some(co_access.read().as_ulid()))
        }
        // The consumer publishes nothing else; another event is still
        // given an id that is a function of it, stamped now.
        BusEvent::Ingest(_) | BusEvent::Detect(_) | BusEvent::Insight(_) | BusEvent::Changed(_) => {
            (b"event", None)
        }
    };
    let wire = serde_json::to_vec(event).map_err(|error| PublishError::Encode {
        reason: error.to_string(),
    })?;
    let at = about.map_or(now, |ulid| {
        let millis = u64::try_from(ulid >> RANDOM_BITS).unwrap_or(u64::MAX);
        Timestamp::from_micros(millis.saturating_mul(1_000))
    });
    let derived = Derive::new("crosstalk.flow.envelope.v1")
        .bytes(label)
        .ulid(about.unwrap_or_default())
        .bytes(&wire)
        .at(at);
    Ok(EventId::from_ulid(derived))
}

/// The consumer's publishing half.
pub struct Publisher<B> {
    bus: B,
    clock: Arc<dyn Clock>,
}

impl<B: EventBus> Publisher<B> {
    pub fn new(bus: B, clock: Arc<dyn Clock>) -> Self {
        Self { bus, clock }
    }

    pub fn bus(&self) -> &B {
        &self.bus
    }

    pub async fn publish(&mut self, event: BusEvent) -> Result<(), PublishError> {
        let at = self.clock.now();
        let id = envelope_id(&event, at)?;
        let subject = event.subject();
        self.bus
            .publish(Envelope { id, at, event })
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
