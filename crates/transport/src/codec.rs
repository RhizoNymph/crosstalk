//! The bus codec: an [`Envelope`] crosses the bus as its wire JSON, even
//! inside one process.
//!
//! The in-process bus encodes on publish and decodes, strictly, on every
//! delivery, so it runs the same decode path as a cluster: an unknown field
//! or variant from a newer publisher, or bytes a foreign publisher wrote, is
//! a [`BusError::Decode`] here exactly as it would be on a NATS consumer.
//!
//! Error reasons never quote the payload
//! (`transport.confidentiality.no-payload-in-logs`). serde_json's messages
//! do (an unknown variant's name, an invalid value), so a reason names only
//! the error's category and position.

use std::sync::Arc;

use crosstalk_spec::events::{Envelope, Subject};
use crosstalk_spec::ids::EventId;
use crosstalk_spec::interfaces::l2_transport::BusError;
use serde_json::error::Category;

/// One message as the bus stores it: the encoded envelope, the subject it
/// is routed under, and its event id when the publisher's envelope is
/// known (for logs). The bytes are shared, read-only, between the groups
/// that hold the message.
#[derive(Debug, Clone)]
pub(crate) struct Message {
    pub(crate) subject: Subject,
    pub(crate) id: Option<EventId>,
    pub(crate) bytes: Arc<[u8]>,
}

/// Encode `envelope` for the bus, routed by its event's own subject
/// (`transport.delivery.subject-filter`).
pub(crate) fn encode(envelope: &Envelope) -> Result<Message, BusError> {
    let bytes = serde_json::to_vec(envelope).map_err(|error| BusError::Encode {
        reason: reason(&error),
    })?;
    Ok(Message {
        subject: envelope.event.subject(),
        id: Some(envelope.id),
        bytes: bytes.into(),
    })
}

/// Decode a delivered message. Strict: the spec types deny unknown fields
/// and variants, and checked types decode through their constructors. An
/// envelope whose event is not of the subject it was routed under is
/// refused too, so a subscription never yields a subject it did not ask
/// for, whatever a foreign publisher claimed.
pub(crate) fn decode(message: &Message) -> Result<Envelope, BusError> {
    let envelope: Envelope =
        serde_json::from_slice(&message.bytes).map_err(|error| BusError::Decode {
            reason: reason(&error),
        })?;
    let subject = envelope.event.subject();
    if subject != message.subject {
        return Err(BusError::Decode {
            reason: format!(
                "event subject {subject:?} differs from the routed subject {:?}",
                message.subject
            ),
        });
    }
    Ok(envelope)
}

/// A payload-free description of a serde_json error.
pub(crate) fn reason(error: &serde_json::Error) -> String {
    let category = match error.classify() {
        Category::Io => "io",
        Category::Syntax => "syntax",
        Category::Data => "data",
        Category::Eof => "eof",
    };
    format!(
        "{category} error at line {} column {}",
        error.line(),
        error.column()
    )
}
