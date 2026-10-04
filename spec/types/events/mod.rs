//! Events on the bus.
//!
//! Every event crossing a component boundary is a [`BusEvent`] inside an
//! [`Envelope`]. Events inside one process (the proxy handing a finished
//! exchange to its capture task) are not bus events; see
//! [`crate::interfaces::l0_ingress::RawExchange`].
//!
//! Delivery is at least once, so every consumer is idempotent on
//! [`Envelope::id`] and on the entity ids inside the event.
//!
//! Grouped by producer:
//! - [`ingest`]: L1 and L3 (capture, reconstruction).
//! - [`detect`]: L4 and L5 (provenance, flow detection).
//! - [`insight`]: L6 to L8 (analysis, topology, surface).
//! - [`changed`]: any store: which entity a surface query returns changed,
//!   for the live feed.

pub mod changed;
pub mod detect;
pub mod ingest;
pub mod insight;

use crate::ids::EventId;
use crate::support::Timestamp;

#[derive(Debug, Clone, PartialEq)]
pub struct Envelope {
    pub id: EventId,
    pub at: Timestamp,
    pub event: BusEvent,
}

#[derive(Debug, Clone, PartialEq)]
pub enum BusEvent {
    Ingest(ingest::IngestEvent),
    Detect(detect::DetectEvent),
    Insight(insight::InsightEvent),
    Changed(changed::Changed),
}

/// What a consumer subscribes to. One subject per event variant.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Subject {
    ExchangeCaptured,
    ConversationDelta,
    AgentSeen,
    AgentMerged,
    AgentUnmerged,
    SpanOriginated,
    SpanRelayed,
    ContentMatched,
    AccessRecorded,
    ChannelDiscovered,
    ChannelCrossAccessed,
    DeclaredChannelUnused,
    TransmissionConfirmed,
    TransmissionSuspected,
    TransmissionDismissed,
    TransmissionClassified,
    TopicVersionReady,
    TopicVersionActivated,
    EdgeUpdated,
    AlertOpened,
    AlertChanged,
    PolicyChanged,
    Changed,
}

impl BusEvent {
    pub fn subject(&self) -> Subject {
        match self {
            Self::Ingest(e) => e.subject(),
            Self::Detect(e) => e.subject(),
            Self::Insight(e) => e.subject(),
            Self::Changed(e) => e.subject(),
        }
    }
}
