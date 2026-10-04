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

use serde::{Deserialize, Serialize};

use crate::ids::EventId;
use crate::support::Timestamp;

/// A bus payload between nodes, never a request: the node stamps its id and
/// time. On the wire, `{"id": .., "at": .., "event": {"type": "detect",
/// "data": {"type": "content_matched", "data": {..}}}}`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct Envelope {
    pub id: EventId,
    pub at: Timestamp,
    pub event: BusEvent,
}

/// Every event on the bus, by producing layer. On the wire, two levels of
/// adjacent tagging: the layer (`ingest`, `detect`, `insight`, `changed`),
/// then the event, whose tag is its [`Subject`]'s string for every layer
/// event (`{"type": "detect", "data": {"type": "content_matched", ..}}`);
/// a change notification's inner tag is the entity kind (`{"type":
/// "changed", "data": {"type": "channel", "data": ".."}}`), and its subject
/// is `changed`. A bus payload, never a request: its events carry the
/// operators and times the producing node stamped.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "type",
    content = "data",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum BusEvent {
    Ingest(ingest::IngestEvent),
    Detect(detect::DetectEvent),
    Insight(insight::InsightEvent),
    Changed(changed::Changed),
}

/// What a consumer subscribes to. One subject per event variant. On the
/// wire, a string: the variant in snake_case (`"content_matched"`), the
/// same text as the event's tag inside its layer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Subject {
    ExchangeCaptured,
    ConversationDelta,
    AgentSeen,
    AgentMerged,
    AgentUnmerged,
    AgentRenamed,
    SpanOriginated,
    SpanRelayed,
    ContentMatched,
    AccessRecorded,
    ChannelDiscovered,
    ChannelCrossAccessed,
    DeclaredChannelUnused,
    ChannelPromoted,
    TransmissionConfirmed,
    TransmissionSuspected,
    VerdictSet,
    TransmissionClassified,
    TopicVersionReady,
    TopicVersionActivated,
    TopicVersionDropped,
    WatermarkAdvanced,
    EdgeUpdated,
    AlertOpened,
    AlertChanged,
    AlertRuleChanged,
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
