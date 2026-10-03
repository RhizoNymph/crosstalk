//! Spans: located runs of text, the unit of provenance.
//!
//! Not an OpenTelemetry span. A span is a byte range inside one part of one
//! message.
//!
//! ```text
//! Extracted ─┬─ classify: absent from inputs ──▶ Originated ─index─▶ Indexed ─hit─▶ Propagated
//!            ├─ classify: present in inputs ───▶ Relayed (final)        │                │
//!            └─ classify: otherwise ───────────▶ Common (final)         └─ retention ────┴─▶ Expired
//! ```

use std::num::NonZeroU32;

use crate::ids::{AgentId, ExchangeId, MessageHash, SpanId};
use crate::observed::message::PartRef;
use crate::support::{ByteRange, Timestamp};

/// Where a span's text sits.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SpanLocation {
    pub part: PartRef,
    pub range: ByteRange,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Span {
    pub id: SpanId,
    pub location: SpanLocation,
    /// The agent whose output contains the span.
    pub agent: AgentId,
    /// The exchange whose response contains the span.
    pub exchange: ExchangeId,
    pub state: SpanState,
}

/// The classification a segmenter assigns, before any indexing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Origin {
    /// Text that was not in the exchange's inputs: this agent wrote it.
    Originated,
    /// Text copied from the exchange's inputs.
    Relayed(RelaySource),
    /// Boilerplate: every one of its fingerprints is above the frequency
    /// cutoff. Never indexed.
    Common,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RelaySource {
    /// The copied text is itself an indexed span (another agent's, or this
    /// agent's from earlier).
    Span(SpanId),
    /// The copied text came from an input that is not an indexed span, such
    /// as a fetched web page.
    Input(MessageHash),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SpanState {
    Extracted,
    Common,
    Relayed {
        source: RelaySource,
    },
    /// Classified as originated; fingerprints not written yet.
    Originated,
    Indexed {
        at: Timestamp,
    },
    /// Seen in at least one other agent's input.
    Propagated {
        indexed_at: Timestamp,
        first_hit_at: Timestamp,
        hits: NonZeroU32,
    },
    /// Past retention. Its fingerprints have been removed from the index.
    Expired {
        at: Timestamp,
    },
}

impl SpanState {
    pub fn origin(&self) -> Option<Origin> {
        match self {
            Self::Extracted => None,
            Self::Common => Some(Origin::Common),
            Self::Relayed { source } => Some(Origin::Relayed(*source)),
            Self::Originated
            | Self::Indexed { .. }
            | Self::Propagated { .. }
            | Self::Expired { .. } => Some(Origin::Originated),
        }
    }
}
