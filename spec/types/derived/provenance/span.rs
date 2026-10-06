//! Spans: located runs of text, the unit of provenance.
//!
//! Not an OpenTelemetry span. A span is a byte range inside one part of one
//! message.
//!
//! ```text
//! Extracted ─┬─ classify: absent from inputs ──▶ Originated ─index─▶ Indexed ─hit─▶ Propagated
//!            ├─ classify: present in inputs ───▶ Relayed (final)        │                │
//!            │   (from an input: also indexed, state unchanged)          │                │
//!            └─ classify: otherwise ───────────▶ Common (final)         └─ retention ────┴─▶ Expired
//! ```

use std::num::NonZeroU32;

use serde::{Deserialize, Serialize};

use crate::ids::{AgentId, ExchangeId, MessageHash, SpanId};
use crate::observed::message::PartRef;
use crate::support::{ByteRange, Timestamp};

/// Where a span's text sits. Ordered by part, then range start.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
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
    /// Text copied from the exchange's inputs, or text that matches another
    /// agent's indexed span although no visible input contains it (the copy
    /// came through a channel the gateway cannot see; provenance also emits a
    /// `ReaderOutput` match for it).
    Relayed(RelaySource),
    /// Boilerplate: every one of its fingerprints is above the frequency
    /// cutoff. Never indexed.
    Common,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "type",
    content = "data",
    rename_all = "snake_case",
    deny_unknown_fields
)]
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

/// What can happen to a span.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpanEvent {
    Classify(Origin),
    Index {
        at: Timestamp,
    },
    /// Found in another agent's input or output.
    Hit {
        at: Timestamp,
    },
    Expire {
        at: Timestamp,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IllegalTransition {
    pub from: SpanState,
    pub event: SpanEvent,
}

impl SpanState {
    /// The only way a span's state changes. Rejects every edge not in the
    /// lifecycle above, and hits or expiry timestamped before the span was
    /// indexed.
    pub fn advance(&self, event: SpanEvent) -> Result<SpanState, IllegalTransition> {
        let next = match (self, event) {
            (Self::Extracted, SpanEvent::Classify(Origin::Originated)) => Some(Self::Originated),
            (Self::Extracted, SpanEvent::Classify(Origin::Relayed(source))) => {
                Some(Self::Relayed { source })
            }
            (Self::Extracted, SpanEvent::Classify(Origin::Common)) => Some(Self::Common),
            (Self::Originated, SpanEvent::Index { at }) => Some(Self::Indexed { at }),
            (Self::Indexed { at: indexed_at }, SpanEvent::Hit { at }) if at >= *indexed_at => {
                Some(Self::Propagated {
                    indexed_at: *indexed_at,
                    first_hit_at: at,
                    hits: NonZeroU32::MIN,
                })
            }
            (
                Self::Propagated {
                    indexed_at,
                    first_hit_at,
                    hits,
                },
                SpanEvent::Hit { at },
            ) if at >= *indexed_at => Some(Self::Propagated {
                indexed_at: *indexed_at,
                first_hit_at: *first_hit_at,
                hits: hits.saturating_add(1),
            }),
            (
                Self::Indexed { at: indexed_at } | Self::Propagated { indexed_at, .. },
                SpanEvent::Expire { at },
            ) if at >= *indexed_at => Some(Self::Expired { at }),
            _ => None,
        };
        next.ok_or_else(|| IllegalTransition {
            from: self.clone(),
            event,
        })
    }

    /// Whether the span is forwarded: relayed from one of its agent's
    /// inputs, and so indexed under that agent although its state stays
    /// `Relayed` (see [`OriginatedSpan`]).
    pub fn is_forwarded(&self) -> bool {
        matches!(
            self,
            Self::Relayed {
                source: RelaySource::Input(_)
            }
        )
    }

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

/// A span its agent is the matchable author of: the only kind the
/// fingerprint index accepts.
///
/// - A span classified as originated and not yet expired (`Originated`,
///   `Indexed` or `Propagated`).
/// - A forwarded span, when the provenance configuration turns forwarding
///   on: `Relayed { source: RelaySource::Input(_) }`, text the
///   agent copied from one of its own inputs (a tool result, a user turn)
///   and passed on. It is indexed under the forwarding agent, so a peer's
///   later read of the forwarded text matches it, and its state stays
///   `Relayed` (`provenance.index.forwarded-indexed`). Whether the sender
///   wrote the resource a reader read is L5's question
///   (`flow.route.shared-upstream-stays-suspected`).
///
/// Never a span relayed from another indexed span (that text's author is
/// the source span's agent), a common span, or an expired one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OriginatedSpan(Span);

impl OriginatedSpan {
    /// `None` unless the span is `Originated`, `Indexed`, `Propagated`, or
    /// `Relayed` from an input.
    pub fn new(span: Span) -> Option<Self> {
        match span.state {
            SpanState::Originated
            | SpanState::Indexed { .. }
            | SpanState::Propagated { .. }
            | SpanState::Relayed {
                source: RelaySource::Input(_),
            } => Some(Self(span)),
            SpanState::Extracted
            | SpanState::Common
            | SpanState::Relayed {
                source: RelaySource::Span(_),
            }
            | SpanState::Expired { .. } => None,
        }
    }

    pub fn span(&self) -> &Span {
        &self.0
    }
}
