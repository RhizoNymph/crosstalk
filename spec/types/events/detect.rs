//! Events from provenance (L4) and flow detection (L5).

use crate::derived::flow::access::Access;
use crate::derived::flow::evidence::CoAccess;
use crate::derived::flow::transmission::Route;
use crate::derived::provenance::matching::ContentMatch;
use crate::derived::provenance::span::RelaySource;
use crate::events::Subject;
use crate::ids::{AccessId, AgentId, ChannelId, SpanId, TransmissionId};
use std::num::NonZeroU64;

use crate::support::{NonEmpty, Timestamp};

#[derive(Debug, Clone, PartialEq)]
pub enum DetectEvent {
    SpanOriginated {
        span: SpanId,
        agent: AgentId,
    },
    SpanRelayed {
        span: SpanId,
        source: RelaySource,
    },
    ContentMatched(ContentMatch),
    AccessRecorded(Access),
    /// A channel no config declared. Raises `NewChannel`.
    ChannelDiscovered {
        channel: ChannelId,
        first_access: AccessId,
    },
    /// Written by one agent, then read by another. Opens a transmission.
    ChannelCrossAccessed {
        channel: ChannelId,
        co_access: CoAccess,
        reader: AgentId,
    },
    DeclaredChannelUnused {
        channel: ChannelId,
        since: Timestamp,
    },
    TransmissionConfirmed {
        transmission: TransmissionId,
        from: AgentId,
        to: AgentId,
        route: Route,
        at: Timestamp,
        matched_bytes: NonZeroU64,
    },
    TransmissionSuspected {
        transmission: TransmissionId,
        to: AgentId,
        channel: ChannelId,
        co_access: NonEmpty<CoAccess>,
    },
}

impl DetectEvent {
    pub fn subject(&self) -> Subject {
        match self {
            Self::SpanOriginated { .. } => Subject::SpanOriginated,
            Self::SpanRelayed { .. } => Subject::SpanRelayed,
            Self::ContentMatched(_) => Subject::ContentMatched,
            Self::AccessRecorded(_) => Subject::AccessRecorded,
            Self::ChannelDiscovered { .. } => Subject::ChannelDiscovered,
            Self::ChannelCrossAccessed { .. } => Subject::ChannelCrossAccessed,
            Self::DeclaredChannelUnused { .. } => Subject::DeclaredChannelUnused,
            Self::TransmissionConfirmed { .. } => Subject::TransmissionConfirmed,
            Self::TransmissionSuspected { .. } => Subject::TransmissionSuspected,
        }
    }
}
