//! Events from provenance (L4) and flow detection (L5).

use crate::derived::flow::access::Access;
use crate::derived::flow::channel::Declaration;
use crate::derived::flow::channel::policy::PolicyDecision;
use crate::derived::flow::evidence::CoAccess;
use crate::derived::flow::transmission::Route;
use crate::derived::provenance::matching::ContentMatch;
use crate::derived::provenance::span::RelaySource;
use crate::events::Subject;
use crate::ids::{AccessId, AgentId, ChannelId, OperatorId, SpanId, TransmissionId};
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
    /// An access, with the channel the registry resolved its locator to when
    /// it was recorded: always a canonical channel, since lookups never
    /// return a superseded one. L5 correlates it; L7 counts it into its
    /// access bucket.
    AccessRecorded {
        access: Access,
        channel: ChannelId,
    },
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
    /// An operator promoted a discovered channel (`ChannelRegistry::promote`):
    /// it keeps its id, gains `declaration`, records `policy` in its policy
    /// history, and supersedes every channel in `superseded` as of
    /// `declaration.at`. Published once per accepted promotion, after the
    /// transaction commits. Alert triage treats `policy` like a
    /// `PolicyChanged` on `channel` (suppressing on `Sanctioned`, which
    /// covers alerts on the superseded channels); the live feed reports a
    /// channel change for `channel` and each superseded channel; readers
    /// that cache the channel directory point each superseded channel at
    /// `channel`.
    ChannelPromoted {
        channel: ChannelId,
        declaration: Declaration,
        policy: PolicyDecision,
        superseded: Vec<ChannelId>,
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
    /// An operator dismissed a suspected transmission; it is now
    /// `Discarded` with reason `Dismissed`. Alert triage suppresses its
    /// `SuspectedTransmission` alerts.
    TransmissionDismissed {
        transmission: TransmissionId,
        by: OperatorId,
        at: Timestamp,
    },
}

impl DetectEvent {
    pub fn subject(&self) -> Subject {
        match self {
            Self::SpanOriginated { .. } => Subject::SpanOriginated,
            Self::SpanRelayed { .. } => Subject::SpanRelayed,
            Self::ContentMatched(_) => Subject::ContentMatched,
            Self::AccessRecorded { .. } => Subject::AccessRecorded,
            Self::ChannelDiscovered { .. } => Subject::ChannelDiscovered,
            Self::ChannelCrossAccessed { .. } => Subject::ChannelCrossAccessed,
            Self::DeclaredChannelUnused { .. } => Subject::DeclaredChannelUnused,
            Self::ChannelPromoted { .. } => Subject::ChannelPromoted,
            Self::TransmissionConfirmed { .. } => Subject::TransmissionConfirmed,
            Self::TransmissionSuspected { .. } => Subject::TransmissionSuspected,
            Self::TransmissionDismissed { .. } => Subject::TransmissionDismissed,
        }
    }
}
