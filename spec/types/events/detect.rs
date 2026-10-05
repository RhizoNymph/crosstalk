//! Events from provenance (L4) and flow detection (L5).

use serde::{Deserialize, Serialize};

use crate::derived::flow::access::Access;
use crate::derived::flow::channel::policy::PolicyDecision;
use crate::derived::flow::channel::{Declaration, Seed};
use crate::derived::flow::evidence::CoAccess;
use crate::derived::flow::transmission::Route;
use crate::derived::flow::verdict::{Verdict, VerdictRevision};
use crate::derived::provenance::matching::ContentMatch;
use crate::derived::provenance::span::RelaySource;
use crate::events::Subject;
use crate::ids::{AgentId, ChannelId, OperatorId, SpanId, TransmissionId};
use std::num::NonZeroU64;

use crate::support::{NonEmpty, Timestamp};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "type",
    content = "data",
    rename_all = "snake_case",
    deny_unknown_fields
)]
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
    /// return a superseded one, and `None` for a resource on no channel (a
    /// resource only, until a cross-agent transmission goes through it). L5
    /// correlates it; L7 counts it into its resource's access bucket.
    AccessRecorded {
        access: Access,
        channel: Option<ChannelId>,
    },
    /// A channel no config declared, created by the first cross-agent
    /// transmission through a resource on no channel
    /// (`ChannelTraffic::discover`), never by an access alone. Published by
    /// the registry once per discovered channel, from the transaction that
    /// creates it. Raises `NewChannel`.
    ChannelDiscovered {
        channel: ChannelId,
        seed: Seed,
    },
    /// Written by one agent, then read by another. Opens a transmission.
    ChannelCrossAccessed {
        channel: ChannelId,
        co_access: CoAccess,
        reader: AgentId,
    },
    /// A channel declared before traffic saw no cross-agent transmission
    /// within its idle window.
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
    /// An operator's verdict on a transmission was appended to its
    /// `VerdictLog` at `revision` (`verdict` is `None` for a withdrawal).
    /// Published once per appended record by L5's verdict store, in the
    /// record's transaction; a request that appended nothing publishes
    /// nothing. Readers keep the highest revision per transmission
    /// (`CurrentVerdict::observe`). Alert triage suppresses the
    /// transmission's active alerts on `FalseDetection`; the edge store,
    /// search and the projection read it for
    /// `TopologyFilter::false_detections`.
    VerdictSet {
        transmission: TransmissionId,
        verdict: Option<Verdict>,
        revision: VerdictRevision,
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
            Self::VerdictSet { .. } => Subject::VerdictSet,
        }
    }
}
