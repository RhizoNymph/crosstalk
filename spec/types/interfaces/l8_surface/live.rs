//! The live feed: what the UI receives over SSE without polling.
//!
//! ```text
//! bus ─(group `live`)─▶ feed writer ─append─▶ feed log: epoch E, seq 1, 2, 3, …
//!                       (bus event → LiveUpdate        │ (kept for `LiveConfig::retention`)
//!                        + its ScopeKeys)              ▼ fan-out, one bounded buffer per stream
//!                                  stream task: kinds ∩ caller permissions, TopologyFilter
//!                                                      ▼
//!                                       SSE: `id: <LiveCursor>`, `data: <LiveItem>`
//! ```
//!
//! **Sources.** The feed writer turns bus events into [`LiveUpdate`]s
//! ([`LiveUpdateKind::sources`]) and appends each to the feed log, which
//! numbers entries consecutively within one [`FeedEpoch`]. A new epoch
//! starts whenever the log is recreated (a fresh deployment, a wiped
//! store), so cursors from an older log are recognised, never misread.
//!
//! | Update | Bus subject(s) | Permission |
//! | --- | --- | --- |
//! | `AlertOpened` | `AlertOpened` | View |
//! | `AlertChanged` | `AlertChanged` | View |
//! | `EdgeUpdated` | `EdgeUpdated` | View |
//! | `ChannelDiscovered` | `ChannelDiscovered` | View |
//! | `ChannelChanged` | `ChannelCrossAccessed`, `DeclaredChannelUnused`, channel-routed `TransmissionConfirmed` | View |
//! | `PolicyChanged` | `PolicyChanged` | View |
//! | `TransmissionConfirmed` | `TransmissionConfirmed` | Content |
//! | `TopicVersionActivated` | `TopicVersionActivated` | View |
//!
//! Updates are facts or invalidations, never derived snapshots that could
//! be stale: an alert carries its [`AlertRevision`] (keep the highest), a
//! policy update carries the decision (keep the latest by decision time, as
//! `PolicyHistory` does), and edge and channel updates say what changed so
//! the client refetches through `QueryApi`. A channel turning dormant is
//! clock-driven and has no bus event, so it is not pushed.
//!
//! **Permissions.** Each kind needs one permission
//! ([`LiveUpdateKind::required_permission`]): `Content` for
//! `TransmissionConfirmed`, `View` for the rest, the same permission the
//! `QueryApi` endpoint serving that data needs. `subscribe` returns
//! `Forbidden` if the caller lacks any requested kind's permission. A stream
//! ends with [`LiveEnd::SessionEnded`] when the caller's session expires or
//! is revoked.
//!
//! **Filter.** The subscription's `TopologyFilter` is applied on the server
//! to each entry's [`LiveScope`], with the same meaning it has for a graph
//! query ([`LiveScope::admitted_by`]). Agent ids on both sides are resolved
//! through the `AgentDirectory` when matched, and channel ids through the
//! `ChannelDirectory`. Topic ids name topics of the
//! topic-model version active when the stream started; when another version
//! is activated, a stream whose filter names topics ends with
//! [`LiveEnd::TopicVersionChanged`] so the client remaps its topics.
//!
//! **Resumption.** Every item carries a [`LiveCursor`], sent as the SSE
//! event id. A client reconnects with its last one (`Last-Event-ID`), and
//! [`FeedWindow::resume`] decides: replay every retained entry after it,
//! then continue live; or, if the cursor is too old, from another epoch,
//! ahead of the log or unreadable, send [`LiveItem::Resync`] first. Resync
//! means: refetch everything through `QueryApi`, then apply the updates
//! that follow. Nothing is lost silently. Heartbeats carry the newest
//! cursor the stream has passed, so a client whose filter admits little
//! still holds a recent cursor.
//!
//! **Backpressure.** Each stream has a buffer of `LiveConfig::buffer` items.
//! The feed writer and other streams never wait for it: when it is full,
//! the stream ends with [`LiveEnd::Lagged`] and the client reconnects with
//! its last cursor, replaying from the log (or resyncing if it fell out of
//! retention).

use std::num::{NonZeroU32, NonZeroU64};
use std::time::Duration;

use crate::aggregates::alert::{Alert, AlertRevision};
use crate::aggregates::edge::{EdgeKey, RouteKind, TopologyFilter};
use crate::aggregates::topic::TopicModelVersion;
use crate::derived::flow::channel::policy::PolicyDecision;
use crate::derived::flow::evidence::CoAccess;
use crate::derived::flow::transmission::Route;
use crate::events::Subject;
use crate::ids::{AccessId, AgentId, ChannelId, TopicId, TransmissionId};
use crate::interfaces::l8_surface::{Caller, Permission, QueryError};
use crate::support::Timestamp;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum LiveUpdateKind {
    AlertOpened,
    AlertChanged,
    EdgeUpdated,
    ChannelDiscovered,
    ChannelChanged,
    PolicyChanged,
    TransmissionConfirmed,
    TopicVersionActivated,
}

impl LiveUpdateKind {
    pub const ALL: [Self; 8] = [
        Self::AlertOpened,
        Self::AlertChanged,
        Self::EdgeUpdated,
        Self::ChannelDiscovered,
        Self::ChannelChanged,
        Self::PolicyChanged,
        Self::TransmissionConfirmed,
        Self::TopicVersionActivated,
    ];

    /// The permission a caller needs to receive this kind: the one the
    /// `QueryApi` endpoint serving the same data needs.
    pub fn required_permission(self) -> Permission {
        match self {
            Self::TransmissionConfirmed => Permission::Content,
            Self::AlertOpened
            | Self::AlertChanged
            | Self::EdgeUpdated
            | Self::ChannelDiscovered
            | Self::ChannelChanged
            | Self::PolicyChanged
            | Self::TopicVersionActivated => Permission::View,
        }
    }

    /// The bus subjects the feed writer turns into this kind.
    pub fn sources(self) -> &'static [Subject] {
        match self {
            Self::AlertOpened => &[Subject::AlertOpened],
            Self::AlertChanged => &[Subject::AlertChanged],
            Self::EdgeUpdated => &[Subject::EdgeUpdated],
            Self::ChannelDiscovered => &[Subject::ChannelDiscovered],
            Self::ChannelChanged => &[
                Subject::ChannelCrossAccessed,
                Subject::DeclaredChannelUnused,
                Subject::TransmissionConfirmed,
            ],
            Self::PolicyChanged => &[Subject::PolicyChanged],
            Self::TransmissionConfirmed => &[Subject::TransmissionConfirmed],
            Self::TopicVersionActivated => &[Subject::TopicVersionActivated],
        }
    }

    const fn bit(self) -> u16 {
        1 << self as u16
    }
}

/// The kinds a stream receives. Built only through [`UpdateKinds::new`] or
/// [`UpdateKinds::all`], so it is never empty.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct UpdateKinds(u16);

/// A subscription asked for no kinds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NoUpdateKinds;

impl UpdateKinds {
    pub fn new(kinds: impl IntoIterator<Item = LiveUpdateKind>) -> Result<Self, NoUpdateKinds> {
        let bits = kinds.into_iter().fold(0, |bits, kind| bits | kind.bit());
        if bits == 0 {
            Err(NoUpdateKinds)
        } else {
            Ok(Self(bits))
        }
    }

    pub fn all() -> Self {
        Self(
            LiveUpdateKind::ALL
                .iter()
                .fold(0, |bits, kind| bits | kind.bit()),
        )
    }

    pub fn contains(self, kind: LiveUpdateKind) -> bool {
        self.0 & kind.bit() != 0
    }

    pub fn iter(self) -> impl Iterator<Item = LiveUpdateKind> {
        LiveUpdateKind::ALL
            .into_iter()
            .filter(move |kind| self.contains(*kind))
    }

    /// The first permission a requested kind needs that `caller` lacks.
    /// `subscribe` answers `Forbidden` when there is one.
    pub fn missing_permission(self, caller: &Caller) -> Option<Permission> {
        self.iter()
            .map(LiveUpdateKind::required_permission)
            .find(|permission| !caller.has(*permission))
    }
}

/// What a channel update says changed. The client refetches the channel.
#[derive(Debug, Clone, PartialEq)]
pub enum ChannelChange {
    /// Written by one agent and read by another (`ChannelCrossAccessed`).
    CrossAccessed {
        co_access: CoAccess,
        reader: AgentId,
    },
    /// A declared channel saw no traffic in its idle window
    /// (`DeclaredChannelUnused`).
    Unused { since: Timestamp },
    /// A transmission routed through the channel was confirmed. Carries
    /// only what `QueryApi::channel` shows (its detection's last
    /// transmission), not the transmission itself.
    TrafficConfirmed {
        transmission: TransmissionId,
        at: Timestamp,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub enum LiveUpdate {
    /// The alert as opened, at revision 1.
    AlertOpened(Alert),
    /// The alert after a change. Keep the highest revision per alert.
    AlertChanged {
        alert: Alert,
        revision: AlertRevision,
    },
    /// Counts in this bucket changed; refetch the topology for windows
    /// that cover it. Agent ids are as attributed, before alias resolution.
    EdgeUpdated(EdgeKey),
    ChannelDiscovered {
        channel: ChannelId,
        first_access: AccessId,
    },
    ChannelChanged {
        channel: ChannelId,
        change: ChannelChange,
    },
    /// A decision recorded in the channel's policy history. The channel's
    /// policy is the latest decision by decision time, which may be an
    /// earlier update if this one arrived late.
    PolicyChanged {
        channel: ChannelId,
        decision: PolicyDecision,
    },
    TransmissionConfirmed {
        transmission: TransmissionId,
        from: AgentId,
        to: AgentId,
        route: Route,
        at: Timestamp,
        matched_bytes: NonZeroU64,
    },
    /// Topology queries now answer under `version`; refetch the graph.
    TopicVersionActivated { version: TopicModelVersion },
}

impl LiveUpdate {
    pub fn kind(&self) -> LiveUpdateKind {
        match self {
            Self::AlertOpened(_) => LiveUpdateKind::AlertOpened,
            Self::AlertChanged { .. } => LiveUpdateKind::AlertChanged,
            Self::EdgeUpdated(_) => LiveUpdateKind::EdgeUpdated,
            Self::ChannelDiscovered { .. } => LiveUpdateKind::ChannelDiscovered,
            Self::ChannelChanged { .. } => LiveUpdateKind::ChannelChanged,
            Self::PolicyChanged { .. } => LiveUpdateKind::PolicyChanged,
            Self::TransmissionConfirmed { .. } => LiveUpdateKind::TransmissionConfirmed,
            Self::TopicVersionActivated { .. } => LiveUpdateKind::TopicVersionActivated,
        }
    }
}

/// What an update is about, for matching a `TopologyFilter`. The feed
/// writer computes it once per entry, looking up what the event does not
/// carry (a transmission alert's agents, route and topic; a cross access's
/// writer).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LiveScope {
    /// About the whole graph (`TopicVersionActivated`). Every filter admits
    /// it.
    Global,
    Scoped(ScopeKeys),
}

/// The graph dimensions one update touches. A dimension the update does not
/// have is empty or `None`, and a filter restricting that dimension does not
/// admit it.
///
/// | Update | agents | channel | route | topic |
/// | --- | --- | --- | --- | --- |
/// | alert on a channel | — | the channel | `Channel` | — |
/// | alert on a transmission | sender (once known), reader | if channel-routed | its route | its topic |
/// | alert on an agent | the agent | — | — | — |
/// | `EdgeUpdated` | sender, reader | if channel-routed | its route | its topic |
/// | `ChannelDiscovered`, `PolicyChanged` | — | the channel | `Channel` | — |
/// | `ChannelChanged` | writer and reader, or sender and reader | the channel | `Channel` | — |
/// | `TransmissionConfirmed` | sender, reader | if channel-routed | its route | — (classified later) |
///
/// A topic counts only under the topic-model version the stream started
/// with; a key from another version is `None`.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ScopeKeys {
    pub agents: Vec<AgentId>,
    pub channel: Option<ChannelId>,
    pub route: Option<RouteKind>,
    pub topic: Option<TopicId>,
}

impl ScopeKeys {
    /// Keys of something that travelled `route` between `agents`.
    pub fn routed(agents: Vec<AgentId>, route: &Route, topic: Option<TopicId>) -> Self {
        let (kind, channel) = match route {
            Route::Channel(channel) => (RouteKind::Channel, Some(*channel)),
            Route::Delegation(_) => (RouteKind::Delegation, None),
            Route::Direct(_) => (RouteKind::Direct, None),
            Route::Unobserved => (RouteKind::Unobserved, None),
        };
        Self {
            agents,
            channel,
            route: Some(kind),
            topic,
        }
    }

    /// Keys of an update about a channel itself.
    pub fn channel(channel: ChannelId, agents: Vec<AgentId>) -> Self {
        Self {
            agents,
            channel: Some(channel),
            route: Some(RouteKind::Channel),
            topic: None,
        }
    }
}

impl LiveScope {
    /// Whether a stream with `filter` receives the update. Agent ids on both
    /// sides must already be resolved through merge aliases, and channel ids
    /// through supersession ([`crate::aliases`]).
    pub fn admitted_by(&self, filter: &TopologyFilter) -> bool {
        let Self::Scoped(keys) = self else {
            return true;
        };
        let agents = filter.agents.is_empty()
            || keys
                .agents
                .iter()
                .any(|agent| filter.agents.contains(agent));
        let channels = filter.channels.is_empty()
            || keys
                .channel
                .is_some_and(|channel| filter.channels.contains(&channel));
        let routes = filter.route_kinds.is_empty()
            || keys
                .route
                .is_some_and(|route| filter.route_kinds.contains(&route));
        let topics = filter.topics.is_empty()
            || keys
                .topic
                .is_some_and(|topic| filter.topics.contains(&topic));
        agents && channels && routes && topics
    }
}

/// One incarnation of the feed log.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct FeedEpoch(pub u64);

/// A position in the feed log: the SSE event id. `seq` 0 is before the
/// first entry. Cursors of different epochs are not comparable, so this
/// type has no ordering.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct LiveCursor {
    pub epoch: FeedEpoch,
    pub seq: u64,
}

impl LiveCursor {
    /// `<epoch>-<seq>`, both in decimal.
    pub fn encode(self) -> String {
        format!("{}-{}", self.epoch.0, self.seq)
    }

    /// Reads what [`LiveCursor::encode`] wrote. `None` for anything else.
    pub fn decode(text: &str) -> Option<Self> {
        let (epoch, seq) = text.split_once('-')?;
        Some(Self {
            epoch: FeedEpoch(decimal(epoch)?),
            seq: decimal(seq)?,
        })
    }
}

fn decimal(text: &str) -> Option<u64> {
    if text.is_empty() || !text.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    text.parse().ok()
}

/// Where a subscription starts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Resume {
    /// No `Last-Event-ID`: start with the next entry.
    Fresh,
    From(LiveCursor),
    /// A `Last-Event-ID` that is not a cursor.
    Unreadable,
}

impl Resume {
    pub fn from_last_event_id(header: Option<&str>) -> Self {
        match header {
            None => Self::Fresh,
            Some(text) => LiveCursor::decode(text).map_or(Self::Unreadable, Self::From),
        }
    }
}

/// The span of the feed log a stream can replay from.
///
/// Built only through [`FeedWindow::new`]: entries `floor + 1 ..= head` are
/// retained, so `floor` is never above `head`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FeedWindow {
    epoch: FeedEpoch,
    floor: u64,
    head: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FloorAboveHead;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResumePlan {
    /// Start with the entry after the head.
    Live,
    /// Replay every retained entry after `after`, then continue live.
    Replay { after: u64 },
    /// Send `LiveItem::Resync` at the head, then continue live.
    Resync(ResyncReason),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResyncReason {
    /// Entries after the cursor have left retention.
    Expired,
    /// The cursor is from another incarnation of the log.
    OtherEpoch,
    /// The cursor is past the head of this log.
    AheadOfHead,
    /// The `Last-Event-ID` was not a cursor.
    Unreadable,
}

impl FeedWindow {
    /// `floor` is the last entry dropped by retention (0 if none) and `head`
    /// the last entry appended (0 if none).
    pub fn new(epoch: FeedEpoch, floor: u64, head: u64) -> Result<Self, FloorAboveHead> {
        if floor > head {
            return Err(FloorAboveHead);
        }
        Ok(Self { epoch, floor, head })
    }

    /// The cursor of the newest entry: where `Live` and `Resync` start.
    pub fn head(self) -> LiveCursor {
        LiveCursor {
            epoch: self.epoch,
            seq: self.head,
        }
    }

    /// How a subscription with `resume` starts. A cursor can be replayed
    /// from when every entry after it is still retained.
    pub fn resume(self, resume: Resume) -> ResumePlan {
        match resume {
            Resume::Fresh => ResumePlan::Live,
            Resume::Unreadable => ResumePlan::Resync(ResyncReason::Unreadable),
            Resume::From(cursor) if cursor.epoch != self.epoch => {
                ResumePlan::Resync(ResyncReason::OtherEpoch)
            }
            Resume::From(cursor) if cursor.seq > self.head => {
                ResumePlan::Resync(ResyncReason::AheadOfHead)
            }
            Resume::From(cursor) if cursor.seq < self.floor => {
                ResumePlan::Resync(ResyncReason::Expired)
            }
            Resume::From(cursor) => ResumePlan::Replay { after: cursor.seq },
        }
    }
}

/// One SSE event. Its cursor is the event id.
#[derive(Debug, Clone, PartialEq)]
pub enum LiveItem {
    Update {
        cursor: LiveCursor,
        update: LiveUpdate,
    },
    /// Refetch through `QueryApi`, then apply the updates that follow.
    Resync {
        cursor: LiveCursor,
        reason: ResyncReason,
    },
    /// Sent at least once per `LiveConfig::heartbeat`. Its cursor is the
    /// newest entry the stream has passed, delivered or filtered out.
    Heartbeat { cursor: LiveCursor },
}

impl LiveItem {
    pub fn cursor(&self) -> LiveCursor {
        match self {
            Self::Update { cursor, .. }
            | Self::Resync { cursor, .. }
            | Self::Heartbeat { cursor } => *cursor,
        }
    }
}

/// Why a stream ended. After any of these, the client reconnects with its
/// last cursor (after re-authenticating, or remapping its topics).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LiveEnd {
    /// The stream's buffer filled: the client read too slowly.
    Lagged,
    /// The caller's session expired or was revoked.
    SessionEnded,
    /// The filter names topics and `version` was activated.
    TopicVersionChanged {
        version: TopicModelVersion,
    },
    ShuttingDown,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LiveSubscription {
    pub kinds: UpdateKinds,
    pub filter: TopologyFilter,
    pub resume: Resume,
}

/// Feed limits. Built only through [`LiveConfig::new`]: the heartbeat is
/// non-zero and retention outlasts it, so a cursor from the last heartbeat
/// is still replayable when the client reconnects at once.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LiveConfig {
    buffer: NonZeroU32,
    heartbeat: Duration,
    retention: Duration,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InvalidLiveConfig {
    ZeroHeartbeat,
    /// Retention must be longer than the heartbeat interval.
    RetentionTooShort,
}

impl LiveConfig {
    pub fn new(
        buffer: NonZeroU32,
        heartbeat: Duration,
        retention: Duration,
    ) -> Result<Self, InvalidLiveConfig> {
        if heartbeat.is_zero() {
            return Err(InvalidLiveConfig::ZeroHeartbeat);
        }
        if retention <= heartbeat {
            return Err(InvalidLiveConfig::RetentionTooShort);
        }
        Ok(Self {
            buffer,
            heartbeat,
            retention,
        })
    }

    /// Items a stream may hold undelivered before it ends with `Lagged`.
    pub fn buffer(self) -> NonZeroU32 {
        self.buffer
    }

    pub fn heartbeat(self) -> Duration {
        self.heartbeat
    }

    /// How long feed log entries are kept for replay.
    pub fn retention(self) -> Duration {
        self.retention
    }
}

pub trait LiveFeed {
    type Stream: LiveStream;

    /// `Forbidden` if the caller lacks a requested kind's permission
    /// (`UpdateKinds::missing_permission`). The stream starts as
    /// `FeedWindow::resume` plans for `subscription.resume`.
    async fn subscribe(
        &self,
        caller: &Caller,
        subscription: LiveSubscription,
    ) -> Result<Self::Stream, QueryError>;
}

pub trait LiveStream {
    /// The next item, or why the stream ended. After `Err` the stream is
    /// closed.
    async fn next(&mut self) -> Result<LiveItem, LiveEnd>;
}
