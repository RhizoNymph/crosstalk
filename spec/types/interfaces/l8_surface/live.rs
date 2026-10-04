//! The live feed: what the UI receives over SSE without polling.
//!
//! ```text
//! store commits a change ─▶ bus: Changed(id) ─(group `live`)─▶ feed writer ─append─▶ feed log
//!                                                                epoch E, seq 1, 2, 3, …
//!                                         (kept for `LiveConfig::retention`) │
//!                                     fan-out, one bounded buffer per stream ▼
//!                                stream task: drop events the caller may not receive
//!                                                                            ▼
//!                                       SSE: `id: <LiveCursor>`, `data: <UiEvent>`
//! UI: on each UiEvent, re-query what it names through `QueryApi`
//! ```
//!
//! **Ids only.** A [`UiEvent`] says which entity changed and nothing about
//! how; the UI re-queries it. So an event can never carry stale state or
//! content, events for one entity may arrive in any order or twice, and the
//! re-query after the last one returns the stored state.
//!
//! **Sources.** Every event comes from one `Changed` bus notification
//! ([`UiEvent::from`]), which the store owning the entity publishes after
//! every committed change to it, and never before the change is visible to
//! the query below. The feed writer does not interpret other bus events, so
//! new actions and events need no mapping here, only their stores' `Changed`.
//!
//! | `UiEvent` | From `Changed` | Published by, after | Re-query |
//! | --- | --- | --- | --- |
//! | `AlertChanged { id }` | `Alert` | L6 triage (open, dedup, suppress), L8 acknowledge and resolve | `alerts` |
//! | `ChannelChanged { id }` | `Channel` | L5: discovery, config declaration, new resource, detection change (incl. dormant and unused), recorded policy decision (config or `PolicyChanged`, once applied), promotion | `channel`, `policy_history` |
//! | `AgentChanged { id }` | `Agent` | L3: new or registered agent, state change, merge (both agents), unmerge (the agent, its former target, restored agents), label | `agents` |
//! | `RuleChanged { id }` | `Rule` | L6 rule store: create (operator or config), update, status, turning stale | `alert_rules` |
//! | `Watermark { at }` | `Watermark` | L7 edge store: watermark advance | `topology`, `series`, `edge_transmissions` |
//! | `TopicVersionReady { version }` | `TopicVersion` | L6 catalog: version ready, active or superseded | `topic_versions`, then topic-scoped queries if the active version changed |
//! | `ProjectionReady { id }` | `Projection` | L6 projection index: new current layout | `projection` |
//!
//! **Permissions.** Subscribing needs `View`. Every event except
//! `ProjectionReady` needs only `View`: it names an alert, channel, agent,
//! rule, watermark or topic version, all of which View queries list.
//! `ProjectionReady` names a layout token, which only `projection` (Content)
//! returns, so it reaches only callers with `Content`
//! ([`UiEvent::required_permission`]). A stream ends with
//! [`LiveEnd::SessionEnded`] when the caller's session expires or is
//! revoked, or a config load changes or removes its operator.
//!
//! **No server-side filter.** Events carry no content, so filtering is not
//! a confidentiality boundary, only a bandwidth saving on events of a few
//! dozen bytes. The UI knows exactly which ids it is showing and drops the
//! rest; a server-side filter would have to reproduce every view's filter
//! (alert states, graph filters, topic versions, merge resolution) and could
//! only approximate them.
//!
//! **Resumption.** Every item carries a [`LiveCursor`], sent as the SSE
//! event id. A client reconnects with its last one (`Last-Event-ID`), and
//! [`FeedWindow::resume`] decides: replay every retained entry after it,
//! then continue live; or, if the cursor is too old, from another epoch,
//! ahead of the log or unreadable, send [`LiveItem::Resync`] first. Resync
//! means: re-query everything shown, then apply the events that follow.
//! Nothing is lost silently. Heartbeats carry the newest cursor the stream
//! has passed, so a client that receives few events still holds a recent
//! cursor.
//!
//! **Backpressure.** Each stream has a buffer of `LiveConfig::buffer` items.
//! The feed writer and other streams never wait for it: when it is full,
//! the stream ends with [`LiveEnd::Lagged`] and the client reconnects with
//! its last cursor, replaying from the log (or resyncing if it fell out of
//! retention).

use std::num::NonZeroU32;
use std::time::Duration;

use crate::aggregates::projection::ProjectionToken;
use crate::aggregates::topic::TopicModelVersion;
use crate::events::changed::Changed;
use crate::ids::{AgentId, AlertId, AlertRuleId, ChannelId};
use crate::interfaces::l8_surface::{Caller, Permission, QueryError};
use crate::support::Watermark;

/// One live event: an id to re-query, never the entity's state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum UiEvent {
    AlertChanged {
        id: AlertId,
    },
    ChannelChanged {
        id: ChannelId,
    },
    /// A merge, an unmerge, a label or a state change.
    AgentChanged {
        id: AgentId,
    },
    RuleChanged {
        id: AlertRuleId,
    },
    /// Every aggregate bucket before `at` is now final.
    Watermark {
        at: Watermark,
    },
    /// `version`'s status changed; it may now be the active version.
    TopicVersionReady {
        version: TopicModelVersion,
    },
    /// `id` is the current projection layout. A client holding points of
    /// another layout re-queries.
    ProjectionReady {
        id: ProjectionToken,
    },
}

impl From<Changed> for UiEvent {
    fn from(changed: Changed) -> Self {
        match changed {
            Changed::Alert(id) => Self::AlertChanged { id },
            Changed::Channel(id) => Self::ChannelChanged { id },
            Changed::Agent(id) => Self::AgentChanged { id },
            Changed::Rule(id) => Self::RuleChanged { id },
            Changed::Watermark(at) => Self::Watermark { at },
            Changed::TopicVersion(version) => Self::TopicVersionReady { version },
            Changed::Projection(id) => Self::ProjectionReady { id },
        }
    }
}

impl UiEvent {
    /// The permission of the query the event tells the client to re-run.
    pub fn required_permission(self) -> Permission {
        match self {
            Self::ProjectionReady { .. } => Permission::Content,
            Self::AlertChanged { .. }
            | Self::ChannelChanged { .. }
            | Self::AgentChanged { .. }
            | Self::RuleChanged { .. }
            | Self::Watermark { .. }
            | Self::TopicVersionReady { .. } => Permission::View,
        }
    }

    /// Whether a stream for `caller` delivers this event. Other events are
    /// passed over: they advance the stream's cursor but are not sent.
    pub fn visible_to(self, caller: &Caller) -> bool {
        caller.has(self.required_permission())
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
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LiveItem {
    Event {
        cursor: LiveCursor,
        event: UiEvent,
    },
    /// Re-query everything shown, then apply the events that follow.
    Resync {
        cursor: LiveCursor,
        reason: ResyncReason,
    },
    /// Sent at least once per `LiveConfig::heartbeat`. Its cursor is the
    /// newest entry the stream has passed, delivered or passed over.
    Heartbeat {
        cursor: LiveCursor,
    },
}

impl LiveItem {
    pub fn cursor(&self) -> LiveCursor {
        match self {
            Self::Event { cursor, .. }
            | Self::Resync { cursor, .. }
            | Self::Heartbeat { cursor } => *cursor,
        }
    }
}

/// Why a stream ended. After any of these, the client reconnects with its
/// last cursor (after re-authenticating, for `SessionEnded`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LiveEnd {
    /// The stream's buffer filled: the client read too slowly.
    Lagged,
    /// The caller's session expired or was revoked, or a config load changed
    /// or removed its operator, so its permissions may be stale.
    SessionEnded,
    ShuttingDown,
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

    /// `Forbidden { missing: View }` if the caller lacks View. The stream
    /// starts as `FeedWindow::resume` plans for `resume`, and delivers every
    /// event `UiEvent::visible_to` the caller.
    async fn subscribe(&self, caller: &Caller, resume: Resume) -> Result<Self::Stream, QueryError>;
}

pub trait LiveStream {
    /// The next item, or why the stream ended. After `Err` the stream is
    /// closed.
    async fn next(&mut self) -> Result<LiveItem, LiveEnd>;
}
