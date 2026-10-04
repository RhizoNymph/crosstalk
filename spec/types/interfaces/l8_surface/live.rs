//! The live feed: what the UI receives over SSE without polling.
//!
//! ```text
//! store commits a change ─▶ bus: Changed(id) ─(group `live`)─▶ feed writer ─append─▶ feed log
//!                                                                epoch E, seq 1, 2, 3, …
//!                                         (kept for `LiveConfig::retention`) │
//!                                     fan-out, one bounded buffer per stream ▼
//!                                stream task: drop events the caller may not receive
//!                                                                            ▼
//!                  SSE: `event: <LiveItem type>`, `id: <LiveCursor>`, `data: <LiveItem JSON>`
//! UI: on each UiEvent, re-query what it names through `QueryApi`
//! ```
//!
//! **SSE framing.** Each [`LiveItem`] is one SSE event of three fields:
//!
//! ```text
//! event: event
//! id: 7-1042
//! data: {"type":"event","data":{"cursor":"7-1042","event":{"type":"alert_changed","data":{"id":"01J9Z3K8M4Q7R2T5V6W8X9Y0ZA"}}}}
//!
//! ```
//!
//! - `event` is the item's variant, the same snake_case name as its JSON
//!   `type` ([`LiveItem::event_name`]): `event`, `resync` or `heartbeat`, so
//!   an `EventSource` client can listen per kind.
//! - `id` is the item's cursor as text ([`LiveCursor::encode`],
//!   `<epoch>-<seq>`), the same string as the JSON's `cursor`. The browser
//!   keeps the last one and sends it back as `Last-Event-ID` on reconnect,
//!   which [`Resume::from_last_event_id`] reads. Heartbeats carry an id too,
//!   so the last id always holds the newest cursor the stream has passed.
//! - `data` is the whole item as JSON on one line (`serde_json::to_string`,
//!   which never writes a newline), decoded by the client as a `LiveItem`.
//!
//! When the stream ends, the server sends one last event named
//! [`LiveEnd::EVENT_NAME`] (`end`) whose `data` is the [`LiveEnd`] JSON
//! (`"lagged"`), with no `id` field, so the client's `Last-Event-ID` stays
//! the last cursor it received, and then closes the response.
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
//! | `AlertChanged { id }` | `Alert` | L6 triage (open, dedup, suppress), L8 acknowledge and resolve | `alerts`, `alert`, `overview` |
//! | `ChannelChanged { id }` | `Channel` | L5: discovery, config declaration, new resource, detection change (incl. dormant and unused), recorded policy decision (config or `PolicyChanged`, once applied), promotion (the promoted channel and each channel it superseded) | `channel`, `channels`, `channel_names`, `policy_history`, `channel_resources`, an open `promotion_preview`, `overview` |
//! | `AgentChanged { id }` | `Agent` | L3: new or registered agent, state change, merge (source, target, repointed agents), unmerge (the source, its former target, restored agents), rename | `agents`, `agent`, `agent_names` |
//! | `RuleChanged { id }` | `Rule` | L6 rule store: create (operator or config), update, enable or disable, turning stale | `alert_rules` |
//! | `VerdictChanged { id }` | `Verdict` | L5 verdict store: a verdict set or withdrawn | `verdicts`, `detection_quality`, `transmissions_by_id`, and views filtered with `FalseDetections::Exclude` |
//! | `Watermark { at }` | `Watermark` | L7 edge store: watermark advance | every `Watermarked` query: `topology`, `channel_topology`, `series`, `overview`, `edge_transmissions`, `channel_resources`, `channel`, `channels`, `agents`, `agent`, `topic_sizes` |
//! | `TopicVersionReady { version }` | `TopicVersion` | L6 catalog: version ready, active or superseded; pinned, unpinned or dropped | `topic_versions`, then topic-scoped queries if the active version changed |
//! | `ProjectionReady { id }` | `Projection` | L6 projection store: job ready or failed, frame expired | `projection_status`, then `projection` |
//!
//! **Permissions.** Subscribing needs `View`. Every event except
//! `ProjectionReady` needs only `View`: it names an alert, channel, agent,
//! rule, transmission (whose verdict log `verdicts` returns), watermark or
//! topic version, all of which View queries return.
//! `ProjectionReady` names a projection job, which only `projection_status`
//! and `projection` (Content) return, so it reaches only callers with
//! `Content`
//! ([`UiEvent::required_permission`]). A stream ends with
//! [`LiveEnd::SessionEnded`] when the caller's session expires or is
//! revoked, or a config load changes or removes its operator.
//!
//! **No server-side filter.** Events carry no content, so filtering is not
//! a confidentiality boundary, only a bandwidth saving on events of a few
//! dozen bytes. The UI knows exactly which ids it is showing and drops the
//! rest; a server-side filter would have to reproduce every view's filter
//! (alert states, graph filters, topic versions, merge resolution) and could
//! only approximate them. Ids are not resolved through merges or
//! supersession either: an event names the stored id that changed, and
//! the stores announce every id a merge, unmerge or promotion re-points
//! (the agents of the record, every superseded channel), so a client
//! showing an alias learns that it now resolves elsewhere and re-queries.
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

use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::aggregates::topic::TopicModelVersion;
use crate::events::changed::Changed;
use crate::ids::{AgentId, AlertId, AlertRuleId, ChannelId, ProjectionId, TransmissionId};
use crate::interfaces::l8_surface::{Caller, Permission, QueryError};
use crate::support::Watermark;
use crate::wire::decode_text;

/// One live event: an id to re-query, never the entity's state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(
    tag = "type",
    content = "data",
    rename_all = "snake_case",
    deny_unknown_fields
)]
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
    /// A verdict was set on, or withdrawn from, transmission `id`.
    VerdictChanged {
        id: TransmissionId,
    },
    /// Every aggregate bucket before `at` is now final.
    Watermark {
        at: Watermark,
    },
    /// `version`'s status changed; it may now be the active version.
    TopicVersionReady {
        version: TopicModelVersion,
    },
    /// Projection job `id` finished (ready or failed) or its frame expired.
    /// A client waiting on it, or showing it, re-queries.
    ProjectionReady {
        id: ProjectionId,
    },
}

impl From<Changed> for UiEvent {
    fn from(changed: Changed) -> Self {
        match changed {
            Changed::Alert(id) => Self::AlertChanged { id },
            Changed::Channel(id) => Self::ChannelChanged { id },
            Changed::Agent(id) => Self::AgentChanged { id },
            Changed::Rule(id) => Self::RuleChanged { id },
            Changed::Verdict(id) => Self::VerdictChanged { id },
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
            | Self::VerdictChanged { .. }
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
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct FeedEpoch(pub u64);

/// A position in the feed log: the SSE event id. `seq` 0 is before the
/// first entry. Cursors of different epochs are not comparable, so this
/// type has no ordering. On the wire, its text ([`LiveCursor::encode`]), the
/// same string the SSE event id carries: `"7-1042"`.
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

    /// Reads what [`LiveCursor::encode`] wrote. `None` for anything else,
    /// a leading zero included, so a cursor's text is unique.
    pub fn decode(text: &str) -> Option<Self> {
        let (epoch, seq) = text.split_once('-')?;
        Some(Self {
            epoch: FeedEpoch(decimal(epoch)?),
            seq: decimal(seq)?,
        })
    }
}

/// Text that is not a cursor [`LiveCursor::encode`] wrote.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InvalidLiveCursor;

/// The cursor's text.
impl Serialize for LiveCursor {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.encode())
    }
}

/// A string [`LiveCursor::decode`] reads.
impl<'de> Deserialize<'de> for LiveCursor {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        decode_text(deserializer, "live cursor", |text| {
            Self::decode(&text).ok_or(InvalidLiveCursor)
        })
    }
}

/// A decimal number as `encode` writes it: digits only, and no leading
/// zero, so each cursor has exactly one text.
fn decimal(text: &str) -> Option<u64> {
    let canonical = text == "0" || !text.starts_with('0');
    if text.is_empty() || !canonical || !text.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    text.parse().ok()
}

/// Where a subscription starts.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "type",
    content = "data",
    rename_all = "snake_case",
    deny_unknown_fields
)]
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
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

/// One SSE event: its variant is the event name
/// ([`LiveItem::event_name`]), its cursor the event id, and the whole item
/// the event's data (see the module's SSE framing).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "type",
    content = "data",
    rename_all = "snake_case",
    deny_unknown_fields
)]
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
    /// The SSE `event` field of this item: its variant's snake_case name,
    /// the same as its JSON `type`.
    pub fn event_name(&self) -> &'static str {
        match self {
            Self::Event { .. } => "event",
            Self::Resync { .. } => "resync",
            Self::Heartbeat { .. } => "heartbeat",
        }
    }

    /// The item's cursor: its SSE `id`, as [`LiveCursor::encode`] writes it.
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
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LiveEnd {
    /// The stream's buffer filled: the client read too slowly.
    Lagged,
    /// The caller's session expired or was revoked, or a config load changed
    /// or removed its operator, so its permissions may be stale.
    SessionEnded,
    ShuttingDown,
}

impl LiveEnd {
    /// The SSE `event` field of the stream's last event, whose `data` is
    /// the `LiveEnd` and which has no `id`. No `LiveItem` has this name.
    pub const EVENT_NAME: &'static str = "end";
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
