//! Why a query or an operator action failed.
//!
//! Every variant is something the UI can act on. An action fails with an
//! [`ActionError`], a strict subset of [`QueryError`] (`From` converts it
//! unchanged); how each store's error becomes one or the other is defined
//! once, in [`super::query_errors`].
//!
//! All four enums are responses, adjacently tagged on the wire
//! ([`crate::wire`]): `{"type": "not_found"}`,
//! `{"type": "conflict", "data": {"type": "rule_stale", "data": {"rule": ..}}}`.
//!
//! **Client-only variants.** `QueryError::Unavailable` and
//! `ActionError::Unavailable` are produced only by a client of the surface
//! (`crosstalk-client`): the call never reached a surface that answered it,
//! and [`UnavailableKind`] says why. A surface never returns one, and a
//! server never answers one: it answers [`QueryError::served`] (or
//! [`ActionError::served`]) of what it was handed, which turns a
//! client-only variant into `Store` with the same reason
//! (`surface.http.client-only-errors-never-served`).

use serde::{Deserialize, Serialize};

use crate::aggregates::projection::{FitFailure, ProjectionStatusKind};
use crate::aggregates::topic::TopicModelVersion;
use crate::ids::{
    AgentId, AlertId, AlertRuleId, ChannelId, MergeId, ProjectionId, SinkId, TopicId,
    TransmissionId,
};

use crate::wire::DecodeErrorKind;

use super::Permission;
use super::export::ExportFormat;

/// Why a query failed. Every variant is something the UI can act on.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "type",
    content = "data",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum QueryError {
    /// A store or bus failure; retrying may succeed.
    Store {
        reason: String,
    },
    /// Client-only: the call never reached a surface that answered it, for
    /// the reason `kind` names; retrying may succeed (after signing in
    /// again, for `Unauthenticated`). `reason` is the client's description
    /// of the cause. A surface never returns it and a server never answers
    /// it ([`QueryError::served`]).
    Unavailable {
        kind: UnavailableKind,
        reason: String,
    },
    NotFound,
    Forbidden {
        missing: Permission,
    },
    /// A topic-model version, pinned by the filter or by a cursor, that was
    /// activated but whose buckets or assignments are no longer retained.
    VersionNotRetained {
        version: TopicModelVersion,
    },
    /// The request is well-formed but the state does not allow it.
    Conflict(ConflictKind),
    InvalidInput(InputError),
    /// A cursor the surface did not issue, or issued for a different list
    /// or request. The client restarts from the first page.
    InvalidCursor,
    /// A projection whose frame was dropped after the retention period. Its
    /// spec is still readable with `projection_status`.
    ProjectionNotRetained {
        projection: ProjectionId,
    },
}

/// Why an operator action was refused. A strict subset of what a query can
/// fail with: an action takes no cursor, reads no projection and reads no
/// version's buckets, so those variants cannot be returned (or recorded in
/// the audit log) for one. Pinning a dropped version is
/// `Conflict(TopicVersionDropped)`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "type",
    content = "data",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum ActionError {
    Store {
        reason: String,
    },
    /// Client-only, as [`QueryError::Unavailable`]: the action never reached
    /// a surface, so it had no effect and was not audited.
    Unavailable {
        kind: UnavailableKind,
        reason: String,
    },
    NotFound,
    Forbidden {
        missing: Permission,
    },
    Conflict(ConflictKind),
    InvalidInput(InputError),
}

/// Why a client's call never reached a surface that answered it. Each kind
/// is one way the HTTP exchange failed before the surface decided anything.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UnavailableKind {
    /// The server answered `401`: the request had no caller (no
    /// credential, an invalid one, or an expired session). Sign in again.
    Unauthenticated,
    /// Connecting, sending the request or reading the response head
    /// failed.
    Transport,
    /// Reading the response body failed: the connection was cut or reset.
    Body,
    /// No response, or no bytes of a streamed body, within the client's
    /// configured time.
    Timeout,
}

impl UnavailableKind {
    pub const ALL: [Self; 4] = [
        Self::Unauthenticated,
        Self::Transport,
        Self::Body,
        Self::Timeout,
    ];
}

impl QueryError {
    /// What a server answers for this error: the error itself, except that
    /// a client-only `Unavailable` (which a surface never returns, so a
    /// server can only be handed one by a client it relays to) becomes
    /// `Store` with the same reason. Never `Unavailable`, and always of the
    /// same `ErrorStatus` (503).
    pub fn served(self) -> Self {
        match self {
            Self::Unavailable { reason, .. } => Self::Store { reason },
            Self::Store { .. }
            | Self::NotFound
            | Self::Forbidden { .. }
            | Self::VersionNotRetained { .. }
            | Self::Conflict(_)
            | Self::InvalidInput(_)
            | Self::InvalidCursor
            | Self::ProjectionNotRetained { .. } => self,
        }
    }

    /// Whether only a client produces this error ([`QueryError::served`]).
    pub fn is_client_only(&self) -> bool {
        matches!(self, Self::Unavailable { .. })
    }
}

impl ActionError {
    /// As [`QueryError::served`]: never `Unavailable`.
    pub fn served(self) -> Self {
        match self {
            Self::Unavailable { reason, .. } => Self::Store { reason },
            Self::Store { .. }
            | Self::NotFound
            | Self::Forbidden { .. }
            | Self::Conflict(_)
            | Self::InvalidInput(_) => self,
        }
    }

    /// Whether only a client produces this error ([`ActionError::served`]).
    pub fn is_client_only(&self) -> bool {
        matches!(self, Self::Unavailable { .. })
    }
}

/// A request that is valid on its own but not in the current state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "type",
    content = "data",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum ConflictKind {
    /// Acknowledging or resolving an alert that is no longer active.
    AlertNotActive { alert: AlertId },
    /// Resolving an open alert: it is acknowledged first.
    AlertNotAcknowledged { alert: AlertId },
    /// Acting on a merged agent where only its canonical agent is valid:
    /// renaming it, or naming it in a merge.
    AgentMerged { agent: AgentId, into: AgentId },
    /// Reverting a merge that was already reverted.
    MergeAlreadyReverted { merge: MergeId },
    /// Merging two different agents that already resolve to one canonical
    /// agent, `canonical`: one is merged into the other, or both into
    /// `canonical`. Checked before `AgentMerged`, since naming the canonical
    /// agent instead would still merge it into itself. The same request
    /// with one id twice is `InvalidInput(SelfMerge)` and never reaches the
    /// merge table.
    MergeIntoSelf {
        from: AgentId,
        into: AgentId,
        canonical: AgentId,
    },
    /// Acting on a channel that has been superseded by another.
    ChannelSuperseded { channel: ChannelId, by: ChannelId },
    /// Promoting a channel that is not a discovered channel.
    ChannelNotDiscovered { channel: ChannelId },
    /// A declared pattern that overlaps another declared channel's.
    PatternOverlaps { existing: ChannelId },
    /// Changing an alert rule's kind, or editing a built-in rule.
    RuleNotEditable { rule: AlertRuleId },
    /// Enabling a stale alert rule. `UpdateRule` retargets it to the current
    /// topic version or embedding model and enables it.
    RuleStale { rule: AlertRuleId },
    /// A watched-topic rule on a topic-model version that is no longer, or
    /// not yet, the one rules are written against.
    TopicVersionNotCurrent {
        requested: TopicModelVersion,
        current: TopicModelVersion,
    },
    /// A verdict on a transmission whose state does not take one
    /// (`Detected`, `AwaitingContent`).
    TransmissionNotJudgeable { transmission: TransmissionId },
    /// Querying or pinning a topic-model version that is still being fitted.
    TopicVersionFitting { version: TopicModelVersion },
    /// Pinning a topic-model version whose data retention has dropped.
    TopicVersionDropped { version: TopicModelVersion },
    /// A linked view pinned to a version that was never activated, so its
    /// edge buckets were never complete.
    TopicVersionNotActivated { version: TopicModelVersion },
    /// A filter listing topics that are not in the version it resolved to,
    /// usually because `Current` moved on. The client re-reads the topics of
    /// `version`, or pins the version its topics came from.
    TopicsNotInVersion {
        version: TopicModelVersion,
        topics: Vec<TopicId>,
    },
    /// The embedding model changed between embedding the query and running
    /// it, or between two pages of one search.
    EmbeddingModelChanged,
    /// Reading a projection that is queued or fitting.
    ProjectionNotReady {
        projection: ProjectionId,
        status: ProjectionStatusKind,
    },
    /// Reading a projection whose fit failed.
    ProjectionFailed {
        projection: ProjectionId,
        failure: FitFailure,
    },
    /// Fitting a projection while the job queue is full.
    ProjectionQueueFull,
    /// An export that would hold `rows` rows, more than the configured
    /// `limit` (`ExportLimits`). A smaller window or narrower filter passes.
    ExportTooLarge { rows: u64, limit: u64 },
}

/// A request that is invalid whatever the state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "type",
    content = "data",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum InputError {
    /// A window that does not start and end on bucket boundaries.
    UnalignedWindow,
    /// A series grid built for another bucket width.
    BucketWidthMismatch,
    /// A promotion pattern that does not cover the channel's seed resource.
    PatternMissesSeed,
    /// A watched-topic rule naming topics or a version that do not exist.
    UnknownTopics,
    /// A rule naming a sink that is not configured.
    UnknownSink { sink: SinkId },
    /// Text the embedding model cannot embed because it is longer than the
    /// model's context: a search's text, or a semantic rule's query.
    QueryTooLong,
    /// A merge naming one agent as both source and target
    /// (`MergeRequest::new` refuses it): invalid whatever the merge table
    /// holds.
    SelfMerge,
    /// A transmission selection naming no transmission
    /// (`TransmissionSelection::new` refuses it).
    EmptySelection,
    /// An excerpt window wider than `ExcerptWindow::MAX_CONTEXT` bytes of
    /// context a side (`ExcerptWindow::new` refuses it).
    ExcerptContextTooLong { max: u16, got: u16 },
    /// More distinct ids than a request takes: a batch lookup
    /// (`agent_names`, `channel_names`, `exchange_turns`, `span_points`)
    /// over `IdBatch::MAX`, or a
    /// transmission selection over `TransmissionSelection::MAX`. `max` is
    /// the bound that applied and `got` the distinct ids asked for.
    TooManyIds { max: usize, got: usize },
    /// A text limit outside `1..=TextLimit::MAX` bytes (`TextLimit::new`
    /// refuses it). `max` is the bound, `got` the limit asked for.
    TextLimitOutOfRange { max: u32, got: u32 },
    /// A part text read naming a part with no text (media, opaque
    /// reasoning, an unknown block, a tool result without text) or a part
    /// the message does not have (`NoPartText`).
    PartWithoutText { index: u16 },
    /// A part text slice starting past the end of the part's text, or
    /// inside a character. `part_len` is the text's length in bytes.
    SliceOutsideText { from: u32, part_len: u32 },
    /// An export in a format the gateway does not write: one outside
    /// `Present::export_formats`. Refused after the permission check and
    /// before anything is read (`ExportFormats::check`).
    UnsupportedFormat { format: ExportFormat },
    /// Client input the HTTP layer could not decode as the route's request
    /// type (`crate::wire::decode_request`): not JSON, cut short, or JSON of
    /// the wrong shape, including an unknown field or variant and a value a
    /// checked constructor refuses. `reason` is the decoder's description,
    /// with the line and column. Such a request never reaches a store, and
    /// an action request that fails to decode never becomes an action, so
    /// it is not audited.
    MalformedRequest {
        kind: DecodeErrorKind,
        reason: String,
    },
}

impl From<ActionError> for QueryError {
    fn from(error: ActionError) -> Self {
        match error {
            ActionError::Store { reason } => Self::Store { reason },
            ActionError::Unavailable { kind, reason } => Self::Unavailable { kind, reason },
            ActionError::NotFound => Self::NotFound,
            ActionError::Forbidden { missing } => Self::Forbidden { missing },
            ActionError::Conflict(kind) => Self::Conflict(kind),
            ActionError::InvalidInput(input) => Self::InvalidInput(input),
        }
    }
}
