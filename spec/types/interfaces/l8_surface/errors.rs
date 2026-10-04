//! Why a query or an operator action failed.
//!
//! Every variant is something the UI can act on. An action fails with an
//! [`ActionError`], a strict subset of [`QueryError`] (`From` converts it
//! unchanged); how each store's error becomes one or the other is defined
//! once, in [`super::query_errors`].

use crate::aggregates::projection::{FitFailure, ProjectionStatusKind};
use crate::aggregates::topic::TopicModelVersion;
use crate::ids::{
    AgentId, AlertId, AlertRuleId, ChannelId, MergeId, ProjectionId, SinkId, TopicId,
    TransmissionId,
};

use super::Permission;

/// Why a query failed. Every variant is something the UI can act on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QueryError {
    /// A store or bus failure; retrying may succeed.
    Store {
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
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ActionError {
    Store { reason: String },
    NotFound,
    Forbidden { missing: Permission },
    Conflict(ConflictKind),
    InvalidInput(InputError),
}

/// A request that is valid on its own but not in the current state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConflictKind {
    /// Acknowledging or resolving an alert that is no longer active.
    AlertNotActive { alert: AlertId },
    /// Acting on a merged agent where only its canonical agent is valid:
    /// renaming it, or naming it in a merge.
    AgentMerged { agent: AgentId, into: AgentId },
    /// Reverting a merge that was already reverted.
    MergeAlreadyReverted { merge: MergeId },
    /// Acting on a channel that has been superseded by another.
    ChannelSuperseded { channel: ChannelId, by: ChannelId },
    /// Promoting a channel that is not a discovered channel.
    ChannelNotDiscovered { channel: ChannelId },
    /// A declared pattern that overlaps another declared channel's.
    PatternOverlaps { existing: ChannelId },
    /// Changing an alert rule's kind, or editing a built-in rule.
    RuleNotEditable { rule: AlertRuleId },
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
#[derive(Debug, Clone, PartialEq, Eq)]
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
}

impl From<ActionError> for QueryError {
    fn from(error: ActionError) -> Self {
        match error {
            ActionError::Store { reason } => Self::Store { reason },
            ActionError::NotFound => Self::NotFound,
            ActionError::Forbidden { missing } => Self::Forbidden { missing },
            ActionError::Conflict(kind) => Self::Conflict(kind),
            ActionError::InvalidInput(input) => Self::InvalidInput(input),
        }
    }
}
