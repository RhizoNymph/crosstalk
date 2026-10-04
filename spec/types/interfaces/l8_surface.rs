//! L8 surface: the query API, the UI's data, operator actions, the live
//! feed, the audit log and alert delivery.
//!
//! Operator actions flow back down the stack: policy changes are published
//! as `PolicyChanged` (applied by L5), and agent merges go to L3's identity
//! resolver. Every action call, whatever its outcome, leaves one
//! [`audit::AuditRecord`].
//!
//! Implementations:
//! - `QueryApi`: the axum HTTP service backing `GraphView` (topology, edge
//!   share), `ContentExplorer` (search, topics, UMAP), the channel policy
//!   history and the audit log.
//! - `LiveFeed` ([`live`]): the SSE endpoint the UI subscribes to for new
//!   alerts and changed edges, channels and policies.
//! - `AuditLog` ([`audit`]): `PgAuditLog`, append-only.
//! - `AlertSink`: `WebhookSink`, `SlackSink`, `LogSink`.

pub mod audit;
pub mod live;

use crate::aggregates::alert::Alert;
use crate::aggregates::edge::{TopologyFilter, TopologyGraph, Weighting};
use crate::aggregates::topic::{Topic, TopicModelVersion};
use crate::derived::flow::channel::Channel;
use crate::derived::flow::channel::policy::PolicyHistory;
use crate::derived::flow::transmission::Transmission;
use crate::ids::{AlertId, ChannelId, EventId, OperatorId, TransmissionId};
use crate::interfaces::l2_transport::ConsumerGroup;
use crate::interfaces::l6_analysis::{SearchHit, SearchQuery};
use crate::observed::agent::MergeRequest;
use crate::support::TimeWindow;

use audit::{ActionEffect, AuditPage, AuditQuery};

/// The policy an operator asks for. The surface stamps the author and time
/// from the authenticated caller; callers cannot supply them.
pub use crate::derived::flow::channel::policy::PolicyKind;

/// A point in a 2-D projection of transmission embeddings.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ProjectedPoint {
    pub transmission: TransmissionId,
    pub x: f32,
    pub y: f32,
}

/// The authenticated caller of a query or action.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Caller {
    pub operator: OperatorId,
    pub permissions: Vec<Permission>,
}

impl Caller {
    pub fn has(&self, permission: Permission) -> bool {
        self.permissions.contains(&permission)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Permission {
    /// Topology, channels, channel policy history, alerts: no message
    /// content.
    View,
    /// Transmission content, search, topics (their labels and terms come
    /// from message text) and projections.
    Content,
    /// Policy changes and agent merges.
    Govern,
    /// Acknowledge and resolve alerts.
    Triage,
    /// Operate the pipeline: replay dead-lettered deliveries. A replay
    /// re-runs a consumer on an old event, so it can reopen alerts or
    /// re-apply stale decisions.
    Operate,
    /// Read the audit log: every operator action, who asked for it and
    /// what came of it, including refused ones.
    Audit,
}

/// Empty `states` means every state. `channel` keeps alerts whose subject is
/// that channel or a transmission routed through it.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct AlertFilter {
    pub states: Vec<AlertStateKind>,
    pub channel: Option<ChannelId>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AlertStateKind {
    Open,
    Acknowledged,
    Resolved,
    Suppressed,
}

pub trait QueryApi {
    async fn channel(&self, caller: &Caller, id: ChannelId) -> Result<Option<Channel>, QueryError>;

    /// Every policy decision recorded for the channel, config and operator
    /// alike, oldest first; its last entry is the channel's current policy.
    /// `None` for an unknown channel. Needs `View`.
    async fn policy_history(
        &self,
        caller: &Caller,
        channel: ChannelId,
    ) -> Result<Option<PolicyHistory>, QueryError>;

    async fn alerts(&self, caller: &Caller, filter: &AlertFilter)
    -> Result<Vec<Alert>, QueryError>;

    async fn topology(
        &self,
        caller: &Caller,
        window: TimeWindow,
        weighting: Weighting,
        filter: &TopologyFilter,
    ) -> Result<TopologyGraph, QueryError>;

    async fn search(
        &self,
        caller: &Caller,
        query: &SearchQuery,
        window: Option<TimeWindow>,
        limit: u32,
    ) -> Result<Vec<SearchHit>, QueryError>;

    async fn transmission(
        &self,
        caller: &Caller,
        id: TransmissionId,
    ) -> Result<Option<Transmission>, QueryError>;

    async fn topics(
        &self,
        caller: &Caller,
        version: Option<TopicModelVersion>,
    ) -> Result<Vec<Topic>, QueryError>;

    async fn projection(
        &self,
        caller: &Caller,
        window: TimeWindow,
    ) -> Result<Vec<ProjectedPoint>, QueryError>;

    /// One page of the audit log, newest first. Needs `Audit`.
    async fn audit(&self, caller: &Caller, query: &AuditQuery) -> Result<AuditPage, QueryError>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OperatorAction {
    SetPolicy {
        channel: ChannelId,
        policy: PolicyKind,
        note: Option<String>,
    },
    /// Built with `MergeAuthor::Operator` of the caller; self-merges cannot
    /// be expressed.
    MergeAgents(MergeRequest),
    Acknowledge {
        alert: AlertId,
    },
    Resolve {
        alert: AlertId,
        note: Option<String>,
    },
    /// Redeliver a dead-lettered envelope to its consumer group.
    ReplayDeadLetter {
        group: ConsumerGroup,
        id: EventId,
    },
}

/// Which action, without its arguments. The audit log filters on it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ActionKind {
    SetPolicy,
    MergeAgents,
    Acknowledge,
    Resolve,
    ReplayDeadLetter,
}

impl OperatorAction {
    pub fn kind(&self) -> ActionKind {
        match self {
            Self::SetPolicy { .. } => ActionKind::SetPolicy,
            Self::MergeAgents(_) => ActionKind::MergeAgents,
            Self::Acknowledge { .. } => ActionKind::Acknowledge,
            Self::Resolve { .. } => ActionKind::Resolve,
            Self::ReplayDeadLetter { .. } => ActionKind::ReplayDeadLetter,
        }
    }

    /// The permission the caller must hold. Checked before any effect; a
    /// caller without it gets `Forbidden`.
    pub fn required_permission(&self) -> Permission {
        match self {
            Self::SetPolicy { .. } | Self::MergeAgents(_) => Permission::Govern,
            Self::Acknowledge { .. } | Self::Resolve { .. } => Permission::Triage,
            Self::ReplayDeadLetter { .. } => Permission::Operate,
        }
    }
}

pub trait OperatorActions {
    /// Check the permission, apply the action and record it. A call that
    /// returns `Ok`, `Forbidden`, `NotFound` or `BadRequest` leaves exactly
    /// one audit record, whose outcome is what it returns
    /// (`AuditOutcome::of`): an `Applied` or `Unchanged` record is written in
    /// the same transaction as the action's effect, and a `Forbidden` or
    /// `Rejected` one with no effect. A `Store` error had no effect and
    /// leaves at most one record, written when the audit log is still
    /// reachable.
    async fn act(
        &self,
        caller: &Caller,
        action: OperatorAction,
    ) -> Result<ActionEffect, QueryError>;
}

pub trait AlertSink {
    async fn deliver(&self, alert: &Alert) -> Result<(), SinkError>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QueryError {
    Store { reason: String },
    NotFound,
    Forbidden,
    BadRequest { reason: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SinkError {
    Unreachable { reason: String },
    Rejected { status: u16 },
}
