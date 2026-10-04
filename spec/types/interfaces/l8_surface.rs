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
//!   share, the time brush and trend lines), `TopicHistory` (versions, sizes,
//!   lineage), `ContentExplorer` (search, topics, UMAP), the channel policy
//!   history and the audit log.
//! - `LiveFeed` ([`live`]): the SSE endpoint the UI subscribes to for new
//!   alerts and changed edges, channels and policies.
//! - `AuditLog` ([`audit`]): `PgAuditLog`, append-only.
//! - `AlertSink`: `WebhookSink`, `SlackSink`, `LogSink`.
//!
//! **Lists.** Channels, agents, alert rules, dead letters and the
//! transmissions behind an edge are read a page at a time with the cursors
//! of [`crate::paging`], so a traversal is stable under concurrent inserts.
//! Their filters and request types are in [`lists`].
//!
//! **Linked views.** `topology`, `search`, `projection` and
//! `edge_transmissions` take the same [`TopologyFilter`] and apply it as
//! [`TopologyFilter::admits`] defines, so a selection in one view narrows
//! the others to the same transmissions. Each response reports the
//! topic-model version its topics are under; responses with different
//! versions are not linkable and the client re-queries.

pub mod audit;
pub mod lists;
pub mod live;

use crate::aggregates::alert::{Alert, AlertRuleDef};
use crate::aggregates::edge::{
    EdgeSelector, EdgeTransmissionPage, TopologyFilter, TopologyGraph, Weighting,
};
use crate::aggregates::projection::{Projection, ProjectionToken};
use crate::aggregates::series::{SeriesGrid, SeriesGrouping, TopologySeries};
use crate::aggregates::topic::{Topic, TopicModelVersion};
use crate::aggregates::topic_history::{TopicLineage, TopicSizes, TopicVersionHistory};
use crate::derived::flow::channel::Channel;
use crate::derived::flow::channel::policy::PolicyHistory;
use crate::derived::flow::transmission::Transmission;
use crate::ids::{AlertId, ChannelId, EventId, OperatorId, TransmissionId};
use crate::interfaces::l2_transport::{ConsumerGroup, DeadLetter};
use crate::interfaces::l6_analysis::{SearchQuery, SearchResults};
use crate::observed::agent::{Agent, MergeRequest};
use crate::paging::{
    AgentList, AlertRuleList, ChannelList, DeadLetterList, EdgeTransmissionList, Page, PageRequest,
};
use crate::support::TimeWindow;

use audit::{ActionEffect, AuditPage, AuditQuery};
use lists::{AgentFilter, AlertRuleFilter, ChannelFilter, ProjectionRequest};

/// The policy an operator asks for. The surface stamps the author and time
/// from the authenticated caller; callers cannot supply them.
pub use crate::derived::flow::channel::policy::PolicyKind;

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
    /// Topology, series, the transmissions behind an edge (ids, times, byte
    /// counts and topic ids), channels, channel policy history, agents, alert
    /// rules, alerts and the topic history (versions, sizes, lineage): ids,
    /// counts, times and similarities, no message content and no topic
    /// labels or terms.
    View,
    /// Transmission content, search, topics (their labels and terms come
    /// from message text) and projections.
    Content,
    /// Policy changes and agent merges.
    Govern,
    /// Acknowledge and resolve alerts.
    Triage,
    /// Operate the pipeline: list and replay dead-lettered deliveries. A
    /// replay re-runs a consumer on an old event, so it can reopen alerts or
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

/// Every method checks the caller's permission first and returns
/// `Forbidden` without reading anything when it is missing. List methods
/// return `InvalidCursor` for a cursor issued for a different request.
pub trait QueryApi {
    /// View.
    async fn channel(&self, caller: &Caller, id: ChannelId) -> Result<Option<Channel>, QueryError>;

    /// View. Every policy decision recorded for the channel, config and
    /// operator alike, oldest first; its last entry is the channel's current
    /// policy. `None` for an unknown channel.
    async fn policy_history(
        &self,
        caller: &Caller,
        channel: ChannelId,
    ) -> Result<Option<PolicyHistory>, QueryError>;

    /// View. Newest channel first.
    async fn channels(
        &self,
        caller: &Caller,
        filter: &ChannelFilter,
        page: &PageRequest<ChannelList>,
    ) -> Result<Page<Channel, ChannelList>, QueryError>;

    /// View. Every stored agent, merged ones included (their state names
    /// their canonical agent). Newest agent first.
    async fn agents(
        &self,
        caller: &Caller,
        filter: &AgentFilter,
        page: &PageRequest<AgentList>,
    ) -> Result<Page<Agent, AgentList>, QueryError>;

    /// View. Newest rule first.
    async fn alert_rules(
        &self,
        caller: &Caller,
        filter: &AlertRuleFilter,
        page: &PageRequest<AlertRuleList>,
    ) -> Result<Page<AlertRuleDef, AlertRuleList>, QueryError>;

    /// Operate. Dead letters of one consumer group, or of every group,
    /// newest envelope first.
    async fn dead_letters(
        &self,
        caller: &Caller,
        group: Option<&ConsumerGroup>,
        page: &PageRequest<DeadLetterList>,
    ) -> Result<Page<DeadLetter, DeadLetterList>, QueryError>;

    /// View.
    async fn alerts(&self, caller: &Caller, filter: &AlertFilter)
    -> Result<Vec<Alert>, QueryError>;

    /// View.
    async fn topology(
        &self,
        caller: &Caller,
        window: TimeWindow,
        weighting: Weighting,
        filter: &TopologyFilter,
    ) -> Result<TopologyGraph, QueryError>;

    /// View. The transmissions `topology` counts into one of its edges for
    /// the same window and filter (`EdgeStore::transmissions`): ids, times,
    /// byte counts and topic ids, no content. Content is behind
    /// `transmission` and `search`.
    async fn edge_transmissions(
        &self,
        caller: &Caller,
        edge: &EdgeSelector,
        window: TimeWindow,
        filter: &TopologyFilter,
        page: &PageRequest<EdgeTransmissionList>,
    ) -> Result<EdgeTransmissionPage, QueryError>;

    /// View. Exactly [`EdgeStore::series`]; a grid for another bucket width
    /// is `BadRequest`, like an unaligned graph window.
    ///
    /// [`EdgeStore::series`]: crate::interfaces::l7_topology::EdgeStore::series
    async fn series(
        &self,
        caller: &Caller,
        grid: SeriesGrid,
        weighting: Weighting,
        grouping: SeriesGrouping,
        filter: &TopologyFilter,
    ) -> Result<TopologySeries, QueryError>;

    /// View.
    async fn topic_versions(&self, caller: &Caller) -> Result<TopicVersionHistory, QueryError>;

    /// View. `None` is the active version. An unknown version is
    /// `NotFound`; a fitting one is `BadRequest`.
    async fn topic_sizes(
        &self,
        caller: &Caller,
        version: Option<TopicModelVersion>,
        window: Option<TimeWindow>,
    ) -> Result<TopicSizes, QueryError>;

    /// View. The lineage from `from` to its successor; `None` while it has
    /// none. An unknown version is `NotFound`.
    async fn topic_lineage(
        &self,
        caller: &Caller,
        from: TopicModelVersion,
    ) -> Result<Option<TopicLineage>, QueryError>;

    /// Content.
    async fn search(
        &self,
        caller: &Caller,
        query: &SearchQuery,
        window: Option<TimeWindow>,
        filter: &TopologyFilter,
        limit: u32,
    ) -> Result<SearchResults, QueryError>;

    /// Content.
    async fn transmission(
        &self,
        caller: &Caller,
        id: TransmissionId,
    ) -> Result<Option<Transmission>, QueryError>;

    /// Content.
    async fn topics(
        &self,
        caller: &Caller,
        version: Option<TopicModelVersion>,
    ) -> Result<Vec<Topic>, QueryError>;

    /// Content. When `request.layout` names a layout that is no longer
    /// current, returns `StaleProjection` with the current token and no
    /// points, so a client never merges points from two layouts.
    async fn projection(
        &self,
        caller: &Caller,
        request: &ProjectionRequest,
    ) -> Result<Projection, QueryError>;

    /// Audit. One page of the audit log, newest first.
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
    Store {
        reason: String,
    },
    NotFound,
    Forbidden,
    BadRequest {
        reason: String,
    },
    /// A cursor the surface did not issue, issued for a different list or
    /// request, or no longer resumable (its pinned topic version is gone).
    /// The client restarts from the first page.
    InvalidCursor,
    /// The projection layout the client holds is no longer current.
    StaleProjection {
        current: ProjectionToken,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SinkError {
    Unreachable { reason: String },
    Rejected { status: u16 },
}
