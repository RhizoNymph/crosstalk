//! L8 surface: the query API, the UI's data, operator actions and alert
//! delivery.
//!
//! Operator actions flow back down the stack: policy changes are published
//! as `PolicyChanged` (applied by L5), and agent merges go to L3's identity
//! resolver.
//!
//! Implementations:
//! - `QueryApi`: the axum HTTP service backing `GraphView` (topology, edge
//!   share, the time brush and trend lines), `TopicHistory` (versions, sizes,
//!   lineage) and `ContentExplorer` (search, topics, UMAP).
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

pub mod lists;

use crate::aggregates::alert::{Alert, AlertRuleDef};
use crate::aggregates::edge::{
    EdgeSelector, EdgeTransmissionPage, TopologyFilter, TopologyGraph, Weighting,
};
use crate::aggregates::projection::{Projection, ProjectionToken};
use crate::aggregates::series::{SeriesGrid, SeriesGrouping, TopologySeries};
use crate::aggregates::topic::{Topic, TopicModelVersion};
use crate::aggregates::topic_history::{TopicLineage, TopicSizes, TopicVersionHistory};
use crate::derived::flow::channel::Channel;
use crate::derived::flow::channel::policy::Policy;
use crate::derived::flow::transmission::Transmission;
use crate::ids::{AlertId, ChannelId, EventId, OperatorId, TransmissionId};
use crate::interfaces::l2_transport::{ConsumerGroup, DeadLetter};
use crate::interfaces::l6_analysis::{SearchQuery, SearchResults};
use crate::observed::agent::{Agent, MergeRequest};
use crate::paging::{
    AgentList, AlertRuleList, ChannelList, DeadLetterList, EdgeTransmissionList, Page, PageRequest,
};
use crate::support::TimeWindow;

use lists::{AgentFilter, AlertRuleFilter, ChannelFilter, ProjectionRequest};

/// The authenticated caller of a query or action.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Caller {
    pub operator: OperatorId,
    pub permissions: Vec<Permission>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Permission {
    /// Topology, series, the transmissions behind an edge (ids, times, byte
    /// counts and topic ids), channels, agents, alert rules, alerts and the
    /// topic history (versions, sizes, lineage): ids, counts, times and
    /// similarities, no message content and no topic labels or terms.
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
}

/// The policy an operator asks for. The surface stamps the author and time
/// from the authenticated caller; callers cannot supply them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PolicyKind {
    Unreviewed,
    Sanctioned,
    Unsanctioned,
}

impl PolicyKind {
    pub fn of(policy: &Policy) -> Self {
        match policy {
            Policy::Unreviewed(_) => Self::Unreviewed,
            Policy::Sanctioned(_) => Self::Sanctioned,
            Policy::Unsanctioned(_) => Self::Unsanctioned,
        }
    }
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

pub trait OperatorActions {
    async fn act(&self, caller: &Caller, action: OperatorAction) -> Result<(), QueryError>;
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
