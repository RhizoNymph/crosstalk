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

use crate::aggregates::alert::Alert;
use crate::aggregates::edge::{TopologyFilter, TopologyGraph, Weighting};
use crate::aggregates::series::{SeriesGrid, SeriesGrouping, TopologySeries};
use crate::aggregates::topic::{Topic, TopicModelVersion};
use crate::aggregates::topic_history::{TopicLineage, TopicSizes, TopicVersionHistory};
use crate::derived::flow::channel::Channel;
use crate::derived::flow::transmission::Transmission;
use crate::ids::{AlertId, ChannelId, EventId, OperatorId, TransmissionId};
use crate::interfaces::l2_transport::ConsumerGroup;
use crate::interfaces::l6_analysis::{SearchHit, SearchQuery};
use crate::observed::agent::MergeRequest;
use crate::support::TimeWindow;

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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Permission {
    /// Topology, series, channels, alerts, and the topic history (versions,
    /// sizes, lineage): ids, counts, times and similarities, no message
    /// content and no topic labels or terms.
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

    async fn alerts(&self, caller: &Caller, filter: &AlertFilter)
    -> Result<Vec<Alert>, QueryError>;

    async fn topology(
        &self,
        caller: &Caller,
        window: TimeWindow,
        weighting: Weighting,
        filter: &TopologyFilter,
    ) -> Result<TopologyGraph, QueryError>;

    /// Needs [`Permission::View`]. Exactly [`EdgeStore::series`]; a grid
    /// for another bucket width is `BadRequest`, like an unaligned graph
    /// window.
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

    /// Needs [`Permission::View`].
    async fn topic_versions(&self, caller: &Caller) -> Result<TopicVersionHistory, QueryError>;

    /// Needs [`Permission::View`]. `None` is the active version. An unknown
    /// version is `NotFound`; a fitting one is `BadRequest`.
    async fn topic_sizes(
        &self,
        caller: &Caller,
        version: Option<TopicModelVersion>,
        window: Option<TimeWindow>,
    ) -> Result<TopicSizes, QueryError>;

    /// Needs [`Permission::View`]. The lineage from `from` to its successor;
    /// `None` while it has none. An unknown version is `NotFound`.
    async fn topic_lineage(
        &self,
        caller: &Caller,
        from: TopicModelVersion,
    ) -> Result<Option<TopicLineage>, QueryError>;

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
}

/// The policy an operator asks for. The surface stamps the author and time
/// from the authenticated caller; callers cannot supply them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PolicyKind {
    Unreviewed,
    Sanctioned,
    Unsanctioned,
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
