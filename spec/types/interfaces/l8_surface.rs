//! L8 surface: the query API, the UI's data, operator actions, the live
//! feed, the audit log and alert delivery.
//!
//! Operator actions flow back down the stack: policy changes are published
//! as `PolicyChanged` (applied by L5); agent merges, unmerges and renames go
//! to L3's identity resolver; channel promotion goes to L5
//! (`ChannelRegistry::promote`), and so do verdicts on transmissions
//! (`TransmissionVerdicts::set`); alert rule management goes to L6's
//! `AlertRuleStore`; topic-version pins go to L6's `TopicCatalog` (`pin`,
//! `unpin`). Every action names its
//! permission ([`OperatorAction::required_permission`]), checked before any
//! effect. Wherever an action records an author or time, the surface stamps
//! them from the authenticated caller and the time it accepted the action;
//! callers cannot supply them. Every action call, whatever its outcome,
//! leaves one [`audit::AuditEntry`], and so does every change config makes.
//! Acknowledging or resolving an alert changes the alert store, so the
//! surface publishes `AlertChanged` and `Changed::Alert` for it; every other
//! action's store publishes its own `Changed`.
//!
//! **Callers.** A [`Caller`] is built only by the [`operators::OperatorDirectory`]
//! for one request, from the operator config defines: in trusted mode the
//! one configured operator with every permission, otherwise the operator
//! the request's verified session names, with that operator's permissions.
//!
//! Implementations:
//! - `QueryApi`: the axum HTTP service backing `GraphView` (topology, edge
//!   share, the time brush and trend lines), `TopicHistory` (versions, sizes,
//!   lineage), `ContentExplorer` (search, topics, UMAP), the channel policy
//!   history, the operator directory and the audit log.
//! - `LiveFeed` ([`live`]): the SSE endpoint that tells the UI, by id, what
//!   to re-query.
//! - `AuditLog` ([`audit`]): `PgAuditLog`, append-only.
//! - `AlertSink`: `WebhookSink`, `SlackSink`, `LogSink`. Each configured
//!   sink has a [`SinkId`]; an alert is delivered to the sinks its rule
//!   lists, and `QueryApi::sinks` reports each sink's last delivery.
//!
//! **Lists.** Channels, agents, alert rules, alerts, dead letters, the audit
//! log, the transmissions behind an edge, search hits, the topics of a
//! version and stored projections are read a page at a time with the
//! cursors of [`crate::paging`], so a traversal is stable under concurrent
//! inserts. Their filters and request types are in [`lists`]. Whole values
//! with their own invariants (a policy history, the topic version history,
//! topic sizes, a lineage, a graph, a series grid) are returned whole.
//!
//! **Linked views.** `topology`, `channel_topology`, `series`, `search`,
//! `edge_transmissions` and `fit_projection` take the same
//! [`TopologyFilter`] and apply it as [`TopologyFilter::admits`] defines
//! (and, for the channel-centred view's accesses,
//! [`TopologyFilter::admits_access`]), so a selection in one view narrows
//! the others to the same transmissions. The filter's
//! [`TopicVersionSelector`] is resolved as [`crate::aggregates::filter`]
//! defines; each response reports the version it resolved to, and a client
//! links views by pinning that version in the others. A pinned version that
//! is unknown is `NotFound`, still fitting or never activated is a
//! `Conflict`, and no longer retained is `VersionNotRetained`. A filter
//! naming topics outside the resolved version is
//! `Conflict(TopicsNotInVersion)`.
//!
//! **Aliases.** Merged agents and superseded channels are resolved at read
//! time ([`crate::aliases`]): every id a response names is canonical, and
//! every id a request names is resolved before matching. Actions that change
//! a channel (policy, promotion) refuse a superseded one with
//! `Conflict(ChannelSuperseded)`, naming the channel to act on instead.
//!
//! **Watermarks.** `topology`, `channel_topology`, `series`,
//! `edge_transmissions`, `channel_resources` and `topic_sizes` return their
//! result [`Watermarked`]: with L7's watermark (`EdgeStore::watermark`),
//! read before the data. They all count by event time (`Confirmed::at`,
//! `Access::at`), so everything in the result before the watermark is final
//! ([`crate::aggregates::watermark`]). `watermark` returns the current one,
//! and the feed reports each advance. A stored projection is not wrapped:
//! its frame is fixed when fitted and carries the watermark its sample was
//! read under (`Projection::watermark`, from `Fitted::watermark`).
//!
//! **Retention.** A topic-model version that retention has dropped
//! ([`crate::aggregates::retention`]) is `VersionNotRetained` wherever its
//! buckets or assignments would be read; its history entry, topics, lineage
//! and all-time sizes stay readable.
//!
//! **Projections.** `fit_projection` resolves and pins the filter's version,
//! records a queued job and returns its id at once; the fit runs in the
//! background ([`crate::aggregates::projection`]). `projection_status` and
//! `projections` report jobs; `projection` returns a ready projection's
//! stored frame, identical on every read until its frame expires.
//!
//! **Export.** `export` streams one dataset (transmissions, edge or access
//! buckets, topics, a stored projection, verdicts) between a header and a
//! trailer, reading only data settled before the watermark read at its
//! start ([`export`]). It is not paged and every export is audited.
//!
//! **Errors.** Every method fails with a [`QueryError`]. How each store's
//! error becomes one is defined once, by the `From` impls in
//! [`query_errors`].

pub mod actions;
pub mod audit;
pub mod errors;
pub mod export;
pub mod lists;
pub mod live;
pub mod operators;
pub mod query_errors;

use std::fmt;

use crate::aggregates::access::{BipartiteGraph, ResourceUsePage};
use crate::aggregates::alert::{Alert, AlertRuleDef};
use crate::aggregates::edge::{
    EdgeSelector, EdgeTransmissionPage, TopologyFilter, TopologyGraph, Weighting,
};
use crate::aggregates::filter::TopicVersionSelector;
use crate::aggregates::projection::{Projection, ProjectionInfo, ProjectionParams};
use crate::aggregates::quality::DetectionQuality;
use crate::aggregates::series::{SeriesGrid, SeriesGrouping, TopologySeries};
use crate::aggregates::topic::TopicModelVersion;
use crate::aggregates::topic_history::{TopicLineage, TopicSizes, TopicVersionHistory};
use crate::aggregates::watermark::{Watermark, Watermarked};
use crate::derived::flow::channel::Channel;
use crate::derived::flow::channel::policy::PolicyHistory;
use crate::derived::flow::transmission::Transmission;
use crate::derived::flow::verdict::VerdictLog;
use crate::ids::{ChannelId, OperatorId, ProjectionId, SinkId, TransmissionId};
use crate::interfaces::l2_transport::{ConsumerGroup, DeadLetter};
use crate::interfaces::l6_analysis::SearchResults;
use crate::observed::agent::Agent;
use crate::paging::{
    AgentList, AlertList, AlertRuleList, AuditList, ChannelList, DeadLetterList,
    EdgeTransmissionList, Page, PageRequest, ProjectionList, ResourceUseList, SearchList,
    TopicList,
};
use crate::support::{TimeWindow, Timestamp};

use audit::{AuditEntry, AuditFilter};
use export::{Export, ExportRequest, ExportStream};
use lists::{AgentFilter, AlertRuleFilter, ChannelFilter, SearchRequest, TopicPage};
use operators::Operator;

pub use actions::{ActionKind, ActionOutcome, OperatorAction};
pub use errors::{ActionError, ConflictKind, InputError, QueryError};

/// The policy an operator asks for. The surface stamps the author and time
/// from the authenticated caller; callers cannot supply them.
pub use crate::derived::flow::channel::policy::PolicyKind;

/// The authenticated caller of one request: an operator and the
/// permissions it holds.
///
/// Built only by [`OperatorDirectory::caller`](operators::OperatorDirectory::caller),
/// so its permissions are always those config gives its operator, and it
/// always holds at least one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Caller {
    operator: OperatorId,
    permissions: PermissionSet,
}

impl Caller {
    pub fn operator(&self) -> OperatorId {
        self.operator
    }

    pub fn permissions(&self) -> PermissionSet {
        self.permissions
    }

    pub fn has(&self, permission: Permission) -> bool {
        self.permissions.contains(permission)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum Permission {
    /// Topology (agent-centred and channel-centred, with node metadata and
    /// harness claims), series, the transmissions behind an edge (ids, times,
    /// byte counts and topic ids), channels, a channel's resources and who
    /// used them, channel policy history, agents, alert
    /// rules, alerts and the topic history (versions, sizes, lineage): ids,
    /// counts, times and similarities, no message content and no topic
    /// labels or terms. Also verdict logs and detection quality.
    View,
    /// Transmission content, search, topics (their labels and terms come
    /// from message text) and projections: fitting them, their jobs and
    /// their points.
    Content,
    /// Identity and policy: channel policy and promotion, agent merges,
    /// unmerges and renames, alert rules and their sinks (what the gateway
    /// alerts on, and where), and topic-version pins (what history the
    /// gateway keeps).
    Govern,
    /// Work alerts: acknowledge and resolve. Judge transmissions: set and
    /// withdraw verdicts.
    Triage,
    /// Operate the pipeline: list and replay dead-lettered deliveries. A
    /// replay re-runs a consumer on an old event, so it can reopen alerts or
    /// re-apply stale decisions.
    Operate,
    /// Read the audit log: every operator action, who asked for it and
    /// what came of it, including refused ones, and every change config
    /// made.
    Audit,
}

impl Permission {
    pub const ALL: [Self; 6] = [
        Self::View,
        Self::Content,
        Self::Govern,
        Self::Triage,
        Self::Operate,
        Self::Audit,
    ];

    const fn bit(self) -> u8 {
        1 << self as u8
    }
}

/// A set of permissions.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct PermissionSet(u8);

impl PermissionSet {
    pub const EMPTY: Self = Self(0);

    /// Every permission: what the trusted operator holds.
    pub const ALL: Self = {
        let mut bits = 0;
        let mut i = 0;
        while i < Permission::ALL.len() {
            bits |= Permission::ALL[i].bit();
            i += 1;
        }
        Self(bits)
    };

    pub fn of(permissions: impl IntoIterator<Item = Permission>) -> Self {
        Self(
            permissions
                .into_iter()
                .fold(0, |bits, permission| bits | permission.bit()),
        )
    }

    pub fn contains(self, permission: Permission) -> bool {
        self.0 & permission.bit() != 0
    }

    pub fn is_empty(self) -> bool {
        self.0 == 0
    }

    /// In `Permission::ALL` order.
    pub fn iter(self) -> impl Iterator<Item = Permission> {
        Permission::ALL
            .into_iter()
            .filter(move |permission| self.contains(*permission))
    }
}

impl fmt::Debug for PermissionSet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_set().entries(self.iter()).finish()
    }
}

/// Empty `states` means every state. `channel` keeps alerts whose subject is
/// that channel or a transmission routed through it, with the listed
/// channel, the subject's channel and the transmission's route all resolved
/// through supersession: filtering on a promoted channel shows the alerts
/// still stored under the channels it superseded.
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
/// return `InvalidCursor` for a cursor the surface did not issue or issued
/// for a different request.
pub trait QueryApi {
    /// The stream `export` returns.
    type ExportRows: ExportStream;

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

    /// View. Built-in rules first, in [`BuiltinRule::ALL`] order, then user
    /// rules newest first. Every rule is listed: none is ever deleted.
    ///
    /// [`BuiltinRule::ALL`]: crate::aggregates::alert::BuiltinRule::ALL
    async fn alert_rules(
        &self,
        caller: &Caller,
        filter: &AlertRuleFilter,
        page: &PageRequest<AlertRuleList>,
    ) -> Result<Page<AlertRuleDef, AlertRuleList>, QueryError>;

    /// Govern. Every configured alert sink and how its last delivery went,
    /// for choosing a rule's sinks. Govern rather than View because a
    /// delivery error can name the sink's endpoint.
    async fn sinks(&self, caller: &Caller) -> Result<Vec<SinkInfo>, QueryError>;

    /// Operate. Dead letters of one consumer group, or of every group,
    /// newest envelope first.
    async fn dead_letters(
        &self,
        caller: &Caller,
        group: Option<&ConsumerGroup>,
        page: &PageRequest<DeadLetterList>,
    ) -> Result<Page<DeadLetter, DeadLetterList>, QueryError>;

    /// View. Newest alert first.
    async fn alerts(
        &self,
        caller: &Caller,
        filter: &AlertFilter,
        page: &PageRequest<AlertList>,
    ) -> Result<Page<Alert, AlertList>, QueryError>;

    /// View. L7's exposed watermark (`EdgeStore::watermark`).
    async fn watermark(&self, caller: &Caller) -> Result<Watermark, QueryError>;

    /// View. Exactly [`EdgeStore::graph`], under the version the filter's
    /// selector resolves to.
    ///
    /// [`EdgeStore::graph`]: crate::interfaces::l7_topology::EdgeStore::graph
    async fn topology(
        &self,
        caller: &Caller,
        window: TimeWindow,
        weighting: Weighting,
        filter: &TopologyFilter,
    ) -> Result<Watermarked<TopologyGraph>, QueryError>;

    /// View. Exactly [`EdgeStore::channel_topology`]: agents and channels as
    /// nodes, access edges (writes nobody read included) and the same
    /// transmission edges as `topology`, with the watermark read before the
    /// buckets. An unaligned window is `InvalidInput(UnalignedWindow)`.
    ///
    /// [`EdgeStore::channel_topology`]: crate::interfaces::l7_topology::EdgeStore::channel_topology
    async fn channel_topology(
        &self,
        caller: &Caller,
        window: TimeWindow,
        weighting: Weighting,
        filter: &TopologyFilter,
    ) -> Result<Watermarked<BipartiteGraph>, QueryError>;

    /// View. Exactly [`ChannelRegistry::resource_use`]: the resources of
    /// `channel`'s canonical channel accessed in `window`, newest first, with
    /// canonical writers and readers. A superseded `channel` answers for the
    /// channel that superseded it, named in the page. Unknown is `NotFound`.
    /// The surface reads L7's watermark (`EdgeStore::watermark`) before the
    /// registry: accesses are stored by event time, so every access before it
    /// is already counted.
    ///
    /// [`ChannelRegistry::resource_use`]: crate::interfaces::l5_flow::ChannelRegistry::resource_use
    async fn channel_resources(
        &self,
        caller: &Caller,
        channel: ChannelId,
        window: TimeWindow,
        page: &PageRequest<ResourceUseList>,
    ) -> Result<Watermarked<ResourceUsePage>, QueryError>;

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
    ) -> Result<Watermarked<EdgeTransmissionPage>, QueryError>;

    /// View. Exactly [`EdgeStore::series`], under the version the filter's
    /// selector resolves to; a grid for another bucket width is
    /// `InvalidInput(BucketWidthMismatch)`, like an unaligned graph window.
    ///
    /// [`EdgeStore::series`]: crate::interfaces::l7_topology::EdgeStore::series
    async fn series(
        &self,
        caller: &Caller,
        grid: SeriesGrid,
        weighting: Weighting,
        grouping: SeriesGrouping,
        filter: &TopologyFilter,
    ) -> Result<Watermarked<TopologySeries>, QueryError>;

    /// View.
    async fn topic_versions(&self, caller: &Caller) -> Result<TopicVersionHistory, QueryError>;

    /// View. `None` is the active version. An unknown version is
    /// `NotFound`; a fitting one is `Conflict(TopicVersionFitting)`. A
    /// dropped version answers without a window with its frozen all-time
    /// sizes, and with a window `VersionNotRetained`. The watermark is read
    /// from L7 before the catalog.
    async fn topic_sizes(
        &self,
        caller: &Caller,
        version: Option<TopicModelVersion>,
        window: Option<TimeWindow>,
    ) -> Result<Watermarked<TopicSizes>, QueryError>;

    /// View. The lineage from `from` to its successor; `None` while it has
    /// none. An unknown version is `NotFound`.
    async fn topic_lineage(
        &self,
        caller: &Caller,
        from: TopicModelVersion,
    ) -> Result<Option<TopicLineage>, QueryError>;

    /// Content. Embeds `request`'s text with the current embedding model
    /// for the semantic and hybrid modes, then runs [`SearchIndex::query`]: a
    /// page of admitted hits in rank order. Text too long to embed is
    /// `InvalidInput(QueryTooLong)`; a page after an embedding-model change
    /// is `Conflict(EmbeddingModelChanged)`.
    ///
    /// [`SearchIndex::query`]: crate::interfaces::l6_analysis::SearchIndex::query
    async fn search(
        &self,
        caller: &Caller,
        request: &SearchRequest,
        window: Option<TimeWindow>,
        filter: &TopologyFilter,
        page: &PageRequest<SearchList>,
    ) -> Result<SearchResults, QueryError>;

    /// Content.
    async fn transmission(
        &self,
        caller: &Caller,
        id: TransmissionId,
    ) -> Result<Option<Transmission>, QueryError>;

    /// Content. A version's topics, newest id first, and the version they
    /// belong to. `Current` is the catalog's active version; a pinned one may
    /// be any version whose fit has returned (unlike a linked view, it need
    /// not have been activated): unknown is `NotFound`, still fitting is
    /// `Conflict(TopicVersionFitting)`.
    async fn topics(
        &self,
        caller: &Caller,
        version: TopicVersionSelector,
        page: &PageRequest<TopicList>,
    ) -> Result<TopicPage, QueryError>;

    /// Content. Validate and record a projection job, and return its id
    /// without waiting for the fit. Resolves the filter's version (errors as
    /// for any linked view) and pins it, and records the current embedding
    /// model, `params` (seed included), the caller and the time. Fails with
    /// `Conflict(ProjectionQueueFull)` when
    /// [`ProjectionStore::MAX_PENDING`] jobs are pending. Each call records
    /// a new job.
    ///
    /// [`ProjectionStore::MAX_PENDING`]: crate::interfaces::l6_analysis::ProjectionStore::MAX_PENDING
    async fn fit_projection(
        &self,
        caller: &Caller,
        window: TimeWindow,
        filter: &TopologyFilter,
        params: ProjectionParams,
    ) -> Result<ProjectionId, QueryError>;

    /// Content. A job's spec, requester and status. Unknown is `NotFound`.
    async fn projection_status(
        &self,
        caller: &Caller,
        id: ProjectionId,
    ) -> Result<ProjectionInfo, QueryError>;

    /// Content. Every job, newest first.
    async fn projections(
        &self,
        caller: &Caller,
        page: &PageRequest<ProjectionList>,
    ) -> Result<Page<ProjectionInfo, ProjectionList>, QueryError>;

    /// Content. A ready projection: its job record and stored frame, the
    /// same on every call. Unknown is `NotFound`; queued or fitting is
    /// `Conflict(ProjectionNotReady)`; failed is `Conflict(ProjectionFailed)`;
    /// expired is `ProjectionNotRetained`.
    async fn projection(&self, caller: &Caller, id: ProjectionId)
    -> Result<Projection, QueryError>;

    /// View. Every verdict record of the transmission, oldest first
    /// (`TransmissionVerdicts::log`); its last record is the current
    /// verdict. An empty log for a transmission never judged, `None` for an
    /// unknown one. Records hold ids, verdicts, times and operator notes, no
    /// message content.
    async fn verdicts(
        &self,
        caller: &Caller,
        transmission: TransmissionId,
    ) -> Result<Option<VerdictLog>, QueryError>;

    /// View. Operator verdicts tallied against the detector's calls for the
    /// judgeable transmissions opened in `window`
    /// (`TransmissionVerdicts::quality`; see [`crate::aggregates::quality`]).
    /// Rows hold route kinds, match classes and counts only.
    async fn detection_quality(
        &self,
        caller: &Caller,
        window: TimeWindow,
    ) -> Result<DetectionQuality, QueryError>;

    /// Audit. The audit entries `filter` matches, operator and config
    /// alike, newest first by time and id (`AuditLog::query`).
    async fn audit(
        &self,
        caller: &Caller,
        filter: &AuditFilter,
        page: &PageRequest<AuditList>,
    ) -> Result<Page<AuditEntry, AuditList>, QueryError>;

    /// View. Every operator the directory holds, by id: the ones config
    /// defines now, and every one it defined before, listed with no
    /// permissions, so past decisions and audit entries can still show a
    /// name (`OperatorDirectory::operators`).
    async fn operators(&self, caller: &Caller) -> Result<Vec<Operator>, QueryError>;

    /// View, or Content when `request` includes content or names a
    /// projection ([`ExportRequest::required_permission`]); without it,
    /// `Forbidden { missing }` before anything is read. Reads L7's
    /// watermark first, then plans the export (`ExportSource::plan`): the
    /// filter's version resolved and pinned as for any linked view (errors
    /// as for one), the window cut at the watermark, agent and channel
    /// resolution and current verdicts captured for the whole export, the
    /// rows counted. More rows than `ExportLimits::max_rows` is
    /// `Conflict(ExportTooLarge)`; an unaligned window for edges or
    /// accesses is `InvalidInput(UnalignedWindow)`; a projection that is
    /// unknown, not ready, failed or expired fails as `projection` does.
    /// Returns the header and the stream of rows, which always ends with a
    /// trailer, `Complete` or recording why it failed. Every call is
    /// audited ([`export::record`]): `Started` is appended before the
    /// header is returned, and a failed append fails the call with `Store`.
    async fn export(
        &self,
        caller: &Caller,
        request: &ExportRequest,
    ) -> Result<Export<Self::ExportRows>, QueryError>;
}

pub trait OperatorActions {
    /// Check the permission, apply the action and record it. `SetPolicy` on
    /// a superseded channel is refused with `Conflict(ChannelSuperseded)`
    /// (read through `ChannelDirectory`) before `PolicyChanged` is
    /// published; `PromoteChannel` maps the registry's refusal
    /// (`ActionError::from`). A call that returns `Ok` or an `ActionError` other than `Store` leaves exactly one
    /// operator audit entry, whose outcome is what it returns
    /// (`AuditOutcome::of`): a `Succeeded` entry is written in the same
    /// transaction as the action's effect, and a `Forbidden` or `Rejected`
    /// one with no effect. A `Store` error had no effect and leaves at most
    /// one entry, written when the audit log is still reachable.
    async fn act(
        &self,
        caller: &Caller,
        action: OperatorAction,
    ) -> Result<ActionOutcome, ActionError>;
}

pub trait AlertSink {
    fn id(&self) -> SinkId;

    async fn deliver(&self, alert: &Alert) -> Result<(), SinkError>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SinkKind {
    Webhook,
    Slack,
    Log,
}

/// A configured sink, as `QueryApi::sinks` reports it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SinkInfo {
    pub id: SinkId,
    pub kind: SinkKind,
    /// The name from config.
    pub name: String,
    /// When its last delivery succeeded, or why it failed. `None` before
    /// its first delivery.
    pub last_delivery: Option<Result<Timestamp, SinkError>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SinkError {
    Unreachable { reason: String },
    Rejected { status: u16 },
}
