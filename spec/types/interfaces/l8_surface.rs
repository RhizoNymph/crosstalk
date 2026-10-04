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
//! **Linked views.** `topology`, `series`, `search`, `edge_transmissions`
//! and `fit_projection` take the same [`TopologyFilter`] and apply it as
//! [`TopologyFilter::admits`] defines, so a selection in one view narrows
//! the others to the same transmissions. The filter's
//! [`TopicVersionSelector`] is resolved as [`crate::aggregates::filter`]
//! defines; each response reports the version it resolved to, and a client
//! links views by pinning that version in the others. A pinned version that
//! is unknown is `NotFound`, still fitting or never activated is a
//! `Conflict`, and no longer retained is `VersionNotRetained`. A filter
//! naming topics outside the resolved version is
//! `Conflict(TopicsNotInVersion)`.
//!
//! **Watermarks.** `topology`, `series`, `edge_transmissions` and
//! `topic_sizes` return their result [`Watermarked`]: with L7's watermark,
//! read before the data. Everything in the result before the watermark is
//! final ([`crate::aggregates::watermark`]). `watermark` returns the current
//! one, and the feed reports each advance.
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
//! **Errors.** Every method fails with a [`QueryError`]. How each store's
//! error becomes one is defined once, by the `From` impls in
//! [`query_errors`].

pub mod audit;
pub mod lists;
pub mod live;
pub mod operators;
pub mod query_errors;

use std::fmt;

use crate::aggregates::alert::{Alert, AlertRuleDef, RuleName, UserRule};
use crate::aggregates::edge::{
    EdgeSelector, EdgeTransmissionPage, TopologyFilter, TopologyGraph, Weighting,
};
use crate::aggregates::filter::TopicVersionSelector;
use crate::aggregates::projection::{
    FitFailure, Projection, ProjectionInfo, ProjectionParams, ProjectionStatusKind,
};
use crate::aggregates::quality::DetectionQuality;
use crate::aggregates::series::{SeriesGrid, SeriesGrouping, TopologySeries};
use crate::aggregates::topic::TopicModelVersion;
use crate::aggregates::topic_history::{TopicLineage, TopicSizes, TopicVersionHistory};
use crate::aggregates::watermark::{Watermark, Watermarked};
use crate::derived::flow::channel::Channel;
use crate::derived::flow::channel::policy::PolicyHistory;
use crate::derived::flow::resource::ResourcePattern;
use crate::derived::flow::transmission::Transmission;
use crate::derived::flow::verdict::{Verdict, VerdictLog};
use crate::ids::{
    AgentId, AlertId, AlertRuleId, ChannelId, EventId, MergeId, OperatorId, ProjectionId, SinkId,
    TopicId, TransmissionId,
};
use crate::interfaces::l2_transport::{ConsumerGroup, DeadLetter};
use crate::interfaces::l6_analysis::SearchResults;
use crate::observed::agent::{Agent, AgentLabel, MergeRequest};
use crate::paging::{
    AgentList, AlertList, AlertRuleList, AuditList, ChannelList, DeadLetterList,
    EdgeTransmissionList, Page, PageRequest, ProjectionList, SearchList, TopicList,
};
use crate::support::{TimeWindow, Timestamp};

use audit::{AuditEntry, AuditFilter, AuditSubject};
use lists::{AgentFilter, AlertRuleFilter, ChannelFilter, SearchRequest, TopicPage};
use operators::Operator;

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
    /// Topology, series, the transmissions behind an edge (ids, times, byte
    /// counts and topic ids), channels, channel policy history, agents, alert
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
/// return `InvalidCursor` for a cursor the surface did not issue or issued
/// for a different request.
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
}

/// `OperatorAction` is `PartialEq` but not `Eq`: user rules hold
/// similarity thresholds, which are floats.
#[derive(Debug, Clone, PartialEq)]
pub enum OperatorAction {
    SetPolicy {
        channel: ChannelId,
        policy: PolicyKind,
        note: Option<String>,
    },
    /// Built with `MergeAuthor::Operator` of the caller; self-merges cannot
    /// be expressed. Both agents must be canonical. Returns
    /// `ActionOutcome::Merged` with the new record's id.
    MergeAgents(MergeRequest),
    /// Revert one merge record exactly (`IdentityResolver::unmerge`).
    Unmerge {
        merge: MergeId,
    },
    /// Set (`Some`) or clear (`None`) an active agent's display label. A
    /// merged agent is refused, not redirected.
    RenameAgent {
        agent: AgentId,
        label: Option<AgentLabel>,
    },
    /// Attach `pattern` to a discovered channel, making it declared.
    PromoteChannel {
        channel: ChannelId,
        pattern: ResourcePattern,
    },
    Acknowledge {
        alert: AlertId,
    },
    Resolve {
        alert: AlertId,
        note: Option<String>,
    },
    /// Create an enabled user rule. The server assigns its id and returns
    /// `ActionOutcome::RuleCreated`. "Watch this topic" is
    /// [`UserRule::watch_topic`].
    CreateRule {
        name: RuleName,
        rule: UserRule,
        sinks: Vec<SinkId>,
    },
    /// Set (`Some`) or withdraw (`None`) the operator's verdict on a
    /// transmission (`TransmissionVerdicts::set`). The transmission's state
    /// never changes. `Applied` when a record was appended, `Unchanged` when
    /// the verdict was already current; an unknown transmission is
    /// `NotFound`, and a `Detected` or `AwaitingContent` one is
    /// `Conflict(TransmissionNotJudgeable)`. A `FalseDetection` verdict
    /// suppresses the transmission's active alerts once L6 sees `VerdictSet`.
    SetVerdict {
        transmission: TransmissionId,
        verdict: Option<Verdict>,
        note: Option<String>,
    },
    /// Replace a user rule's name, definition and sinks. A stale rule is
    /// retargeted to the current version or model and enabled.
    UpdateRule {
        id: AlertRuleId,
        name: RuleName,
        rule: UserRule,
        sinks: Vec<SinkId>,
    },
    /// Enable or disable any rule. Staleness cannot be set.
    SetRuleEnabled {
        id: AlertRuleId,
        enabled: bool,
    },
    /// Redeliver a dead-lettered envelope to its consumer group.
    ReplayDeadLetter {
        group: ConsumerGroup,
        id: EventId,
    },
    /// Keep `version`'s data whatever the retention policy
    /// (`TopicCatalog::pin`), stamped with the caller and the acceptance
    /// time. `Applied` when it pins, `Unchanged` when already pinned;
    /// `NotFound` for an unknown version, `Conflict(TopicVersionFitting)` for
    /// a fitting one and `Conflict(TopicVersionDropped)` for a dropped one.
    PinTopicVersion {
        version: TopicModelVersion,
    },
    /// Remove `version`'s pin (`TopicCatalog::unpin`); retention may then
    /// drop it. `Applied` when it was pinned, `Unchanged` otherwise (a
    /// dropped version included); `NotFound` for an unknown version.
    UnpinTopicVersion {
        version: TopicModelVersion,
    },
}

/// Which action, without its arguments. The audit log filters on it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ActionKind {
    SetPolicy,
    MergeAgents,
    Unmerge,
    RenameAgent,
    PromoteChannel,
    Acknowledge,
    Resolve,
    SetVerdict,
    CreateRule,
    UpdateRule,
    SetRuleEnabled,
    ReplayDeadLetter,
    PinTopicVersion,
    UnpinTopicVersion,
}

impl OperatorAction {
    pub fn kind(&self) -> ActionKind {
        match self {
            Self::SetPolicy { .. } => ActionKind::SetPolicy,
            Self::MergeAgents(_) => ActionKind::MergeAgents,
            Self::Unmerge { .. } => ActionKind::Unmerge,
            Self::RenameAgent { .. } => ActionKind::RenameAgent,
            Self::PromoteChannel { .. } => ActionKind::PromoteChannel,
            Self::Acknowledge { .. } => ActionKind::Acknowledge,
            Self::Resolve { .. } => ActionKind::Resolve,
            Self::SetVerdict { .. } => ActionKind::SetVerdict,
            Self::CreateRule { .. } => ActionKind::CreateRule,
            Self::UpdateRule { .. } => ActionKind::UpdateRule,
            Self::SetRuleEnabled { .. } => ActionKind::SetRuleEnabled,
            Self::ReplayDeadLetter { .. } => ActionKind::ReplayDeadLetter,
            Self::PinTopicVersion { .. } => ActionKind::PinTopicVersion,
            Self::UnpinTopicVersion { .. } => ActionKind::UnpinTopicVersion,
        }
    }

    /// The permission the caller must hold, checked before any effect; a
    /// caller without it gets `Forbidden`. Govern for identity, policy,
    /// rules and topic-version pins, Triage for alerts and verdicts, Operate
    /// for the pipeline. No action needs View, Content or Audit, which are
    /// read permissions.
    ///
    /// `SetVerdict` needs Triage alone, not Content as well: it reveals no
    /// content (its outcome and the records it writes hold no message text),
    /// and reading the text to judge from is already gated by `transmission`
    /// and `search`. One permission per action keeps `OperatorRecord`'s
    /// `Forbidden` check exact.
    pub fn required_permission(&self) -> Permission {
        match self {
            Self::SetPolicy { .. }
            | Self::MergeAgents(_)
            | Self::Unmerge { .. }
            | Self::RenameAgent { .. }
            | Self::PromoteChannel { .. }
            | Self::CreateRule { .. }
            | Self::UpdateRule { .. }
            | Self::SetRuleEnabled { .. }
            | Self::PinTopicVersion { .. }
            | Self::UnpinTopicVersion { .. } => Permission::Govern,
            Self::Acknowledge { .. } | Self::Resolve { .. } | Self::SetVerdict { .. } => {
                Permission::Triage
            }
            Self::ReplayDeadLetter { .. } => Permission::Operate,
        }
    }

    /// The entities the action names, as requested (not resolved through
    /// merges). The audit log's subject filter matches these, together with
    /// any id the outcome created ([`ActionOutcome::subject`]). A dead-letter
    /// replay names no entity, and a rule creation names none until its
    /// outcome (`RuleCreated`) carries the new rule's id.
    pub fn subjects(&self) -> Vec<AuditSubject> {
        match self {
            Self::SetPolicy { channel, .. } | Self::PromoteChannel { channel, .. } => {
                vec![AuditSubject::Channel(*channel)]
            }
            Self::MergeAgents(request) => vec![
                AuditSubject::Agent(request.source()),
                AuditSubject::Agent(request.target()),
            ],
            Self::Unmerge { merge } => vec![AuditSubject::Merge(*merge)],
            Self::RenameAgent { agent, .. } => vec![AuditSubject::Agent(*agent)],
            Self::Acknowledge { alert } | Self::Resolve { alert, .. } => {
                vec![AuditSubject::Alert(*alert)]
            }
            Self::SetVerdict { transmission, .. } => {
                vec![AuditSubject::Transmission(*transmission)]
            }
            Self::CreateRule { .. } => Vec::new(),
            Self::UpdateRule { id, .. } | Self::SetRuleEnabled { id, .. } => {
                vec![AuditSubject::Rule(*id)]
            }
            Self::PinTopicVersion { version } | Self::UnpinTopicVersion { version } => {
                vec![AuditSubject::TopicVersion(*version)]
            }
            Self::ReplayDeadLetter { .. } => Vec::new(),
        }
    }
}

pub trait OperatorActions {
    /// Check the permission, apply the action and record it. A call that
    /// returns `Ok` or an `ActionError` other than `Store` leaves exactly one
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
    /// A semantic query whose text could not be embedded (too long for the
    /// model).
    QueryNotEmbeddable,
    /// Search text longer than the embedding model's context.
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

/// What an accepted operator action did, including any ids it created so
/// the UI can navigate to them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ActionOutcome {
    /// The action changed state.
    Applied,
    /// Accepted, but the state already matched: acknowledging an
    /// acknowledged alert, or the losing request of a race.
    Unchanged,
    RuleCreated(AlertRuleId),
    ChannelPromoted(ChannelId),
    Merged(MergeId),
}

impl ActionOutcome {
    /// The entity the outcome names, when it names one. A merge's id exists
    /// only once the merge is recorded, so this is the only place an audit
    /// entry can take it from.
    pub fn subject(self) -> Option<AuditSubject> {
        match self {
            Self::Applied | Self::Unchanged => None,
            Self::RuleCreated(rule) => Some(AuditSubject::Rule(rule)),
            Self::ChannelPromoted(channel) => Some(AuditSubject::Channel(channel)),
            Self::Merged(merge) => Some(AuditSubject::Merge(merge)),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SinkError {
    Unreachable { reason: String },
    Rejected { status: u16 },
}
