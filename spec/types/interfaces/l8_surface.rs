//! L8 surface: the query API, the UI's data, operator actions, the live
//! feed, the audit log and alert delivery.
//!
//! Operator actions flow back down the stack: policy changes are published
//! as `PolicyChanged` (applied by L5); agent merges, unmerges and labels go
//! to L3's identity resolver; channel promotion and transmission dismissal go
//! to L5 (`ChannelRegistry::promote`, `TransmissionReview::dismiss`); alert
//! rule management goes to L6's `AlertRuleStore`. Every action names its
//! permission ([`OperatorAction::required_permission`]), checked before any
//! effect. Wherever an action records an author or time, the surface stamps
//! them from the authenticated caller and the time it accepted the action;
//! callers cannot supply them. Every action call, whatever its outcome,
//! leaves one [`audit::AuditRecord`].
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
//! **Lists.** Channels, agents, alert rules, dead letters, the audit log and
//! the transmissions behind an edge are read a page at a time with the cursors
//! of [`crate::paging`], so a traversal is stable under concurrent inserts.
//! Their filters and request types are in [`lists`].
//!
//! **Linked views.** `topology`, `channel_topology`, `search`, `projection`
//! and `edge_transmissions` take the same [`TopologyFilter`] and apply it as
//! [`TopologyFilter::admits`] defines (and, for the channel-centred view's
//! accesses, [`TopologyFilter::admits_access`]), so a selection in one view
//! narrows the others to the same transmissions. Each response reports the
//! topic-model version its topics are under; responses with different
//! versions are not linkable and the client re-queries.
//!
//! **Aliases.** Merged agents and superseded channels are resolved at read
//! time ([`crate::aliases`]): every id a response names is canonical, and
//! every id a request names is resolved before matching. Actions that change
//! a channel (policy, promotion) refuse a superseded one with
//! `Conflict(ChannelSuperseded)`, naming the channel to act on instead.

pub mod audit;
pub mod lists;
pub mod live;

use crate::aggregates::access::{BipartiteGraph, ResourceUsePage};
use crate::aggregates::alert::{Alert, AlertRuleDef, RuleStatus};
use crate::aggregates::edge::{
    EdgeSelector, EdgeTransmissionPage, TopologyFilter, TopologyGraph, Weighting,
};
use crate::aggregates::projection::{Projection, ProjectionToken};
use crate::aggregates::series::{SeriesGrid, SeriesGrouping, TopologySeries};
use crate::aggregates::topic::{Topic, TopicModelVersion};
use crate::aggregates::topic_history::{TopicLineage, TopicSizes, TopicVersionHistory};
use crate::derived::flow::channel::Channel;
use crate::derived::flow::channel::policy::PolicyHistory;
use crate::derived::flow::channel::promotion::PromotionRefusal;
use crate::derived::flow::resource::ResourcePattern;
use crate::derived::flow::transmission::Transmission;
use crate::ids::{
    AgentId, AlertId, AlertRuleId, ChannelId, EventId, MergeId, OperatorId, TransmissionId,
};
use crate::interfaces::l2_transport::{ConsumerGroup, DeadLetter};
use crate::interfaces::l5_flow::{PromoteError, RegistryError};
use crate::interfaces::l6_analysis::{RuleRequest, SearchQuery, SearchResults};
use crate::observed::agent::{Agent, AgentLabel, MergeRequest};
use crate::paging::{
    AgentList, AlertRuleList, AuditList, ChannelList, DeadLetterList, EdgeTransmissionList, Page,
    PageRequest, ResourceUseList,
};
use crate::support::TimeWindow;

use audit::{AuditFilter, AuditRecord};
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
    /// Topology (agent-centred and channel-centred, with node metadata and
    /// harness claims), series, the transmissions behind an edge (ids, times,
    /// byte counts and topic ids), channels, a channel's resources and who
    /// used them, channel policy history, agents, alert
    /// rules, alerts and the topic history (versions, sizes, lineage): ids,
    /// counts, times and similarities, no message content and no topic
    /// labels or terms.
    View,
    /// Transmission content, search, topics (their labels and terms come
    /// from message text) and projections.
    Content,
    /// Identity and policy: channel policy and promotion, agent merges,
    /// unmerges and labels, and alert rules (what the gateway alerts on).
    Govern,
    /// Work alerts: acknowledge, resolve, and dismiss the suspected
    /// transmissions they are about.
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

    /// View. Exactly [`EdgeStore::channel_topology`]: agents and channels as
    /// nodes, access edges (writes nobody read included) and the same
    /// transmission edges as `topology`. An unaligned window is
    /// `InvalidInput(UnalignedWindow)`.
    ///
    /// [`EdgeStore::channel_topology`]: crate::interfaces::l7_topology::EdgeStore::channel_topology
    async fn channel_topology(
        &self,
        caller: &Caller,
        window: TimeWindow,
        weighting: Weighting,
        filter: &TopologyFilter,
    ) -> Result<BipartiteGraph, QueryError>;

    /// View. Exactly [`ChannelRegistry::resource_use`]: the resources of
    /// `channel`'s canonical channel accessed in `window`, newest first, with
    /// canonical writers and readers. A superseded `channel` answers for the
    /// channel that superseded it, named in the page. Unknown is `NotFound`.
    ///
    /// [`ChannelRegistry::resource_use`]: crate::interfaces::l5_flow::ChannelRegistry::resource_use
    async fn channel_resources(
        &self,
        caller: &Caller,
        channel: ChannelId,
        window: TimeWindow,
        page: &PageRequest<ResourceUseList>,
    ) -> Result<ResourceUsePage, QueryError>;

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
    /// is `InvalidInput(BucketWidthMismatch)`, like an unaligned graph window.
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
    /// `NotFound`; a fitting one is `Conflict(TopicVersionFitting)`.
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

    /// Audit. The audit records `filter` matches, newest first by time and
    /// id (`AuditLog::query`).
    async fn audit(
        &self,
        caller: &Caller,
        filter: &AuditFilter,
        page: &PageRequest<AuditList>,
    ) -> Result<Page<AuditRecord, AuditList>, QueryError>;
}

/// `OperatorAction` is `PartialEq` but not `Eq`: rule requests hold
/// similarity thresholds, which are floats.
#[derive(Debug, Clone, PartialEq)]
pub enum OperatorAction {
    SetPolicy {
        channel: ChannelId,
        policy: PolicyKind,
        note: Option<String>,
    },
    /// Built with `MergeAuthor::Operator` of the caller; self-merges cannot
    /// be expressed.
    MergeAgents(MergeRequest),
    /// Undo `agent`'s merge exactly (`IdentityResolver::unmerge`).
    UnmergeAgent {
        agent: AgentId,
    },
    /// Set (`Some`) or clear (`None`) the display label of `agent`'s
    /// canonical agent.
    LabelAgent {
        agent: AgentId,
        label: Option<AgentLabel>,
    },
    /// Promote a discovered channel: attach `pattern`, making it declared
    /// under the same id, record `policy` (with `note`) as the operator's
    /// decision, and supersede every other discovered channel whose seed the
    /// pattern matches. The surface stamps the operator and time into a
    /// `Promotion` and calls `ChannelRegistry::promote`; success is
    /// `ChannelPromoted(channel)`, the same id.
    PromoteChannel {
        channel: ChannelId,
        pattern: ResourcePattern,
        policy: PolicyKind,
        note: Option<String>,
    },
    Acknowledge {
        alert: AlertId,
    },
    Resolve {
        alert: AlertId,
        note: Option<String>,
    },
    /// Discard a suspected transmission with reason `Dismissed`, which
    /// suppresses its `SuspectedTransmission` alerts.
    DismissTransmission {
        transmission: TransmissionId,
        note: Option<String>,
    },
    /// The client chooses the rule's id (a ULID), so a retried create is
    /// idempotent.
    CreateAlertRule {
        id: AlertRuleId,
        rule: RuleRequest,
        status: RuleStatus,
    },
    UpdateAlertRule {
        rule: AlertRuleId,
        definition: RuleRequest,
    },
    SetAlertRuleStatus {
        rule: AlertRuleId,
        status: RuleStatus,
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
    UnmergeAgent,
    LabelAgent,
    PromoteChannel,
    Acknowledge,
    Resolve,
    DismissTransmission,
    CreateAlertRule,
    UpdateAlertRule,
    SetAlertRuleStatus,
    ReplayDeadLetter,
}

impl OperatorAction {
    pub fn kind(&self) -> ActionKind {
        match self {
            Self::SetPolicy { .. } => ActionKind::SetPolicy,
            Self::MergeAgents(_) => ActionKind::MergeAgents,
            Self::UnmergeAgent { .. } => ActionKind::UnmergeAgent,
            Self::LabelAgent { .. } => ActionKind::LabelAgent,
            Self::PromoteChannel { .. } => ActionKind::PromoteChannel,
            Self::Acknowledge { .. } => ActionKind::Acknowledge,
            Self::Resolve { .. } => ActionKind::Resolve,
            Self::DismissTransmission { .. } => ActionKind::DismissTransmission,
            Self::CreateAlertRule { .. } => ActionKind::CreateAlertRule,
            Self::UpdateAlertRule { .. } => ActionKind::UpdateAlertRule,
            Self::SetAlertRuleStatus { .. } => ActionKind::SetAlertRuleStatus,
            Self::ReplayDeadLetter { .. } => ActionKind::ReplayDeadLetter,
        }
    }

    /// The permission the caller must hold, checked before any effect; a
    /// caller without it gets `Forbidden`. Govern for identity, policy and
    /// rules, Triage for alerts, Operate for the pipeline. No action needs
    /// View, Content or Audit, which are read permissions.
    pub fn required_permission(&self) -> Permission {
        match self {
            Self::SetPolicy { .. }
            | Self::MergeAgents(_)
            | Self::UnmergeAgent { .. }
            | Self::LabelAgent { .. }
            | Self::PromoteChannel { .. }
            | Self::CreateAlertRule { .. }
            | Self::UpdateAlertRule { .. }
            | Self::SetAlertRuleStatus { .. } => Permission::Govern,
            Self::Acknowledge { .. } | Self::Resolve { .. } | Self::DismissTransmission { .. } => {
                Permission::Triage
            }
            Self::ReplayDeadLetter { .. } => Permission::Operate,
        }
    }
}

pub trait OperatorActions {
    /// Check the permission, apply the action and record it. `SetPolicy` on
    /// a superseded channel is refused with `Conflict(ChannelSuperseded)`
    /// (read through `ChannelDirectory`) before `PolicyChanged` is
    /// published; `PromoteChannel` maps the registry's refusal
    /// (`ActionError::from`). A call that
    /// returns, `Ok` or any `ActionError`, leaves exactly
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
    ) -> Result<ActionOutcome, ActionError>;
}

pub trait AlertSink {
    async fn deliver(&self, alert: &Alert) -> Result<(), SinkError>;
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
    /// A pinned topic-model version whose buckets and assignments are no
    /// longer retained.
    VersionNotRetained {
        version: TopicModelVersion,
    },
    /// The request is well-formed but the state does not allow it.
    Conflict(ConflictKind),
    InvalidInput(InputError),
    /// A cursor the surface did not issue, issued for a different list or
    /// request, or no longer resumable (its pinned topic version is gone).
    /// The client restarts from the first page.
    InvalidCursor,
    /// The projection layout the client holds is no longer current.
    StaleProjection {
        current: ProjectionToken,
    },
}

/// Why an operator action was refused. A strict subset of what a query can
/// fail with: an action takes no cursor, reads no projection and pins no
/// version, so those variants cannot be returned (or recorded in the audit
/// log) for one.
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
    /// Acting on a merged agent where only its canonical agent is valid
    /// (renaming it, for example).
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
    /// A verdict on a transmission whose state does not take one.
    TransmissionNotJudgeable { transmission: TransmissionId },
    /// Querying a topic-model version that is still being fitted.
    TopicVersionFitting { version: TopicModelVersion },
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
}

impl From<PromotionRefusal> for ActionError {
    fn from(refusal: PromotionRefusal) -> Self {
        match refusal {
            PromotionRefusal::UnknownChannel(_) => Self::NotFound,
            PromotionRefusal::Superseded { channel, by } => {
                Self::Conflict(ConflictKind::ChannelSuperseded { channel, by })
            }
            PromotionRefusal::NotDiscovered(channel) => {
                Self::Conflict(ConflictKind::ChannelNotDiscovered { channel })
            }
            PromotionRefusal::PatternMissesSeed => {
                Self::InvalidInput(InputError::PatternMissesSeed)
            }
            PromotionRefusal::PatternOverlaps { existing } => {
                Self::Conflict(ConflictKind::PatternOverlaps { existing })
            }
        }
    }
}

impl From<PromoteError> for ActionError {
    fn from(error: PromoteError) -> Self {
        match error {
            PromoteError::Store { reason } => Self::Store { reason },
            PromoteError::Refused(refusal) => refusal.into(),
        }
    }
}

impl From<RegistryError> for QueryError {
    fn from(error: RegistryError) -> Self {
        match error {
            RegistryError::Store { reason } => Self::Store { reason },
            RegistryError::UnknownChannel(_) => Self::NotFound,
            RegistryError::OverlappingDeclaration { existing } => {
                Self::Conflict(ConflictKind::PatternOverlaps { existing })
            }
            RegistryError::Superseded { channel, by } => {
                Self::Conflict(ConflictKind::ChannelSuperseded { channel, by })
            }
            RegistryError::InvalidCursor => Self::InvalidCursor,
        }
    }
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SinkError {
    Unreachable { reason: String },
    Rejected { status: u16 },
}
