//! L8 surface: the query API, the UI's data, operator actions, the live
//! feed, the audit log and alert delivery.
//!
//! Operator actions flow back down the stack: policy changes are published
//! as `PolicyChanged` (applied by L5); agent merges, unmerges and labels go
//! to L3's identity resolver; channel promotion and transmission dismissal go
//! to L5 (`ChannelRegistry::promote`, `TransmissionReview::dismiss`), and so
//! do verdicts on transmissions (`TransmissionVerdicts::set`); alert rule
//! management goes to L6's `AlertRuleStore`; topic-version pins go to L6's
//! `TopicCatalog` (`pin`, `unpin`). Every action names its
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
//! - `AlertSink`: `WebhookSink`, `SlackSink`, `LogSink`.
//!
//! **Lists.** Channels, agents, alert rules, dead letters, the audit log and
//! the transmissions behind an edge are read a page at a time with the cursors
//! of [`crate::paging`], so a traversal is stable under concurrent inserts.
//! Their filters and request types are in [`lists`].
//!
//! **Linked views.** `topology`, `search`, `projection` and
//! `edge_transmissions` take the same [`TopologyFilter`] and apply it as
//! [`TopologyFilter::admits`] defines, so a selection in one view narrows
//! the others to the same transmissions. Each response reports the
//! topic-model version its topics are under; responses with different
//! versions are not linkable and the client re-queries.
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

pub mod audit;
pub mod lists;
pub mod live;
pub mod operators;

use std::fmt;

use crate::aggregates::alert::{Alert, AlertRuleDef, RuleStatus};
use crate::aggregates::edge::{
    EdgeSelector, EdgeTransmissionPage, TopologyFilter, TopologyGraph, Weighting,
};
use crate::aggregates::projection::{Projection, ProjectionToken};
use crate::aggregates::quality::DetectionQuality;
use crate::aggregates::series::{SeriesGrid, SeriesGrouping, TopologySeries};
use crate::aggregates::topic::{Topic, TopicModelVersion};
use crate::aggregates::topic_history::{TopicLineage, TopicSizes, TopicVersionHistory};
use crate::aggregates::watermark::{Watermark, Watermarked};
use crate::derived::flow::channel::Channel;
use crate::derived::flow::channel::policy::PolicyHistory;
use crate::derived::flow::resource::ResourcePattern;
use crate::derived::flow::transmission::Transmission;
use crate::derived::flow::verdict::{Verdict, VerdictLog};
use crate::ids::{
    AgentId, AlertId, AlertRuleId, ChannelId, EventId, MergeId, OperatorId, TransmissionId,
};
use crate::interfaces::l2_transport::{ConsumerGroup, DeadLetter};
use crate::interfaces::l6_analysis::{RuleRequest, SearchQuery, SearchResults};
use crate::observed::agent::{Agent, AgentLabel, MergeRequest};
use crate::paging::{
    AgentList, AlertRuleList, AuditList, ChannelList, DeadLetterList, EdgeTransmissionList, Page,
    PageRequest,
};
use crate::support::TimeWindow;

use audit::{AuditEntry, AuditFilter, AuditSubject};
use lists::{AgentFilter, AlertRuleFilter, ChannelFilter, ProjectionRequest};
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
    /// from message text) and projections.
    Content,
    /// Identity and policy: channel policy and promotion, agent merges,
    /// unmerges and labels, alert rules (what the gateway alerts on), and
    /// topic-version pins (what history the gateway keeps).
    Govern,
    /// Work alerts: acknowledge, resolve, and dismiss the suspected
    /// transmissions they are about. Judge transmissions: set and withdraw
    /// verdicts.
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
/// return `InvalidCursor` for a cursor issued for a different request.
/// `EdgeError::VersionNotRetained` and `CatalogError::VersionNotRetained`
/// become `VersionNotRetained` with the same version.
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

    /// View. L7's exposed watermark (`EdgeStore::watermark`).
    async fn watermark(&self, caller: &Caller) -> Result<Watermark, QueryError>;

    /// View.
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
    /// Discard a suspected transmission with reason `Dismissed`, which
    /// suppresses its `SuspectedTransmission` alerts.
    DismissTransmission {
        transmission: TransmissionId,
        note: Option<String>,
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
    UnmergeAgent,
    LabelAgent,
    PromoteChannel,
    Acknowledge,
    Resolve,
    DismissTransmission,
    SetVerdict,
    CreateAlertRule,
    UpdateAlertRule,
    SetAlertRuleStatus,
    ReplayDeadLetter,
    PinTopicVersion,
    UnpinTopicVersion,
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
            Self::SetVerdict { .. } => ActionKind::SetVerdict,
            Self::CreateAlertRule { .. } => ActionKind::CreateAlertRule,
            Self::UpdateAlertRule { .. } => ActionKind::UpdateAlertRule,
            Self::SetAlertRuleStatus { .. } => ActionKind::SetAlertRuleStatus,
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
            | Self::UnmergeAgent { .. }
            | Self::LabelAgent { .. }
            | Self::PromoteChannel { .. }
            | Self::CreateAlertRule { .. }
            | Self::UpdateAlertRule { .. }
            | Self::SetAlertRuleStatus { .. }
            | Self::PinTopicVersion { .. }
            | Self::UnpinTopicVersion { .. } => Permission::Govern,
            Self::Acknowledge { .. }
            | Self::Resolve { .. }
            | Self::DismissTransmission { .. }
            | Self::SetVerdict { .. } => Permission::Triage,
            Self::ReplayDeadLetter { .. } => Permission::Operate,
        }
    }

    /// The entities the action names, as requested (not resolved through
    /// merges). The audit log's subject filter matches these, together with
    /// any id the outcome created ([`ActionOutcome::subject`]). A dead-letter
    /// replay names no entity.
    pub fn subjects(&self) -> Vec<AuditSubject> {
        match self {
            Self::SetPolicy { channel, .. } | Self::PromoteChannel { channel, .. } => {
                vec![AuditSubject::Channel(*channel)]
            }
            Self::MergeAgents(request) => vec![
                AuditSubject::Agent(request.source()),
                AuditSubject::Agent(request.target()),
            ],
            Self::UnmergeAgent { agent } | Self::LabelAgent { agent, .. } => {
                vec![AuditSubject::Agent(*agent)]
            }
            Self::Acknowledge { alert } | Self::Resolve { alert, .. } => {
                vec![AuditSubject::Alert(*alert)]
            }
            Self::DismissTransmission { transmission, .. }
            | Self::SetVerdict { transmission, .. } => {
                vec![AuditSubject::Transmission(*transmission)]
            }
            Self::CreateAlertRule { id: rule, .. }
            | Self::UpdateAlertRule { rule, .. }
            | Self::SetAlertRuleStatus { rule, .. } => vec![AuditSubject::Rule(*rule)],
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
    /// A verdict on a transmission whose state does not take one
    /// (`Detected`, `AwaitingContent`).
    TransmissionNotJudgeable { transmission: TransmissionId },
    /// Querying or pinning a topic-model version that is still being fitted.
    TopicVersionFitting { version: TopicModelVersion },
    /// Pinning a topic-model version whose data retention has dropped.
    TopicVersionDropped { version: TopicModelVersion },
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
