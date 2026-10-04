//! L8 surface: the query API, the UI's data, operator actions and alert
//! delivery.
//!
//! Operator actions flow back down the stack: policy changes are published
//! as `PolicyChanged` (applied by L5); agent merges, unmerges and labels go
//! to L3's identity resolver; channel promotion and transmission dismissal go
//! to L5 (`ChannelRegistry::promote`, `TransmissionReview::dismiss`); alert
//! rule management goes to L6's `AlertRuleStore`. Every action names its
//! permission ([`OperatorAction::required_permission`]), checked before any
//! effect. Wherever an action records an author or time, the surface stamps
//! them from the authenticated caller and the time it accepted the action;
//! callers cannot supply them.
//!
//! Implementations:
//! - `QueryApi`: the axum HTTP service backing `GraphView` (topology, edge
//!   share) and `ContentExplorer` (search, topics, UMAP).
//! - `AlertSink`: `WebhookSink`, `SlackSink`, `LogSink`.

use crate::aggregates::alert::{Alert, RuleStatus};
use crate::aggregates::edge::{TopologyFilter, TopologyGraph, Weighting};
use crate::aggregates::topic::{Topic, TopicModelVersion};
use crate::derived::flow::channel::Channel;
use crate::derived::flow::resource::ResourcePattern;
use crate::derived::flow::transmission::Transmission;
use crate::ids::{AgentId, AlertId, AlertRuleId, ChannelId, EventId, OperatorId, TransmissionId};
use crate::interfaces::l2_transport::ConsumerGroup;
use crate::interfaces::l6_analysis::{RuleRequest, SearchHit, SearchQuery};
use crate::observed::agent::{AgentLabel, MergeRequest};
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
    /// Topology, channels, alerts: no message content.
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

impl OperatorAction {
    /// The permission the caller must hold. Govern for identity and policy,
    /// Triage for alerts, Operate for the pipeline.
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
