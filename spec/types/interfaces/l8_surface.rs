//! L8 surface: the query API, the UI's data, operator actions and alert
//! delivery.
//!
//! Operator actions flow back down the stack: policy changes are published
//! as `PolicyChanged` (applied by L5), and agent merges go to L3's identity
//! resolver.
//!
//! Implementations:
//! - `QueryApi`: the axum HTTP service backing `GraphView` (topology, edge
//!   share) and `ContentExplorer` (search, topics, UMAP).
//! - `AlertSink`: `WebhookSink`, `SlackSink`, `LogSink`.

use crate::aggregates::alert::Alert;
use crate::aggregates::edge::{TopologyFilter, TopologyGraph, Weighting};
use crate::aggregates::topic::{Topic, TopicModelVersion};
use crate::derived::flow::channel::policy::Policy;
use crate::derived::flow::transmission::Transmission;
use crate::ids::{AgentId, AlertId, ChannelId, OperatorId, TransmissionId};
use crate::interfaces::l6_analysis::{SearchHit, SearchQuery};
use crate::support::TimeWindow;

/// A point in a 2-D projection of transmission embeddings.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ProjectedPoint {
    pub transmission: TransmissionId,
    pub x: f32,
    pub y: f32,
}

pub trait QueryApi {
    async fn topology(
        &self,
        window: TimeWindow,
        weighting: Weighting,
        filter: &TopologyFilter,
    ) -> Result<TopologyGraph, QueryError>;

    async fn search(
        &self,
        query: &SearchQuery,
        window: Option<TimeWindow>,
        limit: u32,
    ) -> Result<Vec<SearchHit>, QueryError>;

    async fn transmission(&self, id: TransmissionId) -> Result<Option<Transmission>, QueryError>;

    async fn topics(&self, version: Option<TopicModelVersion>) -> Result<Vec<Topic>, QueryError>;

    async fn projection(&self, window: TimeWindow) -> Result<Vec<ProjectedPoint>, QueryError>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OperatorAction {
    SetPolicy {
        channel: ChannelId,
        policy: Policy,
    },
    MergeAgents {
        from: AgentId,
        into: AgentId,
    },
    Acknowledge {
        alert: AlertId,
    },
    Resolve {
        alert: AlertId,
        note: Option<String>,
    },
}

pub trait OperatorActions {
    async fn act(&self, by: OperatorId, action: OperatorAction) -> Result<(), QueryError>;
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
