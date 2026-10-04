//! The data source behind every page: L8's `QueryApi` and
//! `OperatorActions` plus the contract additions.
//!
//! Implementations enforce permissions themselves and return
//! `QueryError::Forbidden`; pages also check, to render the "content
//! hidden" state instead of an error.

pub mod fixture;

use std::future::Future;

use crosstalk_spec::aggregates::alert::Alert;
use crosstalk_spec::aggregates::edge::Weighting;
use crosstalk_spec::aggregates::topic::{Topic, TopicModelVersion};
use crosstalk_spec::ids::{AgentId, ChannelId, TransmissionId};
use crosstalk_spec::interfaces::l2_transport::DeadLetter;
use crosstalk_spec::interfaces::l6_analysis::SearchHit;
use crosstalk_spec::interfaces::l8_surface::{AlertFilter, Caller};
use crosstalk_spec::support::TimeWindow;

use crate::contract::ProjectionId;
use crate::contract::actions::{ActionOutcome, OperatorAction};
use crate::contract::agents::{AgentDetail, AgentSummary};
use crate::contract::channels::{ChannelListFilter, ChannelSummary, ResourceUse};
use crate::contract::errors::QueryError;
use crate::contract::evidence::TransmissionEvidence;
use crate::contract::graph::{
    BipartiteView, Timeline, TopologyView, TransmissionSelector, TransmissionSummary,
};
use crate::contract::lists::{Page, PageRequest};
use crate::contract::research::{
    AuditEntry, AuditFilter, Operator, ProjectionJob, ProjectionParams, ProjectionPoints,
    QualityRow,
};
use crate::contract::rules::{RuleDef, SinkInfo};
use crate::contract::scope::Scope;
use crate::contract::search::SearchRequest;
use crate::contract::topics::{TopicStats, TopicVersionInfo, TopicVersionRemap};

pub type Result<T> = std::result::Result<T, QueryError>;

/// Every read and action the UI performs. Methods return `Send` futures so
/// pages can call them from Topcoat's multi-threaded runtime.
pub trait Backend: Send + Sync + 'static {
    // Topology (items 3, 4, 6).

    fn topology(
        &self,
        caller: &Caller,
        scope: &Scope,
        weighting: Weighting,
    ) -> impl Future<Output = Result<TopologyView>> + Send;

    fn channel_topology(
        &self,
        caller: &Caller,
        scope: &Scope,
        weighting: Weighting,
    ) -> impl Future<Output = Result<BipartiteView>> + Send;

    fn timeline(
        &self,
        caller: &Caller,
        scope: &Scope,
        buckets: std::num::NonZeroU32,
    ) -> impl Future<Output = Result<Timeline>> + Send;

    // Transmissions (items 1, 17, 22).

    fn transmissions(
        &self,
        caller: &Caller,
        scope: &Scope,
        selector: &TransmissionSelector,
        page: &PageRequest,
    ) -> impl Future<Output = Result<Page<TransmissionSummary>>> + Send;

    /// Needs `Content`.
    fn transmission(
        &self,
        caller: &Caller,
        id: TransmissionId,
    ) -> impl Future<Output = Result<Option<TransmissionEvidence>>> + Send;

    // Content (items 7, 9, 23). All need `Content`.

    fn search(
        &self,
        caller: &Caller,
        request: &SearchRequest,
        scope: &Scope,
        page: &PageRequest,
    ) -> impl Future<Output = Result<Page<SearchHit>>> + Send;

    fn topic_versions(
        &self,
        caller: &Caller,
    ) -> impl Future<Output = Result<Vec<TopicVersionInfo>>> + Send;

    fn topics(
        &self,
        caller: &Caller,
        version: TopicModelVersion,
    ) -> impl Future<Output = Result<Vec<Topic>>> + Send;

    fn topic_stats(
        &self,
        caller: &Caller,
        scope: &Scope,
        buckets: std::num::NonZeroU32,
    ) -> impl Future<Output = Result<Vec<TopicStats>>> + Send;

    /// The remap from `from` to the next version, if there is one.
    fn topic_remap(
        &self,
        caller: &Caller,
        from: TopicModelVersion,
    ) -> impl Future<Output = Result<Option<TopicVersionRemap>>> + Send;

    fn fit_projection(
        &self,
        caller: &Caller,
        scope: &Scope,
        params: ProjectionParams,
    ) -> impl Future<Output = Result<ProjectionId>> + Send;

    fn projection_job(
        &self,
        caller: &Caller,
        id: ProjectionId,
    ) -> impl Future<Output = Result<ProjectionJob>> + Send;

    fn projection(
        &self,
        caller: &Caller,
        id: ProjectionId,
    ) -> impl Future<Output = Result<ProjectionPoints>> + Send;

    // Channels and agents (items 1, 5).

    fn channels(
        &self,
        caller: &Caller,
        filter: &ChannelListFilter,
        page: &PageRequest,
    ) -> impl Future<Output = Result<Page<ChannelSummary>>> + Send;

    fn channel(
        &self,
        caller: &Caller,
        id: ChannelId,
    ) -> impl Future<Output = Result<Option<ChannelSummary>>> + Send;

    fn channel_resources(
        &self,
        caller: &Caller,
        id: ChannelId,
        window: TimeWindow,
    ) -> impl Future<Output = Result<Vec<ResourceUse>>> + Send;

    fn agents(
        &self,
        caller: &Caller,
        page: &PageRequest,
    ) -> impl Future<Output = Result<Page<AgentSummary>>> + Send;

    /// Resolves aliases: asking for a merged agent returns its canonical
    /// agent.
    fn agent(
        &self,
        caller: &Caller,
        id: AgentId,
    ) -> impl Future<Output = Result<Option<AgentDetail>>> + Send;

    // Alerts and rules (items 1, 18).

    fn alerts(
        &self,
        caller: &Caller,
        filter: &AlertFilter,
        page: &PageRequest,
    ) -> impl Future<Output = Result<Page<Alert>>> + Send;

    fn rules(&self, caller: &Caller) -> impl Future<Output = Result<Vec<RuleDef>>> + Send;

    fn sinks(&self, caller: &Caller) -> impl Future<Output = Result<Vec<SinkInfo>>> + Send;

    // Research and pipeline (items 10, 12).

    fn detection_quality(
        &self,
        caller: &Caller,
        window: TimeWindow,
    ) -> impl Future<Output = Result<Vec<QualityRow>>> + Send;

    fn audit(
        &self,
        caller: &Caller,
        filter: &AuditFilter,
        page: &PageRequest,
    ) -> impl Future<Output = Result<Page<AuditEntry>>> + Send;

    fn operators(&self, caller: &Caller) -> impl Future<Output = Result<Vec<Operator>>> + Send;

    /// Needs `Operate`.
    fn dead_letters(
        &self,
        caller: &Caller,
        page: &PageRequest,
    ) -> impl Future<Output = Result<Page<DeadLetter>>> + Send;

    // Actions (item 13).

    fn act(
        &self,
        caller: &Caller,
        action: OperatorAction,
    ) -> impl Future<Output = Result<ActionOutcome>> + Send;
}
