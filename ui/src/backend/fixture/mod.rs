//! A backend over deterministic synthetic data, for development, tests and
//! demos. Same seed, same world.
//!
//! This is a stub that returns an empty world; the generator replaces it.

use std::num::NonZeroU32;

use crosstalk_spec::aggregates::alert::Alert;
use crosstalk_spec::aggregates::edge::{TopologyGraph, Weighting};
use crosstalk_spec::aggregates::topic::{Topic, TopicModelVersion};
use crosstalk_spec::ids::{AgentId, ChannelId, TransmissionId};
use crosstalk_spec::interfaces::l2_transport::DeadLetter;
use crosstalk_spec::interfaces::l6_analysis::SearchHit;
use crosstalk_spec::interfaces::l8_surface::{AlertFilter, Caller};
use crosstalk_spec::support::{TimeWindow, Timestamp};

use super::{Backend, Result};
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

#[derive(Debug)]
pub struct FixtureBackend {
    seed: u64,
}

impl FixtureBackend {
    pub fn new(seed: u64) -> Self {
        Self { seed }
    }

    pub fn seed(&self) -> u64 {
        self.seed
    }

    /// The end of the generated data; every bucket before it is final.
    pub fn now(&self) -> Timestamp {
        Timestamp::from_micros(1_790_985_600_000_000)
    }

    pub fn current_topic_version(&self) -> TopicModelVersion {
        TopicModelVersion(0)
    }
}

fn empty<T>() -> Page<T> {
    Page {
        items: Vec::new(),
        next: None,
    }
}

impl Backend for FixtureBackend {
    async fn topology(
        &self,
        _caller: &Caller,
        scope: &Scope,
        weighting: Weighting,
    ) -> Result<TopologyView> {
        let graph = TopologyGraph {
            window: scope.window,
            weighting,
            topic_version: scope.topic_version,
            edges: Vec::new(),
        };
        TopologyView::new(graph, Vec::new(), self.now()).map_err(|e| QueryError::Store {
            reason: e.to_string(),
        })
    }

    async fn channel_topology(
        &self,
        _caller: &Caller,
        scope: &Scope,
        weighting: Weighting,
    ) -> Result<BipartiteView> {
        BipartiteView::new(
            scope.window,
            weighting,
            scope.topic_version,
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            self.now(),
        )
        .map_err(|e| QueryError::Store {
            reason: e.to_string(),
        })
    }

    async fn timeline(
        &self,
        _caller: &Caller,
        _scope: &Scope,
        _buckets: NonZeroU32,
    ) -> Result<Timeline> {
        Ok(Timeline {
            bucket_width: std::time::Duration::from_secs(3600),
            buckets: Vec::new(),
            watermark: self.now(),
        })
    }

    async fn transmissions(
        &self,
        _caller: &Caller,
        _scope: &Scope,
        _selector: &TransmissionSelector,
        _page: &PageRequest,
    ) -> Result<Page<TransmissionSummary>> {
        Ok(empty())
    }

    async fn transmission(
        &self,
        _caller: &Caller,
        _id: TransmissionId,
    ) -> Result<Option<TransmissionEvidence>> {
        Ok(None)
    }

    async fn search(
        &self,
        _caller: &Caller,
        _request: &SearchRequest,
        _scope: &Scope,
        _page: &PageRequest,
    ) -> Result<Page<SearchHit>> {
        Ok(empty())
    }

    async fn topic_versions(&self, _caller: &Caller) -> Result<Vec<TopicVersionInfo>> {
        Ok(Vec::new())
    }

    async fn topics(&self, _caller: &Caller, _version: TopicModelVersion) -> Result<Vec<Topic>> {
        Ok(Vec::new())
    }

    async fn topic_stats(
        &self,
        _caller: &Caller,
        _scope: &Scope,
        _buckets: NonZeroU32,
    ) -> Result<Vec<TopicStats>> {
        Ok(Vec::new())
    }

    async fn topic_remap(
        &self,
        _caller: &Caller,
        _from: TopicModelVersion,
    ) -> Result<Option<TopicVersionRemap>> {
        Ok(None)
    }

    async fn fit_projection(
        &self,
        _caller: &Caller,
        _scope: &Scope,
        _params: ProjectionParams,
    ) -> Result<ProjectionId> {
        Err(QueryError::NotFound)
    }

    async fn projection_job(&self, _caller: &Caller, _id: ProjectionId) -> Result<ProjectionJob> {
        Err(QueryError::NotFound)
    }

    async fn projection(&self, _caller: &Caller, _id: ProjectionId) -> Result<ProjectionPoints> {
        Err(QueryError::NotFound)
    }

    async fn channels(
        &self,
        _caller: &Caller,
        _filter: &ChannelListFilter,
        _page: &PageRequest,
    ) -> Result<Page<ChannelSummary>> {
        Ok(empty())
    }

    async fn channel(&self, _caller: &Caller, _id: ChannelId) -> Result<Option<ChannelSummary>> {
        Ok(None)
    }

    async fn channel_resources(
        &self,
        _caller: &Caller,
        _id: ChannelId,
        _window: TimeWindow,
    ) -> Result<Vec<ResourceUse>> {
        Ok(Vec::new())
    }

    async fn agents(&self, _caller: &Caller, _page: &PageRequest) -> Result<Page<AgentSummary>> {
        Ok(empty())
    }

    async fn agent(&self, _caller: &Caller, _id: AgentId) -> Result<Option<AgentDetail>> {
        Ok(None)
    }

    async fn alerts(
        &self,
        _caller: &Caller,
        _filter: &AlertFilter,
        _page: &PageRequest,
    ) -> Result<Page<Alert>> {
        Ok(empty())
    }

    async fn rules(&self, _caller: &Caller) -> Result<Vec<RuleDef>> {
        Ok(Vec::new())
    }

    async fn sinks(&self, _caller: &Caller) -> Result<Vec<SinkInfo>> {
        Ok(Vec::new())
    }

    async fn detection_quality(
        &self,
        _caller: &Caller,
        _window: TimeWindow,
    ) -> Result<Vec<QualityRow>> {
        Ok(Vec::new())
    }

    async fn audit(
        &self,
        _caller: &Caller,
        _filter: &AuditFilter,
        _page: &PageRequest,
    ) -> Result<Page<AuditEntry>> {
        Ok(empty())
    }

    async fn operators(&self, _caller: &Caller) -> Result<Vec<Operator>> {
        Ok(Vec::new())
    }

    async fn dead_letters(
        &self,
        _caller: &Caller,
        _page: &PageRequest,
    ) -> Result<Page<DeadLetter>> {
        Ok(empty())
    }

    async fn act(&self, _caller: &Caller, _action: OperatorAction) -> Result<ActionOutcome> {
        Err(QueryError::NotFound)
    }
}
