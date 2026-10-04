//! A backend over deterministic synthetic data, for development, tests and
//! demos. Same seed, same world, same answers.
//!
//! - [`world`] generates seven days of traffic between about forty agents
//!   over fifteen channels, with topics, alerts, rules and operator history.
//!   It is immutable once built.
//! - [`store`] holds what operator actions change, behind one lock.
//! - [`queries`] reads both, resolving merged agents and superseded
//!   channels at read time; [`actions`] applies operator actions and audits
//!   every one.
//!
//! The scenarios the world contains are listed in `docs/features/ui.md`.

mod actions;
mod clock;
mod queries;
mod rng;
mod store;
mod text;
mod world;

#[cfg(test)]
mod tests;

use std::collections::HashMap;
use std::num::NonZeroU32;

use crosstalk_spec::aggregates::edge::Weighting;
use crosstalk_spec::aggregates::topic::{Topic, TopicModelVersion};
use crosstalk_spec::derived::flow::resource::ResourcePattern;
use crosstalk_spec::ids::{AgentId, AlertId, ChannelId, TransmissionId};
use crosstalk_spec::interfaces::l2_transport::DeadLetter;
use crosstalk_spec::interfaces::l6_analysis::SearchHit;
use crosstalk_spec::interfaces::l8_surface::{AlertFilter, Caller, Permission};
use crosstalk_spec::support::{TimeWindow, Timestamp};
use tokio::sync::RwLock;

use super::{Backend, Result};
use crate::contract::alerts::Alert;
use crate::contract::ProjectionId;
use crate::contract::actions::{ActionOutcome, OperatorAction};
use crate::contract::agents::{AgentDetail, AgentListFilter, AgentName, AgentSummary};
use crate::contract::channels::{
    ChannelListFilter, ChannelName, ChannelSummary, PromotionPreview, ResourceUse,
};
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

use queries::{Ctx, require};
use store::State;
use world::World;

pub use world::GenError;
#[cfg(test)]
pub use world::ChannelKey;

#[derive(Debug)]
pub struct FixtureBackend {
    world: World,
    state: RwLock<State>,
}

impl FixtureBackend {
    /// Generates the world for `seed`. Generation only fails on a fixture
    /// bug; then the error is logged and the backend serves an empty world.
    pub fn new(seed: u64) -> Self {
        Self::try_new(seed).unwrap_or_else(|error| {
            tracing::error!(seed, %error, "fixture generation failed; serving an empty world");
            let (world, state) = world::empty(seed);
            Self {
                world,
                state: RwLock::new(state),
            }
        })
    }

    pub fn try_new(seed: u64) -> std::result::Result<Self, GenError> {
        let (world, state) = world::generate(seed)?;
        Ok(Self {
            world,
            state: RwLock::new(state),
        })
    }

    pub fn seed(&self) -> u64 {
        self.world.seed
    }

    /// Named handles into the generated world, for tests.
    #[cfg(test)]
    pub fn scenario(&self) -> &world::Scenario {
        &self.world.scenario
    }

    /// Runs a read under the state's read lock.
    async fn read<T>(&self, f: impl FnOnce(&Ctx) -> Result<T>) -> Result<T> {
        let state = self.state.read().await;
        f(&Ctx::new(&self.world, &state))
    }
}

impl Backend for FixtureBackend {
    /// The end of the generated data. Buckets before the watermark (ten
    /// minutes earlier) are final.
    async fn now(&self, caller: &Caller) -> Result<Timestamp> {
        require(caller, Permission::View)?;
        Ok(clock::NOW)
    }

    async fn current_topic_version(&self, caller: &Caller) -> Result<TopicModelVersion> {
        require(caller, Permission::View)?;
        Ok(self.world.topics.latest())
    }

    async fn topology(
        &self,
        caller: &Caller,
        scope: &Scope,
        weighting: Weighting,
    ) -> Result<TopologyView> {
        require(caller, Permission::View)?;
        self.read(|ctx| queries::graph::topology(ctx, scope, weighting))
            .await
    }

    async fn channel_topology(
        &self,
        caller: &Caller,
        scope: &Scope,
        weighting: Weighting,
    ) -> Result<BipartiteView> {
        require(caller, Permission::View)?;
        self.read(|ctx| queries::graph::channel_topology(ctx, scope, weighting))
            .await
    }

    async fn timeline(
        &self,
        caller: &Caller,
        scope: &Scope,
        buckets: NonZeroU32,
    ) -> Result<Timeline> {
        require(caller, Permission::View)?;
        self.read(|ctx| queries::graph::timeline(ctx, scope, buckets))
            .await
    }

    async fn transmissions(
        &self,
        caller: &Caller,
        scope: &Scope,
        selector: &TransmissionSelector,
        page: &PageRequest,
    ) -> Result<Page<TransmissionSummary>> {
        require(caller, Permission::View)?;
        self.read(|ctx| queries::transmissions::list(ctx, scope, selector, page))
            .await
    }

    async fn transmission(
        &self,
        caller: &Caller,
        id: TransmissionId,
    ) -> Result<Option<TransmissionEvidence>> {
        require(caller, Permission::Content)?;
        self.read(|ctx| Ok(queries::transmissions::evidence(ctx, id)))
            .await
    }

    async fn search(
        &self,
        caller: &Caller,
        request: &SearchRequest,
        scope: &Scope,
        page: &PageRequest,
    ) -> Result<Page<SearchHit>> {
        require(caller, Permission::Content)?;
        self.read(|ctx| queries::transmissions::search(ctx, request, scope, page))
            .await
    }

    async fn topic_versions(&self, caller: &Caller) -> Result<Vec<TopicVersionInfo>> {
        require(caller, Permission::Content)?;
        Ok(self.world.topics.versions.clone())
    }

    async fn topics(&self, caller: &Caller, version: TopicModelVersion) -> Result<Vec<Topic>> {
        require(caller, Permission::Content)?;
        self.read(|ctx| queries::content::topics(ctx, version))
            .await
    }

    async fn topic_stats(
        &self,
        caller: &Caller,
        scope: &Scope,
        buckets: NonZeroU32,
    ) -> Result<Vec<TopicStats>> {
        require(caller, Permission::Content)?;
        self.read(|ctx| queries::content::stats(ctx, scope, buckets))
            .await
    }

    async fn topic_remap(
        &self,
        caller: &Caller,
        from: TopicModelVersion,
    ) -> Result<Option<TopicVersionRemap>> {
        require(caller, Permission::Content)?;
        self.read(|ctx| queries::content::remap(ctx, from)).await
    }

    async fn fit_projection(
        &self,
        caller: &Caller,
        scope: &Scope,
        params: ProjectionParams,
    ) -> Result<ProjectionId> {
        require(caller, Permission::Content)?;
        queries::retained(&self.world, scope.topic_version)?;
        let mut state = self.state.write().await;
        let same = |p: &ProjectionPoints| p.meta().scope == *scope && p.meta().params == params;
        if let Some((id, _)) = state.projections.iter().find(|(_, p)| same(p)) {
            return Ok(*id);
        }
        let id = ProjectionId::from_ulid(state.mint.ulid(clock::NOW));
        let points = queries::content::project(&Ctx::new(&self.world, &state), scope, params, id)?;
        state.projections.push((id, points));
        Ok(id)
    }

    async fn projection_job(&self, caller: &Caller, id: ProjectionId) -> Result<ProjectionJob> {
        require(caller, Permission::Content)?;
        let state = self.state.read().await;
        state
            .projections
            .iter()
            .find(|(p, _)| *p == id)
            .map(|(_, points)| ProjectionJob::Ready(points.meta().clone()))
            .ok_or(QueryError::NotFound)
    }

    async fn projection(&self, caller: &Caller, id: ProjectionId) -> Result<ProjectionPoints> {
        require(caller, Permission::Content)?;
        let state = self.state.read().await;
        state
            .projections
            .iter()
            .find(|(p, _)| *p == id)
            .map(|(_, points)| points.clone())
            .ok_or(QueryError::NotFound)
    }

    async fn channels(
        &self,
        caller: &Caller,
        filter: &ChannelListFilter,
        page: &PageRequest,
    ) -> Result<Page<ChannelSummary>> {
        require(caller, Permission::View)?;
        self.read(|ctx| queries::lists::channels(ctx, filter, page))
            .await
    }

    async fn channel(&self, caller: &Caller, id: ChannelId) -> Result<Option<ChannelSummary>> {
        require(caller, Permission::View)?;
        self.read(|ctx| Ok(queries::lists::channel(ctx, id))).await
    }

    async fn channel_resources(
        &self,
        caller: &Caller,
        id: ChannelId,
        window: TimeWindow,
    ) -> Result<Vec<ResourceUse>> {
        require(caller, Permission::View)?;
        self.read(|ctx| queries::lists::channel_resources(ctx, id, window))
            .await
    }

    async fn promotion_preview(
        &self,
        caller: &Caller,
        id: ChannelId,
        pattern: &ResourcePattern,
    ) -> Result<PromotionPreview> {
        require(caller, Permission::View)?;
        let state = self.state.read().await;
        let plan = queries::promotion::plan(&self.world, &state, id, pattern)?;
        Ok(queries::promotion::preview(&self.world, plan, id))
    }

    async fn agents(
        &self,
        caller: &Caller,
        filter: &AgentListFilter,
        page: &PageRequest,
    ) -> Result<Page<AgentSummary>> {
        require(caller, Permission::View)?;
        self.read(|ctx| queries::lists::agents(ctx, filter, page))
            .await
    }

    async fn agent(&self, caller: &Caller, id: AgentId) -> Result<Option<AgentDetail>> {
        require(caller, Permission::View)?;
        self.read(|ctx| Ok(queries::lists::agent(ctx, id))).await
    }

    async fn agent_names(
        &self,
        caller: &Caller,
        ids: &[AgentId],
    ) -> Result<HashMap<AgentId, AgentName>> {
        require(caller, Permission::View)?;
        self.read(|ctx| Ok(queries::names::agents(ctx, ids))).await
    }

    async fn channel_names(
        &self,
        caller: &Caller,
        ids: &[ChannelId],
    ) -> Result<HashMap<ChannelId, ChannelName>> {
        require(caller, Permission::View)?;
        self.read(|ctx| Ok(queries::names::channels(ctx, ids))).await
    }

    async fn alerts(
        &self,
        caller: &Caller,
        filter: &AlertFilter,
        page: &PageRequest,
    ) -> Result<Page<Alert>> {
        require(caller, Permission::View)?;
        self.read(|ctx| queries::lists::alerts(ctx, filter, page))
            .await
    }

    async fn alert(&self, caller: &Caller, id: AlertId) -> Result<Option<Alert>> {
        require(caller, Permission::View)?;
        let state = self.state.read().await;
        Ok(state.alerts.iter().find(|a| a.id == id).cloned())
    }

    async fn rules(&self, caller: &Caller) -> Result<Vec<RuleDef>> {
        require(caller, Permission::View)?;
        Ok(self.state.read().await.rules.clone())
    }

    async fn sinks(&self, caller: &Caller) -> Result<Vec<SinkInfo>> {
        require(caller, Permission::View)?;
        Ok(self.world.sinks.clone())
    }

    async fn detection_quality(
        &self,
        caller: &Caller,
        window: TimeWindow,
    ) -> Result<Vec<QualityRow>> {
        require(caller, Permission::View)?;
        self.read(|ctx| Ok(queries::lists::quality(ctx, window)))
            .await
    }

    async fn audit(
        &self,
        caller: &Caller,
        filter: &AuditFilter,
        page: &PageRequest,
    ) -> Result<Page<AuditEntry>> {
        require(caller, Permission::View)?;
        self.read(|ctx| queries::lists::audit(ctx, filter, page))
            .await
    }

    async fn operators(&self, caller: &Caller) -> Result<Vec<Operator>> {
        require(caller, Permission::View)?;
        Ok(self.world.operators.clone())
    }

    async fn dead_letters(&self, caller: &Caller, page: &PageRequest) -> Result<Page<DeadLetter>> {
        require(caller, Permission::Operate)?;
        self.read(|ctx| queries::lists::dead_letters(ctx, page))
            .await
    }

    async fn act(&self, caller: &Caller, action: OperatorAction) -> Result<ActionOutcome> {
        let mut state = self.state.write().await;
        actions::act(&self.world, &mut state, caller, action)
    }
}
