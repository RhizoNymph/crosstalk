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
mod audit;
mod clock;
pub mod export;
mod identity;
mod queries;
mod rng;
mod store;
mod text;
mod world;

#[cfg(test)]
mod tests;

use std::collections::HashMap;
use std::sync::Arc;

use crosstalk_spec::aggregates::access::{BipartiteGraph, ResourceUsePage};
use crosstalk_spec::aggregates::agents::filter::AgentFilter;
use crosstalk_spec::aggregates::agents::{AgentDetail, AgentName, AgentRow};
use crosstalk_spec::aggregates::alert::{Alert, AlertRuleDef};
use crosstalk_spec::aggregates::edge::{
    EdgeSelector, EdgeTransmissionPage, TopologyFilter, TopologyGraph, Weighting,
};
use crosstalk_spec::aggregates::filter::TopicVersionSelector;
use crosstalk_spec::aggregates::projection::{Projection, ProjectionInfo, ProjectionParams};
use crosstalk_spec::aggregates::quality::DetectionQuality;
use crosstalk_spec::aggregates::series::{SeriesGrid, SeriesGrouping, TopologySeries};
use crosstalk_spec::aggregates::topic::TopicModelVersion;
use crosstalk_spec::aggregates::topic_history::{TopicLineage, TopicSizes, TopicVersionHistory};
use crosstalk_spec::aggregates::watermark::{Watermark, Watermarked};
use crosstalk_spec::batch::IdBatch;
use crosstalk_spec::derived::flow::channel::Declaration;
use crosstalk_spec::derived::flow::channel::policy::{PolicyAuthor, PolicyHistory};
use crosstalk_spec::derived::flow::resource::ResourcePattern;
use crosstalk_spec::derived::flow::transmission::Transmission;
use crosstalk_spec::derived::flow::verdict::VerdictLog;
use crosstalk_spec::ids::{AgentId, AlertId, ChannelId, TransmissionId};
use crosstalk_spec::interfaces::l2_transport::{ConsumerGroup, DeadLetter};
use crosstalk_spec::interfaces::l6_analysis::SearchResults;
use crosstalk_spec::interfaces::l8_surface::audit::{AuditEntry, AuditFilter};
use crosstalk_spec::interfaces::l8_surface::channels::{ChannelName, ChannelRow, PromotionPreview};
use crosstalk_spec::interfaces::l8_surface::evidence::TransmissionEvidence;
use crosstalk_spec::interfaces::l8_surface::excerpt::ExcerptWindow;
use crosstalk_spec::interfaces::l8_surface::export::{
    Export, ExportFormat, ExportLimits, ExportRequest,
};
use crosstalk_spec::interfaces::l8_surface::lists::{
    AlertRuleFilter, ChannelFilter, SearchRequest, TopicPage,
};
use crosstalk_spec::interfaces::l8_surface::operators::Operator;
use crosstalk_spec::interfaces::l8_surface::overview::OverviewCounts;
use crosstalk_spec::interfaces::l8_surface::summary::{TransmissionPage, TransmissionSelection};
use crosstalk_spec::interfaces::l8_surface::{
    ActionError, ActionOutcome, AlertFilter, Caller, OperatorAction, Permission, SinkInfo,
};
use crosstalk_spec::support::{TimeWindow, Timestamp};
use tokio::sync::RwLock;

use super::{Backend, Result};
use crate::contract::formats::ExportFormats;
use crate::contract::present::Present;
use crosstalk_spec::aggregates::series::BucketWidth;
use crosstalk_spec::ids::ProjectionId;
use crosstalk_spec::paging::{
    AgentList, AlertList, AlertRuleList, AuditList, ChannelList, DeadLetterList,
    EdgeTransmissionList, Page, PageRequest, ProjectionList, ResourceUseList, SearchList,
    TopicList, TransmissionList,
};

use queries::{Ctx, require};
use store::State;
use world::World;

#[cfg(test)]
pub use world::ChannelKey;
pub use world::GenError;

#[derive(Debug)]
pub struct FixtureBackend {
    world: World,
    /// Shared with the streams of started exports, which audit how they
    /// end.
    state: Arc<RwLock<State>>,
    export_limits: ExportLimits,
}

impl FixtureBackend {
    /// Generates the world for `seed`, with its seeded projection jobs.
    /// Generation only fails on a fixture bug.
    pub fn try_new(seed: u64) -> std::result::Result<Self, GenError> {
        let (world, mut state) = world::generate(seed)?;
        queries::projection::seed::seed(&world, &mut state, world::OPERATOR_RESEARCHER)
            .map_err(|e| GenError::invalid("projection seed", e))?;
        Ok(Self {
            world,
            state: Arc::new(RwLock::new(state)),
            export_limits: export::limits(),
        })
    }

    /// The same world with another `export.max_rows`.
    #[cfg(test)]
    pub fn with_export_limits(mut self, limits: ExportLimits) -> Self {
        self.export_limits = limits;
        self
    }

    #[cfg(test)]
    pub fn seed(&self) -> u64 {
        self.world.seed
    }

    /// Named handles into the generated world, for tests.
    #[cfg(test)]
    pub fn scenario(&self) -> &world::Scenario {
        &self.world.scenario
    }

    /// Every transmission id the world holds, newest id first, for tests
    /// that page through them with `transmissions_by_id`.
    #[cfg(test)]
    pub fn transmission_ids(&self) -> Vec<TransmissionId> {
        let mut ids: Vec<TransmissionId> = self
            .world
            .transmissions
            .iter()
            .map(|t| t.transmission.id)
            .collect();
        ids.sort_unstable_by(|a, b| b.cmp(a));
        ids
    }

    /// Runs a read under the state's read lock.
    async fn read<T>(&self, f: impl FnOnce(&Ctx) -> Result<T>) -> Result<T> {
        let state = self.state.read().await;
        f(&Ctx::new(&self.world, &state))
    }
}

impl Present for FixtureBackend {
    /// Five minutes: the watermark, ten minutes before `now`, is a bucket
    /// boundary.
    fn bucket_width(&self) -> BucketWidth {
        clock::BUCKET
    }

    /// The end of the generated data. Buckets before the watermark (ten
    /// minutes earlier) are final.
    async fn now(&self, caller: &Caller) -> Result<Timestamp> {
        require(caller, Permission::View)?;
        Ok(clock::NOW)
    }
}

impl ExportFormats for FixtureBackend {
    /// JSONL only: a Parquet export is refused with `Store`.
    fn export_formats(&self) -> &'static [ExportFormat] {
        export::FORMATS
    }
}

impl Backend for FixtureBackend {
    async fn watermark(&self, caller: &Caller) -> Result<Watermark> {
        require(caller, Permission::View)?;
        Ok(queries::graph::watermark())
    }

    async fn topology(
        &self,
        caller: &Caller,
        window: TimeWindow,
        weighting: Weighting,
        filter: &TopologyFilter,
    ) -> Result<Watermarked<TopologyGraph>> {
        require(caller, Permission::View)?;
        self.read(|ctx| queries::graph::topology(ctx, window, weighting, filter))
            .await
    }

    async fn overview(
        &self,
        caller: &Caller,
        window: TimeWindow,
        filter: &TopologyFilter,
    ) -> Result<Watermarked<OverviewCounts>> {
        require(caller, Permission::View)?;
        self.read(|ctx| queries::graph::overview(ctx, window, filter))
            .await
    }

    async fn channel_topology(
        &self,
        caller: &Caller,
        window: TimeWindow,
        weighting: Weighting,
        filter: &TopologyFilter,
    ) -> Result<Watermarked<BipartiteGraph>> {
        require(caller, Permission::View)?;
        self.read(|ctx| queries::graph::channel_topology(ctx, window, weighting, filter))
            .await
    }

    async fn series(
        &self,
        caller: &Caller,
        grid: SeriesGrid,
        weighting: Weighting,
        grouping: SeriesGrouping,
        filter: &TopologyFilter,
    ) -> Result<Watermarked<TopologySeries>> {
        require(caller, Permission::View)?;
        self.read(|ctx| queries::series::series(ctx, grid, weighting, grouping, filter))
            .await
    }

    async fn edge_transmissions(
        &self,
        caller: &Caller,
        edge: &EdgeSelector,
        window: TimeWindow,
        filter: &TopologyFilter,
        page: &PageRequest<EdgeTransmissionList>,
    ) -> Result<Watermarked<EdgeTransmissionPage>> {
        require(caller, Permission::View)?;
        self.read(|ctx| queries::transmissions::edge(ctx, edge, window, filter, page))
            .await
    }

    async fn transmissions_by_id(
        &self,
        caller: &Caller,
        selection: &TransmissionSelection,
        version: TopicVersionSelector,
        page: &PageRequest<TransmissionList>,
    ) -> Result<TransmissionPage> {
        require(caller, Permission::View)?;
        self.read(|ctx| queries::transmissions::by_id(ctx, selection, version, page))
            .await
    }

    async fn transmission(
        &self,
        caller: &Caller,
        id: TransmissionId,
    ) -> Result<Option<Transmission>> {
        require(caller, Permission::Content)?;
        self.read(|ctx| Ok(queries::evidence::transmission(ctx, id)))
            .await
    }

    async fn transmission_evidence(
        &self,
        caller: &Caller,
        id: TransmissionId,
        window: ExcerptWindow,
    ) -> Result<Option<TransmissionEvidence>> {
        require(caller, Permission::Content)?;
        self.read(|ctx| queries::evidence::evidence(ctx, id, window))
            .await
    }

    async fn verdicts(
        &self,
        caller: &Caller,
        transmission: TransmissionId,
    ) -> Result<Option<VerdictLog>> {
        require(caller, Permission::View)?;
        self.read(|ctx| Ok(queries::evidence::verdicts(ctx, transmission)))
            .await
    }

    async fn search(
        &self,
        caller: &Caller,
        request: &SearchRequest,
        window: Option<TimeWindow>,
        filter: &TopologyFilter,
        page: &PageRequest<SearchList>,
    ) -> Result<SearchResults> {
        require(caller, Permission::Content)?;
        self.read(|ctx| queries::search::search(ctx, request, window, filter, page))
            .await
    }

    async fn topic_versions(&self, caller: &Caller) -> Result<TopicVersionHistory> {
        require(caller, Permission::View)?;
        self.read(|ctx| Ok(queries::topics::versions(ctx))).await
    }

    async fn topic_sizes(
        &self,
        caller: &Caller,
        version: Option<TopicModelVersion>,
        window: Option<TimeWindow>,
    ) -> Result<Watermarked<TopicSizes>> {
        require(caller, Permission::View)?;
        self.read(|ctx| queries::topics::sizes(ctx, version, window))
            .await
    }

    async fn topic_lineage(
        &self,
        caller: &Caller,
        from: TopicModelVersion,
    ) -> Result<Option<TopicLineage>> {
        require(caller, Permission::View)?;
        self.read(|ctx| queries::topics::lineage(ctx, from)).await
    }

    async fn topics(
        &self,
        caller: &Caller,
        version: TopicVersionSelector,
        page: &PageRequest<TopicList>,
    ) -> Result<TopicPage> {
        require(caller, Permission::Content)?;
        self.read(|ctx| queries::topics::topics(ctx, version, page))
            .await
    }

    async fn fit_projection(
        &self,
        caller: &Caller,
        window: TimeWindow,
        filter: &TopologyFilter,
        params: ProjectionParams,
    ) -> Result<ProjectionId> {
        require(caller, Permission::Content)?;
        let mut state = self.state.write().await;
        queries::projection::fit(
            &self.world,
            &mut state,
            caller.operator(),
            window,
            filter,
            params,
        )
    }

    async fn projection_status(&self, caller: &Caller, id: ProjectionId) -> Result<ProjectionInfo> {
        require(caller, Permission::Content)?;
        queries::projection::status(&*self.state.read().await, id)
    }

    async fn projections(
        &self,
        caller: &Caller,
        page: &PageRequest<ProjectionList>,
    ) -> Result<Page<ProjectionInfo, ProjectionList>> {
        require(caller, Permission::Content)?;
        queries::projection::list(&*self.state.read().await, page)
    }

    async fn projection(&self, caller: &Caller, id: ProjectionId) -> Result<Projection> {
        require(caller, Permission::Content)?;
        queries::projection::read(&*self.state.read().await, id)
    }

    async fn channels(
        &self,
        caller: &Caller,
        filter: &ChannelFilter,
        page: &PageRequest<ChannelList>,
    ) -> Result<Watermarked<Page<ChannelRow, ChannelList>>> {
        require(caller, Permission::View)?;
        self.read(|ctx| queries::channels::rows::list(ctx, filter, page))
            .await
    }

    async fn channel(
        &self,
        caller: &Caller,
        id: ChannelId,
        window: Option<TimeWindow>,
    ) -> Result<Option<Watermarked<ChannelRow>>> {
        require(caller, Permission::View)?;
        self.read(|ctx| queries::channels::rows::one(ctx, id, window))
            .await
    }

    async fn policy_history(
        &self,
        caller: &Caller,
        channel: ChannelId,
    ) -> Result<Option<PolicyHistory>> {
        require(caller, Permission::View)?;
        self.read(|ctx| Ok(queries::channels::policy_history(ctx, channel)))
            .await
    }

    async fn channel_resources(
        &self,
        caller: &Caller,
        channel: ChannelId,
        window: TimeWindow,
        page: &PageRequest<ResourceUseList>,
    ) -> Result<Watermarked<ResourceUsePage>> {
        require(caller, Permission::View)?;
        self.read(|ctx| queries::channels::resources::page(ctx, channel, window, page))
            .await
    }

    /// The declaration `PromoteChannel` would record now (the caller, the
    /// fixture's clock, `pattern`), previewed against the registry's
    /// coverage.
    async fn promotion_preview(
        &self,
        caller: &Caller,
        channel: ChannelId,
        pattern: &ResourcePattern,
    ) -> Result<PromotionPreview> {
        require(caller, Permission::View)?;
        let declaration = Declaration {
            pattern: pattern.clone(),
            by: PolicyAuthor::Operator(caller.operator()),
            at: clock::NOW,
        };
        let state = self.state.read().await;
        PromotionPreview::from_registry(queries::channels::coverage(
            &self.world,
            &state,
            channel,
            &declaration,
        ))
    }

    async fn agents(
        &self,
        caller: &Caller,
        filter: &AgentFilter,
        window: TimeWindow,
        page: &PageRequest<AgentList>,
    ) -> Result<Watermarked<Page<AgentRow, AgentList>>> {
        require(caller, Permission::View)?;
        self.read(|ctx| queries::agents::list(ctx, filter, window, page))
            .await
    }

    async fn agent(
        &self,
        caller: &Caller,
        id: AgentId,
        window: TimeWindow,
    ) -> Result<Option<Watermarked<AgentDetail>>> {
        require(caller, Permission::View)?;
        self.read(|ctx| queries::agents::one(ctx, id, window)).await
    }

    async fn agent_names(
        &self,
        caller: &Caller,
        ids: &IdBatch<AgentId>,
    ) -> Result<HashMap<AgentId, AgentName>> {
        require(caller, Permission::View)?;
        self.read(|ctx| queries::agents::names(ctx, ids)).await
    }

    async fn channel_names(
        &self,
        caller: &Caller,
        ids: &IdBatch<ChannelId>,
    ) -> Result<HashMap<ChannelId, ChannelName>> {
        require(caller, Permission::View)?;
        self.read(|ctx| queries::channels::names(ctx, ids)).await
    }

    async fn alerts(
        &self,
        caller: &Caller,
        filter: &AlertFilter,
        page: &PageRequest<AlertList>,
    ) -> Result<Page<Alert, AlertList>> {
        require(caller, Permission::View)?;
        self.read(|ctx| queries::alerts::alerts(ctx, filter, page))
            .await
    }

    async fn alert(&self, caller: &Caller, id: AlertId) -> Result<Option<Alert>> {
        require(caller, Permission::View)?;
        self.read(|ctx| Ok(queries::alerts::alert(ctx, id))).await
    }

    async fn alert_rules(
        &self,
        caller: &Caller,
        filter: &AlertRuleFilter,
        page: &PageRequest<AlertRuleList>,
    ) -> Result<Page<AlertRuleDef, AlertRuleList>> {
        require(caller, Permission::View)?;
        self.read(|ctx| queries::alerts::alert_rules(ctx, filter, page))
            .await
    }

    /// Govern: a delivery error can name the sink's endpoint.
    async fn sinks(&self, caller: &Caller) -> Result<Vec<SinkInfo>> {
        require(caller, Permission::Govern)?;
        Ok(self.world.sinks.clone())
    }

    async fn detection_quality(
        &self,
        caller: &Caller,
        window: TimeWindow,
    ) -> Result<DetectionQuality> {
        require(caller, Permission::View)?;
        self.read(|ctx| Ok(queries::transmissions::quality(ctx, window)))
            .await
    }

    async fn audit(
        &self,
        caller: &Caller,
        filter: &AuditFilter,
        page: &PageRequest<AuditList>,
    ) -> Result<Page<AuditEntry, AuditList>> {
        require(caller, Permission::Audit)?;
        self.read(|ctx| queries::lists::audit(ctx, filter, page))
            .await
    }

    /// The directory's operators by id, former ones with no permissions.
    async fn operators(&self, caller: &Caller) -> Result<Vec<Operator>> {
        require(caller, Permission::View)?;
        Ok(self.world.directory.operators().cloned().collect())
    }

    async fn dead_letters(
        &self,
        caller: &Caller,
        group: Option<&ConsumerGroup>,
        page: &PageRequest<DeadLetterList>,
    ) -> Result<Page<DeadLetter, DeadLetterList>> {
        require(caller, Permission::Operate)?;
        self.read(|ctx| queries::lists::dead_letters(ctx, group, page))
            .await
    }

    type ExportRows = export::ExportRows;

    async fn export(
        &self,
        caller: &Caller,
        request: &ExportRequest,
    ) -> Result<Export<export::ExportRows>> {
        export::export(
            &self.world,
            &self.state,
            self.export_limits,
            caller,
            request,
        )
        .await
    }

    async fn act(
        &self,
        caller: &Caller,
        action: OperatorAction,
    ) -> std::result::Result<ActionOutcome, ActionError> {
        let mut state = self.state.write().await;
        actions::act(&self.world, &mut state, caller, action)
    }
}
