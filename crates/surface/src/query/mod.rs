//! `QueryApi` over the stores: each method checks its permission, then
//! reads, in a focused module per area:
//!
//! | Module | Methods |
//! | --- | --- |
//! | [`channels`], [`channel_rows`] | `channel`, `policy_history`, `channels`, `channel_names`, `promotion_preview`, `channel_resources` |
//! | [`agents`] | `agents`, `agent`, `agent_names` |
//! | [`alerts`] | `alert_rules`, `alert_rule`, `sinks`, `dead_letters`, `alerts`, `alert` |
//! | [`topology`] | `watermark`, `present`, `topology`, `overview`, `channel_topology`, `edge_transmissions`, `series` |
//! | [`topics`] | `topic_versions`, `topic_sizes`, `topic_lineage`, `topics` |
//! | [`content`] | `search`, `transmission`, `transmissions_by_id` |
//! | [`evidence`] | `transmission_evidence` |
//! | [`projections`] | `fit_projection`, `projection_status`, `projections`, `projection` |
//! | [`admin`] | `verdicts`, `detection_quality`, `audit`, `operators` |
//! | `crate::export` | `export` |

mod admin;
mod agents;
mod alerts;
mod channel_rows;
mod channels;
pub(crate) mod content;
mod evidence;
mod projections;
pub(crate) mod topics;
mod topology;

use std::collections::BTreeMap;

use crosstalk_spec::aggregates::access::{BipartiteGraph, ResourceUsePage};
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
use crosstalk_spec::derived::flow::channel::policy::PolicyHistory;
use crosstalk_spec::derived::flow::resource::ResourcePattern;
use crosstalk_spec::derived::flow::transmission::Transmission;
use crosstalk_spec::derived::flow::verdict::VerdictLog;
use crosstalk_spec::ids::{AgentId, AlertId, AlertRuleId, ChannelId, ProjectionId, TransmissionId};
use crosstalk_spec::interfaces::l2_transport::{ConsumerGroup, DeadLetter};
use crosstalk_spec::interfaces::l6_analysis::SearchResults;
use crosstalk_spec::interfaces::l8_surface::audit::{AuditEntry, AuditFilter};
use crosstalk_spec::interfaces::l8_surface::channels::{ChannelName, ChannelRow, PromotionPreview};
use crosstalk_spec::interfaces::l8_surface::evidence::TransmissionEvidence;
use crosstalk_spec::interfaces::l8_surface::excerpt::ExcerptWindow;
use crosstalk_spec::interfaces::l8_surface::export::{Export, ExportRequest};
use crosstalk_spec::interfaces::l8_surface::lists::{
    AgentFilter, AlertRuleFilter, ChannelFilter, SearchRequest, TopicPage,
};
use crosstalk_spec::interfaces::l8_surface::operators::Operator;
use crosstalk_spec::interfaces::l8_surface::overview::OverviewCounts;
use crosstalk_spec::interfaces::l8_surface::sinks::SinkInfo;
use crosstalk_spec::interfaces::l8_surface::summary::{TransmissionPage, TransmissionSelection};
use crosstalk_spec::interfaces::l8_surface::{AlertFilter, Caller, Present, QueryApi, QueryError};
use crosstalk_spec::paging::{
    AgentList, AlertList, AlertRuleList, AuditList, ChannelList, DeadLetterList,
    EdgeTransmissionList, Page, PageRequest, ProjectionList, ResourceUseList, SearchList,
    TopicList, TransmissionList,
};
use crosstalk_spec::support::TimeWindow;

use crate::export::ExportRowsOf;
use crate::service::Surface;
use crate::stores::SurfaceStores;

impl<S: SurfaceStores> QueryApi for Surface<S> {
    type ExportRows = ExportRowsOf<S>;

    async fn channel(
        &self,
        caller: &Caller,
        id: ChannelId,
        window: Option<TimeWindow>,
    ) -> Result<Option<Watermarked<ChannelRow>>, QueryError> {
        self.channel_query(caller, id, window).await
    }

    async fn policy_history(
        &self,
        caller: &Caller,
        channel: ChannelId,
    ) -> Result<Option<PolicyHistory>, QueryError> {
        self.policy_history_query(caller, channel).await
    }

    async fn channels(
        &self,
        caller: &Caller,
        filter: &ChannelFilter,
        page: &PageRequest<ChannelList>,
    ) -> Result<Watermarked<Page<ChannelRow, ChannelList>>, QueryError> {
        self.channels_query(caller, filter, page).await
    }

    async fn channel_names(
        &self,
        caller: &Caller,
        ids: &IdBatch<ChannelId>,
    ) -> Result<BTreeMap<ChannelId, ChannelName>, QueryError> {
        self.channel_names_query(caller, ids).await
    }

    async fn promotion_preview(
        &self,
        caller: &Caller,
        channel: ChannelId,
        pattern: &ResourcePattern,
    ) -> Result<PromotionPreview, QueryError> {
        self.promotion_preview_query(caller, channel, pattern).await
    }

    async fn agents(
        &self,
        caller: &Caller,
        filter: &AgentFilter,
        window: TimeWindow,
        page: &PageRequest<AgentList>,
    ) -> Result<Watermarked<Page<AgentRow, AgentList>>, QueryError> {
        self.agents_query(caller, filter, window, page).await
    }

    async fn agent(
        &self,
        caller: &Caller,
        id: AgentId,
        window: TimeWindow,
    ) -> Result<Option<Watermarked<AgentDetail>>, QueryError> {
        self.agent_query(caller, id, window).await
    }

    async fn agent_names(
        &self,
        caller: &Caller,
        ids: &IdBatch<AgentId>,
    ) -> Result<BTreeMap<AgentId, AgentName>, QueryError> {
        self.agent_names_query(caller, ids).await
    }

    async fn alert_rules(
        &self,
        caller: &Caller,
        filter: &AlertRuleFilter,
        page: &PageRequest<AlertRuleList>,
    ) -> Result<Page<AlertRuleDef, AlertRuleList>, QueryError> {
        self.alert_rules_query(caller, filter, page).await
    }

    async fn alert_rule(
        &self,
        caller: &Caller,
        id: AlertRuleId,
    ) -> Result<Option<AlertRuleDef>, QueryError> {
        self.alert_rule_query(caller, id).await
    }

    async fn sinks(&self, caller: &Caller) -> Result<Vec<SinkInfo>, QueryError> {
        self.sinks_query(caller).await
    }

    async fn dead_letters(
        &self,
        caller: &Caller,
        group: Option<&ConsumerGroup>,
        page: &PageRequest<DeadLetterList>,
    ) -> Result<Page<DeadLetter, DeadLetterList>, QueryError> {
        self.dead_letters_query(caller, group, page).await
    }

    async fn alerts(
        &self,
        caller: &Caller,
        filter: &AlertFilter,
        page: &PageRequest<AlertList>,
    ) -> Result<Page<Alert, AlertList>, QueryError> {
        self.alerts_query(caller, filter, page).await
    }

    async fn alert(&self, caller: &Caller, id: AlertId) -> Result<Option<Alert>, QueryError> {
        self.alert_query(caller, id).await
    }

    async fn watermark(&self, caller: &Caller) -> Result<Watermark, QueryError> {
        self.watermark_query(caller).await
    }

    async fn present(&self, caller: &Caller) -> Result<Present, QueryError> {
        self.present_query(caller).await
    }

    async fn topology(
        &self,
        caller: &Caller,
        window: TimeWindow,
        weighting: Weighting,
        filter: &TopologyFilter,
    ) -> Result<Watermarked<TopologyGraph>, QueryError> {
        self.topology_query(caller, window, weighting, filter).await
    }

    async fn overview(
        &self,
        caller: &Caller,
        window: TimeWindow,
        filter: &TopologyFilter,
    ) -> Result<Watermarked<OverviewCounts>, QueryError> {
        self.overview_query(caller, window, filter).await
    }

    async fn channel_topology(
        &self,
        caller: &Caller,
        window: TimeWindow,
        weighting: Weighting,
        filter: &TopologyFilter,
    ) -> Result<Watermarked<BipartiteGraph>, QueryError> {
        self.channel_topology_query(caller, window, weighting, filter)
            .await
    }

    async fn channel_resources(
        &self,
        caller: &Caller,
        channel: ChannelId,
        window: TimeWindow,
        page: &PageRequest<ResourceUseList>,
    ) -> Result<Watermarked<ResourceUsePage>, QueryError> {
        self.channel_resources_query(caller, channel, window, page)
            .await
    }

    async fn edge_transmissions(
        &self,
        caller: &Caller,
        edge: &EdgeSelector,
        window: TimeWindow,
        filter: &TopologyFilter,
        page: &PageRequest<EdgeTransmissionList>,
    ) -> Result<Watermarked<EdgeTransmissionPage>, QueryError> {
        self.edge_transmissions_query(caller, edge, window, filter, page)
            .await
    }

    async fn transmissions_by_id(
        &self,
        caller: &Caller,
        selection: &TransmissionSelection,
        version: TopicVersionSelector,
        page: &PageRequest<TransmissionList>,
    ) -> Result<TransmissionPage, QueryError> {
        self.transmissions_by_id_query(caller, selection, version, page)
            .await
    }

    async fn series(
        &self,
        caller: &Caller,
        grid: SeriesGrid,
        weighting: Weighting,
        grouping: SeriesGrouping,
        filter: &TopologyFilter,
    ) -> Result<Watermarked<TopologySeries>, QueryError> {
        self.series_query(caller, grid, weighting, grouping, filter)
            .await
    }

    async fn topic_versions(&self, caller: &Caller) -> Result<TopicVersionHistory, QueryError> {
        self.topic_versions_query(caller).await
    }

    async fn topic_sizes(
        &self,
        caller: &Caller,
        version: Option<TopicModelVersion>,
        window: Option<TimeWindow>,
    ) -> Result<Watermarked<TopicSizes>, QueryError> {
        self.topic_sizes_query(caller, version, window).await
    }

    async fn topic_lineage(
        &self,
        caller: &Caller,
        from: TopicModelVersion,
    ) -> Result<Option<TopicLineage>, QueryError> {
        self.topic_lineage_query(caller, from).await
    }

    async fn search(
        &self,
        caller: &Caller,
        request: &SearchRequest,
        window: Option<TimeWindow>,
        filter: &TopologyFilter,
        page: &PageRequest<SearchList>,
    ) -> Result<SearchResults, QueryError> {
        self.search_query(caller, request, window, filter, page)
            .await
    }

    async fn transmission(
        &self,
        caller: &Caller,
        id: TransmissionId,
    ) -> Result<Option<Transmission>, QueryError> {
        self.transmission_query(caller, id).await
    }

    async fn transmission_evidence(
        &self,
        caller: &Caller,
        id: TransmissionId,
        window: ExcerptWindow,
    ) -> Result<Option<TransmissionEvidence>, QueryError> {
        self.transmission_evidence_query(caller, id, window).await
    }

    async fn topics(
        &self,
        caller: &Caller,
        version: TopicVersionSelector,
        page: &PageRequest<TopicList>,
    ) -> Result<TopicPage, QueryError> {
        self.topics_query(caller, version, page).await
    }

    async fn fit_projection(
        &self,
        caller: &Caller,
        window: TimeWindow,
        filter: &TopologyFilter,
        params: ProjectionParams,
    ) -> Result<ProjectionId, QueryError> {
        self.fit_projection_query(caller, window, filter, params)
            .await
    }

    async fn projection_status(
        &self,
        caller: &Caller,
        id: ProjectionId,
    ) -> Result<ProjectionInfo, QueryError> {
        self.projection_status_query(caller, id).await
    }

    async fn projections(
        &self,
        caller: &Caller,
        page: &PageRequest<ProjectionList>,
    ) -> Result<Page<ProjectionInfo, ProjectionList>, QueryError> {
        self.projections_query(caller, page).await
    }

    async fn projection(
        &self,
        caller: &Caller,
        id: ProjectionId,
    ) -> Result<Projection, QueryError> {
        self.projection_query(caller, id).await
    }

    async fn verdicts(
        &self,
        caller: &Caller,
        transmission: TransmissionId,
    ) -> Result<Option<VerdictLog>, QueryError> {
        self.verdicts_query(caller, transmission).await
    }

    async fn detection_quality(
        &self,
        caller: &Caller,
        window: TimeWindow,
    ) -> Result<DetectionQuality, QueryError> {
        self.detection_quality_query(caller, window).await
    }

    async fn audit(
        &self,
        caller: &Caller,
        filter: &AuditFilter,
        page: &PageRequest<AuditList>,
    ) -> Result<Page<AuditEntry, AuditList>, QueryError> {
        self.audit_query(caller, filter, page).await
    }

    async fn operators(&self, caller: &Caller) -> Result<Vec<Operator>, QueryError> {
        self.operators_query(caller).await
    }

    async fn export(
        &self,
        caller: &Caller,
        request: &ExportRequest,
    ) -> Result<Export<Self::ExportRows>, QueryError> {
        self.export_query(caller, request).await
    }
}
