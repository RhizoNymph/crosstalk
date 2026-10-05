//! A backend that owns what it needs and forwards every L8 trait method to
//! the implementation [`Route::route`] picks for the call's caller.
//!
//! - In process: one surface for every caller (the caller travels with
//!   the call).
//! - Over HTTP: the client whose bearer token authenticates the caller's
//!   operator, since the surface derives the caller from the credential,
//!   never from the argument.

use std::collections::BTreeMap;

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
use crosstalk_spec::derived::flow::channel::policy::PolicyHistory;
use crosstalk_spec::derived::flow::resource::ResourcePattern;
use crosstalk_spec::derived::flow::transmission::Transmission;
use crosstalk_spec::derived::flow::verdict::VerdictLog;
use crosstalk_spec::ids::{AgentId, AlertId, AlertRuleId, ChannelId, ProjectionId, TransmissionId};
use crosstalk_spec::interfaces::l2_transport::{ConsumerGroup, DeadLetter};
use crosstalk_spec::interfaces::l6_analysis::SearchResults;
use crosstalk_spec::interfaces::l8_surface::audit::{AuditEntry, AuditFilter};
use crosstalk_spec::interfaces::l8_surface::channel_traffic::{
    ChannelTransmissionFilter, ChannelTransmissionPage,
};
use crosstalk_spec::interfaces::l8_surface::channels::{ChannelName, ChannelRow, PromotionPreview};
use crosstalk_spec::interfaces::l8_surface::evidence::TransmissionEvidence;
use crosstalk_spec::interfaces::l8_surface::excerpt::ExcerptWindow;
use crosstalk_spec::interfaces::l8_surface::export::{Export, ExportRequest};
use crosstalk_spec::interfaces::l8_surface::lists::{
    AlertRuleFilter, ChannelFilter, SearchRequest, TopicPage,
};
use crosstalk_spec::interfaces::l8_surface::live::{LiveFeed, Resume};
use crosstalk_spec::interfaces::l8_surface::operators::Operator;
use crosstalk_spec::interfaces::l8_surface::overview::OverviewCounts;
use crosstalk_spec::interfaces::l8_surface::present::Present;
use crosstalk_spec::interfaces::l8_surface::summary::{TransmissionPage, TransmissionSelection};
use crosstalk_spec::interfaces::l8_surface::{
    ActionError, ActionOutcome, AlertFilter, Caller, OperatorAction, OperatorActions, QueryApi,
    QueryError, SinkInfo,
};
use crosstalk_spec::paging::{
    AgentList, AlertList, AlertRuleList, AuditList, ChannelList, ChannelTransmissionList,
    DeadLetterList, EdgeTransmissionList, Page, PageRequest, ProjectionList, ResourceUseList,
    SearchList, TopicList, TransmissionList,
};
use crosstalk_spec::support::TimeWindow;

/// Picks the implementation a call goes to, by its caller.
pub trait Route: Send + Sync {
    type Target: QueryApi + OperatorActions + LiveFeed + Send + Sync;
    fn route(&self, caller: &Caller) -> &Self::Target;
}

/// The backend over a [`Route`]: owns it, forwards every call.
#[derive(Debug)]
pub struct Routed<R>(pub R);

type Result<T> = std::result::Result<T, QueryError>;

macro_rules! forward_reads {
    ($(fn $name:ident(&self, caller: &Caller $(, $arg:ident: $ty:ty)*) -> $ret:ty;)*) => {
        $(
            async fn $name(&self, caller: &Caller $(, $arg: $ty)*) -> Result<$ret> {
                QueryApi::$name(self.0.route(caller), caller $(, $arg)*).await
            }
        )*
    };
}

impl<R: Route> QueryApi for Routed<R> {
    type ExportRows = <R::Target as QueryApi>::ExportRows;

    forward_reads! {
        fn channel(&self, caller: &Caller, id: ChannelId, window: Option<TimeWindow>)
            -> Option<Watermarked<ChannelRow>>;
        fn policy_history(&self, caller: &Caller, channel: ChannelId) -> Option<PolicyHistory>;
        fn channels(&self, caller: &Caller, filter: &ChannelFilter, page: &PageRequest<ChannelList>)
            -> Watermarked<Page<ChannelRow, ChannelList>>;
        fn channel_transmissions(&self, caller: &Caller, channel: ChannelId, filter: &ChannelTransmissionFilter, version: TopicVersionSelector, page: &PageRequest<ChannelTransmissionList>)
            -> ChannelTransmissionPage;
        fn channel_names(&self, caller: &Caller, ids: &IdBatch<ChannelId>)
            -> BTreeMap<ChannelId, ChannelName>;
        fn promotion_preview(&self, caller: &Caller, channel: ChannelId, pattern: &ResourcePattern)
            -> PromotionPreview;
        fn agents(&self, caller: &Caller, filter: &AgentFilter, window: TimeWindow, page: &PageRequest<AgentList>)
            -> Watermarked<Page<AgentRow, AgentList>>;
        fn agent(&self, caller: &Caller, id: AgentId, window: TimeWindow)
            -> Option<Watermarked<AgentDetail>>;
        fn agent_names(&self, caller: &Caller, ids: &IdBatch<AgentId>) -> BTreeMap<AgentId, AgentName>;
        fn alert_rules(&self, caller: &Caller, filter: &AlertRuleFilter, page: &PageRequest<AlertRuleList>)
            -> Page<AlertRuleDef, AlertRuleList>;
        fn alert_rule(&self, caller: &Caller, id: AlertRuleId) -> Option<AlertRuleDef>;
        fn sinks(&self, caller: &Caller) -> Vec<SinkInfo>;
        fn dead_letters(&self, caller: &Caller, group: Option<&ConsumerGroup>, page: &PageRequest<DeadLetterList>)
            -> Page<DeadLetter, DeadLetterList>;
        fn alerts(&self, caller: &Caller, filter: &AlertFilter, page: &PageRequest<AlertList>)
            -> Page<Alert, AlertList>;
        fn alert(&self, caller: &Caller, id: AlertId) -> Option<Alert>;
        fn watermark(&self, caller: &Caller) -> Watermark;
        fn present(&self, caller: &Caller) -> Present;
        fn topology(&self, caller: &Caller, window: TimeWindow, weighting: Weighting, filter: &TopologyFilter)
            -> Watermarked<TopologyGraph>;
        fn overview(&self, caller: &Caller, window: TimeWindow, filter: &TopologyFilter)
            -> Watermarked<OverviewCounts>;
        fn channel_topology(&self, caller: &Caller, window: TimeWindow, weighting: Weighting, filter: &TopologyFilter)
            -> Watermarked<BipartiteGraph>;
        fn channel_resources(&self, caller: &Caller, channel: ChannelId, window: TimeWindow, page: &PageRequest<ResourceUseList>)
            -> Watermarked<ResourceUsePage>;
        fn edge_transmissions(&self, caller: &Caller, edge: &EdgeSelector, window: TimeWindow, filter: &TopologyFilter, page: &PageRequest<EdgeTransmissionList>)
            -> Watermarked<EdgeTransmissionPage>;
        fn transmissions_by_id(&self, caller: &Caller, selection: &TransmissionSelection, version: TopicVersionSelector, page: &PageRequest<TransmissionList>)
            -> TransmissionPage;
        fn series(&self, caller: &Caller, grid: SeriesGrid, weighting: Weighting, grouping: SeriesGrouping, filter: &TopologyFilter)
            -> Watermarked<TopologySeries>;
        fn topic_versions(&self, caller: &Caller) -> TopicVersionHistory;
        fn topic_sizes(&self, caller: &Caller, version: Option<TopicModelVersion>, window: Option<TimeWindow>)
            -> Watermarked<TopicSizes>;
        fn topic_lineage(&self, caller: &Caller, from: TopicModelVersion) -> Option<TopicLineage>;
        fn search(&self, caller: &Caller, request: &SearchRequest, window: Option<TimeWindow>, filter: &TopologyFilter, page: &PageRequest<SearchList>)
            -> SearchResults;
        fn transmission(&self, caller: &Caller, id: TransmissionId) -> Option<Transmission>;
        fn transmission_evidence(&self, caller: &Caller, id: TransmissionId, window: ExcerptWindow)
            -> Option<TransmissionEvidence>;
        fn topics(&self, caller: &Caller, version: TopicVersionSelector, page: &PageRequest<TopicList>)
            -> TopicPage;
        fn fit_projection(&self, caller: &Caller, window: TimeWindow, filter: &TopologyFilter, params: ProjectionParams)
            -> ProjectionId;
        fn projection_status(&self, caller: &Caller, id: ProjectionId) -> ProjectionInfo;
        fn projections(&self, caller: &Caller, page: &PageRequest<ProjectionList>)
            -> Page<ProjectionInfo, ProjectionList>;
        fn projection(&self, caller: &Caller, id: ProjectionId) -> Projection;
        fn verdicts(&self, caller: &Caller, transmission: TransmissionId) -> Option<VerdictLog>;
        fn detection_quality(&self, caller: &Caller, window: TimeWindow) -> DetectionQuality;
        fn audit(&self, caller: &Caller, filter: &AuditFilter, page: &PageRequest<AuditList>)
            -> Page<AuditEntry, AuditList>;
        fn operators(&self, caller: &Caller) -> Vec<Operator>;
    }

    async fn export(
        &self,
        caller: &Caller,
        request: &ExportRequest,
    ) -> Result<Export<Self::ExportRows>> {
        QueryApi::export(self.0.route(caller), caller, request).await
    }
}

impl<R: Route> OperatorActions for Routed<R> {
    async fn act(
        &self,
        caller: &Caller,
        action: OperatorAction,
    ) -> std::result::Result<ActionOutcome, ActionError> {
        OperatorActions::act(self.0.route(caller), caller, action).await
    }
}

impl<R: Route> LiveFeed for Routed<R> {
    type Stream = <R::Target as LiveFeed>::Stream;

    async fn subscribe(&self, caller: &Caller, resume: Resume) -> Result<Self::Stream> {
        LiveFeed::subscribe(self.0.route(caller), caller, resume).await
    }
}
