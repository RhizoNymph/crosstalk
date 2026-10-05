//! [`AppBackend`] behind every trait the pages use: the spec's `QueryApi`,
//! `OperatorActions` and `LiveFeed`. Each forwards to the configured
//! backend; the export rows and live streams are enums over the backends'
//! own. Beside them, [`AppBackend::view_end`]: where a default view ends.

use std::collections::BTreeMap;

use crosstalk_spec::aggregates::access::{BipartiteGraph, ResourceUsePage};
use crosstalk_spec::aggregates::agents::filter::AgentFilter;
use crosstalk_spec::aggregates::agents::{AgentDetail, AgentName, AgentRow};
use crosstalk_spec::aggregates::alert::{Alert, AlertRuleDef};
use crosstalk_spec::aggregates::edge::{
    EdgeSelector, EdgeTransmissionPage, TopologyFilter as SpecTopologyFilter, TopologyGraph,
    Weighting,
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
use crosstalk_spec::interfaces::l8_surface::channels::{
    ChannelName, ChannelRow as SpecChannelRow, PromotionPreview,
};
use crosstalk_spec::interfaces::l8_surface::evidence::TransmissionEvidence;
use crosstalk_spec::interfaces::l8_surface::excerpt::ExcerptWindow;
use crosstalk_spec::interfaces::l8_surface::export::{
    Export, ExportRequest, ExportStep, ExportStream,
};
use crosstalk_spec::interfaces::l8_surface::lists::{
    AlertRuleFilter, ChannelFilter as SpecChannelFilter, SearchRequest, TopicPage,
};
use crosstalk_spec::interfaces::l8_surface::live::{
    LiveEnd, LiveFeed, LiveItem, LiveStream, Resume,
};
use crosstalk_spec::interfaces::l8_surface::operators::Operator;
use crosstalk_spec::interfaces::l8_surface::overview::OverviewCounts as SpecOverviewCounts;
use crosstalk_spec::interfaces::l8_surface::present::Present as SpecPresent;
use crosstalk_spec::interfaces::l8_surface::summary::{TransmissionPage, TransmissionSelection};
use crosstalk_spec::interfaces::l8_surface::{
    ActionError, ActionOutcome, AlertFilter, Caller, OperatorAction, OperatorActions, QueryApi,
    SinkInfo,
};
use crosstalk_spec::paging::{
    AgentList, AlertList, AlertRuleList, AuditList, ChannelList, DeadLetterList,
    EdgeTransmissionList, Page, PageRequest, ProjectionList, ResourceUseList, SearchList,
    TopicList, TransmissionList,
};
use crosstalk_spec::support::{TimeWindow, Timestamp};

use super::fixture::{FixtureBackend, export as fixture_export, live as fixture_live};
use super::world::WorldSurface;
use super::{AppBackend, Result};
use crosstalk_spec::interfaces::l8_surface::channel_traffic::{
    ChannelTransmissionFilter, ChannelTransmissionPage,
};
use crosstalk_spec::paging::ChannelTransmissionList;

/// Runs `$body` with `$b` bound to the fixture or to the world's surface,
/// for a method both implement through the same trait.
macro_rules! on_spec {
    ($self:expr, $b:ident => $body:expr) => {
        match $self {
            AppBackend::Fixture(fixture) => {
                let $b: &FixtureBackend = fixture;
                $body
            }
            AppBackend::World(world) => {
                let $b = world.surface();
                $body
            }
            #[cfg(feature = "live")]
            AppBackend::Live(live) => match *live {},
        }
    };
}

/// Forwards each listed `QueryApi` read to the configured backend.
macro_rules! forward_reads {
    ($(fn $name:ident(&self, caller: &Caller $(, $arg:ident: $ty:ty)*) -> $ret:ty;)*) => {
        $(
            async fn $name(&self, caller: &Caller $(, $arg: $ty)*) -> Result<$ret> {
                on_spec!(self, b => QueryApi::$name(b, caller $(, $arg)*).await)
            }
        )*
    };
}

/// The rows of an export, from whichever backend started it.
pub enum AppExportRows {
    Fixture(Box<fixture_export::ExportRows>),
    World(Box<<WorldSurface as QueryApi>::ExportRows>),
}

impl ExportStream for AppExportRows {
    async fn next(self) -> ExportStep<Self> {
        match self {
            Self::Fixture(rows) => match (*rows).next().await {
                ExportStep::Row(row, rest) => ExportStep::Row(row, Self::Fixture(Box::new(rest))),
                ExportStep::End(trailer) => ExportStep::End(trailer),
            },
            Self::World(rows) => match (*rows).next().await {
                ExportStep::Row(row, rest) => ExportStep::Row(row, Self::World(Box::new(rest))),
                ExportStep::End(trailer) => ExportStep::End(trailer),
            },
        }
    }
}

/// A live stream from whichever backend it was subscribed to.
pub enum AppStream {
    Fixture(fixture_live::FeedStream),
    World(<WorldSurface as LiveFeed>::Stream),
}

impl LiveStream for AppStream {
    async fn next(&mut self) -> std::result::Result<LiveItem, LiveEnd> {
        match self {
            Self::Fixture(stream) => stream.next().await,
            Self::World(stream) => stream.next().await,
        }
    }
}

impl QueryApi for AppBackend {
    type ExportRows = AppExportRows;

    forward_reads! {
        fn channel(&self, caller: &Caller, id: ChannelId, window: Option<TimeWindow>)
            -> Option<Watermarked<SpecChannelRow>>;
        fn policy_history(&self, caller: &Caller, channel: ChannelId) -> Option<PolicyHistory>;
        fn channels(&self, caller: &Caller, filter: &SpecChannelFilter, page: &PageRequest<ChannelList>)
            -> Watermarked<Page<SpecChannelRow, ChannelList>>;
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
        fn present(&self, caller: &Caller) -> SpecPresent;
        fn topology(&self, caller: &Caller, window: TimeWindow, weighting: Weighting, filter: &SpecTopologyFilter)
            -> Watermarked<TopologyGraph>;
        fn overview(&self, caller: &Caller, window: TimeWindow, filter: &SpecTopologyFilter)
            -> Watermarked<SpecOverviewCounts>;
        fn channel_topology(&self, caller: &Caller, window: TimeWindow, weighting: Weighting, filter: &SpecTopologyFilter)
            -> Watermarked<BipartiteGraph>;
        fn channel_resources(&self, caller: &Caller, channel: ChannelId, window: TimeWindow, page: &PageRequest<ResourceUseList>)
            -> Watermarked<ResourceUsePage>;
        fn edge_transmissions(&self, caller: &Caller, edge: &EdgeSelector, window: TimeWindow, filter: &SpecTopologyFilter, page: &PageRequest<EdgeTransmissionList>)
            -> Watermarked<EdgeTransmissionPage>;
        fn transmissions_by_id(&self, caller: &Caller, selection: &TransmissionSelection, version: TopicVersionSelector, page: &PageRequest<TransmissionList>)
            -> TransmissionPage;
        fn series(&self, caller: &Caller, grid: SeriesGrid, weighting: Weighting, grouping: SeriesGrouping, filter: &SpecTopologyFilter)
            -> Watermarked<TopologySeries>;
        fn topic_versions(&self, caller: &Caller) -> TopicVersionHistory;
        fn topic_sizes(&self, caller: &Caller, version: Option<TopicModelVersion>, window: Option<TimeWindow>)
            -> Watermarked<TopicSizes>;
        fn topic_lineage(&self, caller: &Caller, from: TopicModelVersion) -> Option<TopicLineage>;
        fn search(&self, caller: &Caller, request: &SearchRequest, window: Option<TimeWindow>, filter: &SpecTopologyFilter, page: &PageRequest<SearchList>)
            -> SearchResults;
        fn transmission(&self, caller: &Caller, id: TransmissionId) -> Option<Transmission>;
        fn transmission_evidence(&self, caller: &Caller, id: TransmissionId, window: ExcerptWindow)
            -> Option<TransmissionEvidence>;
        fn topics(&self, caller: &Caller, version: TopicVersionSelector, page: &PageRequest<TopicList>)
            -> TopicPage;
        fn fit_projection(&self, caller: &Caller, window: TimeWindow, filter: &SpecTopologyFilter, params: ProjectionParams)
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
    ) -> Result<Export<AppExportRows>> {
        match self {
            AppBackend::Fixture(b) => {
                let export = QueryApi::export(&**b, caller, request).await?;
                Ok(Export {
                    header: export.header,
                    rows: AppExportRows::Fixture(Box::new(export.rows)),
                })
            }
            AppBackend::World(world) => {
                let export = QueryApi::export(world.surface(), caller, request).await?;
                Ok(Export {
                    header: export.header,
                    rows: AppExportRows::World(Box::new(export.rows)),
                })
            }
            #[cfg(feature = "live")]
            AppBackend::Live(live) => match *live {},
        }
    }
}

impl OperatorActions for AppBackend {
    async fn act(
        &self,
        caller: &Caller,
        action: OperatorAction,
    ) -> std::result::Result<ActionOutcome, ActionError> {
        on_spec!(self, b => OperatorActions::act(b, caller, action).await)
    }
}

impl LiveFeed for AppBackend {
    type Stream = AppStream;

    async fn subscribe(&self, caller: &Caller, resume: Resume) -> Result<AppStream> {
        match self {
            AppBackend::Fixture(b) => LiveFeed::subscribe(&**b, caller, resume)
                .await
                .map(AppStream::Fixture),
            AppBackend::World(world) => world
                .surface()
                .subscribe(caller, resume)
                .await
                .map(AppStream::World),
            #[cfg(feature = "live")]
            AppBackend::Live(live) => match *live {},
        }
    }
}

impl AppBackend {
    /// Where a default view's window ends, given the present the request
    /// read: `present.now`, except for a fixture replaying up to a fixed
    /// end. Not a gateway question: only the fixture replays.
    pub fn view_end(&self, present: &SpecPresent) -> Timestamp {
        match self {
            Self::Fixture(fixture) => fixture.view_end(present),
            Self::World(_) => present.now,
            #[cfg(feature = "live")]
            Self::Live(live) => match *live {},
        }
    }
}

impl From<FixtureBackend> for AppBackend {
    fn from(backend: FixtureBackend) -> Self {
        Self::Fixture(Box::new(backend))
    }
}
