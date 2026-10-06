//! `QueryApi` over HTTP: every method encodes its call with the binding's
//! [`RequestBuilder`](crosstalk_spec::interfaces::l8_surface::http::RequestBuilder)
//! for its route, argument by argument under the names the route table
//! gives them, and decodes the route's answer. The table drives the
//! request: a method names its route and its arguments, and nothing else.
//!
//! `projection` reads two routes (the frame, then the job record) and
//! `export` streams; both are in their own modules ([`crate::frame`],
//! [`crate::export`]).

use crosstalk_spec::interfaces::l8_surface::conversation::ExchangePlacement;
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
use crosstalk_spec::ids::{
    AgentId, AlertId, AlertRuleId, ChannelId, ConversationId, ExchangeId, ProjectionId, SpanId,
    TransmissionId,
};
use crosstalk_spec::interfaces::l2_transport::{ConsumerGroup, DeadLetter};
use crosstalk_spec::interfaces::l6_analysis::SearchResults;
use crosstalk_spec::interfaces::l8_surface::audit::{AuditEntry, AuditFilter};
use crosstalk_spec::interfaces::l8_surface::channel_traffic::{
    ChannelTransmissionFilter, ChannelTransmissionPage,
};
use crosstalk_spec::interfaces::l8_surface::channels::{ChannelName, ChannelRow, PromotionPreview};
use crosstalk_spec::interfaces::l8_surface::conversation::text::{
    ConversationText, PartText, TextLimit, TextSlice,
};
use crosstalk_spec::interfaces::l8_surface::conversation::turn::{Reader, TurnPage};
use crosstalk_spec::interfaces::l8_surface::conversation::{
    ConversationFilter, ConversationHead, ConversationRow, SpanPoint, TurnWindow,
};
use crosstalk_spec::interfaces::l8_surface::evidence::TransmissionEvidence;
use crosstalk_spec::interfaces::l8_surface::excerpt::ExcerptWindow;
use crosstalk_spec::interfaces::l8_surface::export::{Export, ExportRequest, RowHasher};
use crosstalk_spec::interfaces::l8_surface::http::bodies::{
    EdgeTransmissionsBody, FitProjectionBody, GraphBody, OverviewBody, PartTextBody, SearchBody,
    SeriesBody, TransmissionsBody,
};
use crosstalk_spec::interfaces::l8_surface::http::{RequestBuilder, Route};
use crosstalk_spec::interfaces::l8_surface::lists::{
    AgentFilter, AlertRuleFilter, ChannelFilter, SearchRequest, TopicPage,
};
use crosstalk_spec::interfaces::l8_surface::operators::Operator;
use crosstalk_spec::interfaces::l8_surface::overview::OverviewCounts;
use crosstalk_spec::interfaces::l8_surface::summary::{TransmissionPage, TransmissionSelection};
use crosstalk_spec::interfaces::l8_surface::{
    AlertFilter, Caller, Present, QueryApi, QueryError, SinkInfo,
};
use crosstalk_spec::observed::message::PartRef;
use crosstalk_spec::paging::{
    AgentList, AlertList, AlertRuleList, AuditList, ChannelList, ChannelTransmissionList,
    ConversationList, DeadLetterList, EdgeTransmissionList, Page, PageRequest, ProjectionList,
    ResourceUseList, SearchList, SpanReaderList, TopicList, TransmissionList,
};
use crosstalk_spec::support::TimeWindow;
use serde::de::DeserializeOwned;

use crate::client::HttpClient;
use crate::export::HttpExportRows;

impl<H> HttpClient<H> {
    /// One JSON route, its error as the surface's `QueryError`.
    async fn query<T: DeserializeOwned>(
        &self,
        route: Route,
        build: impl FnOnce(RequestBuilder) -> RequestBuilder,
    ) -> Result<T, QueryError> {
        self.call::<T, QueryError>(route, build)
            .await
            .map_err(QueryError::from)
    }
}

impl<H: RowHasher + Default + Send + 'static> QueryApi for HttpClient<H> {
    type ExportRows = HttpExportRows<H>;

    async fn channel(
        &self,
        _: &Caller,
        id: ChannelId,
        window: Option<TimeWindow>,
    ) -> Result<Option<Watermarked<ChannelRow>>, QueryError> {
        self.query(Route::Channel, |b| {
            b.path("id", &id).query("window", &window)
        })
        .await
    }

    async fn policy_history(
        &self,
        _: &Caller,
        channel: ChannelId,
    ) -> Result<Option<PolicyHistory>, QueryError> {
        self.query(Route::PolicyHistory, |b| b.path("id", &channel))
            .await
    }

    async fn channels(
        &self,
        _: &Caller,
        filter: &ChannelFilter,
        page: &PageRequest<ChannelList>,
    ) -> Result<Watermarked<Page<ChannelRow, ChannelList>>, QueryError> {
        self.query(Route::Channels, |b| {
            b.query("filter", filter).query("page", page)
        })
        .await
    }

    async fn channel_transmissions(
        &self,
        _: &Caller,
        channel: ChannelId,
        filter: &ChannelTransmissionFilter,
        version: TopicVersionSelector,
        page: &PageRequest<ChannelTransmissionList>,
    ) -> Result<ChannelTransmissionPage, QueryError> {
        self.query(Route::ChannelTransmissions, |b| {
            b.path("id", &channel)
                .query("filter", filter)
                .query("version", &version)
                .query("page", page)
        })
        .await
    }

    async fn channel_names(
        &self,
        _: &Caller,
        ids: &IdBatch<ChannelId>,
    ) -> Result<BTreeMap<ChannelId, ChannelName>, QueryError> {
        self.query(Route::ChannelNames, |b| b.body(ids)).await
    }

    async fn promotion_preview(
        &self,
        _: &Caller,
        channel: ChannelId,
        pattern: &ResourcePattern,
    ) -> Result<PromotionPreview, QueryError> {
        self.query(Route::PromotionPreview, |b| {
            b.path("id", &channel).query("pattern", pattern)
        })
        .await
    }

    async fn agents(
        &self,
        _: &Caller,
        filter: &AgentFilter,
        window: TimeWindow,
        page: &PageRequest<AgentList>,
    ) -> Result<Watermarked<Page<AgentRow, AgentList>>, QueryError> {
        self.query(Route::Agents, |b| {
            b.query("filter", filter)
                .query("window", &window)
                .query("page", page)
        })
        .await
    }

    async fn agent(
        &self,
        _: &Caller,
        id: AgentId,
        window: TimeWindow,
    ) -> Result<Option<Watermarked<AgentDetail>>, QueryError> {
        self.query(Route::Agent, |b| b.path("id", &id).query("window", &window))
            .await
    }

    async fn agent_names(
        &self,
        _: &Caller,
        ids: &IdBatch<AgentId>,
    ) -> Result<BTreeMap<AgentId, AgentName>, QueryError> {
        self.query(Route::AgentNames, |b| b.body(ids)).await
    }

    async fn conversations(
        &self,
        _: &Caller,
        filter: &ConversationFilter,
        page: &PageRequest<ConversationList>,
    ) -> Result<Page<ConversationRow, ConversationList>, QueryError> {
        self.query(Route::Conversations, |b| {
            b.query("filter", filter).query("page", page)
        })
        .await
    }

    async fn conversation(
        &self,
        _: &Caller,
        id: ConversationId,
    ) -> Result<Option<ConversationHead>, QueryError> {
        self.query(Route::Conversation, |b| b.path("id", &id)).await
    }

    async fn conversation_turns(
        &self,
        _: &Caller,
        id: ConversationId,
        window: &TurnWindow,
    ) -> Result<Option<TurnPage>, QueryError> {
        self.query(Route::ConversationTurns, |b| {
            b.path("id", &id).query("window", window)
        })
        .await
    }

    async fn span_readers(
        &self,
        _: &Caller,
        span: SpanId,
        page: &PageRequest<SpanReaderList>,
    ) -> Result<Option<Page<Reader, SpanReaderList>>, QueryError> {
        self.query(Route::SpanReaders, |b| {
            b.path("id", &span).query("page", page)
        })
        .await
    }

    async fn exchange_turns(
        &self,
        _: &Caller,
        ids: &IdBatch<ExchangeId>,
    ) -> Result<BTreeMap<ExchangeId, ExchangePlacement>, QueryError> {
        self.query(Route::ExchangeTurns, |b| b.body(ids)).await
    }

    async fn span_points(
        &self,
        _: &Caller,
        ids: &IdBatch<SpanId>,
    ) -> Result<BTreeMap<SpanId, SpanPoint>, QueryError> {
        self.query(Route::SpanPoints, |b| b.body(ids)).await
    }

    async fn conversation_text(
        &self,
        _: &Caller,
        id: ConversationId,
        window: &TurnWindow,
        limit: TextLimit,
    ) -> Result<Option<ConversationText>, QueryError> {
        self.query(Route::ConversationText, |b| {
            b.path("id", &id)
                .query("window", window)
                .query("limit", &limit)
        })
        .await
    }

    async fn part_text(
        &self,
        _: &Caller,
        part: PartRef,
        slice: TextSlice,
    ) -> Result<Option<PartText>, QueryError> {
        self.query(Route::PartText, |b| b.body(&PartTextBody { part, slice }))
            .await
    }

    async fn alert_rules(
        &self,
        _: &Caller,
        filter: &AlertRuleFilter,
        page: &PageRequest<AlertRuleList>,
    ) -> Result<Page<AlertRuleDef, AlertRuleList>, QueryError> {
        self.query(Route::AlertRules, |b| {
            b.query("filter", filter).query("page", page)
        })
        .await
    }

    async fn alert_rule(
        &self,
        _: &Caller,
        id: AlertRuleId,
    ) -> Result<Option<AlertRuleDef>, QueryError> {
        self.query(Route::AlertRule, |b| b.path("id", &id)).await
    }

    async fn sinks(&self, _: &Caller) -> Result<Vec<SinkInfo>, QueryError> {
        self.query(Route::Sinks, |b| b).await
    }

    async fn dead_letters(
        &self,
        _: &Caller,
        group: Option<&ConsumerGroup>,
        page: &PageRequest<DeadLetterList>,
    ) -> Result<Page<DeadLetter, DeadLetterList>, QueryError> {
        self.query(Route::DeadLetters, |b| {
            b.query("group", &group.cloned()).query("page", page)
        })
        .await
    }

    async fn alerts(
        &self,
        _: &Caller,
        filter: &AlertFilter,
        page: &PageRequest<AlertList>,
    ) -> Result<Page<Alert, AlertList>, QueryError> {
        self.query(Route::Alerts, |b| {
            b.query("filter", filter).query("page", page)
        })
        .await
    }

    async fn alert(&self, _: &Caller, id: AlertId) -> Result<Option<Alert>, QueryError> {
        self.query(Route::Alert, |b| b.path("id", &id)).await
    }

    async fn watermark(&self, _: &Caller) -> Result<Watermark, QueryError> {
        self.query(Route::Watermark, |b| b).await
    }

    async fn topology(
        &self,
        _: &Caller,
        window: TimeWindow,
        weighting: Weighting,
        filter: &TopologyFilter,
    ) -> Result<Watermarked<TopologyGraph>, QueryError> {
        self.query(Route::Topology, |b| {
            b.body(&GraphBody {
                window,
                weighting,
                filter: filter.clone(),
            })
        })
        .await
    }

    async fn overview(
        &self,
        _: &Caller,
        window: TimeWindow,
        filter: &TopologyFilter,
    ) -> Result<Watermarked<OverviewCounts>, QueryError> {
        self.query(Route::Overview, |b| {
            b.body(&OverviewBody {
                window,
                filter: filter.clone(),
            })
        })
        .await
    }

    async fn channel_topology(
        &self,
        _: &Caller,
        window: TimeWindow,
        weighting: Weighting,
        filter: &TopologyFilter,
    ) -> Result<Watermarked<BipartiteGraph>, QueryError> {
        self.query(Route::ChannelTopology, |b| {
            b.body(&GraphBody {
                window,
                weighting,
                filter: filter.clone(),
            })
        })
        .await
    }

    async fn channel_resources(
        &self,
        _: &Caller,
        channel: ChannelId,
        window: TimeWindow,
        page: &PageRequest<ResourceUseList>,
    ) -> Result<Watermarked<ResourceUsePage>, QueryError> {
        self.query(Route::ChannelResources, |b| {
            b.path("id", &channel)
                .query("window", &window)
                .query("page", page)
        })
        .await
    }

    async fn edge_transmissions(
        &self,
        _: &Caller,
        edge: &EdgeSelector,
        window: TimeWindow,
        filter: &TopologyFilter,
        page: &PageRequest<EdgeTransmissionList>,
    ) -> Result<Watermarked<EdgeTransmissionPage>, QueryError> {
        self.query(Route::EdgeTransmissions, |b| {
            b.body(&EdgeTransmissionsBody {
                edge: edge.clone(),
                window,
                filter: filter.clone(),
                page: page.clone(),
            })
        })
        .await
    }

    async fn transmissions_by_id(
        &self,
        _: &Caller,
        selection: &TransmissionSelection,
        version: TopicVersionSelector,
        page: &PageRequest<TransmissionList>,
    ) -> Result<TransmissionPage, QueryError> {
        self.query(Route::TransmissionsById, |b| {
            b.body(&TransmissionsBody {
                selection: selection.clone(),
                version,
                page: page.clone(),
            })
        })
        .await
    }

    async fn series(
        &self,
        _: &Caller,
        grid: SeriesGrid,
        weighting: Weighting,
        grouping: SeriesGrouping,
        filter: &TopologyFilter,
    ) -> Result<Watermarked<TopologySeries>, QueryError> {
        self.query(Route::Series, |b| {
            b.body(&SeriesBody {
                grid,
                weighting,
                grouping,
                filter: filter.clone(),
            })
        })
        .await
    }

    async fn topic_versions(&self, _: &Caller) -> Result<TopicVersionHistory, QueryError> {
        self.query(Route::TopicVersions, |b| b).await
    }

    async fn topic_sizes(
        &self,
        _: &Caller,
        version: Option<TopicModelVersion>,
        window: Option<TimeWindow>,
    ) -> Result<Watermarked<TopicSizes>, QueryError> {
        self.query(Route::TopicSizes, |b| {
            b.query("version", &version).query("window", &window)
        })
        .await
    }

    async fn topic_lineage(
        &self,
        _: &Caller,
        from: TopicModelVersion,
    ) -> Result<Option<TopicLineage>, QueryError> {
        self.query(Route::TopicLineage, |b| b.path("version", &from))
            .await
    }

    async fn search(
        &self,
        _: &Caller,
        request: &SearchRequest,
        window: Option<TimeWindow>,
        filter: &TopologyFilter,
        page: &PageRequest<SearchList>,
    ) -> Result<SearchResults, QueryError> {
        self.query(Route::Search, |b| {
            b.body(&SearchBody {
                request: request.clone(),
                window,
                filter: filter.clone(),
                page: page.clone(),
            })
        })
        .await
    }

    async fn transmission(
        &self,
        _: &Caller,
        id: TransmissionId,
    ) -> Result<Option<Transmission>, QueryError> {
        self.query(Route::Transmission, |b| b.path("id", &id)).await
    }

    async fn transmission_evidence(
        &self,
        _: &Caller,
        id: TransmissionId,
        window: ExcerptWindow,
    ) -> Result<Option<TransmissionEvidence>, QueryError> {
        self.query(Route::TransmissionEvidence, |b| {
            b.path("id", &id).query("window", &window)
        })
        .await
    }

    async fn topics(
        &self,
        _: &Caller,
        version: TopicVersionSelector,
        page: &PageRequest<TopicList>,
    ) -> Result<TopicPage, QueryError> {
        self.query(Route::Topics, |b| {
            b.query("version", &version).query("page", page)
        })
        .await
    }

    async fn fit_projection(
        &self,
        _: &Caller,
        window: TimeWindow,
        filter: &TopologyFilter,
        params: ProjectionParams,
    ) -> Result<ProjectionId, QueryError> {
        self.query(Route::FitProjection, |b| {
            b.body(&FitProjectionBody {
                window,
                filter: filter.clone(),
                params,
            })
        })
        .await
    }

    async fn projection_status(
        &self,
        _: &Caller,
        id: ProjectionId,
    ) -> Result<ProjectionInfo, QueryError> {
        self.query(Route::ProjectionStatus, |b| b.path("id", &id))
            .await
    }

    async fn projections(
        &self,
        _: &Caller,
        page: &PageRequest<ProjectionList>,
    ) -> Result<Page<ProjectionInfo, ProjectionList>, QueryError> {
        self.query(Route::Projections, |b| b.query("page", page))
            .await
    }

    async fn projection(&self, _: &Caller, id: ProjectionId) -> Result<Projection, QueryError> {
        self.fetch_projection(id).await.map_err(QueryError::from)
    }

    async fn verdicts(
        &self,
        _: &Caller,
        transmission: TransmissionId,
    ) -> Result<Option<VerdictLog>, QueryError> {
        self.query(Route::Verdicts, |b| b.path("id", &transmission))
            .await
    }

    async fn detection_quality(
        &self,
        _: &Caller,
        window: TimeWindow,
    ) -> Result<DetectionQuality, QueryError> {
        self.query(Route::DetectionQuality, |b| b.query("window", &window))
            .await
    }

    async fn audit(
        &self,
        _: &Caller,
        filter: &AuditFilter,
        page: &PageRequest<AuditList>,
    ) -> Result<Page<AuditEntry, AuditList>, QueryError> {
        self.query(Route::Audit, |b| {
            b.query("filter", filter).query("page", page)
        })
        .await
    }

    async fn operators(&self, _: &Caller) -> Result<Vec<Operator>, QueryError> {
        self.query(Route::Operators, |b| b).await
    }

    async fn me(&self, _: &Caller) -> Result<Operator, QueryError> {
        self.query(Route::Me, |b| b).await
    }

    async fn present(&self, _: &Caller) -> Result<Present, QueryError> {
        self.query(Route::Present, |b| b).await
    }

    async fn export(
        &self,
        _: &Caller,
        request: &ExportRequest,
    ) -> Result<Export<Self::ExportRows>, QueryError> {
        self.start_export(request).await.map_err(QueryError::from)
    }
}
