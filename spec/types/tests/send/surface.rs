use crate::aggregates::access::{BipartiteGraph, ResourceUsePage};
use crate::aggregates::agents::filter::AgentFilter;
use crate::aggregates::agents::{AgentDetail, AgentName, AgentRow};
use crate::aggregates::alert::Alert;
use crate::aggregates::alert::rules::AlertRuleDef;
use crate::aggregates::edge::{EdgeSelector, EdgeTransmissionPage, TopologyGraph, Weighting};
use crate::aggregates::filter::{TopicVersionSelector, TopologyFilter};
use crate::aggregates::projection::{Projection, ProjectionInfo, ProjectionParams};
use crate::aggregates::quality::DetectionQuality;
use crate::aggregates::series::{SeriesGrid, SeriesGrouping, TopologySeries};
use crate::aggregates::topic::TopicModelVersion;
use crate::aggregates::topic_history::{TopicLineage, TopicSizes, TopicVersionHistory};
use crate::aggregates::watermark::Watermarked;
use crate::batch::IdBatch;
use crate::derived::flow::channel::policy::PolicyHistory;
use crate::derived::flow::resource::ResourcePattern;
use crate::derived::flow::transmission::Transmission;
use crate::derived::flow::verdict::VerdictLog;
use crate::ids::{
    AgentId, AlertId, AlertRuleId, AuditId, ChannelId, ConversationId, ExchangeId, ProjectionId,
    SinkId, SpanId, TransmissionId,
};
use crate::interfaces::l2_transport::{ConsumerGroup, DeadLetter};
use crate::interfaces::l6_analysis::SearchResults;
use crate::interfaces::l8_surface::actions::{ActionOutcome, OperatorAction};
use crate::interfaces::l8_surface::audit::{
    AuditEntry, AuditError, AuditFilter, AuditIntent, AuditIntents, AuditLog,
};
use crate::interfaces::l8_surface::channel_traffic::{
    ChannelTransmissionFilter, ChannelTransmissionPage,
};
use crate::interfaces::l8_surface::channels::{ChannelName, ChannelRow, PromotionPreview};
use crate::interfaces::l8_surface::conversation::ExchangePlacement;
use crate::interfaces::l8_surface::conversation::text::{
    ConversationText, PartText, TextLimit, TextSlice,
};
use crate::interfaces::l8_surface::conversation::turn::{Reader, TurnPage};
use crate::interfaces::l8_surface::conversation::{
    ConversationFilter, ConversationHead, ConversationRow, SpanPoint, TurnWindow,
};
use crate::interfaces::l8_surface::errors::{ActionError, QueryError};
use crate::interfaces::l8_surface::evidence::TransmissionEvidence;
use crate::interfaces::l8_surface::excerpt::ExcerptWindow;
use crate::interfaces::l8_surface::export::manifest::SourceFailure;
use crate::interfaces::l8_surface::export::request::ExportRequest;
use crate::interfaces::l8_surface::export::rows::ExportRow;
use crate::interfaces::l8_surface::export::stream::{
    Export, ExportPlan, ExportPlanError, ExportSource, ExportStep, ExportStream, RowSource,
};
use crate::interfaces::l8_surface::lists::{
    AlertRuleFilter, ChannelFilter, SearchRequest, TopicPage,
};
use crate::interfaces::l8_surface::live::{LiveEnd, LiveFeed, LiveItem, LiveStream, Resume};
use crate::interfaces::l8_surface::operators::Operator;
use crate::interfaces::l8_surface::overview::OverviewCounts;
use crate::interfaces::l8_surface::permissions::Caller;
use crate::interfaces::l8_surface::sinks::{AlertSink, SinkError, SinkInfo};
use crate::interfaces::l8_surface::summary::{TransmissionPage, TransmissionSelection};
use crate::interfaces::l8_surface::{AlertFilter, OperatorActions, Present, QueryApi};
use crate::observed::message::PartRef;
use crate::paging::{
    AgentList, AlertList, AlertRuleList, AuditList, ChannelList, ChannelTransmissionList,
    DeadLetterList, EdgeTransmissionList, ProjectionList, ResourceUseList, SearchList, TopicList,
    TransmissionList,
};
use crate::paging::{ConversationList, SpanReaderList};
use crate::paging::{Page, PageRequest};
use crate::support::{TimeWindow, Watermark};
use std::collections::BTreeMap;

use super::{Dummy, arg, assert_send, assert_send_static};

// ── L8 surface ─────────────────────────────────────────────────────────

impl QueryApi for Dummy {
    type ExportRows = Dummy;
    async fn channel(
        &self,
        _caller: &Caller,
        _id: ChannelId,
        _window: Option<TimeWindow>,
    ) -> Result<Option<Watermarked<ChannelRow>>, QueryError> {
        match *self {}
    }
    async fn policy_history(
        &self,
        _caller: &Caller,
        _channel: ChannelId,
    ) -> Result<Option<PolicyHistory>, QueryError> {
        match *self {}
    }
    async fn channels(
        &self,
        _caller: &Caller,
        _filter: &ChannelFilter,
        _page: &PageRequest<ChannelList>,
    ) -> Result<Watermarked<Page<ChannelRow, ChannelList>>, QueryError> {
        match *self {}
    }
    async fn channel_transmissions(
        &self,
        _caller: &Caller,
        _channel: ChannelId,
        _filter: &ChannelTransmissionFilter,
        _version: TopicVersionSelector,
        _page: &PageRequest<ChannelTransmissionList>,
    ) -> Result<ChannelTransmissionPage, QueryError> {
        match *self {}
    }
    async fn channel_names(
        &self,
        _caller: &Caller,
        _ids: &IdBatch<ChannelId>,
    ) -> Result<BTreeMap<ChannelId, ChannelName>, QueryError> {
        match *self {}
    }
    async fn promotion_preview(
        &self,
        _caller: &Caller,
        _channel: ChannelId,
        _pattern: &ResourcePattern,
    ) -> Result<PromotionPreview, QueryError> {
        match *self {}
    }
    async fn agents(
        &self,
        _caller: &Caller,
        _filter: &AgentFilter,
        _window: TimeWindow,
        _page: &PageRequest<AgentList>,
    ) -> Result<Watermarked<Page<AgentRow, AgentList>>, QueryError> {
        match *self {}
    }
    async fn agent(
        &self,
        _caller: &Caller,
        _id: AgentId,
        _window: TimeWindow,
    ) -> Result<Option<Watermarked<AgentDetail>>, QueryError> {
        match *self {}
    }
    async fn agent_names(
        &self,
        _caller: &Caller,
        _ids: &IdBatch<AgentId>,
    ) -> Result<BTreeMap<AgentId, AgentName>, QueryError> {
        match *self {}
    }
    async fn conversations(
        &self,
        _caller: &Caller,
        _filter: &ConversationFilter,
        _page: &PageRequest<ConversationList>,
    ) -> Result<Page<ConversationRow, ConversationList>, QueryError> {
        match *self {}
    }
    async fn conversation(
        &self,
        _caller: &Caller,
        _id: ConversationId,
    ) -> Result<Option<ConversationHead>, QueryError> {
        match *self {}
    }
    async fn conversation_turns(
        &self,
        _caller: &Caller,
        _id: ConversationId,
        _window: &TurnWindow,
    ) -> Result<Option<TurnPage>, QueryError> {
        match *self {}
    }
    async fn span_readers(
        &self,
        _caller: &Caller,
        _span: SpanId,
        _page: &PageRequest<SpanReaderList>,
    ) -> Result<Option<Page<Reader, SpanReaderList>>, QueryError> {
        match *self {}
    }
    async fn exchange_turns(
        &self,
        _caller: &Caller,
        _ids: &IdBatch<ExchangeId>,
    ) -> Result<BTreeMap<ExchangeId, ExchangePlacement>, QueryError> {
        match *self {}
    }
    async fn span_points(
        &self,
        _caller: &Caller,
        _ids: &IdBatch<SpanId>,
    ) -> Result<BTreeMap<SpanId, SpanPoint>, QueryError> {
        match *self {}
    }
    async fn conversation_text(
        &self,
        _caller: &Caller,
        _id: ConversationId,
        _window: &TurnWindow,
        _limit: TextLimit,
    ) -> Result<Option<ConversationText>, QueryError> {
        match *self {}
    }
    async fn part_text(
        &self,
        _caller: &Caller,
        _part: PartRef,
        _slice: TextSlice,
    ) -> Result<Option<PartText>, QueryError> {
        match *self {}
    }
    async fn alert_rules(
        &self,
        _caller: &Caller,
        _filter: &AlertRuleFilter,
        _page: &PageRequest<AlertRuleList>,
    ) -> Result<Page<AlertRuleDef, AlertRuleList>, QueryError> {
        match *self {}
    }
    async fn alert_rule(
        &self,
        _caller: &Caller,
        _id: AlertRuleId,
    ) -> Result<Option<AlertRuleDef>, QueryError> {
        match *self {}
    }
    async fn sinks(&self, _caller: &Caller) -> Result<Vec<SinkInfo>, QueryError> {
        match *self {}
    }
    async fn dead_letters(
        &self,
        _caller: &Caller,
        _group: Option<&ConsumerGroup>,
        _page: &PageRequest<DeadLetterList>,
    ) -> Result<Page<DeadLetter, DeadLetterList>, QueryError> {
        match *self {}
    }
    async fn alerts(
        &self,
        _caller: &Caller,
        _filter: &AlertFilter,
        _page: &PageRequest<AlertList>,
    ) -> Result<Page<Alert, AlertList>, QueryError> {
        match *self {}
    }
    async fn alert(&self, _caller: &Caller, _id: AlertId) -> Result<Option<Alert>, QueryError> {
        match *self {}
    }
    async fn watermark(&self, _caller: &Caller) -> Result<Watermark, QueryError> {
        match *self {}
    }
    async fn topology(
        &self,
        _caller: &Caller,
        _window: TimeWindow,
        _weighting: Weighting,
        _filter: &TopologyFilter,
    ) -> Result<Watermarked<TopologyGraph>, QueryError> {
        match *self {}
    }
    async fn overview(
        &self,
        _caller: &Caller,
        _window: TimeWindow,
        _filter: &TopologyFilter,
    ) -> Result<Watermarked<OverviewCounts>, QueryError> {
        match *self {}
    }
    async fn channel_topology(
        &self,
        _caller: &Caller,
        _window: TimeWindow,
        _weighting: Weighting,
        _filter: &TopologyFilter,
    ) -> Result<Watermarked<BipartiteGraph>, QueryError> {
        match *self {}
    }
    async fn channel_resources(
        &self,
        _caller: &Caller,
        _channel: ChannelId,
        _window: TimeWindow,
        _page: &PageRequest<ResourceUseList>,
    ) -> Result<Watermarked<ResourceUsePage>, QueryError> {
        match *self {}
    }
    async fn edge_transmissions(
        &self,
        _caller: &Caller,
        _edge: &EdgeSelector,
        _window: TimeWindow,
        _filter: &TopologyFilter,
        _page: &PageRequest<EdgeTransmissionList>,
    ) -> Result<Watermarked<EdgeTransmissionPage>, QueryError> {
        match *self {}
    }
    async fn transmissions_by_id(
        &self,
        _caller: &Caller,
        _selection: &TransmissionSelection,
        _version: TopicVersionSelector,
        _page: &PageRequest<TransmissionList>,
    ) -> Result<TransmissionPage, QueryError> {
        match *self {}
    }
    async fn series(
        &self,
        _caller: &Caller,
        _grid: SeriesGrid,
        _weighting: Weighting,
        _grouping: SeriesGrouping,
        _filter: &TopologyFilter,
    ) -> Result<Watermarked<TopologySeries>, QueryError> {
        match *self {}
    }
    async fn topic_versions(&self, _caller: &Caller) -> Result<TopicVersionHistory, QueryError> {
        match *self {}
    }
    async fn topic_sizes(
        &self,
        _caller: &Caller,
        _version: Option<TopicModelVersion>,
        _window: Option<TimeWindow>,
    ) -> Result<Watermarked<TopicSizes>, QueryError> {
        match *self {}
    }
    async fn topic_lineage(
        &self,
        _caller: &Caller,
        _from: TopicModelVersion,
    ) -> Result<Option<TopicLineage>, QueryError> {
        match *self {}
    }
    async fn search(
        &self,
        _caller: &Caller,
        _request: &SearchRequest,
        _window: Option<TimeWindow>,
        _filter: &TopologyFilter,
        _page: &PageRequest<SearchList>,
    ) -> Result<SearchResults, QueryError> {
        match *self {}
    }
    async fn transmission(
        &self,
        _caller: &Caller,
        _id: TransmissionId,
    ) -> Result<Option<Transmission>, QueryError> {
        match *self {}
    }
    async fn transmission_evidence(
        &self,
        _caller: &Caller,
        _id: TransmissionId,
        _window: ExcerptWindow,
    ) -> Result<Option<TransmissionEvidence>, QueryError> {
        match *self {}
    }
    async fn topics(
        &self,
        _caller: &Caller,
        _version: TopicVersionSelector,
        _page: &PageRequest<TopicList>,
    ) -> Result<TopicPage, QueryError> {
        match *self {}
    }
    async fn fit_projection(
        &self,
        _caller: &Caller,
        _window: TimeWindow,
        _filter: &TopologyFilter,
        _params: ProjectionParams,
    ) -> Result<ProjectionId, QueryError> {
        match *self {}
    }
    async fn projection_status(
        &self,
        _caller: &Caller,
        _id: ProjectionId,
    ) -> Result<ProjectionInfo, QueryError> {
        match *self {}
    }
    async fn projections(
        &self,
        _caller: &Caller,
        _page: &PageRequest<ProjectionList>,
    ) -> Result<Page<ProjectionInfo, ProjectionList>, QueryError> {
        match *self {}
    }
    async fn projection(
        &self,
        _caller: &Caller,
        _id: ProjectionId,
    ) -> Result<Projection, QueryError> {
        match *self {}
    }
    async fn verdicts(
        &self,
        _caller: &Caller,
        _transmission: TransmissionId,
    ) -> Result<Option<VerdictLog>, QueryError> {
        match *self {}
    }
    async fn detection_quality(
        &self,
        _caller: &Caller,
        _window: TimeWindow,
    ) -> Result<DetectionQuality, QueryError> {
        match *self {}
    }
    async fn audit(
        &self,
        _caller: &Caller,
        _filter: &AuditFilter,
        _page: &PageRequest<AuditList>,
    ) -> Result<Page<AuditEntry, AuditList>, QueryError> {
        match *self {}
    }
    async fn operators(&self, _caller: &Caller) -> Result<Vec<Operator>, QueryError> {
        match *self {}
    }
    async fn me(&self, _caller: &Caller) -> Result<Operator, QueryError> {
        match *self {}
    }
    async fn present(&self, _caller: &Caller) -> Result<Present, QueryError> {
        match *self {}
    }
    async fn export(
        &self,
        _caller: &Caller,
        _request: &ExportRequest,
    ) -> Result<Export<Self::ExportRows>, QueryError> {
        match *self {}
    }
}

impl OperatorActions for Dummy {
    async fn act(
        &self,
        _caller: &Caller,
        _action: OperatorAction,
    ) -> Result<ActionOutcome, ActionError> {
        match *self {}
    }
}

fn query_api<T: QueryApi>(x: &T, never: &Dummy) {
    assert_send_static::<T::ExportRows>();
    assert_send(x.channel(arg(never), arg(never), arg(never)));
    assert_send(x.policy_history(arg(never), arg(never)));
    assert_send(x.channels(arg(never), arg(never), arg(never)));
    assert_send(x.channel_transmissions(
        arg(never),
        arg(never),
        arg(never),
        arg(never),
        arg(never),
    ));
    assert_send(x.channel_names(arg(never), arg(never)));
    assert_send(x.promotion_preview(arg(never), arg(never), arg(never)));
    assert_send(x.agents(arg(never), arg(never), arg(never), arg(never)));
    assert_send(x.agent(arg(never), arg(never), arg(never)));
    assert_send(x.agent_names(arg(never), arg(never)));
    assert_send(x.conversations(arg(never), arg(never), arg(never)));
    assert_send(x.conversation(arg(never), arg(never)));
    assert_send(x.conversation_turns(arg(never), arg(never), arg(never)));
    assert_send(x.span_readers(arg(never), arg(never), arg(never)));
    assert_send(x.exchange_turns(arg(never), arg(never)));
    assert_send(x.span_points(arg(never), arg(never)));
    assert_send(x.conversation_text(arg(never), arg(never), arg(never), arg(never)));
    assert_send(x.part_text(arg(never), arg(never), arg(never)));
    assert_send(x.alert_rules(arg(never), arg(never), arg(never)));
    assert_send(x.alert_rule(arg(never), arg(never)));
    assert_send(x.sinks(arg(never)));
    assert_send(x.dead_letters(arg(never), arg(never), arg(never)));
    assert_send(x.alerts(arg(never), arg(never), arg(never)));
    assert_send(x.alert(arg(never), arg(never)));
    assert_send(x.watermark(arg(never)));
    assert_send(x.topology(arg(never), arg(never), arg(never), arg(never)));
    assert_send(x.overview(arg(never), arg(never), arg(never)));
    assert_send(x.channel_topology(arg(never), arg(never), arg(never), arg(never)));
    assert_send(x.channel_resources(arg(never), arg(never), arg(never), arg(never)));
    assert_send(x.edge_transmissions(arg(never), arg(never), arg(never), arg(never), arg(never)));
    assert_send(x.transmissions_by_id(arg(never), arg(never), arg(never), arg(never)));
    assert_send(x.series(arg(never), arg(never), arg(never), arg(never), arg(never)));
    assert_send(x.topic_versions(arg(never)));
    assert_send(x.topic_sizes(arg(never), arg(never), arg(never)));
    assert_send(x.topic_lineage(arg(never), arg(never)));
    assert_send(x.search(arg(never), arg(never), arg(never), arg(never), arg(never)));
    assert_send(x.transmission(arg(never), arg(never)));
    assert_send(x.transmission_evidence(arg(never), arg(never), arg(never)));
    assert_send(x.topics(arg(never), arg(never), arg(never)));
    assert_send(x.fit_projection(arg(never), arg(never), arg(never), arg(never)));
    assert_send(x.projection_status(arg(never), arg(never)));
    assert_send(x.projections(arg(never), arg(never)));
    assert_send(x.projection(arg(never), arg(never)));
    assert_send(x.verdicts(arg(never), arg(never)));
    assert_send(x.detection_quality(arg(never), arg(never)));
    assert_send(x.audit(arg(never), arg(never), arg(never)));
    assert_send(x.operators(arg(never)));
    assert_send(x.me(arg(never)));
    assert_send(x.present(arg(never)));
    assert_send(x.export(arg(never), arg(never)));
}

fn operator_actions<T: OperatorActions>(x: &T, never: &Dummy) {
    assert_send(x.act(arg(never), arg(never)));
}

impl LiveFeed for Dummy {
    type Stream = Dummy;
    async fn subscribe(
        &self,
        _caller: &Caller,
        _resume: Resume,
    ) -> Result<Self::Stream, QueryError> {
        match *self {}
    }
}

impl LiveStream for Dummy {
    async fn next(&mut self) -> Result<LiveItem, LiveEnd> {
        match *self {}
    }
}

fn live_feed<T: LiveFeed>(x: &T, never: &Dummy) {
    assert_send_static::<T::Stream>();
    assert_send(x.subscribe(arg(never), arg(never)));
}

fn live_stream<T: LiveStream>(x: &mut T) {
    assert_send(x.next());
}

impl AuditLog for Dummy {
    async fn append(&mut self, _entry: AuditEntry) -> Result<(), AuditError> {
        match *self {}
    }
    async fn query(
        &self,
        _filter: &AuditFilter,
        _page: &PageRequest<AuditList>,
    ) -> Result<Page<AuditEntry, AuditList>, AuditError> {
        match *self {}
    }
}

fn audit_log<T: AuditLog>(x: &mut T, never: &Dummy) {
    assert_send(x.append(arg(never)));
    assert_send(x.query(arg(never), arg(never)));
}

impl AuditIntents for Dummy {
    async fn intend(&mut self, _intent: &AuditIntent) -> Result<(), AuditError> {
        match *self {}
    }
    async fn complete(&mut self, _entry: AuditEntry) -> Result<(), AuditError> {
        match *self {}
    }
    async fn recover_interrupted(&mut self) -> Result<Vec<AuditId>, AuditError> {
        match *self {}
    }
}

fn audit_intents<T: AuditIntents>(x: &mut T, never: &Dummy) {
    assert_send(x.intend(arg(never)));
    assert_send(x.complete(arg(never)));
    assert_send(x.recover_interrupted());
}

impl AlertSink for Dummy {
    fn id(&self) -> SinkId {
        match *self {}
    }
    async fn deliver(&self, _alert: &Alert) -> Result<(), SinkError> {
        match *self {}
    }
}

fn alert_sink<T: AlertSink>(x: &T, never: &Dummy) {
    assert_send(x.deliver(arg(never)));
}

impl ExportStream for Dummy {
    async fn next(self) -> ExportStep<Self> {
        match self {}
    }
}

impl RowSource for Dummy {
    async fn next(&mut self) -> Result<Option<ExportRow>, SourceFailure> {
        match *self {}
    }
}

impl ExportSource for Dummy {
    type Rows = Dummy;
    async fn plan(
        &self,
        _request: &ExportRequest,
        _watermark: Watermark,
    ) -> Result<ExportPlan<Self::Rows>, ExportPlanError> {
        match *self {}
    }
}

fn export_stream<T: ExportStream>(x: T) {
    assert_send(x.next());
}

fn row_source<T: RowSource>(x: &mut T) {
    assert_send(x.next());
}

fn export_source<T: ExportSource>(x: &T, never: &Dummy) {
    assert_send_static::<T::Rows>();
    assert_send(x.plan(arg(never), arg(never)));
}

#[test]
fn l8_surface_futures_are_send() {
    let _ = query_api::<Dummy>;
    let _ = operator_actions::<Dummy>;
    let _ = live_feed::<Dummy>;
    let _ = live_stream::<Dummy>;
    let _ = audit_log::<Dummy>;
    let _ = audit_intents::<Dummy>;
    let _ = alert_sink::<Dummy>;
    let _ = export_stream::<Dummy>;
    let _ = row_source::<Dummy>;
    let _ = export_source::<Dummy>;
}
