//! A fake L8 surface: every method checks the route's permission first,
//! as the spec requires (`Forbidden` with nothing recorded), then records
//! the call (method, caller, arguments as JSON under the route's argument
//! names) and answers with what the test set: an error, or the golden
//! response for that method.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::Mutex;

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
    AgentId, AlertId, AlertRuleId, ChannelId, OperatorId, ProjectionId, TransmissionId,
};
use crosstalk_spec::interfaces::l2_transport::{ConsumerGroup, DeadLetter};
use crosstalk_spec::interfaces::l6_analysis::SearchResults;
use crosstalk_spec::interfaces::l8_surface::audit::{AuditEntry, AuditFilter};
use crosstalk_spec::interfaces::l8_surface::channel_traffic::{
    ChannelTransmissionFilter, ChannelTransmissionPage,
};
use crosstalk_spec::interfaces::l8_surface::channels::{ChannelName, ChannelRow, PromotionPreview};
use crosstalk_spec::interfaces::l8_surface::evidence::TransmissionEvidence;
use crosstalk_spec::interfaces::l8_surface::excerpt::ExcerptWindow;
use crosstalk_spec::interfaces::l8_surface::export::{
    Export, ExportHeader, ExportRequest, ExportRow, ExportStep, ExportStream, ExportTrailer,
};
use crosstalk_spec::interfaces::l8_surface::http::{Route, RoutePermission, Source};
use crosstalk_spec::interfaces::l8_surface::lists::{
    AgentFilter, AlertRuleFilter, ChannelFilter, SearchRequest, TopicPage,
};
use crosstalk_spec::interfaces::l8_surface::live::{
    LiveEnd, LiveFeed, LiveItem, LiveStream, Resume,
};
use crosstalk_spec::interfaces::l8_surface::operators::Operator;
use crosstalk_spec::interfaces::l8_surface::overview::OverviewCounts;
use crosstalk_spec::interfaces::l8_surface::summary::{TransmissionPage, TransmissionSelection};
use crosstalk_spec::interfaces::l8_surface::{
    ActionError, ActionOutcome, AlertFilter, Caller, OperatorAction, OperatorActions, Permission,
    Present, QueryApi, QueryError, SinkInfo,
};
use crosstalk_spec::paging::{
    AgentList, AlertList, AlertRuleList, AuditList, ChannelList, ChannelTransmissionList,
    DeadLetterList, EdgeTransmissionList, Page, PageRequest, ProjectionList, ResourceUseList,
    SearchList, TopicList, TransmissionList,
};
use crosstalk_spec::support::TimeWindow;
use serde::de::DeserializeOwned;
use serde_json::{Value, json};

/// One call that passed the permission check.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Call {
    pub method: &'static str,
    pub operator: OperatorId,
    pub args: Value,
}

/// An export the fake streams: its header, rows and trailer.
#[derive(Debug, Clone)]
pub(crate) struct ExportParts {
    pub header: ExportHeader,
    pub rows: Vec<ExportRow>,
    pub trailer: ExportTrailer,
}

#[derive(Default)]
pub(crate) struct Fake {
    /// The golden JSON each query method answers with, by method name.
    responses: Mutex<HashMap<&'static str, String>>,
    /// When set, every method answers this after recording the call.
    fail: Mutex<Option<QueryError>>,
    action: Mutex<Option<Result<ActionOutcome, ActionError>>>,
    projection: Mutex<Option<Projection>>,
    export: Mutex<Option<ExportParts>>,
    live: Mutex<Vec<Result<LiveItem, LiveEnd>>>,
    calls: Mutex<Vec<Call>>,
    actions: Mutex<Vec<(OperatorId, OperatorAction)>>,
}

fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

impl Fake {
    pub fn respond(&self, method: &'static str, json: String) {
        lock(&self.responses).insert(method, json);
    }

    pub fn fail_with(&self, error: QueryError) {
        *lock(&self.fail) = Some(error);
    }

    pub fn act_with(&self, result: Result<ActionOutcome, ActionError>) {
        *lock(&self.action) = Some(result);
    }

    pub fn project(&self, projection: Projection) {
        *lock(&self.projection) = Some(projection);
    }

    pub fn export_with(&self, parts: ExportParts) {
        *lock(&self.export) = Some(parts);
    }

    pub fn live_with(&self, items: Vec<Result<LiveItem, LiveEnd>>) {
        *lock(&self.live) = items;
    }

    pub fn calls(&self) -> Vec<Call> {
        lock(&self.calls).clone()
    }

    pub fn actions(&self) -> Vec<(OperatorId, OperatorAction)> {
        lock(&self.actions).clone()
    }

    /// Nothing reached any method past its permission check.
    pub fn untouched(&self) -> bool {
        lock(&self.calls).is_empty() && lock(&self.actions).is_empty()
    }

    fn method(route: Route) -> &'static str {
        match route.spec().source {
            Source::Query(name) => name,
            Source::Act(_) => "act",
            Source::Subscribe => "subscribe",
        }
    }

    /// The permission check (none for a route any caller may call), then
    /// the record; the configured failure, if any.
    fn enter(
        &self,
        route: Route,
        needs: Option<Permission>,
        caller: &Caller,
        args: Value,
    ) -> Result<(), QueryError> {
        if let Some(needs) = needs.filter(|needs| !caller.has(*needs)) {
            return Err(QueryError::Forbidden { missing: needs });
        }
        lock(&self.calls).push(Call {
            method: Self::method(route),
            operator: caller.operator(),
            args,
        });
        match lock(&self.fail).clone() {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    fn answer<T: DeserializeOwned>(
        &self,
        route: Route,
        caller: &Caller,
        args: Value,
    ) -> Result<T, QueryError> {
        let needs = match route.permission() {
            RoutePermission::Fixed(permission) => Some(permission),
            RoutePermission::AnyCaller => None,
            RoutePermission::ByExportRequest => Some(Permission::Content),
        };
        self.enter(route, needs, caller, args)?;
        let method = Self::method(route);
        let json = lock(&self.responses)
            .get(method)
            .cloned()
            .unwrap_or_else(|| panic!("no response set for {method}"));
        let value: T = serde_json::from_str(&json)
            .unwrap_or_else(|error| panic!("{method}'s response does not decode: {error}"));
        Ok(value)
    }
}

/// The rows of a fake export, then its trailer.
pub(crate) struct FakeRows {
    rows: VecDeque<ExportRow>,
    trailer: ExportTrailer,
}

impl ExportStream for FakeRows {
    async fn next(mut self) -> ExportStep<Self> {
        // A store read is not instant: give the server the chance to send
        // what it has, as it would while waiting on one.
        tokio::task::yield_now().await;
        match self.rows.pop_front() {
            Some(row) => ExportStep::Row(row, self),
            None => ExportStep::End(self.trailer),
        }
    }
}

/// The items of a fake live stream; `ShuttingDown` once they run out.
pub(crate) struct FakeLive(VecDeque<Result<LiveItem, LiveEnd>>);

impl LiveStream for FakeLive {
    async fn next(&mut self) -> Result<LiveItem, LiveEnd> {
        self.0.pop_front().unwrap_or(Err(LiveEnd::ShuttingDown))
    }
}

impl LiveFeed for Fake {
    type Stream = FakeLive;

    async fn subscribe(&self, caller: &Caller, resume: Resume) -> Result<FakeLive, QueryError> {
        self.enter(
            Route::Live,
            Some(Permission::View),
            caller,
            json!({ "resume": resume }),
        )?;
        Ok(FakeLive(lock(&self.live).clone().into()))
    }
}

impl OperatorActions for Fake {
    async fn act(
        &self,
        caller: &Caller,
        action: OperatorAction,
    ) -> Result<ActionOutcome, ActionError> {
        let needs = action.required_permission();
        if !caller.has(needs) {
            return Err(ActionError::Forbidden { missing: needs });
        }
        lock(&self.actions).push((caller.operator(), action));
        lock(&self.action)
            .clone()
            .unwrap_or(Ok(ActionOutcome::Applied))
    }
}

impl QueryApi for Fake {
    type ExportRows = FakeRows;

    async fn channel(
        &self,
        c: &Caller,
        id: ChannelId,
        window: Option<TimeWindow>,
    ) -> Result<Option<Watermarked<ChannelRow>>, QueryError> {
        self.answer(Route::Channel, c, json!({"id": id, "window": window}))
    }

    async fn policy_history(
        &self,
        c: &Caller,
        channel: ChannelId,
    ) -> Result<Option<PolicyHistory>, QueryError> {
        self.answer(Route::PolicyHistory, c, json!({"id": channel}))
    }

    async fn channels(
        &self,
        c: &Caller,
        filter: &ChannelFilter,
        page: &PageRequest<ChannelList>,
    ) -> Result<Watermarked<Page<ChannelRow, ChannelList>>, QueryError> {
        self.answer(Route::Channels, c, json!({"filter": filter, "page": page}))
    }

    async fn channel_transmissions(
        &self,
        c: &Caller,
        channel: ChannelId,
        filter: &ChannelTransmissionFilter,
        version: TopicVersionSelector,
        page: &PageRequest<ChannelTransmissionList>,
    ) -> Result<ChannelTransmissionPage, QueryError> {
        let args = json!({"id": channel, "filter": filter, "version": version, "page": page});
        self.answer(Route::ChannelTransmissions, c, args)
    }

    async fn channel_names(
        &self,
        c: &Caller,
        ids: &IdBatch<ChannelId>,
    ) -> Result<BTreeMap<ChannelId, ChannelName>, QueryError> {
        self.answer(Route::ChannelNames, c, json!({"body": ids}))
    }

    async fn promotion_preview(
        &self,
        c: &Caller,
        channel: ChannelId,
        pattern: &ResourcePattern,
    ) -> Result<PromotionPreview, QueryError> {
        let args = json!({"id": channel, "pattern": pattern});
        self.answer(Route::PromotionPreview, c, args)
    }

    async fn agents(
        &self,
        c: &Caller,
        filter: &AgentFilter,
        window: TimeWindow,
        page: &PageRequest<AgentList>,
    ) -> Result<Watermarked<Page<AgentRow, AgentList>>, QueryError> {
        let args = json!({"filter": filter, "window": window, "page": page});
        self.answer(Route::Agents, c, args)
    }

    async fn agent(
        &self,
        c: &Caller,
        id: AgentId,
        window: TimeWindow,
    ) -> Result<Option<Watermarked<AgentDetail>>, QueryError> {
        self.answer(Route::Agent, c, json!({"id": id, "window": window}))
    }

    async fn agent_names(
        &self,
        c: &Caller,
        ids: &IdBatch<AgentId>,
    ) -> Result<BTreeMap<AgentId, AgentName>, QueryError> {
        self.answer(Route::AgentNames, c, json!({"body": ids}))
    }

    async fn alert_rules(
        &self,
        c: &Caller,
        filter: &AlertRuleFilter,
        page: &PageRequest<AlertRuleList>,
    ) -> Result<Page<AlertRuleDef, AlertRuleList>, QueryError> {
        self.answer(
            Route::AlertRules,
            c,
            json!({"filter": filter, "page": page}),
        )
    }

    async fn alert_rule(
        &self,
        c: &Caller,
        id: AlertRuleId,
    ) -> Result<Option<AlertRuleDef>, QueryError> {
        self.answer(Route::AlertRule, c, json!({"id": id}))
    }

    async fn sinks(&self, c: &Caller) -> Result<Vec<SinkInfo>, QueryError> {
        self.answer(Route::Sinks, c, json!({}))
    }

    async fn dead_letters(
        &self,
        c: &Caller,
        group: Option<&ConsumerGroup>,
        page: &PageRequest<DeadLetterList>,
    ) -> Result<Page<DeadLetter, DeadLetterList>, QueryError> {
        self.answer(Route::DeadLetters, c, json!({"group": group, "page": page}))
    }

    async fn alerts(
        &self,
        c: &Caller,
        filter: &AlertFilter,
        page: &PageRequest<AlertList>,
    ) -> Result<Page<Alert, AlertList>, QueryError> {
        self.answer(Route::Alerts, c, json!({"filter": filter, "page": page}))
    }

    async fn alert(&self, c: &Caller, id: AlertId) -> Result<Option<Alert>, QueryError> {
        self.answer(Route::Alert, c, json!({"id": id}))
    }

    async fn watermark(&self, c: &Caller) -> Result<Watermark, QueryError> {
        self.answer(Route::Watermark, c, json!({}))
    }

    async fn present(&self, c: &Caller) -> Result<Present, QueryError> {
        self.answer(Route::Present, c, json!({}))
    }

    async fn topology(
        &self,
        c: &Caller,
        window: TimeWindow,
        weighting: Weighting,
        filter: &TopologyFilter,
    ) -> Result<Watermarked<TopologyGraph>, QueryError> {
        let args = json!({"window": window, "weighting": weighting, "filter": filter});
        self.answer(Route::Topology, c, args)
    }

    async fn overview(
        &self,
        c: &Caller,
        window: TimeWindow,
        filter: &TopologyFilter,
    ) -> Result<Watermarked<OverviewCounts>, QueryError> {
        self.answer(
            Route::Overview,
            c,
            json!({"window": window, "filter": filter}),
        )
    }

    async fn channel_topology(
        &self,
        c: &Caller,
        window: TimeWindow,
        weighting: Weighting,
        filter: &TopologyFilter,
    ) -> Result<Watermarked<BipartiteGraph>, QueryError> {
        let args = json!({"window": window, "weighting": weighting, "filter": filter});
        self.answer(Route::ChannelTopology, c, args)
    }

    async fn channel_resources(
        &self,
        c: &Caller,
        channel: ChannelId,
        window: TimeWindow,
        page: &PageRequest<ResourceUseList>,
    ) -> Result<Watermarked<ResourceUsePage>, QueryError> {
        let args = json!({"id": channel, "window": window, "page": page});
        self.answer(Route::ChannelResources, c, args)
    }

    async fn edge_transmissions(
        &self,
        c: &Caller,
        edge: &EdgeSelector,
        window: TimeWindow,
        filter: &TopologyFilter,
        page: &PageRequest<EdgeTransmissionList>,
    ) -> Result<Watermarked<EdgeTransmissionPage>, QueryError> {
        let args = json!({"edge": edge, "window": window, "filter": filter, "page": page});
        self.answer(Route::EdgeTransmissions, c, args)
    }

    async fn transmissions_by_id(
        &self,
        c: &Caller,
        selection: &TransmissionSelection,
        version: TopicVersionSelector,
        page: &PageRequest<TransmissionList>,
    ) -> Result<TransmissionPage, QueryError> {
        let args = json!({"selection": selection, "version": version, "page": page});
        self.answer(Route::TransmissionsById, c, args)
    }

    async fn series(
        &self,
        c: &Caller,
        grid: SeriesGrid,
        weighting: Weighting,
        grouping: SeriesGrouping,
        filter: &TopologyFilter,
    ) -> Result<Watermarked<TopologySeries>, QueryError> {
        let args = json!({
            "grid": grid, "weighting": weighting, "grouping": grouping, "filter": filter
        });
        self.answer(Route::Series, c, args)
    }

    async fn topic_versions(&self, c: &Caller) -> Result<TopicVersionHistory, QueryError> {
        self.answer(Route::TopicVersions, c, json!({}))
    }

    async fn topic_sizes(
        &self,
        c: &Caller,
        version: Option<TopicModelVersion>,
        window: Option<TimeWindow>,
    ) -> Result<Watermarked<TopicSizes>, QueryError> {
        let args = json!({"version": version, "window": window});
        self.answer(Route::TopicSizes, c, args)
    }

    async fn topic_lineage(
        &self,
        c: &Caller,
        from: TopicModelVersion,
    ) -> Result<Option<TopicLineage>, QueryError> {
        self.answer(Route::TopicLineage, c, json!({"version": from}))
    }

    async fn search(
        &self,
        c: &Caller,
        request: &SearchRequest,
        window: Option<TimeWindow>,
        filter: &TopologyFilter,
        page: &PageRequest<SearchList>,
    ) -> Result<SearchResults, QueryError> {
        let args = json!({"request": request, "window": window, "filter": filter, "page": page});
        self.answer(Route::Search, c, args)
    }

    async fn transmission(
        &self,
        c: &Caller,
        id: TransmissionId,
    ) -> Result<Option<Transmission>, QueryError> {
        self.answer(Route::Transmission, c, json!({"id": id}))
    }

    async fn transmission_evidence(
        &self,
        c: &Caller,
        id: TransmissionId,
        window: ExcerptWindow,
    ) -> Result<Option<TransmissionEvidence>, QueryError> {
        let args = json!({"id": id, "window": window});
        self.answer(Route::TransmissionEvidence, c, args)
    }

    async fn topics(
        &self,
        c: &Caller,
        version: TopicVersionSelector,
        page: &PageRequest<TopicList>,
    ) -> Result<TopicPage, QueryError> {
        self.answer(Route::Topics, c, json!({"version": version, "page": page}))
    }

    async fn fit_projection(
        &self,
        c: &Caller,
        window: TimeWindow,
        filter: &TopologyFilter,
        params: ProjectionParams,
    ) -> Result<ProjectionId, QueryError> {
        let args = json!({"window": window, "filter": filter, "params": params});
        self.answer(Route::FitProjection, c, args)
    }

    async fn projection_status(
        &self,
        c: &Caller,
        id: ProjectionId,
    ) -> Result<ProjectionInfo, QueryError> {
        self.answer(Route::ProjectionStatus, c, json!({"id": id}))
    }

    async fn projections(
        &self,
        c: &Caller,
        page: &PageRequest<ProjectionList>,
    ) -> Result<Page<ProjectionInfo, ProjectionList>, QueryError> {
        self.answer(Route::Projections, c, json!({"page": page}))
    }

    async fn projection(&self, c: &Caller, id: ProjectionId) -> Result<Projection, QueryError> {
        self.enter(
            Route::ProjectionFrame,
            Some(Permission::Content),
            c,
            json!({"id": id}),
        )?;
        lock(&self.projection).clone().ok_or(QueryError::NotFound)
    }

    async fn verdicts(
        &self,
        c: &Caller,
        transmission: TransmissionId,
    ) -> Result<Option<VerdictLog>, QueryError> {
        self.answer(Route::Verdicts, c, json!({"id": transmission}))
    }

    async fn detection_quality(
        &self,
        c: &Caller,
        window: TimeWindow,
    ) -> Result<DetectionQuality, QueryError> {
        self.answer(Route::DetectionQuality, c, json!({"window": window}))
    }

    async fn audit(
        &self,
        c: &Caller,
        filter: &AuditFilter,
        page: &PageRequest<AuditList>,
    ) -> Result<Page<AuditEntry, AuditList>, QueryError> {
        self.answer(Route::Audit, c, json!({"filter": filter, "page": page}))
    }

    async fn operators(&self, c: &Caller) -> Result<Vec<Operator>, QueryError> {
        self.answer(Route::Operators, c, json!({}))
    }

    async fn me(&self, c: &Caller) -> Result<Operator, QueryError> {
        self.answer(Route::Me, c, json!({}))
    }

    async fn export(
        &self,
        c: &Caller,
        request: &ExportRequest,
    ) -> Result<Export<FakeRows>, QueryError> {
        let needs = request.required_permission();
        self.enter(Route::Export, Some(needs), c, json!({"body": request}))?;
        let parts = lock(&self.export)
            .clone()
            .ok_or_else(|| QueryError::Store {
                reason: "no export set".to_owned(),
            })?;
        Ok(Export {
            header: parts.header,
            rows: FakeRows {
                rows: parts.rows.into(),
                trailer: parts.trailer,
            },
        })
    }
}
