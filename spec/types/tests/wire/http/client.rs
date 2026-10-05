//! A client over the route table: [`TableClient`] implements `QueryApi` by
//! encoding each call with [`RequestBuilder`] for the method's route and
//! recording it. Implementing the trait means a `QueryApi` method added
//! without a route does not compile here; the tests then check that every
//! recorded request resolves back to its route and carries exactly the
//! table's arguments, and that every query route is called by its method.

use std::collections::{BTreeMap, HashSet};
use std::sync::Mutex;

use super::super::{ULID_A, ULID_B, id};
use super::{agent, channel, edge, filter, grid, page, params, ready, transmission, window};
use crate::aggregates::access::{BipartiteGraph, ResourceUsePage};
use crate::aggregates::agents::{AgentDetail, AgentName, AgentRow};
use crate::aggregates::alert::{Alert, AlertRuleDef};
use crate::aggregates::edge::{
    EdgeSelector, EdgeTransmissionPage, TopologyFilter, TopologyGraph, Weighting,
};
use crate::aggregates::filter::TopicVersionSelector;
use crate::aggregates::projection::{Projection, ProjectionInfo, ProjectionParams};
use crate::aggregates::quality::DetectionQuality;
use crate::aggregates::series::{SeriesGrid, SeriesGrouping, TopologySeries};
use crate::aggregates::topic::TopicModelVersion;
use crate::aggregates::topic_history::{TopicLineage, TopicSizes, TopicVersionHistory};
use crate::aggregates::watermark::{Watermark, Watermarked};
use crate::batch::IdBatch;
use crate::derived::flow::channel::confirmation::Confirmation;
use crate::derived::flow::channel::policy::PolicyHistory;
use crate::derived::flow::resource::{Host, ResourcePattern};
use crate::derived::flow::transmission::Transmission;
use crate::derived::flow::verdict::VerdictLog;
use crate::ids::{AgentId, AlertId, AlertRuleId, ChannelId, ProjectionId, TransmissionId};
use crate::interfaces::l2_transport::{ConsumerGroup, DeadLetter};
use crate::interfaces::l6_analysis::SearchResults;
use crate::interfaces::l8_surface::audit::{AuditEntry, AuditFilter};
use crate::interfaces::l8_surface::channel_traffic::{
    ChannelTransmissionFilter, ChannelTransmissionPage,
};
use crate::interfaces::l8_surface::channels::{ChannelName, ChannelRow, PromotionPreview};
use crate::interfaces::l8_surface::evidence::TransmissionEvidence;
use crate::interfaces::l8_surface::excerpt::ExcerptWindow;
use crate::interfaces::l8_surface::export::{
    Export, ExportDataset, ExportFormat, ExportRequest, ExportStep, ExportStream,
};
use crate::interfaces::l8_surface::http::bodies::{
    EdgeTransmissionsBody, FitProjectionBody, GraphBody, OverviewBody, SearchBody, SeriesBody,
    TransmissionsBody,
};
use crate::interfaces::l8_surface::http::request::check_body;
use crate::interfaces::l8_surface::http::{
    EncodedRequest, Place, QueryParams, RequestBuilder, Route, Source, Target, resolve,
};
use crate::interfaces::l8_surface::lists::{
    AgentFilter, AlertRuleFilter, ChannelFilter, SearchMode, SearchRequest, TopicPage,
};
use crate::interfaces::l8_surface::operators::Operator;
use crate::interfaces::l8_surface::overview::OverviewCounts;
use crate::interfaces::l8_surface::summary::{TransmissionPage, TransmissionSelection};
use crate::interfaces::l8_surface::{
    AlertFilter, Caller, Permission, Present, QueryApi, QueryError, SinkInfo,
};
use crate::paging::{
    AgentList, AlertList, AlertRuleList, AuditList, ChannelList, ChannelTransmissionList,
    DeadLetterList, EdgeTransmissionList, Page, PageRequest, ProjectionList, ResourceUseList,
    SearchList, TopicList, TransmissionList,
};
use crate::support::{NonBlank, TimeWindow};

/// No export is ever streamed by the table client.
pub(super) enum NoRows {}

impl ExportStream for NoRows {
    async fn next(self) -> ExportStep<Self> {
        match self {}
    }
}

/// Records the request each call encodes to, and answers `NotFound`.
#[derive(Default)]
pub(super) struct TableClient {
    calls: Mutex<Vec<(Route, EncodedRequest)>>,
}

impl TableClient {
    fn send<T>(
        &self,
        route: Route,
        encode: impl FnOnce(RequestBuilder) -> RequestBuilder,
    ) -> Result<T, QueryError> {
        let request = encode(RequestBuilder::new(route))
            .build()
            .unwrap_or_else(|error| panic!("{route:?} does not follow the table: {error:?}"));
        self.calls
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .push((route, request));
        Err(QueryError::NotFound)
    }

    pub(super) fn calls(self) -> Vec<(Route, EncodedRequest)> {
        self.calls
            .into_inner()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

impl QueryApi for TableClient {
    type ExportRows = NoRows;

    async fn channel(
        &self,
        _: &Caller,
        id: ChannelId,
        window: Option<TimeWindow>,
    ) -> Result<Option<Watermarked<ChannelRow>>, QueryError> {
        self.send(Route::Channel, |b| {
            b.path("id", &id).query("window", &window)
        })
    }

    async fn policy_history(
        &self,
        _: &Caller,
        channel: ChannelId,
    ) -> Result<Option<PolicyHistory>, QueryError> {
        self.send(Route::PolicyHistory, |b| b.path("id", &channel))
    }

    async fn channels(
        &self,
        _: &Caller,
        filter: &ChannelFilter,
        page: &PageRequest<ChannelList>,
    ) -> Result<Watermarked<Page<ChannelRow, ChannelList>>, QueryError> {
        self.send(Route::Channels, |b| {
            b.query("filter", filter).query("page", page)
        })
    }

    async fn channel_transmissions(
        &self,
        _: &Caller,
        channel: ChannelId,
        filter: &ChannelTransmissionFilter,
        version: TopicVersionSelector,
        page: &PageRequest<ChannelTransmissionList>,
    ) -> Result<ChannelTransmissionPage, QueryError> {
        self.send(Route::ChannelTransmissions, |b| {
            b.path("id", &channel)
                .query("filter", filter)
                .query("version", &version)
                .query("page", page)
        })
    }

    async fn channel_names(
        &self,
        _: &Caller,
        ids: &IdBatch<ChannelId>,
    ) -> Result<BTreeMap<ChannelId, ChannelName>, QueryError> {
        self.send(Route::ChannelNames, |b| b.body(ids))
    }

    async fn promotion_preview(
        &self,
        _: &Caller,
        channel: ChannelId,
        pattern: &ResourcePattern,
    ) -> Result<PromotionPreview, QueryError> {
        self.send(Route::PromotionPreview, |b| {
            b.path("id", &channel).query("pattern", pattern)
        })
    }

    async fn agents(
        &self,
        _: &Caller,
        filter: &AgentFilter,
        window: TimeWindow,
        page: &PageRequest<AgentList>,
    ) -> Result<Watermarked<Page<AgentRow, AgentList>>, QueryError> {
        self.send(Route::Agents, |b| {
            b.query("filter", filter)
                .query("window", &window)
                .query("page", page)
        })
    }

    async fn agent(
        &self,
        _: &Caller,
        id: AgentId,
        window: TimeWindow,
    ) -> Result<Option<Watermarked<AgentDetail>>, QueryError> {
        self.send(Route::Agent, |b| b.path("id", &id).query("window", &window))
    }

    async fn agent_names(
        &self,
        _: &Caller,
        ids: &IdBatch<AgentId>,
    ) -> Result<BTreeMap<AgentId, AgentName>, QueryError> {
        self.send(Route::AgentNames, |b| b.body(ids))
    }

    async fn alert_rules(
        &self,
        _: &Caller,
        filter: &AlertRuleFilter,
        page: &PageRequest<AlertRuleList>,
    ) -> Result<Page<AlertRuleDef, AlertRuleList>, QueryError> {
        self.send(Route::AlertRules, |b| {
            b.query("filter", filter).query("page", page)
        })
    }

    async fn alert_rule(
        &self,
        _: &Caller,
        id: AlertRuleId,
    ) -> Result<Option<AlertRuleDef>, QueryError> {
        self.send(Route::AlertRule, |b| b.path("id", &id))
    }

    async fn sinks(&self, _: &Caller) -> Result<Vec<SinkInfo>, QueryError> {
        self.send(Route::Sinks, |b| b)
    }

    async fn dead_letters(
        &self,
        _: &Caller,
        group: Option<&ConsumerGroup>,
        page: &PageRequest<DeadLetterList>,
    ) -> Result<Page<DeadLetter, DeadLetterList>, QueryError> {
        self.send(Route::DeadLetters, |b| {
            b.query("group", &group.cloned()).query("page", page)
        })
    }

    async fn alerts(
        &self,
        _: &Caller,
        filter: &AlertFilter,
        page: &PageRequest<AlertList>,
    ) -> Result<Page<Alert, AlertList>, QueryError> {
        self.send(Route::Alerts, |b| {
            b.query("filter", filter).query("page", page)
        })
    }

    async fn alert(&self, _: &Caller, id: AlertId) -> Result<Option<Alert>, QueryError> {
        self.send(Route::Alert, |b| b.path("id", &id))
    }

    async fn watermark(&self, _: &Caller) -> Result<Watermark, QueryError> {
        self.send(Route::Watermark, |b| b)
    }

    async fn topology(
        &self,
        _: &Caller,
        window: TimeWindow,
        weighting: Weighting,
        filter: &TopologyFilter,
    ) -> Result<Watermarked<TopologyGraph>, QueryError> {
        self.send(Route::Topology, |b| {
            b.body(&GraphBody {
                window,
                weighting,
                filter: filter.clone(),
            })
        })
    }

    async fn overview(
        &self,
        _: &Caller,
        window: TimeWindow,
        filter: &TopologyFilter,
    ) -> Result<Watermarked<OverviewCounts>, QueryError> {
        self.send(Route::Overview, |b| {
            b.body(&OverviewBody {
                window,
                filter: filter.clone(),
            })
        })
    }

    async fn channel_topology(
        &self,
        _: &Caller,
        window: TimeWindow,
        weighting: Weighting,
        filter: &TopologyFilter,
    ) -> Result<Watermarked<BipartiteGraph>, QueryError> {
        self.send(Route::ChannelTopology, |b| {
            b.body(&GraphBody {
                window,
                weighting,
                filter: filter.clone(),
            })
        })
    }

    async fn channel_resources(
        &self,
        _: &Caller,
        channel: ChannelId,
        window: TimeWindow,
        page: &PageRequest<ResourceUseList>,
    ) -> Result<Watermarked<ResourceUsePage>, QueryError> {
        self.send(Route::ChannelResources, |b| {
            b.path("id", &channel)
                .query("window", &window)
                .query("page", page)
        })
    }

    async fn edge_transmissions(
        &self,
        _: &Caller,
        edge: &EdgeSelector,
        window: TimeWindow,
        filter: &TopologyFilter,
        page: &PageRequest<EdgeTransmissionList>,
    ) -> Result<Watermarked<EdgeTransmissionPage>, QueryError> {
        self.send(Route::EdgeTransmissions, |b| {
            b.body(&EdgeTransmissionsBody {
                edge: edge.clone(),
                window,
                filter: filter.clone(),
                page: page.clone(),
            })
        })
    }

    async fn transmissions_by_id(
        &self,
        _: &Caller,
        selection: &TransmissionSelection,
        version: TopicVersionSelector,
        page: &PageRequest<TransmissionList>,
    ) -> Result<TransmissionPage, QueryError> {
        self.send(Route::TransmissionsById, |b| {
            b.body(&TransmissionsBody {
                selection: selection.clone(),
                version,
                page: page.clone(),
            })
        })
    }

    async fn series(
        &self,
        _: &Caller,
        grid: SeriesGrid,
        weighting: Weighting,
        grouping: SeriesGrouping,
        filter: &TopologyFilter,
    ) -> Result<Watermarked<TopologySeries>, QueryError> {
        self.send(Route::Series, |b| {
            b.body(&SeriesBody {
                grid,
                weighting,
                grouping,
                filter: filter.clone(),
            })
        })
    }

    async fn topic_versions(&self, _: &Caller) -> Result<TopicVersionHistory, QueryError> {
        self.send(Route::TopicVersions, |b| b)
    }

    async fn topic_sizes(
        &self,
        _: &Caller,
        version: Option<TopicModelVersion>,
        window: Option<TimeWindow>,
    ) -> Result<Watermarked<TopicSizes>, QueryError> {
        self.send(Route::TopicSizes, |b| {
            b.query("version", &version).query("window", &window)
        })
    }

    async fn topic_lineage(
        &self,
        _: &Caller,
        from: TopicModelVersion,
    ) -> Result<Option<TopicLineage>, QueryError> {
        self.send(Route::TopicLineage, |b| b.path("version", &from))
    }

    async fn search(
        &self,
        _: &Caller,
        request: &SearchRequest,
        window: Option<TimeWindow>,
        filter: &TopologyFilter,
        page: &PageRequest<SearchList>,
    ) -> Result<SearchResults, QueryError> {
        self.send(Route::Search, |b| {
            b.body(&SearchBody {
                request: request.clone(),
                window,
                filter: filter.clone(),
                page: page.clone(),
            })
        })
    }

    async fn transmission(
        &self,
        _: &Caller,
        id: TransmissionId,
    ) -> Result<Option<Transmission>, QueryError> {
        self.send(Route::Transmission, |b| b.path("id", &id))
    }

    async fn transmission_evidence(
        &self,
        _: &Caller,
        id: TransmissionId,
        window: ExcerptWindow,
    ) -> Result<Option<TransmissionEvidence>, QueryError> {
        self.send(Route::TransmissionEvidence, |b| {
            b.path("id", &id).query("window", &window)
        })
    }

    async fn topics(
        &self,
        _: &Caller,
        version: TopicVersionSelector,
        page: &PageRequest<TopicList>,
    ) -> Result<TopicPage, QueryError> {
        self.send(Route::Topics, |b| {
            b.query("version", &version).query("page", page)
        })
    }

    async fn fit_projection(
        &self,
        _: &Caller,
        window: TimeWindow,
        filter: &TopologyFilter,
        params: ProjectionParams,
    ) -> Result<ProjectionId, QueryError> {
        self.send(Route::FitProjection, |b| {
            b.body(&FitProjectionBody {
                window,
                filter: filter.clone(),
                params,
            })
        })
    }

    async fn projection_status(
        &self,
        _: &Caller,
        id: ProjectionId,
    ) -> Result<ProjectionInfo, QueryError> {
        self.send(Route::ProjectionStatus, |b| b.path("id", &id))
    }

    async fn projections(
        &self,
        _: &Caller,
        page: &PageRequest<ProjectionList>,
    ) -> Result<Page<ProjectionInfo, ProjectionList>, QueryError> {
        self.send(Route::Projections, |b| b.query("page", page))
    }

    async fn projection(&self, _: &Caller, id: ProjectionId) -> Result<Projection, QueryError> {
        self.send(Route::ProjectionFrame, |b| b.path("id", &id))
    }

    async fn verdicts(
        &self,
        _: &Caller,
        transmission: TransmissionId,
    ) -> Result<Option<VerdictLog>, QueryError> {
        self.send(Route::Verdicts, |b| b.path("id", &transmission))
    }

    async fn detection_quality(
        &self,
        _: &Caller,
        window: TimeWindow,
    ) -> Result<DetectionQuality, QueryError> {
        self.send(Route::DetectionQuality, |b| b.query("window", &window))
    }

    async fn audit(
        &self,
        _: &Caller,
        filter: &AuditFilter,
        page: &PageRequest<AuditList>,
    ) -> Result<Page<AuditEntry, AuditList>, QueryError> {
        self.send(Route::Audit, |b| {
            b.query("filter", filter).query("page", page)
        })
    }

    async fn operators(&self, _: &Caller) -> Result<Vec<Operator>, QueryError> {
        self.send(Route::Operators, |b| b)
    }

    async fn me(&self, _: &Caller) -> Result<Operator, QueryError> {
        self.send(Route::Me, |b| b)
    }

    async fn present(&self, _: &Caller) -> Result<Present, QueryError> {
        self.send(Route::Present, |b| b)
    }

    async fn export(
        &self,
        _: &Caller,
        request: &ExportRequest,
    ) -> Result<Export<Self::ExportRows>, QueryError> {
        self.send(Route::Export, |b| b.body(request))
    }
}

fn caller() -> Caller {
    crate::tests::operators::caller(1, &Permission::ALL)
}

/// Calls every `QueryApi` method once, with its `Option` arguments set, and
/// returns what each encoded to. `with_none` leaves them `None` instead.
pub(super) fn every_call(with_none: bool) -> Vec<(Route, EncodedRequest)> {
    let client = TableClient::default();
    let c = &caller();
    let some_window = (!with_none).then(window);
    let version = (!with_none).then_some(TopicModelVersion(3));
    let group = ConsumerGroup("l6-triage".into());
    let group = (!with_none).then_some(&group);
    let pattern = ResourcePattern::UrlPrefix {
        host: Host("wiki.corp.internal".into()),
        path_prefix: "/eng".into(),
    };
    let channels = IdBatch::new([channel()]).expect("one id");
    let agents = IdBatch::new([agent(ULID_A), agent(ULID_B)]).expect("two ids");
    let selection = TransmissionSelection::new(vec![transmission()]).expect("one id");
    let search = SearchRequest {
        mode: SearchMode::Hybrid,
        text: NonBlank::new("deploy credentials").expect("not blank"),
    };
    let export = ExportRequest::new(
        ExportDataset::Verdicts(window()),
        ExportFormat::Jsonl,
        false,
    )
    .expect("a valid request");
    let alert = id(AlertId::from_ulid_text, ULID_A);
    let rule = id(AlertRuleId::from_ulid_text, ULID_B);
    let projection = id(ProjectionId::from_ulid_text, ULID_B);
    let w = window();
    // Every call answers `NotFound`; what matters is what it recorded.
    let _ = ready(client.channel(c, channel(), some_window));
    let _ = ready(client.policy_history(c, channel()));
    let _ = ready(client.channels(c, &ChannelFilter::default(), &page()));
    let _ = ready(client.channel_names(c, &channels));
    let _ = ready(client.promotion_preview(c, channel(), &pattern));
    let _ = ready(client.agents(c, &AgentFilter::default(), w, &page()));
    let _ = ready(client.agent(c, agent(ULID_A), w));
    let _ = ready(client.agent_names(c, &agents));
    let _ = ready(client.alert_rules(c, &AlertRuleFilter::default(), &page()));
    let _ = ready(client.alert_rule(c, rule));
    let _ = ready(client.sinks(c));
    let _ = ready(client.dead_letters(c, group, &page()));
    let _ = ready(client.alerts(c, &AlertFilter::default(), &page()));
    let _ = ready(client.alert(c, alert));
    let _ = ready(client.watermark(c));
    let _ = ready(client.topology(c, w, Weighting::Transmissions, &filter()));
    let _ = ready(client.overview(c, w, &filter()));
    let _ = ready(client.channel_topology(c, w, Weighting::MatchedBytes, &filter()));
    let _ = ready(client.channel_resources(c, channel(), w, &page()));
    let _ = ready(client.channel_transmissions(
        c,
        channel(),
        &ChannelTransmissionFilter {
            confirmation: Some(Confirmation::Unconfirmed),
        },
        TopicVersionSelector::Current,
        &page(),
    ));
    let _ = ready(client.edge_transmissions(c, &edge(), w, &filter(), &page()));
    let _ =
        ready(client.transmissions_by_id(c, &selection, TopicVersionSelector::Current, &page()));
    let _ = ready(client.series(
        c,
        grid(),
        Weighting::Transmissions,
        SeriesGrouping::Topic,
        &filter(),
    ));
    let _ = ready(client.topic_versions(c));
    let _ = ready(client.topic_sizes(c, version, some_window));
    let _ = ready(client.topic_lineage(c, TopicModelVersion(3)));
    let _ = ready(client.search(c, &search, some_window, &filter(), &page()));
    let _ = ready(client.transmission(c, transmission()));
    let _ = ready(client.transmission_evidence(c, transmission(), ExcerptWindow::DEFAULT));
    let _ = ready(client.topics(
        c,
        TopicVersionSelector::Pinned(TopicModelVersion(3)),
        &page(),
    ));
    let _ = ready(client.fit_projection(c, w, &filter(), params()));
    let _ = ready(client.projection_status(c, projection));
    let _ = ready(client.projections(c, &page()));
    let _ = ready(client.projection(c, projection));
    let _ = ready(client.verdicts(c, transmission()));
    let _ = ready(client.detection_quality(c, w));
    let _ = ready(client.audit(c, &AuditFilter::default(), &page()));
    let _ = ready(client.operators(c));
    let _ = ready(client.me(c));
    let _ = ready(client.export(c, &export));
    let _ = ready(client.present(c));
    client.calls()
}

/// The route's arguments in `place`, by name, in table order.
fn names(route: Route, place: Place) -> Vec<&'static str> {
    route.spec().args_in(place).map(|arg| arg.name).collect()
}

/// Checks one recorded request against its route, as the server reads it.
fn check(route: Route, request: &EncodedRequest, with_none: bool) {
    let spec = route.spec();
    assert_eq!(request.method, spec.method, "{route:?}");
    let (target, params) = resolve(request.method, &request.path).unwrap_or_else(|| {
        panic!(
            "{route:?}: {} {} resolves to no route",
            request.method.as_str(),
            request.path
        )
    });
    assert_eq!(target, Target::Route(route), "{route:?}: {}", request.path);
    assert_eq!(
        params.iter().map(|(name, _)| name).collect::<Vec<_>>(),
        names(route, Place::Path),
        "{route:?}: path parameters"
    );
    let sent: Vec<&str> = request.query.iter().map(|(name, _)| *name).collect();
    let expected: Vec<&str> = spec
        .args_in(Place::Query)
        .filter(|arg| !(with_none && arg.optional))
        .map(|arg| arg.name)
        .collect();
    assert_eq!(sent, expected, "{route:?}: query parameters");
    let pairs = request
        .query
        .iter()
        .map(|(name, value)| ((*name).to_owned(), value.clone()));
    QueryParams::new(route, pairs).unwrap_or_else(|error| panic!("{route:?}: {error:?}"));
    assert!(request.headers.is_empty(), "{route:?}: headers");
    let body = check_body(
        route,
        Some("application/json"),
        request.body.as_deref().unwrap_or_default(),
    )
    .unwrap_or_else(|error| panic!("{route:?}: {error:?}"));
    assert_eq!(body.is_some(), spec.has_body(), "{route:?}: body");
    let fields = names(route, Place::BodyField);
    if let Some(body) = body
        && !fields.is_empty()
    {
        let json: serde_json::Value =
            serde_json::from_slice(body).unwrap_or_else(|error| panic!("{route:?}: {error}"));
        let keys: Vec<&str> = json
            .as_object()
            .unwrap_or_else(|| panic!("{route:?}: the body is an object"))
            .keys()
            .map(String::as_str)
            .collect();
        let mut sorted = fields.clone();
        sorted.sort_unstable();
        assert_eq!(keys, sorted, "{route:?}: body fields");
    }
}

/// Every call encodes to a request the server resolves to the same route,
/// with exactly the table's path parameters, query parameters and body.
#[test]
fn every_query_method_encodes_to_its_route() {
    for with_none in [false, true] {
        for (route, request) in every_call(with_none) {
            check(route, &request, with_none);
        }
    }
}

/// Every query route of the table is some method's (none is orphaned), and
/// each is called by exactly one method.
#[test]
fn every_query_route_is_called_by_one_method() {
    let called: Vec<Route> = every_call(false)
        .into_iter()
        .map(|(route, _)| route)
        .collect();
    let unique: HashSet<Route> = called.iter().copied().collect();
    assert_eq!(unique.len(), called.len(), "a route called by two methods");
    for route in Route::all() {
        let is_query = matches!(route.spec().source, Source::Query(_));
        assert_eq!(unique.contains(&route), is_query, "{route:?}");
    }
}

/// An `Option` argument's `None` is left out of the query string, and the
/// server reads the missing parameter back as `None`.
#[test]
fn a_none_argument_is_left_out_and_reads_back_as_none() {
    let calls = every_call(true);
    let (_, request) = calls
        .iter()
        .find(|(route, _)| *route == Route::TopicSizes)
        .unwrap_or_else(|| panic!("topic_sizes was called"));
    assert!(request.query.is_empty(), "{:?}", request.query);
    let params = QueryParams::new(Route::TopicSizes, []).unwrap_or_else(|e| panic!("{e:?}"));
    assert_eq!(
        params.decode::<Option<TopicModelVersion>>("version"),
        Ok(None)
    );
    assert_eq!(params.decode::<Option<TimeWindow>>("window"), Ok(None));
}

/// A query parameter is the argument's compact JSON, and decodes back to it.
#[test]
fn query_parameters_are_compact_json_that_decode_back() {
    let request = RequestBuilder::new(Route::Agents)
        .query("filter", &AgentFilter::default())
        .query("window", &window())
        .query("page", &page::<AgentList>())
        .build()
        .unwrap_or_else(|error| panic!("{error:?}"));
    assert_eq!(request.path, "/agents");
    assert_eq!(
        request.query,
        vec![
            (
                "filter",
                r#"{"states":[],"claimed":[],"text":null,"parents":[]}"#.to_owned()
            ),
            (
                "window",
                r#"{"start":"2026-10-04T12:00:00.000000Z","end":"2026-10-04T13:00:00.000000Z"}"#
                    .to_owned()
            ),
            ("page", r#"{"size":50,"after":null}"#.to_owned()),
        ]
    );
    let pairs = request
        .query
        .iter()
        .map(|(name, value)| ((*name).to_owned(), value.clone()));
    let params = QueryParams::new(Route::Agents, pairs).unwrap_or_else(|e| panic!("{e:?}"));
    assert_eq!(params.decode::<TimeWindow>("window"), Ok(window()));
    assert_eq!(
        params.decode::<AgentFilter>("filter"),
        Ok(AgentFilter::default())
    );
    assert_eq!(params.decode::<PageRequest<AgentList>>("page"), Ok(page()));
}

/// A body route's body decodes, through `decode_request`, into its body
/// type with the call's arguments.
#[test]
fn a_body_decodes_back_into_the_call_arguments() {
    for (route, request) in every_call(false) {
        let body = request.body.as_deref().unwrap_or_default();
        let decoded = match route {
            Route::Topology | Route::ChannelTopology => {
                crate::wire::decode_request::<GraphBody>(body).map(|b| b.filter == filter())
            }
            Route::EdgeTransmissions => crate::wire::decode_request::<EdgeTransmissionsBody>(body)
                .map(|b| b.edge == edge() && b.filter == filter()),
            Route::Series => {
                crate::wire::decode_request::<SeriesBody>(body).map(|b| b.grid == grid())
            }
            Route::FitProjection => {
                crate::wire::decode_request::<FitProjectionBody>(body).map(|b| b.params == params())
            }
            _ => continue,
        };
        assert_eq!(decoded, Ok(true), "{route:?}");
    }
}

/// A projection id in the path: what `GET /projections/{id}/frame` reads.
#[test]
fn a_path_parameter_is_the_id_text() {
    let projection = id(ProjectionId::from_ulid_text, ULID_B);
    let request = RequestBuilder::new(Route::ProjectionFrame)
        .path("id", &projection)
        .build()
        .unwrap_or_else(|error| panic!("{error:?}"));
    assert_eq!(request.path, format!("/projections/{ULID_B}/frame"));
    let (target, params) =
        resolve(request.method, &request.path).unwrap_or_else(|| panic!("resolves"));
    assert_eq!(target, Target::Route(Route::ProjectionFrame));
    assert_eq!(params.decode::<ProjectionId>("id"), Ok(projection));
}
