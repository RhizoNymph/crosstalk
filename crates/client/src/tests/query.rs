//! Every `QueryApi` method sends its route's request: the request the stub
//! receives resolves back to the method's route with exactly the table's
//! path parameters, query parameters and body, and its bodies are the
//! spec's goldens byte for byte in JSON.

use crosstalk_spec::aggregates::filter::TopicVersionSelector;
use crosstalk_spec::aggregates::topic::TopicModelVersion;
use crosstalk_spec::batch::IdBatch;
use crosstalk_spec::derived::flow::channel::confirmation::Confirmation;
use crosstalk_spec::derived::flow::resource::{Host, ResourcePattern};
use crosstalk_spec::ids::{
    AlertId, AlertRuleId, ConversationId, ExchangeId, MessageHash, ProjectionId, SpanId,
};
use crosstalk_spec::interfaces::l2_transport::ConsumerGroup;
use crosstalk_spec::interfaces::l8_surface::audit::AuditFilter;
use crosstalk_spec::interfaces::l8_surface::channel_traffic::ChannelTransmissionFilter;
use crosstalk_spec::interfaces::l8_surface::conversation::text::{TextLimit, TextSlice};
use crosstalk_spec::interfaces::l8_surface::conversation::{
    ConversationFilter, TurnIndex, TurnWindow,
};
use crosstalk_spec::interfaces::l8_surface::excerpt::ExcerptWindow;
use crosstalk_spec::interfaces::l8_surface::export::{ExportDataset, ExportFormat, ExportRequest};
use crosstalk_spec::interfaces::l8_surface::http::bodies::{
    EdgeTransmissionsBody, FitProjectionBody, GraphBody, OverviewBody, SearchBody, SeriesBody,
    TransmissionsBody,
};
use crosstalk_spec::interfaces::l8_surface::http::request::check_body;
use crosstalk_spec::interfaces::l8_surface::http::{
    Method, Place, QueryParams, Route, Source, Target, resolve,
};
use crosstalk_spec::interfaces::l8_surface::lists::{
    AgentFilter, AlertRuleFilter, ChannelFilter, SearchMode, SearchRequest,
};
use crosstalk_spec::interfaces::l8_surface::summary::TransmissionSelection;
use crosstalk_spec::interfaces::l8_surface::{AlertFilter, QueryApi, QueryError};
use crosstalk_spec::observed::message::PartRef;
use crosstalk_spec::paging::AgentList;
use crosstalk_spec::paging::PageSize;
use crosstalk_spec::support::Blake3;
use crosstalk_spec::support::NonBlank;

use super::stub::{Recorded, Reply, Stub};
use super::{
    ULID_A, ULID_B, ULID_C, agent, caller, channel, edge, filter, golden_json, golden_value, grid,
    id, page, params, transmission, window,
};
use crate::HttpClient;

/// Calls every `QueryApi` method once (its `Option` arguments set, or all
/// `None` with `with_none`) and returns each route with the method's
/// result.
async fn every_call(client: &HttpClient, with_none: bool) -> Vec<(Route, Result<(), QueryError>)> {
    let c = &caller();
    let some_window = (!with_none).then(window);
    let version = (!with_none).then_some(TopicModelVersion(3));
    let group = ConsumerGroup("l6-triage".into());
    let group = (!with_none).then_some(&group);
    let pattern = ResourcePattern::UrlPrefix {
        host: Host("wiki.corp.internal".into()),
        path_prefix: "/eng".into(),
    };
    let channels = IdBatch::new([channel()]).unwrap_or_else(|error| panic!("{error:?}"));
    let conversation = id::<ConversationId>(ULID_A);
    let span = id::<SpanId>(ULID_B);
    let turns = TurnWindow {
        from: TurnIndex(20),
        size: PageSize::new(20).unwrap_or_else(|error| panic!("{error:?}")),
    };
    let exchanges =
        IdBatch::new([id::<ExchangeId>(ULID_B)]).unwrap_or_else(|error| panic!("{error:?}"));
    let spans = IdBatch::new([span]).unwrap_or_else(|error| panic!("{error:?}"));
    let part = PartRef {
        message: MessageHash::from_digest(Blake3::from_bytes([7; 32])),
        index: 1,
    };
    let slice = TextSlice {
        from: 8192,
        limit: TextLimit::DEFAULT,
    };
    let agents =
        IdBatch::new([agent(ULID_A), agent(ULID_C)]).unwrap_or_else(|error| panic!("{error:?}"));
    let selection =
        TransmissionSelection::new(vec![transmission()]).unwrap_or_else(|e| panic!("{e:?}"));
    let search = SearchRequest {
        mode: SearchMode::Hybrid,
        text: NonBlank::new("deploy credentials").unwrap_or_else(|e| panic!("{e:?}")),
    };
    let export = ExportRequest::new(
        ExportDataset::Verdicts(window()),
        ExportFormat::Jsonl,
        false,
    )
    .unwrap_or_else(|error| panic!("{error:?}"));
    let alert = id::<AlertId>(ULID_A);
    let rule = id::<AlertRuleId>(ULID_B);
    let projection = id::<ProjectionId>(ULID_C);
    let w = window();
    fn none<T>(result: Result<T, QueryError>) -> Result<(), QueryError> {
        result.map(|_| ())
    }
    vec![
        (
            Route::Channel,
            none(client.channel(c, channel(), some_window).await),
        ),
        (
            Route::PolicyHistory,
            none(client.policy_history(c, channel()).await),
        ),
        (
            Route::Channels,
            none(client.channels(c, &ChannelFilter::default(), &page()).await),
        ),
        (
            Route::ChannelNames,
            none(client.channel_names(c, &channels).await),
        ),
        (
            Route::PromotionPreview,
            none(client.promotion_preview(c, channel(), &pattern).await),
        ),
        (
            Route::ChannelResources,
            none(client.channel_resources(c, channel(), w, &page()).await),
        ),
        (
            Route::ChannelTransmissions,
            none(
                client
                    .channel_transmissions(
                        c,
                        channel(),
                        &ChannelTransmissionFilter {
                            confirmation: Some(Confirmation::Unconfirmed),
                        },
                        TopicVersionSelector::Current,
                        &page(),
                    )
                    .await,
            ),
        ),
        (
            Route::Agents,
            none(client.agents(c, &AgentFilter::default(), w, &page()).await),
        ),
        (Route::Agent, none(client.agent(c, agent(ULID_A), w).await)),
        (
            Route::AgentNames,
            none(client.agent_names(c, &agents).await),
        ),
        (
            Route::Conversations,
            none(
                client
                    .conversations(c, &ConversationFilter::default(), &page())
                    .await,
            ),
        ),
        (
            Route::Conversation,
            none(client.conversation(c, conversation).await),
        ),
        (
            Route::ConversationTurns,
            none(client.conversation_turns(c, conversation, &turns).await),
        ),
        (
            Route::SpanReaders,
            none(client.span_readers(c, span, &page()).await),
        ),
        (
            Route::ExchangeTurns,
            none(client.exchange_turns(c, &exchanges).await),
        ),
        (Route::SpanPoints, none(client.span_points(c, &spans).await)),
        (
            Route::ConversationText,
            none(
                client
                    .conversation_text(c, conversation, &turns, TextLimit::DEFAULT)
                    .await,
            ),
        ),
        (
            Route::PartText,
            none(client.part_text(c, part, slice).await),
        ),
        (
            Route::AlertRules,
            none(
                client
                    .alert_rules(c, &AlertRuleFilter::default(), &page())
                    .await,
            ),
        ),
        (Route::AlertRule, none(client.alert_rule(c, rule).await)),
        (Route::Sinks, none(client.sinks(c).await)),
        (
            Route::DeadLetters,
            none(client.dead_letters(c, group, &page()).await),
        ),
        (
            Route::Alerts,
            none(client.alerts(c, &AlertFilter::default(), &page()).await),
        ),
        (Route::Alert, none(client.alert(c, alert).await)),
        (Route::Watermark, none(client.watermark(c).await)),
        (
            Route::Topology,
            none(
                client
                    .topology(
                        c,
                        w,
                        golden_value("topology/weighting_transmissions.json"),
                        &filter(),
                    )
                    .await,
            ),
        ),
        (
            Route::Overview,
            none(client.overview(c, w, &filter()).await),
        ),
        (
            Route::ChannelTopology,
            none(
                client
                    .channel_topology(
                        c,
                        w,
                        golden_value("topology/weighting_matched_bytes.json"),
                        &filter(),
                    )
                    .await,
            ),
        ),
        (
            Route::EdgeTransmissions,
            none(
                client
                    .edge_transmissions(c, &edge(), w, &filter(), &page())
                    .await,
            ),
        ),
        (
            Route::TransmissionsById,
            none(
                client
                    .transmissions_by_id(c, &selection, TopicVersionSelector::Current, &page())
                    .await,
            ),
        ),
        (
            Route::Series,
            none(
                client
                    .series(
                        c,
                        grid(),
                        golden_value("topology/weighting_transmissions.json"),
                        golden_value("topology/series_grouping_topic.json"),
                        &filter(),
                    )
                    .await,
            ),
        ),
        (Route::TopicVersions, none(client.topic_versions(c).await)),
        (
            Route::TopicSizes,
            none(client.topic_sizes(c, version, some_window).await),
        ),
        (
            Route::TopicLineage,
            none(client.topic_lineage(c, TopicModelVersion(3)).await),
        ),
        (
            Route::Search,
            none(
                client
                    .search(c, &search, some_window, &filter(), &page())
                    .await,
            ),
        ),
        (
            Route::Transmission,
            none(client.transmission(c, transmission()).await),
        ),
        (
            Route::TransmissionEvidence,
            none(
                client
                    .transmission_evidence(c, transmission(), ExcerptWindow::DEFAULT)
                    .await,
            ),
        ),
        (
            Route::Topics,
            none(
                client
                    .topics(
                        c,
                        TopicVersionSelector::Pinned(TopicModelVersion(3)),
                        &page(),
                    )
                    .await,
            ),
        ),
        (
            Route::FitProjection,
            none(client.fit_projection(c, w, &filter(), params()).await),
        ),
        (
            Route::ProjectionStatus,
            none(client.projection_status(c, projection).await),
        ),
        (
            Route::Projections,
            none(client.projections(c, &page()).await),
        ),
        (
            Route::ProjectionFrame,
            none(client.projection(c, projection).await),
        ),
        (
            Route::Verdicts,
            none(client.verdicts(c, transmission()).await),
        ),
        (
            Route::DetectionQuality,
            none(client.detection_quality(c, w).await),
        ),
        (
            Route::Audit,
            none(client.audit(c, &AuditFilter::default(), &page()).await),
        ),
        (Route::Operators, none(client.operators(c).await)),
        (Route::Me, none(client.me(c).await)),
        (
            Route::Export,
            none(client.export(c, &export).await.map(|_| ())),
        ),
        (Route::Present, none(client.present(c).await)),
    ]
}

fn names(route: Route, place: Place) -> Vec<&'static str> {
    route.spec().args_in(place).map(|arg| arg.name).collect()
}

/// Checks one received request against its route, as the server reads it.
fn check(route: Route, request: &Recorded, with_none: bool) {
    let spec = route.spec();
    let method = match spec.method {
        Method::Get => "GET",
        Method::Post => "POST",
    };
    assert_eq!(request.method, method, "{route:?}");
    let (target, params) = resolve(spec.method, &request.path)
        .unwrap_or_else(|| panic!("{route:?}: {} resolves to no route", request.path));
    assert_eq!(target, Target::Route(route), "{route:?}: {}", request.path);
    assert_eq!(
        params.iter().map(|(name, _)| name).collect::<Vec<_>>(),
        names(route, Place::Path),
        "{route:?}: path parameters"
    );
    let sent: Vec<&str> = request
        .query
        .iter()
        .map(|(name, _)| name.as_str())
        .collect();
    let expected: Vec<&str> = spec
        .args_in(Place::Query)
        .filter(|arg| !(with_none && arg.optional))
        .map(|arg| arg.name)
        .collect();
    assert_eq!(sent, expected, "{route:?}: query parameters");
    QueryParams::new(route, request.query.clone())
        .unwrap_or_else(|error| panic!("{route:?}: {error:?}"));
    assert_eq!(request.header("last-event-id"), None, "{route:?}");
    let body = check_body(route, request.header("content-type"), &request.body)
        .unwrap_or_else(|error| panic!("{route:?}: {error:?}"));
    assert_eq!(body.is_some(), spec.has_body(), "{route:?}: body");
    let fields = names(route, Place::BodyField);
    if body.is_some() && !fields.is_empty() {
        let json = request.json();
        let mut keys: Vec<&str> = json
            .as_object()
            .unwrap_or_else(|| panic!("{route:?}: the body is an object"))
            .keys()
            .map(String::as_str)
            .collect();
        keys.sort_unstable();
        let mut fields = fields;
        fields.sort_unstable();
        assert_eq!(keys, fields, "{route:?}: body fields");
    }
    let accept = match route {
        Route::ProjectionFrame => "application/octet-stream",
        Route::Export => "application/x-ndjson",
        _ => "application/json",
    };
    assert_eq!(request.header("accept"), Some(accept), "{route:?}");
}

/// Every method sends a request the surface resolves to that method's
/// route, with exactly the table's path parameters, query parameters
/// (an `Option`'s `None` left out) and body, and reads the route's `404`
/// back as `NotFound`.
#[tokio::test]
async fn every_query_method_sends_its_route() {
    for with_none in [false, true] {
        let mut stub = Stub::always(Reply::json(404, r#"{"type":"not_found"}"#)).await;
        let calls = every_call(&stub.client(), with_none).await;
        let requests = stub.requests();
        assert_eq!(requests.len(), calls.len(), "one request per call");
        for ((route, result), request) in calls.iter().zip(&requests) {
            assert_eq!(result, &Err(QueryError::NotFound), "{route:?}");
            check(*route, request, with_none);
        }
    }
}

/// Every query route of the table is some method's, and each is sent by
/// exactly one method: the client covers the whole `QueryApi`.
#[tokio::test]
async fn every_query_route_is_sent_by_one_method() {
    let stub = Stub::always(Reply::json(404, r#"{"type":"not_found"}"#)).await;
    let routes: Vec<Route> = every_call(&stub.client(), false)
        .await
        .into_iter()
        .map(|(route, _)| route)
        .collect();
    for route in Route::all() {
        let sent = routes.iter().filter(|sent| **sent == route).count();
        let is_query = matches!(route.spec().source, Source::Query(_));
        assert_eq!(sent, usize::from(is_query), "{route:?}");
    }
}

/// The `POST` read bodies are the spec's goldens: a call made with a
/// golden body's fields sends exactly that body.
#[tokio::test]
async fn read_bodies_are_the_goldens() {
    let c = &caller();
    let mut stub = Stub::always(Reply::json(404, r#"{"type":"not_found"}"#)).await;
    let client = stub.client();
    let graph: GraphBody = golden_value("http/bodies/graph.json");
    let _ = client
        .topology(c, graph.window, graph.weighting, &graph.filter)
        .await;
    let _ = client
        .channel_topology(c, graph.window, graph.weighting, &graph.filter)
        .await;
    let overview: OverviewBody = golden_value("http/bodies/overview.json");
    let _ = client.overview(c, overview.window, &overview.filter).await;
    let edges: EdgeTransmissionsBody = golden_value("http/bodies/edge_transmissions.json");
    let _ = client
        .edge_transmissions(c, &edges.edge, edges.window, &edges.filter, &edges.page)
        .await;
    let rows: TransmissionsBody = golden_value("http/bodies/transmissions.json");
    let _ = client
        .transmissions_by_id(c, &rows.selection, rows.version, &rows.page)
        .await;
    let series: SeriesBody = golden_value("http/bodies/series.json");
    let _ = client
        .series(
            c,
            series.grid,
            series.weighting,
            series.grouping,
            &series.filter,
        )
        .await;
    let search: SearchBody = golden_value("http/bodies/search.json");
    let _ = client
        .search(
            c,
            &search.request,
            search.window,
            &search.filter,
            &search.page,
        )
        .await;
    let fit: FitProjectionBody = golden_value("http/bodies/fit_projection.json");
    let _ = client
        .fit_projection(c, fit.window, &fit.filter, fit.params)
        .await;

    let goldens = [
        "graph",
        "graph",
        "overview",
        "edge_transmissions",
        "transmissions",
        "series",
        "search",
        "fit_projection",
    ];
    let requests = stub.requests();
    assert_eq!(requests.len(), goldens.len());
    for (request, name) in requests.iter().zip(goldens) {
        assert_eq!(
            request.json(),
            golden_json(&format!("http/bodies/{name}.json")),
            "{name}"
        );
    }
}

/// Export requests are their goldens.
#[tokio::test]
async fn export_request_bodies_are_the_goldens() {
    for name in [
        "export_request_transmissions",
        "export_request_transmissions_with_content",
        "export_request_edges",
        "export_request_accesses",
        "export_request_topics",
        "export_request_verdicts",
        "export_request_projection",
    ] {
        let path = format!("surface_reads/export/{name}.json");
        let request: ExportRequest = golden_value(&path);
        let mut stub = Stub::always(Reply::json(404, r#"{"type":"not_found"}"#)).await;
        let result = stub.client().export(&caller(), &request).await.map(|_| ());
        assert_eq!(result, Err(QueryError::NotFound), "{name}");
        assert_eq!(stub.only_request().json(), golden_json(&path), "{name}");
    }
}

/// A query parameter is the argument's compact JSON, form-encoded: a space
/// is `+`, reserved and non-ASCII bytes are `%XX`, and the server's form
/// decoding gives back the JSON, which decodes to the argument.
#[tokio::test]
async fn query_parameters_are_form_encoded_compact_json() {
    let mut stub = Stub::always(Reply::json(404, r#"{"type":"not_found"}"#)).await;
    let filter: AgentFilter =
        super::from_json(r#"{"states":[],"claimed":[],"text":"a b&c=d+é/?#%","parents":[]}"#);
    let _ = stub
        .client()
        .agents(&caller(), &filter, window(), &page::<AgentList>())
        .await;
    let request = stub.only_request();
    let raw = request.raw_query.clone().unwrap_or_default();
    assert!(
        raw.starts_with("filter=%7B%22states%22%3A%5B%5D%2C"),
        "{raw}"
    );
    assert!(raw.contains("a+b%26c%3Dd%2B%C3%A9%2F%3F%23%25"), "{raw}");
    assert_eq!(
        request.query,
        vec![
            (
                "filter".to_owned(),
                r#"{"states":[],"claimed":[],"text":"a b&c=d+é/?#%","parents":[]}"#.to_owned()
            ),
            (
                "window".to_owned(),
                r#"{"start":"2026-10-04T12:00:00.000000Z","end":"2026-10-04T13:00:00.000000Z"}"#
                    .to_owned()
            ),
            ("page".to_owned(), r#"{"size":50,"after":null}"#.to_owned()),
        ]
    );
    let params = QueryParams::new(Route::Agents, request.query.clone())
        .unwrap_or_else(|error| panic!("{error:?}"));
    assert_eq!(params.decode::<AgentFilter>("filter"), Ok(filter));
}

/// A list filter in the query string is its golden's compact JSON.
#[tokio::test]
async fn list_filters_travel_as_their_goldens() {
    let mut stub = Stub::always(Reply::json(404, r#"{"type":"not_found"}"#)).await;
    let client = stub.client();
    let alerts: AlertFilter = golden_value("alerts/alert_filter_everything.json");
    let _ = client.alerts(&caller(), &alerts, &page()).await;
    let agents: AgentFilter = golden_value("agents/agent_filter_everything.json");
    let _ = client.agents(&caller(), &agents, window(), &page()).await;
    let audit: AuditFilter = golden_value("surface_actions/audit/audit_filter_everything.json");
    let _ = client.audit(&caller(), &audit, &page()).await;
    let compact = [
        serde_json::to_string(&alerts),
        serde_json::to_string(&agents),
        serde_json::to_string(&audit),
    ]
    .map(|json| json.unwrap_or_else(|error| panic!("{error}")));
    let requests = stub.requests();
    let goldens = [
        "alerts/alert_filter_everything.json",
        "agents/agent_filter_everything.json",
        "surface_actions/audit/audit_filter_everything.json",
    ];
    for ((request, golden), compact) in requests.iter().zip(goldens).zip(compact) {
        let (name, value) = &request.query[0];
        assert_eq!(name, "filter");
        let sent: serde_json::Value =
            serde_json::from_str(value).unwrap_or_else(|error| panic!("{error}"));
        assert_eq!(sent, golden_json(golden), "{golden}");
        assert_eq!(value, &compact, "compact JSON: {golden}");
    }
}

/// The base URL's prefix comes before every template.
#[tokio::test]
async fn the_base_prefix_leads_every_path() {
    let mut stub = Stub::always(Reply::json(404, r#"{"type":"not_found"}"#)).await;
    let base = crate::BaseUrl::parse(&format!("{}/api/", stub.base()))
        .unwrap_or_else(|error| panic!("{error}"));
    let client = HttpClient::new(base, super::stub::fast_config());
    let _ = client.watermark(&caller()).await;
    assert_eq!(stub.only_request().path, "/api/watermark");
}
