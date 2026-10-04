//! One call per query route: its request, built with the spec's
//! `RequestBuilder` from values decoded from the wire goldens, the
//! arguments the surface must then be called with, and the golden response
//! the surface answers with. Every route but the frame, the export, the
//! actions and the live feed (each tested on its own) is here.

use crosstalk_spec::aggregates::filter::TopicVersionSelector;
use crosstalk_spec::aggregates::topic::TopicModelVersion;
use crosstalk_spec::batch::IdBatch;
use crosstalk_spec::derived::flow::resource::ResourcePattern;
use crosstalk_spec::ids::{AgentId, AlertId, AlertRuleId, ChannelId, ProjectionId, TransmissionId};
use crosstalk_spec::interfaces::l2_transport::ConsumerGroup;
use crosstalk_spec::interfaces::l8_surface::AlertFilter;
use crosstalk_spec::interfaces::l8_surface::audit::AuditFilter;
use crosstalk_spec::interfaces::l8_surface::channel_traffic::ChannelTransmissionFilter;
use crosstalk_spec::interfaces::l8_surface::excerpt::ExcerptWindow;
use crosstalk_spec::interfaces::l8_surface::http::bodies::{
    EdgeTransmissionsBody, FitProjectionBody, GraphBody, OverviewBody, SearchBody, SeriesBody,
    TransmissionsBody,
};
use crosstalk_spec::interfaces::l8_surface::http::{
    EncodedRequest, PathArg, Place, RequestBuilder, Route, Source,
};
use crosstalk_spec::interfaces::l8_surface::lists::{AgentFilter, AlertRuleFilter, ChannelFilter};
use crosstalk_spec::paging::PageRequest;
use crosstalk_spec::support::TimeWindow;
use crosstalk_spec::wire::WireRequest;
use serde_json::{Map, Value};

use super::{decode, golden, golden_text};

pub(super) const CHANNEL: &str = "01J9Z3K8M4Q7R2T5V6W8X9Y0ZA";
pub(super) const AGENT: &str = "01J9Z3M2C5D6E7F8G9H0J1K2M3";
pub(super) const ALERT: &str = "01J9Z3N4P5Q6R7S8T9V0W1X2Y3";
pub(super) const RULE: &str = "00000000000000000000000003";
pub(super) const TRANSMISSION: &str = "01J9Z3P5R6S7T8V9W0X1Y2Z3A4";
pub(super) const PROJECTION: &str = "01J9Z3Q6R7S8T9V0W1X2Y3Z4A5";

/// One query route's call.
pub(super) struct Case {
    pub route: Route,
    builder: RequestBuilder,
    args: Map<String, Value>,
    /// The JSON the surface answers with, which the response must equal.
    pub response: String,
}

impl Case {
    fn new(route: Route) -> Self {
        Self {
            route,
            builder: RequestBuilder::new(route),
            args: Map::new(),
            response: String::new(),
        }
    }

    fn path<T: PathArg>(mut self, name: &str, json: &str) -> Self {
        let value: T = decode(json);
        self.args.insert(name.to_owned(), to_value(&value));
        self.builder = self.builder.path(name, &value);
        self
    }

    fn query<T: WireRequest>(mut self, name: &str, json: &str) -> Self {
        let value: T = decode(json);
        self.args.insert(name.to_owned(), to_value(&value));
        self.builder = self.builder.query(name, &value);
        self
    }

    fn body<T: WireRequest>(mut self, json: &str) -> Self {
        let value: T = decode(json);
        let encoded = to_value(&value);
        let whole = self.route.spec().args_in(Place::Body).next().is_some();
        match encoded {
            Value::Object(fields) if !whole => self.args.extend(fields),
            encoded => {
                self.args.insert("body".to_owned(), encoded);
            }
        }
        self.builder = self.builder.body(&value);
        self
    }

    fn returns(mut self, json: String) -> Self {
        self.response = json;
        self
    }

    /// The method the route calls.
    pub fn method(&self) -> &'static str {
        match self.route.spec().source {
            Source::Query(name) => name,
            Source::Act(_) | Source::Subscribe => panic!("{:?} is not a query", self.route),
        }
    }

    /// The arguments the surface must be called with, by the route's names.
    pub fn args(&self) -> Value {
        Value::Object(self.args.clone())
    }

    pub fn request(&self) -> EncodedRequest {
        self.builder
            .clone()
            .build()
            .unwrap_or_else(|error| panic!("{:?}: {error:?}", self.route))
    }
}

fn to_value<T: serde::Serialize>(value: &T) -> Value {
    serde_json::to_value(value).expect("a request value has JSON")
}

fn quoted(text: &str) -> String {
    format!("\"{text}\"")
}

/// `value` with `watermark`, as `Watermarked` encodes it.
fn watermarked(value: &str) -> String {
    format!("{{\"watermark\": \"2026-10-04T12:00:00.000000Z\", \"value\": {value}}}")
}

/// Every query route with a JSON success.
pub(super) fn cases() -> Vec<Case> {
    let window = golden_text("support/time_window");
    let page = golden_text("paging/page_request_first");
    let after = golden_text("paging/page_request_after");
    let filter_graph = golden_text("http/bodies/graph");
    let channel = quoted(CHANNEL);
    let agent = quoted(AGENT);
    vec![
        Case::new(Route::Channel)
            .path::<ChannelId>("id", &channel)
            .query::<Option<TimeWindow>>("window", &window)
            .returns(golden_text("surface_reads/channels/channel_found")),
        Case::new(Route::PolicyHistory)
            .path::<ChannelId>("id", &channel)
            .returns(golden_text("flow/policy_history")),
        Case::new(Route::Channels)
            .query::<ChannelFilter>(
                "filter",
                &golden_text("surface_actions/lists/channel_filter_in_force"),
            )
            .query::<PageRequest<()>>("page", &after)
            .returns(golden_text("surface_reads/channels/channels_page")),
        Case::new(Route::ChannelNames)
            .body::<IdBatch<ChannelId>>(&golden_text("paging/id_batch"))
            .returns(golden_text("surface_reads/channels/channel_names_several")),
        Case::new(Route::PromotionPreview)
            .path::<ChannelId>("id", &channel)
            .query::<ResourcePattern>("pattern", &golden_text("flow/resource_pattern_url_prefix"))
            .returns(golden_text(
                "surface_reads/channels/promotion_preview_promotes",
            )),
        Case::new(Route::ChannelResources)
            .path::<ChannelId>("id", &channel)
            .query::<TimeWindow>("window", &window)
            .query::<PageRequest<()>>("page", &page)
            .returns(golden_text("topology/resource_use_page")),
        Case::new(Route::ChannelTransmissions)
            .path::<ChannelId>("id", &channel)
            .query::<ChannelTransmissionFilter>(
                "filter",
                &golden_text(
                    "surface_reads/channel_traffic/channel_transmission_filter_unconfirmed",
                ),
            )
            .query::<TopicVersionSelector>("version", &golden_text("topology/topic_version_pinned"))
            .query::<PageRequest<()>>("page", &page)
            .returns(golden_text(
                "surface_reads/channel_traffic/channel_transmission_page",
            )),
        Case::new(Route::Agents)
            .query::<AgentFilter>("filter", &golden_text("agents/agent_filter"))
            .query::<TimeWindow>("window", &window)
            .query::<PageRequest<()>>("page", &page)
            .returns(golden_text("agents/agent_rows_page")),
        Case::new(Route::Agent)
            .path::<AgentId>("id", &agent)
            .query::<TimeWindow>("window", &window)
            .returns(golden_text("agents/agent_detail")),
        Case::new(Route::AgentNames)
            .body::<IdBatch<AgentId>>(&golden_text("paging/id_batch"))
            .returns(golden_text("agents/agent_names_several")),
        Case::new(Route::AlertRules)
            .query::<AlertRuleFilter>(
                "filter",
                &golden_text("surface_actions/lists/alert_rule_filter"),
            )
            .query::<PageRequest<()>>("page", &page)
            .returns(golden_text("rules/alert_rules_page")),
        Case::new(Route::AlertRule)
            .path::<AlertRuleId>("id", &quoted(RULE))
            .returns(golden_text("rules/alert_rule_builtin")),
        Case::new(Route::Sinks).returns(golden_text("surface_actions/lists/sinks")),
        Case::new(Route::DeadLetters)
            .query::<Option<ConsumerGroup>>("group", &golden_text("bus/consumer_group"))
            .query::<PageRequest<()>>("page", &page)
            .returns(golden_text("bus/dead_letters_page")),
        Case::new(Route::Alerts)
            .query::<AlertFilter>("filter", &golden_text("alerts/alert_filter"))
            .query::<PageRequest<()>>("page", &page)
            .returns(golden_text("alerts/alerts_page")),
        Case::new(Route::Alert)
            .path::<AlertId>("id", &quoted(ALERT))
            .returns(golden_text("alerts/alert_found")),
        Case::new(Route::Watermark).returns(golden_text("support/watermark")),
        Case::new(Route::Topology)
            .body::<GraphBody>(&filter_graph)
            .returns(golden_text("topology/topology_graph")),
        Case::new(Route::Overview)
            .body::<OverviewBody>(&golden_text("http/bodies/overview"))
            .returns(watermarked(&golden_text(
                "surface_actions/lists/overview_counts",
            ))),
        Case::new(Route::ChannelTopology)
            .body::<GraphBody>(&filter_graph)
            .returns(golden_text("topology/bipartite_graph")),
        Case::new(Route::EdgeTransmissions)
            .body::<EdgeTransmissionsBody>(&golden_text("http/bodies/edge_transmissions"))
            .returns(golden_text("topology/edge_transmission_page")),
        Case::new(Route::TransmissionsById)
            .body::<TransmissionsBody>(&golden_text("http/bodies/transmissions"))
            .returns(golden_text("surface_reads/transmissions/transmission_page")),
        Case::new(Route::Series)
            .body::<SeriesBody>(&golden_text("http/bodies/series"))
            .returns(golden_text("topology/series_total")),
        Case::new(Route::TopicVersions).returns(golden_text("topics/topic_version_history")),
        Case::new(Route::TopicSizes)
            .query::<Option<TopicModelVersion>>("version", "2")
            .query::<Option<TimeWindow>>("window", &window)
            .returns(golden_text("topics/topic_sizes_window")),
        Case::new(Route::TopicLineage)
            .path::<TopicModelVersion>("version", "2")
            .returns(golden_text("topics/topic_lineage")),
        Case::new(Route::Search)
            .body::<SearchBody>(&golden_text("http/bodies/search"))
            .returns(format!(
                "{{\"topic_version\": 3, \"page\": {}}}",
                golden_text("paging/page_last_empty")
            )),
        Case::new(Route::Transmission)
            .path::<TransmissionId>("id", &quoted(TRANSMISSION))
            .returns(golden_text("flow/transmission_confirmed")),
        Case::new(Route::TransmissionEvidence)
            .path::<TransmissionId>("id", &quoted(TRANSMISSION))
            .query::<ExcerptWindow>(
                "window",
                &golden_text("surface_reads/evidence/excerpt_window_default"),
            )
            .returns(golden_text(
                "surface_reads/evidence/transmission_evidence_shown",
            )),
        Case::new(Route::Topics)
            .query::<TopicVersionSelector>("version", &golden_text("topology/topic_version_pinned"))
            .query::<PageRequest<()>>("page", &page)
            .returns(golden_text("surface_actions/lists/topic_page")),
        Case::new(Route::FitProjection)
            .body::<FitProjectionBody>(&golden_text("http/bodies/fit_projection"))
            .returns(quoted(PROJECTION)),
        Case::new(Route::ProjectionStatus)
            .path::<ProjectionId>("id", &quoted(PROJECTION))
            .returns(golden_text("projections/projection_ready")),
        Case::new(Route::Projections)
            .query::<PageRequest<()>>("page", &page)
            .returns(golden_text("projections/projections_page")),
        Case::new(Route::Verdicts)
            .path::<TransmissionId>("id", &quoted(TRANSMISSION))
            .returns(golden_text("flow/verdict_log")),
        Case::new(Route::DetectionQuality)
            .query::<TimeWindow>("window", &window)
            .returns(golden_text("topology/detection_quality")),
        Case::new(Route::Audit)
            .query::<AuditFilter>("filter", &golden_text("surface_actions/audit/audit_filter"))
            .query::<PageRequest<()>>("page", &page)
            .returns(golden_text("surface_actions/audit/audit_page")),
        Case::new(Route::Operators).returns(golden_text("surface_actions/operators/operators")),
        Case::new(Route::Present).returns(golden_text("surface_reads/present/present_jsonl_only")),
    ]
}

/// A value decoded from a golden file, for tests that need one argument.
pub(super) fn window() -> TimeWindow {
    golden("support/time_window")
}
