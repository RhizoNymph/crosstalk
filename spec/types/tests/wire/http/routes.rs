//! The route table: its golden, its completeness, its permissions against
//! the methods' documented ones, and that no request can match two routes.

use std::collections::{HashMap, HashSet};

use serde::{Deserialize, Serialize};

use super::super::harness::assert_golden;
use super::super::surface_actions::actions::{every_action, every_request};
use super::AREA;
use crate::interfaces::l8_surface::http::path::{Segment, match_template, segments};
use crate::interfaces::l8_surface::http::{
    Method, Place, ResponseBody, Route, RoutePermission, Source, Target, resolve,
};
use crate::interfaces::l8_surface::{ActionKind, Permission};

/// One row of the route table as the golden lists it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RouteRow {
    calls: String,
    method: String,
    path: String,
    args: Vec<ArgRow>,
    status: u16,
    content_type: String,
    permission: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ArgRow {
    name: String,
    place: String,
    optional: bool,
}

/// The `type` an `ActionRequest` of this kind has on the wire.
fn action_tag(kind: ActionKind) -> &'static str {
    match kind {
        ActionKind::SetPolicy => "set_policy",
        ActionKind::MergeAgents => "merge_agents",
        ActionKind::Unmerge => "unmerge",
        ActionKind::RenameAgent => "rename_agent",
        ActionKind::PromoteChannel => "promote_channel",
        ActionKind::Acknowledge => "acknowledge",
        ActionKind::Resolve => "resolve",
        ActionKind::SetVerdict => "set_verdict",
        ActionKind::CreateRule => "create_rule",
        ActionKind::UpdateRule => "update_rule",
        ActionKind::SetRuleEnabled => "set_rule_enabled",
        ActionKind::ReplayDeadLetter => "replay_dead_letter",
        ActionKind::PinTopicVersion => "pin_topic_version",
        ActionKind::UnpinTopicVersion => "unpin_topic_version",
    }
}

fn place_name(place: Place) -> &'static str {
    match place {
        Place::Path => "path",
        Place::Query => "query",
        Place::QueryText => "query_text",
        Place::Header => "header",
        Place::Body => "body",
        Place::BodyField => "body_field",
    }
}

fn permission_name(permission: RoutePermission) -> String {
    match permission {
        RoutePermission::Fixed(permission) => serde_json::to_value(permission)
            .ok()
            .and_then(|value| value.as_str().map(str::to_owned))
            .unwrap_or_else(|| panic!("{permission:?} is a string")),
        RoutePermission::AnyCaller => "none: any caller".to_owned(),
        RoutePermission::ByExportRequest => "view, or content by the request".to_owned(),
    }
}

fn row(route: Route) -> RouteRow {
    let spec = route.spec();
    RouteRow {
        calls: match spec.source {
            Source::Query(method) => format!("QueryApi::{method}"),
            Source::Act(kind) => format!("OperatorActions::act ({})", action_tag(kind)),
            Source::Subscribe => "LiveFeed::subscribe".to_owned(),
        },
        method: spec.method.as_str().to_owned(),
        path: spec.path.to_owned(),
        args: spec
            .args
            .iter()
            .map(|arg| ArgRow {
                name: arg.name.to_owned(),
                place: place_name(arg.place).to_owned(),
                optional: arg.optional,
            })
            .collect(),
        status: spec.success.status.code(),
        content_type: spec.success.body.content_type().to_owned(),
        permission: permission_name(spec.permission),
    }
}

/// Every route, each checked by an exhaustive match: a route added to the
/// enum does not compile here until it is considered for the golden.
fn every_route() -> Vec<Route> {
    fn declared(route: Route) -> Route {
        match route {
            Route::Channel
            | Route::PolicyHistory
            | Route::Channels
            | Route::ChannelNames
            | Route::PromotionPreview
            | Route::ChannelResources
            | Route::ChannelTransmissions
            | Route::Agents
            | Route::Agent
            | Route::AgentNames
            | Route::Conversations
            | Route::Conversation
            | Route::ConversationTurns
            | Route::SpanReaders
            | Route::ExchangeTurns
            | Route::SpanPoints
            | Route::ConversationText
            | Route::PartText
            | Route::AlertRules
            | Route::AlertRule
            | Route::Sinks
            | Route::DeadLetters
            | Route::Alerts
            | Route::Alert
            | Route::Watermark
            | Route::Topology
            | Route::Overview
            | Route::ChannelTopology
            | Route::EdgeTransmissions
            | Route::TransmissionsById
            | Route::Series
            | Route::TopicVersions
            | Route::TopicSizes
            | Route::TopicLineage
            | Route::Search
            | Route::Transmission
            | Route::TransmissionEvidence
            | Route::Topics
            | Route::FitProjection
            | Route::ProjectionStatus
            | Route::Projections
            | Route::ProjectionFrame
            | Route::Verdicts
            | Route::DetectionQuality
            | Route::Audit
            | Route::Operators
            | Route::Me
            | Route::Export
            | Route::Present
            | Route::Action(_)
            | Route::Live => route,
        }
    }
    Route::all().into_iter().map(declared).collect()
}

#[test]
fn route_table_golden() {
    let rows: Vec<RouteRow> = every_route().into_iter().map(row).collect();
    assert_golden(AREA, "route_table", &rows);
}

/// `Route::all` lists every route once, at its index, and `ActionKind::ALL`
/// every kind at its index; both indexes are exhaustive matches.
#[test]
fn every_route_is_listed_once_at_its_index() {
    let all = Route::all();
    for (index, route) in all.iter().enumerate() {
        assert_eq!(route.index(), index, "{route:?}");
    }
    assert_eq!(all.iter().collect::<HashSet<_>>().len(), all.len());
    for (index, kind) in ActionKind::ALL.iter().enumerate() {
        assert_eq!(kind.index(), index, "{kind:?}");
    }
    assert_eq!(
        all.last().map(|route| route.index()),
        Some(Route::Live.index())
    );
}

/// Each `QueryApi` method names one route, each action kind one, and the
/// live feed one.
#[test]
fn every_source_has_exactly_one_route() {
    let mut by_source = HashMap::new();
    for route in Route::all() {
        if let Some(other) = by_source.insert(route.spec().source, route) {
            panic!("{route:?} and {other:?} call the same thing");
        }
    }
    for kind in ActionKind::ALL {
        assert_eq!(
            by_source.get(&Source::Act(kind)),
            Some(&Route::Action(kind))
        );
    }
    assert_eq!(by_source.get(&Source::Subscribe), Some(&Route::Live));
}

/// The `QueryApi` methods as `l8_surface.rs` declares them, each with the
/// first line of its documentation, which names its permission. A method
/// is declared `fn name(..) -> impl Future<Output = ..> + Send`.
fn query_api_methods() -> Vec<(String, String)> {
    let source = include_str!("../../../interfaces/l8_surface.rs");
    let start = source
        .find("pub trait QueryApi {")
        .unwrap_or_else(|| panic!("l8_surface.rs declares QueryApi"));
    let body = &source[start..];
    let end = body.find("\n}\n").unwrap_or(body.len());
    let mut methods = Vec::new();
    let mut doc: Option<String> = None;
    for line in body[..end].lines().skip(1) {
        let line = line.trim();
        if let Some(text) = line.strip_prefix("///") {
            doc.get_or_insert_with(|| text.trim().to_owned());
        } else if let Some(rest) = line.strip_prefix("fn ") {
            let name = rest.split('(').next().unwrap_or_default().to_owned();
            methods.push((name, doc.take().unwrap_or_default()));
        } else if line.ends_with(';') || line.ends_with('{') || line.is_empty() {
            doc = None;
        }
    }
    methods
}

/// What a method's documentation says it needs: its first word, View or
/// Content by the request for `export`, or none for `Any caller`.
fn documented(doc: &str) -> RoutePermission {
    if doc.starts_with("View, or Content when `request`") {
        return RoutePermission::ByExportRequest;
    }
    if doc.starts_with("Any caller.") {
        return RoutePermission::AnyCaller;
    }
    let word = doc.split(['.', ',']).next().unwrap_or_default();
    let permission = Permission::ALL
        .into_iter()
        .find(|permission| format!("{permission:?}") == word)
        .unwrap_or_else(|| panic!("`{doc}` names no permission first"));
    RoutePermission::Fixed(permission)
}

/// Every `QueryApi` method has a route, and the route needs exactly the
/// permission the method's documentation names.
#[test]
fn query_routes_need_the_permission_their_method_documents() {
    let methods = query_api_methods();
    assert!(methods.len() >= 39, "parsed {} methods", methods.len());
    let routes: HashMap<&str, Route> = Route::all()
        .into_iter()
        .filter_map(|route| match route.spec().source {
            Source::Query(method) => Some((method, route)),
            Source::Act(_) | Source::Subscribe => None,
        })
        .collect();
    for (method, doc) in &methods {
        let route = routes
            .get(method.as_str())
            .unwrap_or_else(|| panic!("QueryApi::{method} has no route"));
        assert_eq!(
            route.permission(),
            documented(doc),
            "QueryApi::{method}: {doc}"
        );
    }
    assert_eq!(routes.len(), methods.len());
}

/// An action's route needs the action's permission, for every action.
#[test]
fn action_routes_need_the_action_permission() {
    for action in every_action() {
        assert_eq!(
            Route::Action(action.kind()).permission(),
            RoutePermission::Fixed(action.required_permission()),
            "{action:?}"
        );
    }
    for request in every_request() {
        let json = serde_json::to_value(&request).unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(json["type"], action_tag(request.kind()), "{request:?}");
    }
    assert_eq!(
        Route::Live.permission(),
        RoutePermission::Fixed(Permission::View)
    );
}

/// Every action is `POST /actions`, which resolves to the actions endpoint;
/// the kind comes from the body.
#[test]
fn every_action_is_posted_to_one_endpoint() {
    for kind in ActionKind::ALL {
        let spec = Route::Action(kind).spec();
        assert_eq!((spec.method, spec.path), (Method::Post, "/actions"));
        assert_eq!(spec.success.body, ResponseBody::Json);
    }
    assert_eq!(
        resolve(Method::Post, "/actions").map(|(target, _)| target),
        Some(Target::Actions)
    );
}

/// Whether some path matches both templates.
fn overlap(a: &[Segment], b: &[Segment]) -> bool {
    a.len() == b.len()
        && a.iter().zip(b).all(|pair| match pair {
            (Segment::Literal(x), Segment::Literal(y)) => x == y,
            (Segment::Param(_), _) | (_, Segment::Param(_)) => true,
        })
}

/// Every template is well-formed, and no two routes of one method can match
/// one path, except the actions, which are one endpoint.
#[test]
fn path_templates_are_unique_and_unambiguous() {
    let routes = Route::all();
    for route in &routes {
        assert!(
            segments(route.path()).is_some(),
            "{route:?}: {}",
            route.path()
        );
    }
    for (i, a) in routes.iter().enumerate() {
        for b in &routes[i + 1..] {
            if a.method() != b.method() {
                continue;
            }
            let both_actions = matches!((a, b), (Route::Action(_), Route::Action(_)));
            let a_segments = segments(a.path()).unwrap_or_default();
            let b_segments = segments(b.path()).unwrap_or_default();
            assert_eq!(
                overlap(&a_segments, &b_segments),
                both_actions,
                "{a:?} {} and {b:?} {}",
                a.path(),
                b.path()
            );
        }
    }
}

/// Where two templates share a prefix up to a parameter, they name it the
/// same, as a path router (axum's) requires.
#[test]
fn parameters_at_one_position_share_a_name() {
    let templates: Vec<Vec<Segment>> = Route::all()
        .into_iter()
        .map(|route| segments(route.path()).unwrap_or_default())
        .collect();
    for a in &templates {
        for b in &templates {
            for pair in a.iter().zip(b) {
                match pair {
                    (Segment::Literal(x), Segment::Literal(y)) if x == y => {}
                    (Segment::Param(x), Segment::Param(y)) => {
                        assert_eq!(x, y, "{a:?} and {b:?}");
                    }
                    _ => break,
                }
            }
        }
    }
}

/// A route's path parameters are exactly its template's, its arguments are
/// named once each, a `GET` takes no body, and a body is either one
/// argument or an object of fields, never both.
#[test]
fn every_route_places_its_arguments_consistently() {
    for route in Route::all() {
        let spec = route.spec();
        let params: Vec<&str> = segments(spec.path)
            .unwrap_or_default()
            .into_iter()
            .filter_map(|segment| match segment {
                Segment::Param(name) => Some(name),
                Segment::Literal(_) => None,
            })
            .collect();
        let path_args: Vec<&str> = spec.args_in(Place::Path).map(|arg| arg.name).collect();
        assert_eq!(params, path_args, "{route:?}");
        let names: HashSet<&str> = spec.args.iter().map(|arg| arg.name).collect();
        assert_eq!(names.len(), spec.args.len(), "{route:?}: a name used twice");
        let bodies = spec.args_in(Place::Body).count();
        let fields = spec.args_in(Place::BodyField).count();
        assert!(bodies <= 1 && (bodies == 0 || fields == 0), "{route:?}");
        if spec.method == Method::Get {
            assert!(!spec.has_body(), "{route:?}: a GET with a body");
        }
        for arg in spec.args {
            assert!(
                !arg.optional
                    || matches!(arg.place, Place::Query | Place::QueryText | Place::Header),
                "{route:?}: only a query parameter or header may be left out"
            );
        }
    }
}

/// A path resolves only to its own route: wrong methods, unknown paths,
/// trailing slashes and empty segments match nothing.
#[test]
fn paths_resolve_to_their_route_or_none() {
    let id = "01J9Z3K8M4Q7R2T5V6W8X9Y0ZA";
    let cases = [
        (
            Method::Get,
            format!("/channels/{id}"),
            Some(Target::Route(Route::Channel)),
        ),
        (
            Method::Get,
            "/channels".to_owned(),
            Some(Target::Route(Route::Channels)),
        ),
        (
            Method::Get,
            format!("/channels/{id}/resources"),
            Some(Target::Route(Route::ChannelResources)),
        ),
        (
            Method::Get,
            format!("/channels/{id}/transmissions"),
            Some(Target::Route(Route::ChannelTransmissions)),
        ),
        (
            Method::Get,
            "/projections".to_owned(),
            Some(Target::Route(Route::Projections)),
        ),
        (
            Method::Post,
            "/projections".to_owned(),
            Some(Target::Route(Route::FitProjection)),
        ),
        (
            Method::Get,
            format!("/projections/{id}/frame"),
            Some(Target::Route(Route::ProjectionFrame)),
        ),
        (
            Method::Get,
            "/topic-versions/3/lineage".to_owned(),
            Some(Target::Route(Route::TopicLineage)),
        ),
        (
            Method::Get,
            "/live".to_owned(),
            Some(Target::Route(Route::Live)),
        ),
        (Method::Post, "/channels".to_owned(), None),
        (Method::Get, "/actions".to_owned(), None),
        (Method::Get, "/channels/".to_owned(), None),
        (Method::Get, format!("/channels/{id}/"), None),
        (Method::Get, "//channels".to_owned(), None),
        (Method::Get, "/".to_owned(), None),
        (Method::Get, "/nope".to_owned(), None),
    ];
    for (method, path, expected) in cases {
        assert_eq!(
            resolve(method, &path).map(|(target, _)| target),
            expected,
            "{} {path}",
            method.as_str()
        );
    }
    assert!(match_template("/channels/{id}", "/channels/x/y").is_none());
}
