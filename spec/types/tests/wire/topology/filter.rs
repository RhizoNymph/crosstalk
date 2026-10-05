//! The requests of the linked views: the shared `TopologyFilter` with its
//! `TopicVersionSelector`, `FalseDetections` and `RouteKind`s, the
//! `Weighting`, and the `EdgeSelector` of a drill-down.

use super::super::harness::{assert_golden, assert_rejected, assert_request_golden};
use super::super::{ULID_A, ULID_B};
use super::{AREA, a, b, topic, version, wiki};
use crate::aggregates::edge::{EdgeSelector, RouteKind, Weighting};
use crate::aggregates::filter::UnconfirmedChannels;
use crate::aggregates::filter::{FalseDetections, TopicVersionSelector, TopologyFilter};
use crate::derived::flow::transmission::{DelegationDirection, Route};

fn every_route_kind() -> Vec<RouteKind> {
    fn declared(kind: RouteKind) -> RouteKind {
        match kind {
            RouteKind::Channel
            | RouteKind::Delegation
            | RouteKind::Direct
            | RouteKind::Unobserved => kind,
        }
    }
    [
        RouteKind::Channel,
        RouteKind::Delegation,
        RouteKind::Direct,
        RouteKind::Unobserved,
    ]
    .into_iter()
    .map(declared)
    .collect()
}

fn every_false_detections() -> Vec<FalseDetections> {
    fn declared(choice: FalseDetections) -> FalseDetections {
        match choice {
            FalseDetections::Include | FalseDetections::Exclude => choice,
        }
    }
    [FalseDetections::Include, FalseDetections::Exclude]
        .into_iter()
        .map(declared)
        .collect()
}

/// Every field set: two agents, the wiki, two route kinds, a topic of a
/// pinned version, false detections left out.
fn full_filter() -> TopologyFilter {
    TopologyFilter {
        agents: vec![a(), b()],
        channels: vec![wiki()],
        route_kinds: vec![RouteKind::Channel, RouteKind::Delegation],
        topics: vec![topic()],
        topic_version: TopicVersionSelector::Pinned(version()),
        false_detections: FalseDetections::Exclude,
        unconfirmed_channels: UnconfirmedChannels::Exclude,
    }
}

#[test]
fn topology_filter_goldens() {
    assert_request_golden(AREA, "topology_filter_default", &TopologyFilter::default());
    assert_request_golden(AREA, "topology_filter_full", &full_filter());
    assert_golden(AREA, "route_kinds", &every_route_kind());
    assert_golden(AREA, "false_detections", &every_false_detections());
}

#[test]
fn topic_version_selector_goldens() {
    fn declared(selector: TopicVersionSelector) -> TopicVersionSelector {
        match selector {
            TopicVersionSelector::Current | TopicVersionSelector::Pinned(_) => selector,
        }
    }
    assert_request_golden(
        AREA,
        "topic_version_current",
        &declared(TopicVersionSelector::Current),
    );
    assert_request_golden(
        AREA,
        "topic_version_pinned",
        &declared(TopicVersionSelector::Pinned(version())),
    );
}

#[test]
fn weighting_goldens() {
    fn name(weighting: Weighting) -> &'static str {
        match weighting {
            Weighting::Transmissions => "weighting_transmissions",
            Weighting::MatchedBytes => "weighting_matched_bytes",
        }
    }
    for weighting in [Weighting::Transmissions, Weighting::MatchedBytes] {
        assert_request_golden(AREA, name(weighting), &weighting);
    }
}

#[test]
fn edge_selector_golden() {
    let selector = EdgeSelector::new(a(), b(), Route::Channel(wiki())).expect("two agents");
    assert_request_golden(AREA, "edge_selector", &selector);
    let delegation = EdgeSelector::new(
        b(),
        a(),
        Route::Delegation(DelegationDirection::ChildToParent),
    )
    .expect("two agents");
    assert_request_golden(AREA, "edge_selector_delegation", &delegation);
}

const DEFAULT_FILTER: &str = r#""agents": [], "channels": [], "route_kinds": [], "topics": [],
    "topic_version": {"type": "current"}, "false_detections": "include",
    "unconfirmed_channels": "include""#;

#[test]
fn topology_filters_refuse_unknown_fields_and_variants() {
    let decoded: TopologyFilter = serde_json::from_str(&format!("{{{DEFAULT_FILTER}}}"))
        .unwrap_or_else(|error| panic!("the default filter decodes: {error}"));
    assert_eq!(decoded, TopologyFilter::default());
    assert_rejected::<TopologyFilter>(
        &format!(r#"{{{DEFAULT_FILTER}, "window": null}}"#),
        "unknown field `window`",
    );
    // The time window is never part of the filter, and every list is
    // written, even empty.
    assert_rejected::<TopologyFilter>(
        r#"{"agents": [], "channels": [], "route_kinds": [], "topics": [], "false_detections": "include"}"#,
        "missing field `topic_version`",
    );
    assert_rejected::<TopologyFilter>(r#"{"agents": []}"#, "missing field `channels`");
    assert_rejected::<TopologyFilter>(
        &format!(
            r#"{{{}}}"#,
            DEFAULT_FILTER.replace(r#""route_kinds": []"#, r#""route_kinds": ["tool"]"#)
        ),
        "unknown variant `tool`",
    );
    assert_rejected::<TopologyFilter>(
        &format!(
            r#"{{{}}}"#,
            DEFAULT_FILTER.replace(r#""agents": []"#, r#""agents": ["planner"]"#)
        ),
        "invalid ULID text",
    );
    assert_rejected::<TopologyFilter>(
        &format!(
            r#"{{{}}}"#,
            DEFAULT_FILTER.replace(r#""include""#, r#""only""#)
        ),
        "unknown variant `only`",
    );
    assert_rejected::<TopologyFilter>(
        &format!(
            r#"{{{}}}"#,
            DEFAULT_FILTER.replace(
                r#""unconfirmed_channels": "include""#,
                r#""unconfirmed_channels": "hide""#
            )
        ),
        "unknown variant `hide`",
    );
    assert_rejected::<RouteKind>(r#""Channel""#, "unknown variant `Channel`");
}

#[test]
fn topic_version_selectors_refuse_unknown_variants_and_zero_data() {
    assert_rejected::<TopicVersionSelector>(r#"{"type": "latest"}"#, "unknown variant `latest`");
    assert_rejected::<TopicVersionSelector>(r#"{"type": "pinned"}"#, "missing field `data`");
    assert_rejected::<TopicVersionSelector>(r#"{"type": "pinned", "data": "3"}"#, "invalid type");
    assert_rejected::<TopicVersionSelector>(r#""current""#, "invalid type");
    assert_rejected::<Weighting>(r#""bytes""#, "unknown variant `bytes`");
}

#[test]
fn edge_selectors_refuse_self_edges_and_unknown_fields() {
    let route = r#"{"type": "unobserved"}"#;
    assert_rejected::<EdgeSelector>(
        &format!(r#"{{"from": "{ULID_A}", "to": "{ULID_A}", "route": {route}}}"#),
        "invalid edge selector: SelfEdge",
    );
    assert_rejected::<EdgeSelector>(
        &format!(r#"{{"from": "{ULID_A}", "to": "{ULID_B}", "route": {route}, "topic": null}}"#),
        "unknown field `topic`",
    );
    assert_rejected::<EdgeSelector>(
        &format!(r#"{{"from": "{ULID_A}", "to": "{ULID_B}", "route": {{"type": "email"}}}}"#),
        "unknown variant `email`",
    );
}
