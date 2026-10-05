//! The `POST` read bodies: one request golden each, and strict decoding.

use super::super::harness::{assert_rejected, assert_request_golden};
use super::{AREA, edge, filter, grid, page, params, transmission, window};
use crate::aggregates::edge::{TopologyFilter, Weighting};
use crate::aggregates::filter::TopicVersionSelector;
use crate::aggregates::series::SeriesGrouping;
use crate::interfaces::l8_surface::http::bodies::{
    EdgeTransmissionsBody, FitProjectionBody, GraphBody, OverviewBody, SearchBody, SeriesBody,
    TransmissionsBody,
};
use crate::interfaces::l8_surface::lists::{SearchMode, SearchRequest};
use crate::interfaces::l8_surface::summary::TransmissionSelection;
use crate::support::NonBlank;

fn area() -> String {
    format!("{AREA}/bodies")
}

#[test]
fn bodies_golden() {
    let area = area();
    assert_request_golden(
        &area,
        "graph",
        &GraphBody {
            window: window(),
            weighting: Weighting::MatchedBytes,
            filter: filter(),
        },
    );
    assert_request_golden(
        &area,
        "overview",
        &OverviewBody {
            window: window(),
            filter: TopologyFilter::default(),
        },
    );
    assert_request_golden(
        &area,
        "edge_transmissions",
        &EdgeTransmissionsBody {
            edge: edge(),
            window: window(),
            filter: filter(),
            page: page(),
        },
    );
    assert_request_golden(
        &area,
        "transmissions",
        &TransmissionsBody {
            selection: TransmissionSelection::new(vec![transmission()])
                .unwrap_or_else(|error| panic!("{error:?}")),
            version: TopicVersionSelector::Current,
            page: page(),
        },
    );
    assert_request_golden(
        &area,
        "series",
        &SeriesBody {
            grid: grid(),
            weighting: Weighting::Transmissions,
            grouping: SeriesGrouping::Edge,
            filter: filter(),
        },
    );
    assert_request_golden(
        &area,
        "search",
        &SearchBody {
            request: SearchRequest {
                mode: SearchMode::Semantic,
                text: NonBlank::new("rotate the deploy key").unwrap_or_else(|e| panic!("{e:?}")),
            },
            window: None,
            filter: filter(),
            page: page(),
        },
    );
    assert_request_golden(
        &area,
        "fit_projection",
        &FitProjectionBody {
            window: window(),
            filter: filter(),
            params: params(),
        },
    );
}

#[test]
fn bodies_refuse_unknown_and_missing_fields() {
    let window = r#"{"start":"2026-10-04T12:00:00.000000Z","end":"2026-10-04T13:00:00.000000Z"}"#;
    let filter = r#"{"agents":[],"channels":[],"route_kinds":[],"topics":[],
        "topic_version":{"type":"current"},"false_detections":"include",
        "unconfirmed_channels":"include"}"#;
    assert_rejected::<OverviewBody>(
        &format!(
            r#"{{"window":{window},"filter":{filter},"caller":"01J9Z3K8M4Q7R2T5V6W8X9Y0ZA"}}"#
        ),
        "unknown field `caller`",
    );
    assert_rejected::<OverviewBody>(
        &format!(r#"{{"window":{window}}}"#),
        "missing field `filter`",
    );
    assert_rejected::<GraphBody>(
        &format!(r#"{{"window":{window},"weighting":"bytes","filter":{filter}}}"#),
        "unknown variant `bytes`",
    );
    assert_rejected::<TransmissionsBody>(
        r#"{"selection":[],"version":{"type":"current"},"page":{"size":50,"after":null}}"#,
        "invalid transmission selection",
    );
}
