//! The client against a stub surface that speaks the binding: requests
//! checked against the route table and the spec's wire goldens, every
//! error status decoded back, authentication, the live feed's reconnects
//! and exports verified by their trailer.

mod actions;
mod auth;
mod config;
mod errors;
mod export;
mod form;
mod frame;
mod live;
mod query;
mod responses;
mod sse;
mod stub;

use std::path::PathBuf;

use crosstalk_spec::aggregates::edge::{EdgeSelector, TopologyFilter};
use crosstalk_spec::aggregates::projection::ProjectionParams;
use crosstalk_spec::aggregates::series::SeriesGrid;
use crosstalk_spec::ids::{AgentId, ChannelId, OperatorId, TransmissionId};
use crosstalk_spec::interfaces::l8_surface::Caller;
use crosstalk_spec::interfaces::l8_surface::operators::{
    AccessConfig, OperatorDirectory, OperatorName, RequestIdentity, TrustedOperator,
};
use crosstalk_spec::paging::PageRequest;
use crosstalk_spec::support::TimeWindow;
use serde::de::DeserializeOwned;

pub(crate) const ULID_A: &str = "01J9Z3K8M4Q7R2T5V6W8X9Y0ZA";
pub(crate) const ULID_B: &str = "01J9Z3M2C5D6E7F8G9H0J1K2M3";
pub(crate) const ULID_C: &str = "01J9Z3N4P5Q6R7S8T9V0W1X2Y3";

/// The spec's golden file at `path`, under `spec/types/tests/golden/`.
pub(crate) fn golden(path: &str) -> Vec<u8> {
    let file = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../spec/types/tests/golden")
        .join(path);
    std::fs::read(&file).unwrap_or_else(|error| panic!("golden {file:?}: {error}"))
}

/// The golden at `path`, decoded.
pub(crate) fn golden_value<T: DeserializeOwned>(path: &str) -> T {
    serde_json::from_slice(&golden(path)).unwrap_or_else(|error| panic!("golden {path}: {error}"))
}

/// The golden at `path` as JSON.
pub(crate) fn golden_json(path: &str) -> serde_json::Value {
    golden_value(path)
}

/// A value from its JSON.
pub(crate) fn from_json<T: DeserializeOwned>(json: &str) -> T {
    serde_json::from_str(json).unwrap_or_else(|error| panic!("{json}: {error}"))
}

/// An entity id from its ULID text.
pub(crate) fn id<T: DeserializeOwned>(ulid: &str) -> T {
    from_json(&format!("\"{ulid}\""))
}

/// The trusted operator: every permission. The client never sends it.
pub(crate) fn caller() -> Caller {
    let config = AccessConfig::Trusted(TrustedOperator {
        id: id::<OperatorId>(ULID_A),
        name: OperatorName::new("me").unwrap_or_else(|error| panic!("{error:?}")),
    });
    OperatorDirectory::load(None, &config)
        .unwrap_or_else(|error| panic!("{error:?}"))
        .0
        .caller(RequestIdentity::Anonymous)
        .unwrap_or_else(|error| panic!("{error:?}"))
}

pub(crate) fn channel() -> ChannelId {
    id(ULID_B)
}

pub(crate) fn agent(ulid: &str) -> AgentId {
    id(ulid)
}

pub(crate) fn transmission() -> TransmissionId {
    id(ULID_C)
}

/// An hour, on bucket boundaries.
pub(crate) fn window() -> TimeWindow {
    from_json(r#"{"start":"2026-10-04T12:00:00.000000Z","end":"2026-10-04T13:00:00.000000Z"}"#)
}

pub(crate) fn page<L>() -> PageRequest<L> {
    from_json(r#"{"size":50,"after":null}"#)
}

/// The shared filter, from the graph body's golden.
pub(crate) fn filter() -> TopologyFilter {
    serde_json::from_value(golden_json("http/bodies/graph.json")["filter"].clone())
        .unwrap_or_else(|error| panic!("{error}"))
}

pub(crate) fn edge() -> EdgeSelector {
    golden_value("topology/edge_selector.json")
}

pub(crate) fn grid() -> SeriesGrid {
    golden_value("topology/series_grid.json")
}

pub(crate) fn params() -> ProjectionParams {
    golden_value("projections/projection_params.json")
}
