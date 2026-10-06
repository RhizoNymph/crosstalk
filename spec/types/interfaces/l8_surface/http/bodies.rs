//! The JSON bodies of the `POST` reads whose arguments do not fit a URL:
//! one object per route, holding every argument of its method under the
//! method's parameter name. Each is a [`WireRequest`] with every field the
//! client's to choose, decoded strictly (unknown and missing fields
//! refused) through `decode_request`; the surface calls the method with the
//! fields as they are.
//!
//! | Route | Body | Method |
//! | --- | --- | --- |
//! | `POST /query/topology`, `POST /query/channel-topology` | [`GraphBody`] | `topology`, `channel_topology` |
//! | `POST /query/overview` | [`OverviewBody`] | `overview` |
//! | `POST /query/edge-transmissions` | [`EdgeTransmissionsBody`] | `edge_transmissions` |
//! | `POST /query/transmissions` | [`TransmissionsBody`] | `transmissions_by_id` |
//! | `POST /query/series` | [`SeriesBody`] | `series` |
//! | `POST /query/search` | [`SearchBody`] | `search` |
//! | `POST /projections` | [`FitProjectionBody`] | `fit_projection` |
//! | `POST /query/part-text` | [`PartTextBody`] | `part_text` |

use serde::{Deserialize, Serialize};

use crate::aggregates::edge::{EdgeSelector, TopologyFilter, Weighting};
use crate::aggregates::filter::TopicVersionSelector;
use crate::aggregates::projection::ProjectionParams;
use crate::aggregates::series::{SeriesGrid, SeriesGrouping};
use crate::observed::message::PartRef;
use crate::paging::{EdgeTransmissionList, PageRequest, SearchList, TransmissionList};
use crate::support::TimeWindow;
use crate::wire::WireRequest;

use super::super::conversation::text::TextSlice;
use super::super::lists::SearchRequest;
use super::super::summary::TransmissionSelection;

/// `topology` and `channel_topology`:
/// `{"window": .., "weighting": "count", "filter": {..}}`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct GraphBody {
    pub window: TimeWindow,
    pub weighting: Weighting,
    pub filter: TopologyFilter,
}

impl WireRequest for GraphBody {}

/// `overview`: `{"window": .., "filter": {..}}`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct OverviewBody {
    pub window: TimeWindow,
    pub filter: TopologyFilter,
}

impl WireRequest for OverviewBody {}

/// `edge_transmissions`: `{"edge": .., "window": .., "filter": .., "page": ..}`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct EdgeTransmissionsBody {
    pub edge: EdgeSelector,
    pub window: TimeWindow,
    pub filter: TopologyFilter,
    pub page: PageRequest<EdgeTransmissionList>,
}

impl WireRequest for EdgeTransmissionsBody {}

/// `transmissions_by_id`: `{"selection": [..], "version": .., "page": ..}`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct TransmissionsBody {
    pub selection: TransmissionSelection,
    pub version: TopicVersionSelector,
    pub page: PageRequest<TransmissionList>,
}

impl WireRequest for TransmissionsBody {}

/// `series`: `{"grid": .., "weighting": .., "grouping": .., "filter": ..}`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct SeriesBody {
    pub grid: SeriesGrid,
    pub weighting: Weighting,
    pub grouping: SeriesGrouping,
    pub filter: TopologyFilter,
}

impl WireRequest for SeriesBody {}

/// `search`: `{"request": {"mode": .., "text": ..}, "window": .., "filter": .., "page": ..}`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct SearchBody {
    pub request: SearchRequest,
    pub window: Option<TimeWindow>,
    pub filter: TopologyFilter,
    pub page: PageRequest<SearchList>,
}

impl WireRequest for SearchBody {}

/// `fit_projection`: `{"window": .., "filter": .., "params": ..}`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct FitProjectionBody {
    pub window: TimeWindow,
    pub filter: TopologyFilter,
    pub params: ProjectionParams,
}

impl WireRequest for FitProjectionBody {}

/// `part_text`: `{"part": {"message": "…", "index": 0}, "slice": {"from":
/// 8192, "limit": 8192}}`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct PartTextBody {
    pub part: PartRef,
    pub slice: TextSlice,
}

impl WireRequest for PartTextBody {}
