//! L7 topology: edge aggregation. Consumer group `topology`, triggered by
//! `TransmissionClassified` (after analysis, so edges can be filtered by
//! topic).
//!
//! Implementations: `TimescaleEdgeStore` (continuous aggregates),
//! `InMemoryEdgeStore` (tests).

use crate::aggregates::edge::{EdgeKey, TopologyFilter, TopologyGraph, Weighting};
use crate::derived::flow::transmission::{Classification, Route};
use crate::ids::{AgentId, TransmissionId};
use crate::support::{TimeWindow, Timestamp};

/// One classified transmission, as the edge store counts it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EdgeContribution {
    pub transmission: TransmissionId,
    pub from: AgentId,
    pub to: AgentId,
    pub route: Route,
    pub at: Timestamp,
    pub matched_bytes: u64,
    pub classification: Classification,
}

pub trait EdgeStore {
    /// Idempotent on `transmission`. Returns the bucket it landed in.
    async fn apply(&mut self, contribution: &EdgeContribution) -> Result<EdgeKey, EdgeError>;

    async fn graph(
        &self,
        window: TimeWindow,
        weighting: Weighting,
        filter: &TopologyFilter,
    ) -> Result<TopologyGraph, EdgeError>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EdgeError {
    Store {
        reason: String,
    },
    /// The window is not aligned to bucket boundaries.
    UnalignedWindow,
}
