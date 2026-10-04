//! What the transmission and channel areas still read from the contract:
//! the transmissions behind a selection (item 1) and a channel's shape for
//! its name (item 24). Graphs, series and the overview are the spec's
//! (`crosstalk_spec::aggregates::{edge, access, node, series}`).

use crosstalk_spec::aggregates::edge::RouteKind;
use crosstalk_spec::derived::flow::resource::{Locator, ResourcePattern};
use crosstalk_spec::derived::flow::transmission::Route;
use crosstalk_spec::ids::{AgentId, TopicId, TransmissionId};
use crosstalk_spec::support::Timestamp;

use super::verdict::Verdict;

/// What a channel is named by.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ChannelShape {
    /// A declared channel's pattern.
    Pattern(ResourcePattern),
    /// A discovered channel's seed resource.
    Seed(Locator),
}

/// Which transmissions to list.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TransmissionSelector {
    /// The transmissions counted into one edge of the graph (canonical
    /// agents).
    Edge {
        from: AgentId,
        to: AgentId,
        route: Route,
    },
    /// A lasso or search selection.
    Ids(Vec<TransmissionId>),
    /// Everything in the scope.
    All,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TransmissionStateKind {
    Detected,
    AwaitingContent,
    Suspected,
    Confirmed,
    Classified,
    Aggregated,
    Discarded,
}

/// A row in a transmission list. `from` is known once confirmed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransmissionSummary {
    pub id: TransmissionId,
    pub from: Option<AgentId>,
    pub to: AgentId,
    pub route: Route,
    pub route_kind: RouteKind,
    pub state: TransmissionStateKind,
    pub opened_at: Timestamp,
    pub topic: Option<TopicId>,
    pub matched_bytes: u64,
    pub verdict: Option<Verdict>,
}
