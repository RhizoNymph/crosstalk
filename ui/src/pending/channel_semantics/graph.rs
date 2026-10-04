//! The channel-centred graph with each channel node's confirmation.
//! Stand-in for the port's `ChannelNode::confirmation` field.

use std::collections::BTreeMap;
use std::ops::Deref;

use crosstalk_spec::aggregates::access::BipartiteGraph;
use crosstalk_spec::ids::ChannelId;

use super::confirmation::Confirmation;

/// The spec's bipartite graph and the confirmation of every channel node
/// in it, read at query time. Dereferences to the graph.
#[derive(Debug, Clone, PartialEq)]
pub struct ChannelGraph {
    pub graph: BipartiteGraph,
    /// One entry per channel node: only channels listed as channels are
    /// drawn, and each has a confirmation.
    pub confirmations: BTreeMap<ChannelId, Confirmation>,
}

impl ChannelGraph {
    /// The confirmation of the channel node `id`; `None` when the graph
    /// has no such node.
    pub fn confirmation(&self, id: ChannelId) -> Option<Confirmation> {
        self.confirmations.get(&id).copied()
    }
}

impl Deref for ChannelGraph {
    type Target = BipartiteGraph;

    fn deref(&self) -> &BipartiteGraph {
        &self.graph
    }
}
