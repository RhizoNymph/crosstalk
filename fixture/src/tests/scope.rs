//! The UI's view scope as the tests use it (`ui/src/url/scope.rs`): a
//! window, the topic version every linked view pins, and the filter keys.
//! Kept here so the fixture's tests build without the UI.

use crosstalk_spec::aggregates::edge::{RouteKind, TopologyFilter};
use crosstalk_spec::aggregates::filter::{
    FalseDetections, TopicVersionSelector, UnconfirmedChannels,
};
use crosstalk_spec::aggregates::topic::TopicModelVersion;
use crosstalk_spec::ids::{AgentId, ChannelId, TopicId};
use crosstalk_spec::support::TimeWindow;

/// The spec's [`TopologyFilter`] without its version, which the scope
/// supplies.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ViewFilter {
    pub agents: Vec<AgentId>,
    pub channels: Vec<ChannelId>,
    pub route_kinds: Vec<RouteKind>,
    pub topics: Vec<TopicId>,
    pub false_detections: FalseDetections,
    pub unconfirmed_channels: UnconfirmedChannels,
}

impl ViewFilter {
    /// The spec's filter, pinned to `version`.
    pub fn pinned(&self, version: TopicModelVersion) -> TopologyFilter {
        TopologyFilter {
            agents: self.agents.clone(),
            channels: self.channels.clone(),
            route_kinds: self.route_kinds.clone(),
            topics: self.topics.clone(),
            topic_version: TopicVersionSelector::Pinned(version),
            false_detections: self.false_detections,
            unconfirmed_channels: self.unconfirmed_channels,
        }
    }
}

/// The window, version and filter of a view.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Scope {
    pub window: TimeWindow,
    pub topic_version: TopicModelVersion,
    pub filter: ViewFilter,
}

impl Scope {
    /// The filter every linked view of this scope is sent, pinned to the
    /// scope's version.
    pub fn topology_filter(&self) -> TopologyFilter {
        self.filter.pinned(self.topic_version)
    }
}
