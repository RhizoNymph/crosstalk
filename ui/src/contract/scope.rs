//! What every aggregate view is computed over (items 2 and 9).

use crosstalk_spec::aggregates::edge::RouteKind;
use crosstalk_spec::aggregates::topic::TopicModelVersion;
use crosstalk_spec::ids::{AgentId, ChannelId, TopicId};
use crosstalk_spec::support::TimeWindow;

/// Replaces `crosstalk_spec::aggregates::edge::TopologyFilter`, adding the
/// verdict choice. Empty lists do not restrict; non-empty lists combine with
/// AND across fields. Topics are read under the [`Scope`]'s topic version.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct TopologyFilter {
    /// Keep transmissions whose sender OR reader is one of these, after
    /// alias resolution.
    pub agents: Vec<AgentId>,
    /// Keep only channel-routed transmissions on these channels, after
    /// supersession.
    pub channels: Vec<ChannelId>,
    pub route_kinds: Vec<RouteKind>,
    /// Outliers never match a topic filter.
    pub topics: Vec<TopicId>,
    pub verdicts: VerdictFilter,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum VerdictFilter {
    #[default]
    IncludeAll,
    /// Drop transmissions whose latest verdict is `FalseDetection`.
    ExcludeFalseDetections,
}

/// The window, topic-model version and filter of a view. The version is
/// always explicit, so a cited view means the same thing after a re-fit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Scope {
    pub window: TimeWindow,
    pub topic_version: TopicModelVersion,
    pub filter: TopologyFilter,
}
