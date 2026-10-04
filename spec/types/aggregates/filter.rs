//! The one filter shared by every linked view.
//!
//! The topology graph, search, the projection (UMAP) and the list of
//! transmissions behind an edge all take a [`TopologyFilter`] and apply it
//! identically, so selecting agents, channels, route kinds or topics in one
//! view narrows the others to the same transmissions.
//!
//! Every view filters confirmed transmissions. Each transmission is reduced
//! to a [`FilterSubject`], and [`TopologyFilter::admits`] decides it:
//!
//! | View | Unit filtered | Subject |
//! | --- | --- | --- |
//! | topology graph | the transmissions counted into its edges | each counted transmission |
//! | search | each hit | the hit's transmission |
//! | projection | each point | the point's transmission |
//! | edge transmissions | each row | the row's transmission |
//!
//! A subject's agents are canonical (resolved through `AgentDirectory` at
//! query time), and so are the agents the filter lists. Its topic is the
//! transmission's classification under the topic-model version the response
//! reports (`TopologyGraph::topic_version`, `SearchResults::topic_version`,
//! `Projection::topic_version`, `EdgeTransmissionPage::topic_version`); a
//! transmission not yet classified under that version has no topic. Topic
//! ids are never reused across versions, so a filter holding an older
//! version's topic ids matches nothing; the reported version lets a client
//! notice and refetch the topics.
//!
//! The time window is not part of the filter: every view takes it
//! separately and tests it against `Confirmed::at` with
//! [`TimeWindow::contains`](crate::support::TimeWindow::contains).

use crate::aggregates::edge::RouteKind;
use crate::derived::flow::transmission::Route;
use crate::ids::{AgentId, ChannelId, TopicId};

/// Restricts which transmissions a view shows. Empty lists do not restrict.
/// Non-empty lists combine with AND across fields; entries within one list
/// combine with OR. [`TopologyFilter::admits`] is the definition.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct TopologyFilter {
    /// Keep transmissions whose sender OR reader is one of these (after alias
    /// resolution of both sides).
    pub agents: Vec<AgentId>,
    /// Keep only channel-routed transmissions on these channels. Non-channel
    /// routes never match a non-empty list.
    pub channels: Vec<ChannelId>,
    pub route_kinds: Vec<RouteKind>,
    /// Keep only transmissions whose topic, under the response's topic-model
    /// version, is one of these. Outliers and unclassified transmissions never
    /// match a non-empty list.
    pub topics: Vec<TopicId>,
}

/// One confirmed transmission as every view's filter sees it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FilterSubject<'a> {
    /// The canonical sender: `AgentDirectory::canonical(Confirmed::from())`.
    pub from: AgentId,
    /// The canonical reader: `AgentDirectory::canonical(Transmission::to)`.
    pub to: AgentId,
    pub route: &'a Route,
    /// The topic under the response's topic-model version; `None` for an
    /// outlier or a transmission not classified under that version.
    pub topic: Option<TopicId>,
}

impl TopologyFilter {
    /// Whether `subject` passes. `canonical` resolves the filter's listed
    /// agents through the merge aliases (`AgentDirectory::canonical`), so a
    /// listed id that has since been merged away still selects its agent.
    pub fn admits(
        &self,
        subject: &FilterSubject<'_>,
        canonical: impl Fn(AgentId) -> AgentId,
    ) -> bool {
        let agents = self.agents.is_empty()
            || self
                .agents
                .iter()
                .map(|&listed| canonical(listed))
                .any(|listed| listed == subject.from || listed == subject.to);
        let channels = self.channels.is_empty()
            || matches!(subject.route, Route::Channel(channel) if self.channels.contains(channel));
        let route_kinds = self.route_kinds.is_empty()
            || self.route_kinds.contains(&RouteKind::from(subject.route));
        let topics = self.topics.is_empty()
            || subject
                .topic
                .is_some_and(|topic| self.topics.contains(&topic));
        agents && channels && route_kinds && topics
    }
}
