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
//! query time), and so are the agents the filter lists. Its route's channel
//! is canonical (resolved through `ChannelDirectory`, so a transmission on a
//! superseded channel counts on the channel that superseded it), and so are
//! the channels the filter lists: listing a superseded channel selects its
//! superseding channel. See [`crate::aliases`]. Its topic is the
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
//!
//! **Accesses.** The channel-centred view (`channel_topology`) also draws
//! accesses: agent-to-channel reads and writes, counted whether or not
//! anyone read what was written. Each access bucket is reduced to an
//! [`AccessSubject`] and kept when [`TopologyFilter::admits_access`] holds:
//!
//! | Field | Admits an access when |
//! | --- | --- |
//! | `agents` | its canonical agent is the canonical form of a listed agent |
//! | `channels` | its canonical channel is the canonical form of a listed channel |
//! | `route_kinds` | `Channel` is listed: an access is a channel's traffic |
//! | `topics` | its channel carries, in the window, a channel-routed confirmed transmission whose topic is listed |
//! | `false_detections` | always: an access is not a detection; it only narrows which transmissions count for `topics` |
//!
//! An access has no topic of its own, so a topic filter keeps the accesses
//! of channels the listed topics flowed through, and with them the writes
//! there that nobody has read yet. The window is tested against
//! `Access::at` (by bucket, like edges).

use crate::aggregates::edge::RouteKind;
use crate::aggregates::topic::TopicModelVersion;
use crate::aliases::Aliases;
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
    /// Keep only channel-routed transmissions on these channels (after
    /// supersession resolution of both sides). Non-channel routes never match
    /// a non-empty list.
    pub channels: Vec<ChannelId>,
    pub route_kinds: Vec<RouteKind>,
    /// Keep only transmissions whose topic, under the response's topic-model
    /// version, is one of these. Outliers and unclassified transmissions never
    /// match a non-empty list.
    pub topics: Vec<TopicId>,
    /// Which topic-model version `topics` and every reported topic refer
    /// to. `Pinned` makes a view reproducible across re-fits; a pinned
    /// version that is no longer retained is `VersionNotRetained`.
    pub topic_version: TopicVersionSelector,
    /// Whether transmissions an operator judged `FalseDetection` count.
    pub false_detections: FalseDetections,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum TopicVersionSelector {
    /// The active version at query time; the response reports which.
    #[default]
    Current,
    Pinned(TopicModelVersion),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum FalseDetections {
    /// Detector output as is.
    #[default]
    Include,
    /// Leave out transmissions whose latest verdict is `FalseDetection`.
    Exclude,
}

/// One confirmed transmission as every view's filter sees it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FilterSubject<'a> {
    /// The canonical sender: `AgentDirectory::canonical(Confirmed::from())`.
    pub from: AgentId,
    /// The canonical reader: `AgentDirectory::canonical(Transmission::to)`.
    pub to: AgentId,
    /// The route with its channel canonical (`Route::resolved`).
    pub route: &'a Route,
    /// The topic under the response's topic-model version; `None` for an
    /// outlier or a transmission not classified under that version.
    pub topic: Option<TopicId>,
    /// Whether the transmission's latest operator verdict is
    /// `FalseDetection`.
    pub false_detection: bool,
}

/// One access bucket as the channel-centred view's filter sees it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AccessSubject<'a> {
    /// The canonical agent that read or wrote.
    pub agent: AgentId,
    /// The canonical channel accessed.
    pub channel: ChannelId,
    /// The topics, under the response's topic-model version, of the
    /// channel-routed confirmed transmissions on `channel` in the window that
    /// the filter's `false_detections` keeps. Outliers and unclassified
    /// transmissions contribute none.
    pub channel_topics: &'a [TopicId],
}

impl TopologyFilter {
    /// Whether `subject` passes. `aliases` resolves the filter's listed
    /// agents through the merge aliases (`AgentDirectory::canonical`) and its
    /// listed channels through supersession (`ChannelDirectory::canonical`),
    /// so a listed id that has since been merged away or superseded still
    /// selects what it became.
    pub fn admits(&self, subject: &FilterSubject<'_>, aliases: impl Aliases) -> bool {
        let agents = self.agents.is_empty()
            || self
                .agents
                .iter()
                .map(|&listed| aliases.agent(listed))
                .any(|listed| listed == subject.from || listed == subject.to);
        let channels = self.channels.is_empty()
            || matches!(subject.route, Route::Channel(channel) if self.lists_channel(*channel, &aliases));
        let route_kinds = self.route_kinds.is_empty()
            || self.route_kinds.contains(&RouteKind::from(subject.route));
        let topics = self.topics.is_empty()
            || subject
                .topic
                .is_some_and(|topic| self.topics.contains(&topic));
        let verdicts = match self.false_detections {
            FalseDetections::Include => true,
            FalseDetections::Exclude => !subject.false_detection,
        };
        agents && channels && route_kinds && topics && verdicts
    }
}

impl TopologyFilter {
    /// Whether an access bucket passes, as the module table defines.
    /// `subject`'s agent and channel are canonical; `aliases` resolves the
    /// filter's listed ids.
    pub fn admits_access(&self, subject: &AccessSubject<'_>, aliases: impl Aliases) -> bool {
        let agents = self.agents.is_empty()
            || self
                .agents
                .iter()
                .any(|&listed| aliases.agent(listed) == subject.agent);
        let channels = self.channels.is_empty() || self.lists_channel(subject.channel, &aliases);
        let route_kinds =
            self.route_kinds.is_empty() || self.route_kinds.contains(&RouteKind::Channel);
        let topics = self.topics.is_empty()
            || subject
                .channel_topics
                .iter()
                .any(|topic| self.topics.contains(topic));
        agents && channels && route_kinds && topics
    }

    /// Whether the canonical `channel` is the canonical form of a listed
    /// channel.
    fn lists_channel(&self, channel: ChannelId, aliases: &impl Aliases) -> bool {
        self.channels
            .iter()
            .any(|&listed| aliases.channel(listed) == channel)
    }
}
