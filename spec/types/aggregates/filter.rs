//! The one filter shared by every linked view.
//!
//! The topology graph, series, search, the projection (UMAP) and the list
//! of transmissions behind an edge all take a [`TopologyFilter`] and apply
//! it identically, so selecting agents, channels, route kinds or topics in
//! one view narrows the others to the same transmissions.
//!
//! Every view filters confirmed transmissions. Each transmission is reduced
//! to a [`FilterSubject`], and [`TopologyFilter::admits`] decides it:
//!
//! | View | Unit filtered | Subject |
//! | --- | --- | --- |
//! | topology graph, series | the transmissions counted into its edges | each counted transmission |
//! | search | each hit | the hit's transmission |
//! | projection | each point | the point's transmission, at fit time |
//! | edge transmissions | each row | the row's transmission |
//!
//! **Only transmissions between different agents.** `admits` never admits
//! a subject whose sender and reader are one canonical agent: two ids of
//! one transmission merged since it was recorded
//! ([`Crossing::WithinOneAgent`]). So no view counts or lists such a
//! transmission: graphs and series drop it as a self-edge, and search, the
//! projection's sample, the edge drill-down and exports, which all filter
//! through `admits`, leave it out too.
//!
//! A subject's agents are canonical (resolved through `AgentDirectory` when
//! the view is computed; for a stored projection, when it was fitted), and
//! so are the agents the filter lists. Its route's channel is canonical
//! (resolved through `ChannelDirectory`, so a transmission on a superseded
//! channel counts on the channel that superseded it), and so are the
//! channels the filter lists: listing a superseded channel selects its
//! superseding channel. See [`crate::aliases`].
//!
//! **Topic version.** Every view evaluates topics (the filter's and the
//! ones it reports) under one concrete topic-model version, which it reports
//! (`TopologyGraph::topic_version`, `TopologySeries::topic_version`,
//! `SearchResults::topic_version`, `EdgeTransmissionPage::topic_version`,
//! `ProjectionSpec::topic_version`, `TopicPage::version`). The store serving
//! the view resolves the filter's [`TopicVersionSelector`] with
//! [`TopicVersionSelector::resolve`] against the `TopicCatalog`'s history,
//! once: for a paged view on its first page, whose cursor then pins the
//! resolved version for every later page whatever the selector would now
//! resolve to. A pinned version whose data is dropped before a later page is
//! [`VersionUnavailable::NotRetained`], not an invalid cursor. A non-empty
//! `topics` list must name topics of the resolved version only
//! ([`TopologyFilter::topics_outside`]); otherwise the view fails with
//! `ConflictKind::TopicsNotInVersion` instead of silently matching nothing.
//! A client that links several views resolves once (the first response's
//! reported version) and pins it in every other request.
//!
//! The time window is not part of the filter: every view takes it
//! separately and tests it against `Confirmed::at` with
//! [`TimeWindow::contains`](crate::support::TimeWindow::contains).
//!
//! **Accesses.** The channel-centred view (`channel_topology`) also draws
//! accesses: agent-to-channel reads and writes on channels listed as
//! channels ([`Listing::Channel`]: with cross-agent traffic, so neither a
//! resource on no channel, a hidden channel nor a declaration without
//! traffic), counted whether or not anyone read what was written. Each
//! access bucket is reduced to an [`AccessSubject`] and kept when
//! [`TopologyFilter::admits_access`] holds:
//!
//! | Field | Admits an access when |
//! | --- | --- |
//! | `agents` | its canonical agent is the canonical form of a listed agent |
//! | `channels` | its canonical channel is the canonical form of a listed channel |
//! | `route_kinds` | `Channel` is listed: an access is a channel's traffic |
//! | `topics` | its channel carries, in the window, a channel-routed confirmed transmission whose topic is listed |
//! | `false_detections` | always: an access is not a detection; it only narrows which transmissions count for `topics` |
//! | `unconfirmed_channels` | `Include`, or its channel's confirmation is `Confirmed` |
//!
//! An access has no topic of its own, so a topic filter keeps the accesses
//! of channels the listed topics flowed through, and with them the writes
//! there that nobody has read yet. The window is tested against
//! `Access::at` (by bucket, like edges).
//!
//! **Unconfirmed channels.** `unconfirmed_channels` decides whether
//! channels whose cross-agent traffic is all unconfirmed
//! ([`Confirmation::Unconfirmed`]) count: their accesses and channel nodes
//! in the channel-centred view, and the channel counts of the overview. It
//! changes no transmission view: those count confirmed transmissions
//! between different agents only, and a channel such a transmission is
//! routed through is confirmed by it.
//!
//! [`Listing::Channel`]: crate::derived::flow::channel::confirmation::Listing::Channel
//! [`Crossing::WithinOneAgent`]: crate::derived::flow::transmission::Crossing::WithinOneAgent

use serde::{Deserialize, Serialize};

use crate::aggregates::edge::RouteKind;
use crate::aggregates::topic::TopicModelVersion;
use crate::aggregates::topic_history::{TopicVersionHistory, TopicVersionStatus};
use crate::aliases::Aliases;
use crate::derived::flow::channel::confirmation::Confirmation;
use crate::derived::flow::transmission::Route;
use crate::ids::{AgentId, ChannelId, TopicId};
use crate::wire::WireRequest;

/// Restricts which transmissions a view shows. Empty lists do not restrict.
/// Non-empty lists combine with AND across fields; entries within one list
/// combine with OR. [`TopologyFilter::admits`] is the definition.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
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
    /// Whether channels whose cross-agent traffic is all unconfirmed count
    /// (see the module docs, "Unconfirmed channels").
    pub unconfirmed_channels: UnconfirmedChannels,
}

/// Whether views count channels whose cross-agent traffic is all
/// unconfirmed ([`Confirmation::Unconfirmed`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UnconfirmedChannels {
    /// Counted and drawn like any channel; the UI marks them. The default,
    /// so a resource agents only just started passing text through is not
    /// missed while its content evidence is outstanding.
    #[default]
    Include,
    /// Only channels with a confirmed cross-agent transmission count.
    Exclude,
}

impl UnconfirmedChannels {
    /// Whether a channel with `confirmation` counts.
    pub fn keeps(self, confirmation: Confirmation) -> bool {
        match (self, confirmation) {
            (Self::Include, _) | (Self::Exclude, Confirmation::Confirmed) => true,
            (Self::Exclude, Confirmation::Unconfirmed) => false,
        }
    }
}

/// A request (a linked view's version, `transmissions_by_id`, `topics`):
/// `{"type": "current"}` or `{"type": "pinned", "data": 3}`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
#[serde(
    tag = "type",
    content = "data",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum TopicVersionSelector {
    /// The catalog's active version when the view is computed; the response
    /// reports which.
    #[default]
    Current,
    Pinned(TopicModelVersion),
}

/// A client chooses every field of the shared filter.
impl WireRequest for TopologyFilter {}

impl WireRequest for TopicVersionSelector {}

/// Why a selector names no version a linked view can be computed under.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum VersionUnavailable {
    /// The catalog has no such version.
    Unknown(TopicModelVersion),
    /// Still being fitted or re-classified.
    Fitting(TopicModelVersion),
    /// Ready but never activated (not yet, or overtaken by a newer version
    /// before it was), so its edge buckets were never complete.
    NotActivated(TopicModelVersion),
    /// Was activated, but the store no longer retains its data.
    NotRetained(TopicModelVersion),
}

impl TopicVersionSelector {
    /// The concrete version a linked view (graph, series, search, edge
    /// transmissions, projection fit) is computed under. `Current` is the
    /// history's active version, which every store retains. `Pinned(v)`
    /// must have been activated (it is active or superseded after being
    /// active) and be `retained` by the store serving the view.
    pub fn resolve(
        self,
        history: &TopicVersionHistory,
        retained: impl Fn(TopicModelVersion) -> bool,
    ) -> Result<TopicModelVersion, VersionUnavailable> {
        let version = match self {
            Self::Current => return Ok(history.active().version()),
            Self::Pinned(version) => version,
        };
        let info = history
            .get(version)
            .ok_or(VersionUnavailable::Unknown(version))?;
        match info.status() {
            TopicVersionStatus::Fitting { .. } => Err(VersionUnavailable::Fitting(version)),
            TopicVersionStatus::Ready { .. }
            | TopicVersionStatus::Superseded {
                activated_at: None, ..
            } => Err(VersionUnavailable::NotActivated(version)),
            TopicVersionStatus::Active { .. } => Ok(version),
            TopicVersionStatus::Superseded {
                activated_at: Some(_),
                ..
            } => {
                if retained(version) {
                    Ok(version)
                } else {
                    Err(VersionUnavailable::NotRetained(version))
                }
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
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
    /// `FalseDetection`, as the view's copy of current verdicts holds it at
    /// query time ([`CurrentVerdict::is_false_detection`]). Verdicts are
    /// never stored in aggregates, so a verdict changed after aggregation
    /// counts from the next query on.
    ///
    /// [`CurrentVerdict::is_false_detection`]: crate::derived::flow::verdict::CurrentVerdict::is_false_detection
    pub false_detection: bool,
}

/// One access bucket as the channel-centred view's filter sees it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AccessSubject<'a> {
    /// The canonical agent that read or wrote.
    pub agent: AgentId,
    /// The canonical channel accessed: the channel in force that holds the
    /// access's resource at read time, listed as a channel.
    pub channel: ChannelId,
    /// That channel's confirmation at read time (`Listing::Channel`).
    pub confirmation: Confirmation,
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
    /// selects what it became. A subject whose sender and reader are one
    /// agent is never admitted, whatever the filter.
    pub fn admits(&self, subject: &FilterSubject<'_>, aliases: impl Aliases) -> bool {
        if subject.from == subject.to {
            return false;
        }
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

    /// The listed topics that do not belong to `version`, in list order and
    /// without repeats. `version_of` looks a topic id up in the catalog;
    /// topic ids are never reused across versions. A view whose filter has
    /// any fails with `TopicsNotInVersion` rather than matching nothing.
    pub fn topics_outside(
        &self,
        version: TopicModelVersion,
        version_of: impl Fn(TopicId) -> Option<TopicModelVersion>,
    ) -> Vec<TopicId> {
        let mut outside: Vec<TopicId> = Vec::new();
        for &topic in &self.topics {
            if version_of(topic) != Some(version) && !outside.contains(&topic) {
                outside.push(topic);
            }
        }
        outside
    }

    /// This filter with its selector pinned to `version`. A stored
    /// projection keeps its filter in this form.
    pub fn pinned(self, version: TopicModelVersion) -> Self {
        Self {
            topic_version: TopicVersionSelector::Pinned(version),
            ..self
        }
    }
}

impl TopologyFilter {
    /// Whether an access bucket passes, as the module table defines.
    /// `subject`'s agent and channel are canonical; `aliases` resolves the
    /// filter's listed ids.
    ///
    /// `false_detections` is not read here: an access is an observed read or
    /// write, not a detection, so `Exclude` never drops an access bucket on
    /// its own. It applies where it means something, to the transmissions:
    /// the store builds `channel_topics` only from the transmissions the
    /// filter's `false_detections` keeps, so under `Exclude` a topic carried
    /// to a channel only by false detections does not keep its accesses.
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
        let confirmation = self.unconfirmed_channels.keeps(subject.confirmation);
        agents && channels && route_kinds && topics && confirmation
    }

    /// Whether the canonical `channel` is the canonical form of a listed
    /// channel.
    fn lists_channel(&self, channel: ChannelId, aliases: &impl Aliases) -> bool {
        self.channels
            .iter()
            .any(|&listed| aliases.channel(listed) == channel)
    }
}
