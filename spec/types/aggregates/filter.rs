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
//! A subject's agents are canonical (resolved through `AgentDirectory` when
//! the view is computed; for a stored projection, when it was fitted), and
//! so are the agents the filter lists.
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

use crate::aggregates::edge::RouteKind;
use crate::aggregates::topic::TopicModelVersion;
use crate::aggregates::topic_history::{TopicVersionHistory, TopicVersionStatus};
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
    /// Which topic-model version `topics` and every reported topic refer
    /// to. `Pinned` makes a view reproducible across re-fits; a pinned
    /// version that is no longer retained is `VersionNotRetained`.
    pub topic_version: TopicVersionSelector,
    /// Whether transmissions an operator judged `FalseDetection` count.
    pub false_detections: FalseDetections,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum TopicVersionSelector {
    /// The catalog's active version when the view is computed; the response
    /// reports which.
    #[default]
    Current,
    Pinned(TopicModelVersion),
}

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
    pub route: &'a Route,
    /// The topic under the response's topic-model version; `None` for an
    /// outlier or a transmission not classified under that version.
    pub topic: Option<TopicId>,
    /// Whether the transmission's latest operator verdict is
    /// `FalseDetection`.
    pub false_detection: bool,
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
