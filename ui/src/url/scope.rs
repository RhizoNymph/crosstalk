//! What every view of one URL is computed over: a window on bucket
//! boundaries, the topic-model version every linked view pins, and the
//! shared filter.
//!
//! The URL always names its version (`v`), so every request the UI sends
//! pins it ([`TopicVersionSelector::Pinned`]): a cited view means the same
//! thing after a re-fit. [`Scope::topology_filter`] is the one place the
//! [`TopologyFilter`] is built from the URL's filter keys, so the version a
//! filter pins is always the scope's. It is the channel-semantics stand-in
//! (`crate::pending`): the spec's filter plus `unconfirmed_channels`.

use crosstalk_spec::aggregates::edge::{RouteKind, TopologyFilter as SpecFilter};
use crosstalk_spec::aggregates::filter::{FalseDetections, TopicVersionSelector};
use crosstalk_spec::aggregates::series::BucketWidth;
use crosstalk_spec::aggregates::topic::TopicModelVersion;
use crosstalk_spec::ids::{AgentId, ChannelId, TopicId};
use crosstalk_spec::support::{TimeWindow, Timestamp};

use crate::pending::channel_semantics::{TopologyFilter, UnconfirmedChannels};

/// The filter keys of the view state (`a`, `c`, `r`, `t`, `x`, `u`): the
/// spec's [`TopologyFilter`] without its version, which the scope supplies.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ViewFilter {
    pub agents: Vec<AgentId>,
    pub channels: Vec<ChannelId>,
    pub route_kinds: Vec<RouteKind>,
    pub topics: Vec<TopicId>,
    pub false_detections: FalseDetections,
    /// Whether channels whose cross-agent traffic is all unconfirmed count
    /// (`u`): included and marked by default, left out by "confirmed
    /// only". Channel lists, the review queue, the channels-mode graph and
    /// the overview's channel counts honour it.
    pub unconfirmed_channels: UnconfirmedChannels,
}

impl ViewFilter {
    /// Whether "confirmed only" is on.
    pub fn confirmed_only(&self) -> bool {
        self.unconfirmed_channels == UnconfirmedChannels::Exclude
    }

    /// This filter with "confirmed only" switched.
    pub fn toggle_confirmed_only(&self) -> Self {
        Self {
            unconfirmed_channels: match self.unconfirmed_channels {
                UnconfirmedChannels::Include => UnconfirmedChannels::Exclude,
                UnconfirmedChannels::Exclude => UnconfirmedChannels::Include,
            },
            ..self.clone()
        }
    }
}

impl ViewFilter {
    /// The spec's filter, pinned to `version`.
    pub fn pinned(&self, version: TopicModelVersion) -> TopologyFilter {
        TopologyFilter {
            filter: SpecFilter {
                agents: self.agents.clone(),
                channels: self.channels.clone(),
                route_kinds: self.route_kinds.clone(),
                topics: self.topics.clone(),
                topic_version: TopicVersionSelector::Pinned(version),
                false_detections: self.false_detections,
            },
            unconfirmed_channels: self.unconfirmed_channels,
        }
    }
}

/// The window, version and filter of a view.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Scope {
    /// On bucket boundaries when parsed from a URL (`ViewState::parse`
    /// redirects any other window to its snapped form).
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

/// The bucket boundary at or before `at`.
pub fn align_down(at: Timestamp, width: BucketWidth) -> Timestamp {
    let micros = at.as_micros();
    Timestamp::from_micros(micros - micros % width.as_micros())
}

/// The bucket boundary at or after `at`.
pub fn align_up(at: Timestamp, width: BucketWidth) -> Timestamp {
    let down = align_down(at, width);
    if down == at {
        at
    } else {
        Timestamp::from_micros(down.as_micros().saturating_add(width.as_micros().get()))
    }
}

/// Whether both ends of `window` are bucket boundaries.
pub fn is_aligned(window: TimeWindow, width: BucketWidth) -> bool {
    width.is_boundary(window.start()) && width.is_boundary(window.end())
}

/// The smallest window on bucket boundaries that covers `window`.
pub fn snap(window: TimeWindow, width: BucketWidth) -> TimeWindow {
    let start = align_down(window.start(), width);
    let end = align_up(window.end(), width);
    // Snapping outward never empties a non-empty window.
    TimeWindow::new(start, end).unwrap_or(window)
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU64;

    use super::*;

    const MINUTE: u64 = 60_000_000;

    fn width() -> BucketWidth {
        BucketWidth::from_micros(NonZeroU64::new(5 * MINUTE).expect("width"))
    }

    fn at(minutes: u64) -> Timestamp {
        Timestamp::from_micros(minutes * MINUTE)
    }

    #[test]
    fn alignment_rounds_to_bucket_edges() {
        assert_eq!(align_down(at(7), width()), at(5));
        assert_eq!(align_up(at(7), width()), at(10));
        assert_eq!(align_up(at(10), width()), at(10));
        assert_eq!(align_down(at(10), width()), at(10));
    }

    #[test]
    fn snapping_widens_to_buckets() {
        let window = TimeWindow::new(at(7), at(13)).expect("window");
        assert!(!is_aligned(window, width()));
        let snapped = snap(window, width());
        assert_eq!(snapped, TimeWindow::new(at(5), at(15)).expect("window"));
        assert!(is_aligned(snapped, width()));
        assert_eq!(snap(snapped, width()), snapped);
    }

    #[test]
    fn the_filter_pins_the_scope_version() {
        let scope = Scope {
            window: TimeWindow::new(at(0), at(5)).expect("window"),
            topic_version: TopicModelVersion(2),
            filter: ViewFilter {
                agents: vec![AgentId::from_ulid(1)],
                false_detections: FalseDetections::Exclude,
                ..ViewFilter::default()
            },
        };
        let filter = scope.topology_filter();
        assert_eq!(
            filter.topic_version,
            TopicVersionSelector::Pinned(TopicModelVersion(2))
        );
        assert_eq!(filter.agents, vec![AgentId::from_ulid(1)]);
        assert_eq!(filter.false_detections, FalseDetections::Exclude);
    }

    #[test]
    fn confirmed_only_reaches_the_spec_filter() {
        let mut scope = Scope {
            window: TimeWindow::new(at(0), at(5)).expect("window"),
            topic_version: TopicModelVersion(2),
            filter: ViewFilter::default(),
        };
        assert!(!scope.filter.confirmed_only());
        assert_eq!(
            scope.topology_filter().unconfirmed_channels,
            UnconfirmedChannels::Include
        );
        scope.filter = scope.filter.toggle_confirmed_only();
        assert!(scope.filter.confirmed_only());
        assert_eq!(
            scope.topology_filter().unconfirmed_channels,
            UnconfirmedChannels::Exclude
        );
        assert_eq!(scope.filter.toggle_confirmed_only(), ViewFilter::default());
    }
}
