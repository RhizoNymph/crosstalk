//! Resolving a filter's topic-model version, and checking its topics.

use crate::aggregates::edge::TopologyFilter;
use crate::aggregates::filter::{TopicVersionSelector, VersionUnavailable};
use crate::aggregates::topic::TopicModelVersion;
use crate::aggregates::topic_history::{
    CompletedFit, FitRecord, TopicVersionHistory, TopicVersionInfo, TopicVersionStatus,
};
use crate::ids::TopicId;
use crate::tests::fixtures::at;

fn v(n: u32) -> TopicModelVersion {
    TopicModelVersion(n)
}

fn topic(n: u128) -> TopicId {
    TopicId::from_ulid(n)
}

fn fit(t: u64) -> CompletedFit {
    CompletedFit {
        started_at: at(t),
        fitted_at: at(t + 1),
        ready_at: at(t + 2),
        topics: 2,
    }
}

fn info(version: u32, status: TopicVersionStatus) -> TopicVersionInfo {
    TopicVersionInfo::new(v(version), status).expect("valid version info")
}

/// v0 and v1 were active and are superseded; v2 was ready but overtaken
/// before activation; v3 is active; v4 is ready; v5 is fitting.
fn history() -> TopicVersionHistory {
    TopicVersionHistory::new(vec![
        info(
            0,
            TopicVersionStatus::Superseded {
                fit: FitRecord::Unfitted,
                activated_at: Some(at(0)),
                by: v(1),
                superseded_at: at(10),
            },
        ),
        info(
            1,
            TopicVersionStatus::Superseded {
                fit: FitRecord::Fitted(fit(1)),
                activated_at: Some(at(10)),
                by: v(3),
                superseded_at: at(30),
            },
        ),
        info(
            2,
            TopicVersionStatus::Superseded {
                fit: FitRecord::Fitted(fit(11)),
                activated_at: None,
                by: v(3),
                superseded_at: at(30),
            },
        ),
        info(
            3,
            TopicVersionStatus::Active {
                fit: FitRecord::Fitted(fit(20)),
                activated_at: at(30),
            },
        ),
        info(4, TopicVersionStatus::Ready { fit: fit(40) }),
        info(5, TopicVersionStatus::Fitting { started_at: at(50) }),
    ])
    .expect("valid history")
}

/// The store keeps the active version and the one before it.
fn keeps_one_and_three(version: TopicModelVersion) -> bool {
    version == v(1) || version == v(3)
}

#[test]
fn current_resolves_to_the_active_version() {
    assert_eq!(
        TopicVersionSelector::Current.resolve(&history(), |_| false),
        Ok(v(3))
    );
}

#[test]
fn pinned_active_or_retained_superseded_version_resolves_to_itself() {
    for version in [v(3), v(1)] {
        assert_eq!(
            TopicVersionSelector::Pinned(version).resolve(&history(), keeps_one_and_three),
            Ok(version)
        );
    }
}

#[test]
fn pinned_version_no_longer_retained_is_not_retained() {
    assert_eq!(
        TopicVersionSelector::Pinned(v(0)).resolve(&history(), keeps_one_and_three),
        Err(VersionUnavailable::NotRetained(v(0)))
    );
}

#[test]
fn pinned_version_never_activated_is_refused_whatever_is_retained() {
    for version in [v(2), v(4)] {
        assert_eq!(
            TopicVersionSelector::Pinned(version).resolve(&history(), |_| true),
            Err(VersionUnavailable::NotActivated(version))
        );
    }
}

#[test]
fn pinned_fitting_version_is_fitting() {
    assert_eq!(
        TopicVersionSelector::Pinned(v(5)).resolve(&history(), |_| true),
        Err(VersionUnavailable::Fitting(v(5)))
    );
}

#[test]
fn pinned_unknown_version_is_unknown() {
    assert_eq!(
        TopicVersionSelector::Pinned(v(6)).resolve(&history(), |_| true),
        Err(VersionUnavailable::Unknown(v(6)))
    );
}

#[test]
fn topics_outside_lists_foreign_topics_once_in_order() {
    let filter = TopologyFilter {
        topics: vec![topic(30), topic(9), topic(31), topic(9)],
        ..TopologyFilter::default()
    };
    // Topics 30 and 31 belong to v3, 9 to v1.
    let version_of = |id: TopicId| match id.as_ulid() {
        30 | 31 => Some(v(3)),
        9 => Some(v(1)),
        _ => None,
    };
    assert_eq!(filter.topics_outside(v(3), version_of), vec![topic(9)]);
    assert_eq!(
        filter.topics_outside(v(1), version_of),
        vec![topic(30), topic(31)]
    );
}

#[test]
fn unknown_topics_are_outside_every_version() {
    let filter = TopologyFilter {
        topics: vec![topic(99)],
        ..TopologyFilter::default()
    };
    assert_eq!(filter.topics_outside(v(3), |_| None), vec![topic(99)]);
}

#[test]
fn no_topics_are_never_outside() {
    assert!(
        TopologyFilter::default()
            .topics_outside(v(3), |_| None)
            .is_empty()
    );
}

#[test]
fn pinned_keeps_every_other_field() {
    let filter = TopologyFilter {
        topics: vec![topic(30)],
        ..TopologyFilter::default()
    };
    let pinned = filter.clone().pinned(v(3));
    assert_eq!(pinned.topic_version, TopicVersionSelector::Pinned(v(3)));
    assert_eq!(
        TopologyFilter {
            topic_version: TopicVersionSelector::Current,
            ..pinned
        },
        filter
    );
}
