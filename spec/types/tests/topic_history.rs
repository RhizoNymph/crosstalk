use std::num::NonZeroU64;

use crate::aggregates::alert::{TopicWatch, WatchedTopics};
use crate::aggregates::edge::EdgeStats;
use crate::aggregates::topic::TopicModelVersion;
use crate::aggregates::topic_history::{
    CompletedFit, DuplicateTopic, FitRecord, InvalidHistory, InvalidLineage, InvalidLineageEntry,
    InvalidVersionInfo, LineageEntry, LineageLink, RemapError, TopicLineage, TopicSize, TopicSizes,
    TopicVersionHistory, TopicVersionInfo, TopicVersionStatus, TopicVersionStatusKind,
};
use crate::events::Subject;
use crate::events::insight::InsightEvent;
use crate::ids::TopicId;
use crate::support::{NonEmpty, Similarity, TimeWindow};
use crate::tests::fixtures::at;

fn v(n: u32) -> TopicModelVersion {
    TopicModelVersion(n)
}

fn topic(n: u128) -> TopicId {
    TopicId::from_ulid(n)
}

fn sim(value: f32) -> Similarity {
    Similarity::new(value).expect("fixture similarity in range")
}

fn link(topic_n: u128, similarity: f32) -> LineageLink {
    LineageLink {
        topic: topic(topic_n),
        similarity: sim(similarity),
    }
}

/// Started at `t`, fitted at `t + 1`, ready at `t + 2`, with 3 topics.
fn fit(t: u64) -> CompletedFit {
    CompletedFit {
        started_at: at(t),
        fitted_at: at(t + 1),
        ready_at: at(t + 2),
        topics: 3,
    }
}

fn info(version: u32, status: TopicVersionStatus) -> TopicVersionInfo {
    TopicVersionInfo::new(v(version), status).expect("valid version info")
}

fn zero_active() -> TopicVersionStatus {
    TopicVersionStatus::Active {
        fit: FitRecord::Unfitted,
        activated_at: at(0),
    }
}

// ── TopicVersionInfo ────────────────────────────────────────────────────────

#[test]
fn version_zero_is_unfitted_and_activated() {
    let zero = info(0, zero_active());
    assert_eq!(zero.fitted_at(), None);
    assert_eq!(zero.status().kind(), TopicVersionStatusKind::Active);
    assert!(
        TopicVersionInfo::new(
            v(0),
            TopicVersionStatus::Superseded {
                fit: FitRecord::Unfitted,
                activated_at: Some(at(0)),
                by: v(1),
                superseded_at: at(20),
            }
        )
        .is_ok()
    );
}

#[test]
fn version_zero_cannot_be_fitted() {
    for status in [
        TopicVersionStatus::Fitting { started_at: at(1) },
        TopicVersionStatus::Ready { fit: fit(1) },
        TopicVersionStatus::Active {
            fit: FitRecord::Fitted(fit(1)),
            activated_at: at(5),
        },
    ] {
        assert_eq!(
            TopicVersionInfo::new(v(0), status),
            Err(InvalidVersionInfo::VersionZeroFitted)
        );
    }
}

#[test]
fn only_version_zero_is_unfitted() {
    assert_eq!(
        TopicVersionInfo::new(
            v(1),
            TopicVersionStatus::Active {
                fit: FitRecord::Unfitted,
                activated_at: at(5),
            }
        ),
        Err(InvalidVersionInfo::UnfittedNonZero)
    );
}

#[test]
fn unfitted_version_was_always_active() {
    assert_eq!(
        TopicVersionInfo::new(
            v(0),
            TopicVersionStatus::Superseded {
                fit: FitRecord::Unfitted,
                activated_at: None,
                by: v(1),
                superseded_at: at(20),
            }
        ),
        Err(InvalidVersionInfo::UnfittedNeverActivated)
    );
}

#[test]
fn superseded_only_by_newer_version() {
    for by in [v(1), v(2)] {
        assert_eq!(
            TopicVersionInfo::new(
                v(2),
                TopicVersionStatus::Superseded {
                    fit: FitRecord::Fitted(fit(10)),
                    activated_at: None,
                    by,
                    superseded_at: at(20),
                }
            ),
            Err(InvalidVersionInfo::SupersededByOlder)
        );
    }
}

#[test]
fn version_timestamps_never_go_backwards() {
    let backwards_fit = CompletedFit {
        started_at: at(10),
        fitted_at: at(9),
        ready_at: at(11),
        topics: 1,
    };
    assert_eq!(
        TopicVersionInfo::new(v(1), TopicVersionStatus::Ready { fit: backwards_fit }),
        Err(InvalidVersionInfo::TimestampsOutOfOrder)
    );
    assert_eq!(
        TopicVersionInfo::new(
            v(1),
            TopicVersionStatus::Active {
                fit: FitRecord::Fitted(fit(10)),
                activated_at: at(11),
            }
        ),
        Err(InvalidVersionInfo::TimestampsOutOfOrder)
    );
    assert_eq!(
        TopicVersionInfo::new(
            v(1),
            TopicVersionStatus::Superseded {
                fit: FitRecord::Fitted(fit(10)),
                activated_at: Some(at(15)),
                by: v(2),
                superseded_at: at(14),
            }
        ),
        Err(InvalidVersionInfo::TimestampsOutOfOrder)
    );
}

#[test]
fn fitted_at_reports_the_fit() {
    assert_eq!(
        info(1, TopicVersionStatus::Ready { fit: fit(10) }).fitted_at(),
        Some(at(11))
    );
    assert_eq!(
        info(1, TopicVersionStatus::Fitting { started_at: at(10) }).fitted_at(),
        None
    );
}

// ── TopicVersionHistory ─────────────────────────────────────────────────────

/// v0 superseded by v2 at 30; v1 overtaken by v2 before activation; v2
/// active since 30; v3 ready; v4 fitting.
fn full_history() -> Vec<TopicVersionInfo> {
    vec![
        info(
            0,
            TopicVersionStatus::Superseded {
                fit: FitRecord::Unfitted,
                activated_at: Some(at(0)),
                by: v(2),
                superseded_at: at(30),
            },
        ),
        info(
            1,
            TopicVersionStatus::Superseded {
                fit: FitRecord::Fitted(fit(10)),
                activated_at: None,
                by: v(2),
                superseded_at: at(30),
            },
        ),
        info(
            2,
            TopicVersionStatus::Active {
                fit: FitRecord::Fitted(fit(20)),
                activated_at: at(30),
            },
        ),
        info(3, TopicVersionStatus::Ready { fit: fit(40) }),
        info(4, TopicVersionStatus::Fitting { started_at: at(50) }),
    ]
}

#[test]
fn history_accepts_a_consistent_lifecycle() {
    let history = TopicVersionHistory::new(full_history()).expect("consistent");
    assert_eq!(history.active().version(), v(2));
    assert_eq!(history.versions().len(), 5);
    assert_eq!(
        history.get(v(3)).map(|i| i.status().kind()),
        Some(TopicVersionStatusKind::Ready)
    );
    assert_eq!(history.get(v(9)), None);
    let fresh = TopicVersionHistory::new(vec![info(0, zero_active())]).expect("fresh");
    assert_eq!(fresh.active().version(), v(0));
}

#[test]
fn history_starts_at_version_zero() {
    assert_eq!(
        TopicVersionHistory::new(Vec::new()),
        Err(InvalidHistory::MissingVersionZero)
    );
    let without_zero = full_history()[1..].to_vec();
    assert_eq!(
        TopicVersionHistory::new(without_zero),
        Err(InvalidHistory::MissingVersionZero)
    );
}

#[test]
fn history_versions_strictly_increase() {
    let mut versions = full_history();
    versions.swap(3, 4);
    assert_eq!(
        TopicVersionHistory::new(versions),
        Err(InvalidHistory::NotAscending { version: v(3) })
    );
    let mut repeated = full_history();
    repeated.insert(3, repeated[2]);
    assert_eq!(
        TopicVersionHistory::new(repeated),
        Err(InvalidHistory::NotAscending { version: v(2) })
    );
}

#[test]
fn history_has_exactly_one_active() {
    let none_active = vec![
        info(
            0,
            TopicVersionStatus::Superseded {
                fit: FitRecord::Unfitted,
                activated_at: Some(at(0)),
                by: v(1),
                superseded_at: at(30),
            },
        ),
        info(1, TopicVersionStatus::Ready { fit: fit(10) }),
    ];
    assert_eq!(
        TopicVersionHistory::new(none_active),
        Err(InvalidHistory::NoActive)
    );
    let two_active = vec![
        info(0, zero_active()),
        info(
            1,
            TopicVersionStatus::Active {
                fit: FitRecord::Fitted(fit(10)),
                activated_at: at(20),
            },
        ),
    ];
    assert_eq!(
        TopicVersionHistory::new(two_active),
        Err(InvalidHistory::SeveralActive)
    );
}

#[test]
fn history_orders_statuses_around_the_active_version() {
    // Ready before the active version.
    let ready_before = vec![
        info(
            0,
            TopicVersionStatus::Superseded {
                fit: FitRecord::Unfitted,
                activated_at: Some(at(0)),
                by: v(2),
                superseded_at: at(30),
            },
        ),
        info(1, TopicVersionStatus::Ready { fit: fit(10) }),
        info(
            2,
            TopicVersionStatus::Active {
                fit: FitRecord::Fitted(fit(20)),
                activated_at: at(30),
            },
        ),
    ];
    assert_eq!(
        TopicVersionHistory::new(ready_before),
        Err(InvalidHistory::StatusOutOfPlace { version: v(1) })
    );
    // Superseded after the active version.
    let superseded_after = vec![
        info(0, zero_active()),
        info(
            1,
            TopicVersionStatus::Superseded {
                fit: FitRecord::Fitted(fit(10)),
                activated_at: None,
                by: v(2),
                superseded_at: at(30),
            },
        ),
    ];
    assert_eq!(
        TopicVersionHistory::new(superseded_after),
        Err(InvalidHistory::StatusOutOfPlace { version: v(1) })
    );
    // Fitting, but not the newest.
    let fitting_middle = vec![
        info(0, zero_active()),
        info(1, TopicVersionStatus::Fitting { started_at: at(10) }),
        info(2, TopicVersionStatus::Ready { fit: fit(20) }),
    ];
    assert_eq!(
        TopicVersionHistory::new(fitting_middle),
        Err(InvalidHistory::StatusOutOfPlace { version: v(1) })
    );
}

#[test]
fn superseded_names_the_first_later_activation() {
    // v1 was never activated, so v0 was superseded by v2, not v1.
    let mut wrong_by = full_history();
    wrong_by[0] = info(
        0,
        TopicVersionStatus::Superseded {
            fit: FitRecord::Unfitted,
            activated_at: Some(at(0)),
            by: v(1),
            superseded_at: at(30),
        },
    );
    assert_eq!(
        TopicVersionHistory::new(wrong_by),
        Err(InvalidHistory::WrongSupersessor { version: v(0) })
    );
    // Superseded at a time other than v2's activation.
    let mut wrong_time = full_history();
    wrong_time[1] = info(
        1,
        TopicVersionStatus::Superseded {
            fit: FitRecord::Fitted(fit(10)),
            activated_at: None,
            by: v(2),
            superseded_at: at(29),
        },
    );
    assert_eq!(
        TopicVersionHistory::new(wrong_time),
        Err(InvalidHistory::WrongSupersessor { version: v(1) })
    );
}

#[test]
fn topic_version_activated_has_its_own_subject() {
    let event = InsightEvent::TopicVersionActivated {
        version: v(2),
        previous: v(1),
    };
    assert_eq!(event.subject(), Subject::TopicVersionActivated);
}

// ── TopicSizes ──────────────────────────────────────────────────────────────

#[test]
fn sizes_list_each_topic_once() {
    let stats = EdgeStats {
        transmissions: NonZeroU64::MIN,
        matched_bytes: NonZeroU64::MIN,
    };
    let window = TimeWindow::new(at(0), at(10)).ok();
    let sizes = TopicSizes::new(
        v(1),
        window,
        vec![
            TopicSize {
                topic: topic(1),
                stats: Some(stats),
            },
            TopicSize {
                topic: topic(2),
                stats: None,
            },
        ],
        Some(stats),
    )
    .expect("distinct topics");
    assert_eq!(sizes.topics().len(), 2);
    assert_eq!((sizes.version(), sizes.window()), (v(1), window));
    assert_eq!(sizes.outliers(), Some(stats));
    assert_eq!(
        TopicSizes::new(
            v(1),
            None,
            vec![
                TopicSize {
                    topic: topic(1),
                    stats: None,
                },
                TopicSize {
                    topic: topic(1),
                    stats: Some(stats),
                },
            ],
            None,
        ),
        Err(DuplicateTopic(topic(1)))
    );
}

// ── LineageEntry and TopicLineage ───────────────────────────────────────────

#[test]
fn lineage_entry_accepts_lineage_order() {
    let entry = LineageEntry::new(
        topic(12),
        Some(link(31, 0.9)),
        vec![link(32, 0.7), link(30, 0.5)],
    )
    .expect("descending similarity");
    assert_eq!(entry.topic(), topic(12));
    assert_eq!(entry.best(), Some(link(31, 0.9)));
    assert_eq!(entry.others().len(), 2);
    // Ties go to the lower topic id.
    assert!(LineageEntry::new(topic(12), Some(link(30, 0.8)), vec![link(31, 0.8)]).is_ok());
    // No successor topics at all.
    assert!(LineageEntry::new(topic(12), None, Vec::new()).is_ok());
}

#[test]
fn lineage_entry_rejects_others_without_best() {
    assert_eq!(
        LineageEntry::new(topic(12), None, vec![link(31, 0.5)]),
        Err(InvalidLineageEntry::OthersWithoutBest)
    );
}

#[test]
fn lineage_entry_rejects_link_more_similar_than_best() {
    assert_eq!(
        LineageEntry::new(topic(12), Some(link(31, 0.6)), vec![link(32, 0.7)]),
        Err(InvalidLineageEntry::OutOfOrder)
    );
    // A tie must go to the lower id.
    assert_eq!(
        LineageEntry::new(topic(12), Some(link(31, 0.8)), vec![link(30, 0.8)]),
        Err(InvalidLineageEntry::OutOfOrder)
    );
}

#[test]
fn lineage_entry_rejects_unsorted_others() {
    assert_eq!(
        LineageEntry::new(
            topic(12),
            Some(link(31, 0.9)),
            vec![link(32, 0.5), link(33, 0.7)]
        ),
        Err(InvalidLineageEntry::OutOfOrder)
    );
}

#[test]
fn lineage_entry_rejects_duplicate_successor() {
    assert_eq!(
        LineageEntry::new(topic(12), Some(link(31, 0.9)), vec![link(31, 0.5)]),
        Err(InvalidLineageEntry::DuplicateSuccessor(topic(31)))
    );
}

fn entry(from: u128, best: Option<LineageLink>, others: Vec<LineageLink>) -> LineageEntry {
    LineageEntry::new(topic(from), best, others).expect("valid entry")
}

#[test]
fn lineage_goes_forward() {
    for (from, to) in [(2, 2), (3, 2)] {
        assert_eq!(
            TopicLineage::new(v(from), v(to), sim(0.5), Vec::new()),
            Err(InvalidLineage::NotForward)
        );
    }
}

#[test]
fn lineage_has_one_entry_per_topic() {
    assert_eq!(
        TopicLineage::new(
            v(1),
            v(2),
            sim(0.5),
            vec![
                entry(12, Some(link(31, 0.9)), Vec::new()),
                entry(12, Some(link(32, 0.8)), Vec::new()),
            ]
        ),
        Err(InvalidLineage::DuplicateEntry(topic(12)))
    );
}

#[test]
fn lineage_floor_bounds_others_not_best() {
    assert_eq!(
        TopicLineage::new(
            v(1),
            v(2),
            sim(0.5),
            vec![entry(12, Some(link(31, 0.9)), vec![link(32, 0.4)])]
        ),
        Err(InvalidLineage::BelowFloor { topic: topic(32) })
    );
    let lineage = TopicLineage::new(
        v(1),
        v(2),
        sim(0.5),
        vec![entry(12, Some(link(31, 0.3)), Vec::new())],
    )
    .expect("best is kept below the floor");
    assert_eq!(
        lineage.entry(topic(12)).and_then(LineageEntry::best),
        Some(link(31, 0.3))
    );
    assert_eq!(
        (lineage.from(), lineage.to(), lineage.floor()),
        (v(1), v(2), sim(0.5))
    );
}

// ── TopicLineage::remap ─────────────────────────────────────────────────────

/// 12 → 31 (0.9), 13 → 31 (0.8), 14 → 33 (0.6), 15 → 34 (0.2).
fn lineage() -> TopicLineage {
    TopicLineage::new(
        v(1),
        v(2),
        sim(0.1),
        vec![
            entry(12, Some(link(31, 0.9)), vec![link(32, 0.4)]),
            entry(13, Some(link(31, 0.8)), Vec::new()),
            entry(14, Some(link(33, 0.6)), Vec::new()),
            entry(15, Some(link(34, 0.2)), Vec::new()),
        ],
    )
    .expect("valid lineage")
}

fn topics(ids: &[u128]) -> NonEmpty<TopicId> {
    NonEmpty::from_vec(ids.iter().copied().map(topic).collect()).expect("non-empty")
}

fn watched(version: u32, ids: &[u128]) -> WatchedTopics {
    WatchedTopics {
        version: v(version),
        topics: topics(ids),
    }
}

fn current(version: u32, ids: &[u128]) -> TopicWatch {
    TopicWatch::Current(watched(version, ids))
}

#[test]
fn remap_follows_best_links_in_rule_order_without_duplicates() {
    assert_eq!(
        lineage().remap(&watched(1, &[14, 12, 13]), sim(0.5)),
        Ok(current(2, &[33, 31]))
    );
}

#[test]
fn remap_threshold_is_inclusive() {
    assert_eq!(
        lineage().remap(&watched(1, &[14]), sim(0.6)),
        Ok(current(2, &[33]))
    );
}

#[test]
fn remap_is_stale_when_any_best_link_is_below_threshold() {
    assert_eq!(
        lineage().remap(&watched(1, &[12, 15, 14]), sim(0.7)),
        Ok(TopicWatch::Stale {
            last: watched(1, &[12, 15, 14]),
            unmapped_in: v(2),
            unmapped: topics(&[15, 14]),
        })
    );
}

#[test]
fn remap_ignores_the_lineage_floor() {
    // 15's best link (0.2) is above a 0.15 threshold, whatever the floor.
    let lineage = TopicLineage::new(
        v(1),
        v(2),
        sim(0.5),
        vec![entry(15, Some(link(34, 0.2)), Vec::new())],
    )
    .expect("valid lineage");
    assert_eq!(
        lineage.remap(&watched(1, &[15]), sim(0.15)),
        Ok(current(2, &[34]))
    );
}

#[test]
fn remap_is_stale_when_successor_has_no_topics() {
    let lineage = TopicLineage::new(v(1), v(2), sim(0.5), vec![entry(12, None, Vec::new())])
        .expect("valid lineage");
    assert_eq!(
        lineage.remap(&watched(1, &[12]), sim(0.0)),
        Ok(TopicWatch::Stale {
            last: watched(1, &[12]),
            unmapped_in: v(2),
            unmapped: topics(&[12]),
        })
    );
}

#[test]
fn remap_rejects_other_versions_and_unknown_topics() {
    assert_eq!(
        lineage().remap(&watched(0, &[12]), sim(0.5)),
        Err(RemapError::WrongVersion {
            rule: v(0),
            lineage: v(1)
        })
    );
    assert_eq!(
        lineage().remap(&watched(1, &[12, 99]), sim(0.5)),
        Err(RemapError::UnknownTopic(topic(99)))
    );
}
