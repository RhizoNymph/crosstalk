use crate::aggregates::retention::{
    DropError, InvalidRetention, Pin, PinChange, PinError, Retention, RetentionPolicy,
};
use crate::aggregates::topic::TopicModelVersion;
use crate::aggregates::topic_history::{
    CompletedFit, FitRecord, InvalidVersionInfo, TopicVersionHistory, TopicVersionInfo,
    TopicVersionStatus,
};
use crate::events::Subject;
use crate::events::insight::InsightEvent;
use crate::ids::OperatorId;
use crate::interfaces::l8_surface::{ActionKind, OperatorAction, Permission};
use crate::support::Timestamp;
use crate::tests::fixtures::at;

fn v(n: u32) -> TopicModelVersion {
    TopicModelVersion(n)
}

/// Started at `t`, fitted at `t + 1`, ready at `t + 2`.
fn fit(t: u64) -> CompletedFit {
    CompletedFit {
        started_at: at(t),
        fitted_at: at(t + 1),
        ready_at: at(t + 2),
        topics: 3,
    }
}

fn pin(when: u64) -> Pin {
    Pin {
        by: OperatorId::from_ulid(1),
        at: at(when),
    }
}

fn policy(keep_last: u32) -> RetentionPolicy {
    RetentionPolicy::new(keep_last).expect("at least the minimum")
}

fn superseded(
    fit: FitRecord,
    activated_at: Option<Timestamp>,
    by: u32,
    superseded_at: u64,
) -> TopicVersionStatus {
    TopicVersionStatus::Superseded {
        fit,
        activated_at,
        by: v(by),
        superseded_at: at(superseded_at),
    }
}

fn info(version: u32, status: TopicVersionStatus) -> TopicVersionInfo {
    TopicVersionInfo::new(v(version), status).expect("valid version info")
}

/// v0 active from 0, superseded by v1 at 15; v1 active from 15, superseded
/// by v3 at 35; v2 ready, overtaken by v3 at 35 without being active; v3
/// active from 35, superseded by v4 at 45; v4 active from 45; v5 ready.
fn statuses() -> Vec<(u32, TopicVersionStatus)> {
    vec![
        (0, superseded(FitRecord::Unfitted, Some(at(0)), 1, 15)),
        (
            1,
            superseded(FitRecord::Fitted(fit(10)), Some(at(15)), 3, 35),
        ),
        (2, superseded(FitRecord::Fitted(fit(20)), None, 3, 35)),
        (
            3,
            superseded(FitRecord::Fitted(fit(30)), Some(at(35)), 4, 45),
        ),
        (
            4,
            TopicVersionStatus::Active {
                fit: FitRecord::Fitted(fit(40)),
                activated_at: at(45),
            },
        ),
        (5, TopicVersionStatus::Ready { fit: fit(50) }),
    ]
}

fn history() -> TopicVersionHistory {
    TopicVersionHistory::new(
        statuses()
            .into_iter()
            .map(|(version, status)| info(version, status))
            .collect(),
    )
    .expect("consistent history")
}

fn history_with(version: u32, retention: Retention) -> TopicVersionHistory {
    TopicVersionHistory::new(
        statuses()
            .into_iter()
            .map(|(n, status)| {
                if n == version {
                    TopicVersionInfo::with_retention(v(n), status, retention)
                        .expect("valid retention")
                } else {
                    info(n, status)
                }
            })
            .collect(),
    )
    .expect("consistent history")
}

fn retention_of(history: &TopicVersionHistory, version: u32) -> Option<Retention> {
    history.get(v(version)).map(TopicVersionInfo::retention)
}

// ── RetentionPolicy ─────────────────────────────────────────────────────────

#[test]
fn policy_keeps_at_least_two() {
    for keep_last in [0, 1] {
        assert_eq!(
            RetentionPolicy::new(keep_last),
            Err(InvalidRetention::TooFew {
                min: 2,
                got: keep_last
            })
        );
    }
    assert_eq!(policy(2).keep_last().get(), 2);
    assert_eq!(policy(7).keep_last().get(), 7);
}

#[test]
fn policy_protects_active_newer_and_recent_activated() {
    let history = history();
    assert_eq!(policy(2).protected(&history), vec![v(3), v(4), v(5)]);
    // v2 was never active, so it does not count towards keep_last.
    assert_eq!(policy(3).protected(&history), vec![v(1), v(3), v(4), v(5)]);
    assert_eq!(policy(2).to_drop(&history), vec![v(0), v(1), v(2)]);
    assert_eq!(policy(9).to_drop(&history), vec![v(2)]);
}

#[test]
fn policy_protects_pinned_versions() {
    let history = history_with(1, Retention::Retained { pin: Some(pin(40)) });
    assert_eq!(policy(2).protected(&history), vec![v(1), v(3), v(4), v(5)]);
    assert_eq!(policy(2).to_drop(&history), vec![v(0), v(2)]);
}

#[test]
fn policy_never_redrops_dropped_versions() {
    let history = history_with(0, Retention::Dropped { at: at(46) });
    assert_eq!(policy(2).to_drop(&history), vec![v(1), v(2)]);
    // Raising keep_last does not bring a dropped version back.
    assert_eq!(policy(9).to_drop(&history), vec![v(2)]);
    assert_eq!(
        retention_of(&history, 0),
        Some(Retention::Dropped { at: at(46) })
    );
}

// ── TopicVersionInfo retention ──────────────────────────────────────────────

#[test]
fn version_info_starts_retained_and_unpinned() {
    let history = history();
    assert!(
        history
            .versions()
            .iter()
            .all(|info| info.retention() == Retention::UNPINNED)
    );
    assert!(Retention::UNPINNED.is_retained());
    assert_eq!(Retention::Dropped { at: at(1) }.pin(), None);
}

#[test]
fn fitting_version_cannot_be_pinned() {
    assert_eq!(
        TopicVersionInfo::with_retention(
            v(6),
            TopicVersionStatus::Fitting { started_at: at(60) },
            Retention::Retained { pin: Some(pin(61)) },
        ),
        Err(InvalidVersionInfo::PinnedWhileFitting)
    );
}

#[test]
fn only_superseded_versions_are_dropped() {
    for (version, status) in [
        (5, TopicVersionStatus::Ready { fit: fit(50) }),
        (
            4,
            TopicVersionStatus::Active {
                fit: FitRecord::Fitted(fit(40)),
                activated_at: at(45),
            },
        ),
        (6, TopicVersionStatus::Fitting { started_at: at(60) }),
    ] {
        assert_eq!(
            TopicVersionInfo::with_retention(v(version), status, Retention::Dropped { at: at(99) }),
            Err(InvalidVersionInfo::DroppedNotSuperseded)
        );
    }
    let (_, status) = statuses()[1];
    assert!(
        TopicVersionInfo::with_retention(v(1), status, Retention::Dropped { at: at(35) }).is_ok()
    );
}

#[test]
fn retention_times_follow_the_lifecycle() {
    let (_, status) = statuses()[1];
    assert_eq!(
        TopicVersionInfo::with_retention(v(1), status, Retention::Dropped { at: at(34) }),
        Err(InvalidVersionInfo::TimestampsOutOfOrder)
    );
    // Pinned before it became ready at 12.
    assert_eq!(
        TopicVersionInfo::with_retention(v(1), status, Retention::Retained { pin: Some(pin(11)) }),
        Err(InvalidVersionInfo::TimestampsOutOfOrder)
    );
    // Version 0 was never fitted, so any pin time is after it was ready.
    let (_, zero) = statuses()[0];
    assert!(
        TopicVersionInfo::with_retention(v(0), zero, Retention::Retained { pin: Some(pin(0)) })
            .is_ok()
    );
}

#[test]
fn with_retention_keeps_status_checks() {
    assert_eq!(
        TopicVersionInfo::with_retention(
            v(0),
            TopicVersionStatus::Ready { fit: fit(1) },
            Retention::UNPINNED
        ),
        Err(InvalidVersionInfo::VersionZeroFitted)
    );
}

// ── pin, unpin, mark_dropped ────────────────────────────────────────────────

#[test]
fn pin_pins_a_retained_version_once() {
    let mut history = history();
    assert_eq!(history.pin(v(1), pin(50)), Ok(PinChange::Changed));
    assert_eq!(history.pin(v(1), pin(60)), Ok(PinChange::Unchanged));
    assert_eq!(
        retention_of(&history, 1),
        Some(Retention::Retained { pin: Some(pin(50)) })
    );
    assert!(!policy(2).to_drop(&history).contains(&v(1)));
    // Pending and active versions can be pinned too.
    assert_eq!(history.pin(v(5), pin(60)), Ok(PinChange::Changed));
}

#[test]
fn pin_rejects_unknown_fitting_and_dropped_versions() {
    let mut fitting = TopicVersionHistory::new(vec![
        info(
            0,
            TopicVersionStatus::Active {
                fit: FitRecord::Unfitted,
                activated_at: at(0),
            },
        ),
        info(1, TopicVersionStatus::Fitting { started_at: at(10) }),
    ])
    .expect("consistent history");
    let before = fitting.clone();
    assert_eq!(fitting.pin(v(1), pin(11)), Err(PinError::Fitting(v(1))));
    assert_eq!(
        fitting.pin(v(9), pin(11)),
        Err(PinError::UnknownVersion(v(9)))
    );
    assert_eq!(fitting, before);

    let mut dropped = history_with(0, Retention::Dropped { at: at(46) });
    assert_eq!(
        dropped.pin(v(0), pin(50)),
        Err(PinError::Dropped {
            version: v(0),
            at: at(46)
        })
    );
    assert_eq!(
        retention_of(&dropped, 0),
        Some(Retention::Dropped { at: at(46) })
    );
}

#[test]
fn unpin_is_idempotent() {
    let mut history = history_with(1, Retention::Retained { pin: Some(pin(40)) });
    assert_eq!(history.unpin(v(1)), Ok(PinChange::Changed));
    assert_eq!(retention_of(&history, 1), Some(Retention::UNPINNED));
    assert_eq!(history.unpin(v(1)), Ok(PinChange::Unchanged));
    assert_eq!(history.unpin(v(9)), Err(PinError::UnknownVersion(v(9))));

    let mut dropped = history_with(0, Retention::Dropped { at: at(46) });
    assert_eq!(dropped.unpin(v(0)), Ok(PinChange::Unchanged));
}

#[test]
fn mark_dropped_takes_only_what_policy_drops() {
    let mut history = history();
    assert_eq!(
        history.mark_dropped(v(3), at(50), policy(2)),
        Err(DropError::Protected(v(3)))
    );
    assert_eq!(
        history.mark_dropped(v(4), at(50), policy(2)),
        Err(DropError::Protected(v(4)))
    );
    assert_eq!(
        history.mark_dropped(v(9), at(50), policy(2)),
        Err(DropError::UnknownVersion(v(9)))
    );
    assert_eq!(history.mark_dropped(v(1), at(50), policy(2)), Ok(()));
    assert_eq!(
        retention_of(&history, 1),
        Some(Retention::Dropped { at: at(50) })
    );
    assert_eq!(
        history.mark_dropped(v(1), at(51), policy(2)),
        Err(DropError::AlreadyDropped(v(1)))
    );
    assert_eq!(policy(2).to_drop(&history), vec![v(0), v(2)]);
}

#[test]
fn mark_dropped_is_not_before_supersession() {
    let mut history = history();
    assert_eq!(
        history.mark_dropped(v(1), at(34), policy(2)),
        Err(DropError::BeforeSuperseded(v(1)))
    );
    assert_eq!(retention_of(&history, 1), Some(Retention::UNPINNED));
}

#[test]
fn pinned_version_cannot_be_marked_dropped() {
    let mut history = history();
    assert_eq!(history.pin(v(0), pin(50)), Ok(PinChange::Changed));
    assert_eq!(
        history.mark_dropped(v(0), at(51), policy(2)),
        Err(DropError::Protected(v(0)))
    );
    assert_eq!(history.unpin(v(0)), Ok(PinChange::Changed));
    assert_eq!(history.mark_dropped(v(0), at(52), policy(2)), Ok(()));
}

// ── actions and events ──────────────────────────────────────────────────────

#[test]
fn pin_actions_need_govern_and_have_their_own_kind() {
    let pin = OperatorAction::PinTopicVersion { version: v(3) };
    let unpin = OperatorAction::UnpinTopicVersion { version: v(3) };
    assert_eq!(pin.required_permission(), Permission::Govern);
    assert_eq!(unpin.required_permission(), Permission::Govern);
    assert_eq!(pin.kind(), ActionKind::PinTopicVersion);
    assert_eq!(unpin.kind(), ActionKind::UnpinTopicVersion);
}

#[test]
fn topic_version_dropped_has_its_own_subject() {
    let event = InsightEvent::TopicVersionDropped { version: v(1) };
    assert_eq!(event.subject(), Subject::TopicVersionDropped);
}
