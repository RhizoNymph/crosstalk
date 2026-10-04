//! Operator verdicts: judgeable states, the append-only log, readers'
//! copies, and the `SetVerdict` action.

use std::num::NonZeroU32;
use std::time::Duration;

use crate::aggregates::filter::{FalseDetections, FilterSubject, TopologyFilter};
use crate::aggregates::topic::TopicModelVersion;
use crate::derived::flow::evidence::CoAccess;
use crate::derived::flow::transmission::{
    Classification, Confirmed, DiscardReason, Route, Transmission, TransmissionState,
};
use crate::derived::flow::verdict::{
    CurrentVerdict, InvalidVerdictRecord, Judgeable, NotJudgeable, Observed, TransmissionVerdict,
    Verdict, VerdictLog, VerdictRecorded, VerdictRevision,
};
use crate::ids::OperatorId;
use crate::interfaces::l8_surface::audit::{AuditOutcome, InvalidOperatorRecord, OperatorRecord};
use crate::interfaces::l8_surface::{
    ActionError, ActionKind, ActionOutcome, ConflictKind, OperatorAction, Permission,
};
use crate::support::NonEmpty;
use crate::tests::fixtures::{
    agent, at, channel, content_match, read_access, resource, transmission, write_access,
};
use crate::tests::operators::caller;

pub fn co_access() -> CoAccess {
    CoAccess::new(
        &write_access(1, agent(1), resource(1), 1),
        &read_access(2, agent(2), resource(1), 2),
        Duration::from_secs(60),
    )
    .expect("valid co-access")
}

pub fn confirmed() -> Confirmed {
    Confirmed::new(
        NonEmpty::new(content_match(agent(1), agent(2), 8)),
        vec![co_access()],
        at(4),
    )
    .expect("one sender, one reader")
}

fn classification() -> Classification {
    Classification {
        version: TopicModelVersion(1),
        topic: None,
        watched: false,
    }
}

/// One state of every variant, with whether it takes a verdict.
pub fn every_state() -> Vec<(TransmissionState, bool)> {
    vec![
        (TransmissionState::Detected, false),
        (
            TransmissionState::AwaitingContent {
                co_access: co_access(),
                window_closes_at: at(5),
            },
            false,
        ),
        (
            TransmissionState::Suspected {
                co_access: NonEmpty::new(co_access()),
                since: at(5),
            },
            true,
        ),
        (
            TransmissionState::Discarded {
                co_access: NonEmpty::new(co_access()),
                reason: DiscardReason::Expired { at: at(9) },
            },
            true,
        ),
        (TransmissionState::Confirmed(confirmed()), true),
        (
            TransmissionState::Classified {
                confirmed: confirmed(),
                classification: classification(),
            },
            true,
        ),
        (
            TransmissionState::Aggregated {
                confirmed: confirmed(),
                classification: classification(),
            },
            true,
        ),
    ]
}

pub fn transmission_in(n: u128, state: TransmissionState) -> Transmission {
    Transmission {
        id: transmission(n),
        to: agent(2),
        route: Route::Channel(channel(1)),
        opened_at: at(1),
        state,
    }
}

const OPERATOR: u128 = 7;

fn operator() -> OperatorId {
    OperatorId::from_ulid(OPERATOR)
}

fn suspected() -> Transmission {
    transmission_in(
        1,
        TransmissionState::Suspected {
            co_access: NonEmpty::new(co_access()),
            since: at(5),
        },
    )
}

fn record(of: &Transmission, verdict: Option<Verdict>, when: u64) -> TransmissionVerdict {
    TransmissionVerdict::new(of, verdict, operator(), at(when), None).expect("judgeable")
}

fn revision(n: u32) -> VerdictRevision {
    VerdictRevision::new(NonZeroU32::new(n).expect("revisions start at 1"))
}

#[test]
fn judgeable_states_are_suspected_discarded_and_confirmed() {
    for (state, judgeable) in every_state() {
        assert_eq!(state.judgeable().is_ok(), judgeable, "{state:?}");
    }
}

#[test]
fn judgeable_carries_the_detector_evidence() {
    let co = NonEmpty::new(co_access());
    let suspected = TransmissionState::Suspected {
        co_access: co.clone(),
        since: at(5),
    };
    assert_eq!(suspected.judgeable(), Ok(Judgeable::Suspected(&co)));
    let discarded = TransmissionState::Discarded {
        co_access: co.clone(),
        reason: DiscardReason::Expired { at: at(9) },
    };
    assert_eq!(discarded.judgeable(), Ok(Judgeable::Discarded(&co)));
    let aggregated = TransmissionState::Aggregated {
        confirmed: confirmed(),
        classification: classification(),
    };
    let expected = confirmed();
    assert_eq!(aggregated.judgeable(), Ok(Judgeable::Confirmed(&expected)));
}

/// Every forward transition out of a judgeable state lands in a judgeable
/// state, so a verdict once accepted stays valid.
#[test]
fn judgeability_survives_every_forward_transition() {
    let mut expired = TransmissionState::Suspected {
        co_access: NonEmpty::new(co_access()),
        since: at(5),
    };
    expired.expire(at(9)).expect("suspected");
    let successors = [
        // Suspected: late match confirms, or window expires.
        TransmissionState::Confirmed(confirmed()),
        expired,
        // Confirmed: classify, then aggregate.
        TransmissionState::Classified {
            confirmed: confirmed(),
            classification: classification(),
        },
        TransmissionState::Aggregated {
            confirmed: confirmed(),
            classification: classification(),
        },
    ];
    for state in successors {
        assert!(state.judgeable().is_ok(), "{state:?}");
    }
}

#[test]
fn verdict_record_rejects_states_still_collecting_evidence() {
    for (state, judgeable) in every_state() {
        let of = transmission_in(1, state.clone());
        for verdict in [Some(Verdict::Genuine), Some(Verdict::FalseDetection), None] {
            let built =
                TransmissionVerdict::new(&of, verdict, operator(), at(10), Some("note".into()));
            if judgeable {
                let built = built.expect("judgeable state");
                assert_eq!(built.transmission(), of.id);
                assert_eq!(built.verdict(), verdict);
                assert_eq!(built.by(), operator());
                assert_eq!(built.at(), at(10));
                assert_eq!(built.note(), Some("note"));
            } else {
                assert_eq!(built, Err(NotJudgeable), "{state:?}");
            }
        }
    }
}

#[test]
fn empty_log_has_no_verdict_and_no_revision() {
    let log = VerdictLog::new(transmission(1));
    assert_eq!(log.transmission(), transmission(1));
    assert_eq!(log.current(), None);
    assert_eq!(log.revision(), None);
    assert!(log.records().is_empty());
}

#[test]
fn log_appends_with_consecutive_revisions_and_latest_wins() {
    let of = suspected();
    let mut log = VerdictLog::new(of.id);
    let steps = [
        (Some(Verdict::FalseDetection), revision(1)),
        (Some(Verdict::Genuine), revision(2)),
        (None, revision(3)),
        (Some(Verdict::FalseDetection), revision(4)),
    ];
    for (n, (verdict, expected)) in (1u64..).zip(steps) {
        assert_eq!(
            log.record(record(&of, verdict, n)),
            Ok(VerdictRecorded::Appended(expected))
        );
        assert_eq!(log.current(), verdict);
        assert_eq!(log.revision(), Some(expected));
    }
    assert_eq!(log.records().len(), 4);
    let verdicts: Vec<_> = log.records().iter().map(|r| r.verdict()).collect();
    assert_eq!(
        verdicts,
        vec![
            Some(Verdict::FalseDetection),
            Some(Verdict::Genuine),
            None,
            Some(Verdict::FalseDetection)
        ]
    );
}

#[test]
fn repeating_the_current_verdict_appends_nothing() {
    let of = suspected();
    let mut log = VerdictLog::new(of.id);
    assert_eq!(
        log.record(record(&of, None, 1)),
        Ok(VerdictRecorded::Unchanged),
        "withdrawing with no verdict in force"
    );
    log.record(record(&of, Some(Verdict::Genuine), 2))
        .expect("same transmission");
    let before = log.clone();
    assert_eq!(
        log.record(record(&of, Some(Verdict::Genuine), 3)),
        Ok(VerdictRecorded::Unchanged)
    );
    assert_eq!(log, before);
    log.record(record(&of, None, 4)).expect("same transmission");
    assert_eq!(
        log.record(record(&of, None, 5)),
        Ok(VerdictRecorded::Unchanged)
    );
    assert_eq!(log.revision(), Some(revision(2)));
}

#[test]
fn log_rejects_another_transmissions_record() {
    let mut log = VerdictLog::new(transmission(2));
    let before = log.clone();
    assert_eq!(
        log.record(record(&suspected(), Some(Verdict::Genuine), 1)),
        Err(InvalidVerdictRecord::OtherTransmission)
    );
    assert_eq!(log, before);
}

#[test]
fn revision_counts_from_one() {
    assert_eq!(VerdictRevision::FIRST, revision(1));
    assert_eq!(VerdictRevision::FIRST.next(), Some(revision(2)));
    assert_eq!(revision(u32::MAX).next(), None);
    assert_eq!(revision(3).get().get(), 3);
}

/// Every order of a log's events, each delivered twice, ends at the latest.
#[test]
fn current_verdict_copy_is_order_insensitive() {
    let events = [
        (Some(Verdict::Genuine), revision(1)),
        (Some(Verdict::FalseDetection), revision(2)),
        (None, revision(3)),
    ];
    let orders = [
        [0, 1, 2],
        [0, 2, 1],
        [1, 0, 2],
        [1, 2, 0],
        [2, 0, 1],
        [2, 1, 0],
    ];
    for order in orders {
        let (first_verdict, first_revision) = events[order[0]];
        let mut copy = CurrentVerdict {
            verdict: first_verdict,
            revision: first_revision,
        };
        for &index in order.iter().chain(order.iter()) {
            let (verdict, revision) = events[index];
            copy.observe(verdict, revision);
        }
        assert_eq!(
            copy,
            CurrentVerdict {
                verdict: None,
                revision: revision(3)
            },
            "{order:?}"
        );
    }
}

#[test]
fn stale_events_leave_the_copy_unchanged() {
    let mut copy = CurrentVerdict {
        verdict: Some(Verdict::FalseDetection),
        revision: revision(2),
    };
    assert_eq!(
        copy.observe(Some(Verdict::Genuine), revision(1)),
        Observed::Stale
    );
    assert_eq!(copy.observe(None, revision(2)), Observed::Stale);
    assert_eq!(copy.verdict, Some(Verdict::FalseDetection));
    assert_eq!(
        copy.observe(Some(Verdict::Genuine), revision(3)),
        Observed::Newer
    );
    assert_eq!(copy.verdict, Some(Verdict::Genuine));
}

#[test]
fn only_a_current_false_detection_is_a_false_detection() {
    let copy = |verdict| CurrentVerdict {
        verdict,
        revision: revision(1),
    };
    assert!(!CurrentVerdict::is_false_detection(None));
    assert!(!CurrentVerdict::is_false_detection(Some(&copy(None))));
    assert!(!CurrentVerdict::is_false_detection(Some(&copy(Some(
        Verdict::Genuine
    )))));
    assert!(CurrentVerdict::is_false_detection(Some(&copy(Some(
        Verdict::FalseDetection
    )))));
}

/// The filter reads only the verdict copy: excluding false detections drops
/// exactly the transmissions whose copy holds `FalseDetection`.
#[test]
fn exclude_filter_follows_the_verdict_copy() {
    let route = Route::Channel(channel(1));
    let exclude = TopologyFilter {
        false_detections: FalseDetections::Exclude,
        ..TopologyFilter::default()
    };
    let copies = [
        (None, true),
        (Some(None), true),
        (Some(Some(Verdict::Genuine)), true),
        (Some(Some(Verdict::FalseDetection)), false),
    ];
    for (verdict, admitted) in copies {
        let copy = verdict.map(|verdict| CurrentVerdict {
            verdict,
            revision: revision(1),
        });
        let subject = FilterSubject {
            from: agent(1),
            to: agent(2),
            route: &route,
            topic: None,
            false_detection: CurrentVerdict::is_false_detection(copy.as_ref()),
        };
        assert_eq!(exclude.admits(&subject, |id| id), admitted, "{verdict:?}");
        assert!(TopologyFilter::default().admits(&subject, |id| id));
    }
}

fn set_verdicts() -> [OperatorAction; 3] {
    [Some(Verdict::Genuine), Some(Verdict::FalseDetection), None].map(|verdict| {
        OperatorAction::SetVerdict {
            transmission: transmission(1),
            verdict,
            note: Some("checked the wiki page".into()),
        }
    })
}

#[test]
fn set_verdict_needs_triage_alone() {
    for action in set_verdicts() {
        assert_eq!(action.kind(), ActionKind::SetVerdict);
        assert_eq!(action.required_permission(), Permission::Triage);
    }
}

#[test]
fn set_verdict_audit_records_follow_triage() {
    let triager = caller(OPERATOR, &[Permission::Triage]);
    let reader = caller(OPERATOR, &[Permission::View, Permission::Content]);
    for action in set_verdicts() {
        for outcome in [ActionOutcome::Applied, ActionOutcome::Unchanged] {
            OperatorRecord::new(
                triager.clone(),
                action.clone(),
                AuditOutcome::Succeeded(outcome),
            )
            .expect("Triage suffices");
            assert_eq!(
                OperatorRecord::new(
                    reader.clone(),
                    action.clone(),
                    AuditOutcome::Succeeded(outcome),
                ),
                Err(InvalidOperatorRecord::AttemptedWithoutPermission {
                    required: Permission::Triage
                })
            );
        }
        OperatorRecord::new(
            reader.clone(),
            action,
            AuditOutcome::Forbidden {
                missing: Permission::Triage,
            },
        )
        .expect("Content without Triage is forbidden");
    }
}

#[test]
fn not_judgeable_round_trips_through_the_audit_log() {
    let result = Err(ActionError::Conflict(
        ConflictKind::TransmissionNotJudgeable {
            transmission: transmission(1),
        },
    ));
    assert_eq!(AuditOutcome::of(&result).result(), result);
}
