use crate::derived::flow::channel::policy::{
    Decision, InvalidHistory, NoDecision, Policy, PolicyAuthor, PolicyDecision, PolicyHistory,
    PolicyKind, Recorded,
};
use crate::ids::OperatorId;
use crate::tests::fixtures::at;

fn operator(n: u128) -> OperatorId {
    OperatorId::from_ulid(n)
}

fn decided(kind: PolicyKind, by: PolicyAuthor, when: u64) -> PolicyDecision {
    PolicyDecision {
        kind,
        decision: Decision {
            by,
            at: at(when),
            note: None,
        },
    }
}

fn by_operator(kind: PolicyKind, when: u64) -> PolicyDecision {
    decided(kind, PolicyAuthor::Operator(operator(1)), when)
}

#[test]
fn never_reviewed_policy_is_not_a_decision() {
    assert_eq!(
        PolicyDecision::try_from(Policy::Unreviewed(None)),
        Err(NoDecision)
    );
}

#[test]
fn every_decided_policy_round_trips_through_policy_decision() {
    for kind in [
        PolicyKind::Unreviewed,
        PolicyKind::Sanctioned,
        PolicyKind::Unsanctioned,
    ] {
        let entry = by_operator(kind, 5);
        let policy = entry.policy();
        assert_eq!(policy.kind(), kind);
        assert_eq!(policy.decision(), Some(&entry.decision));
        assert_eq!(PolicyDecision::try_from(policy), Ok(entry));
    }
}

#[test]
fn unreviewed_decision_is_a_reset_not_never_reviewed() {
    let reset = by_operator(PolicyKind::Unreviewed, 5).policy();
    assert!(matches!(reset, Policy::Unreviewed(Some(_))));
    assert_eq!(Policy::Unreviewed(None).decision(), None);
}

#[test]
fn empty_history_is_never_reviewed() {
    let history = PolicyHistory::empty();
    assert_eq!(history.current(), Policy::Unreviewed(None));
    assert_eq!(history.in_force_at(at(100)), Policy::Unreviewed(None));
    assert_eq!(history.latest(), None);
    assert!(history.entries().is_empty());
}

#[test]
fn newest_decision_is_appended_and_current() {
    let mut history = PolicyHistory::empty();
    let declared = decided(PolicyKind::Sanctioned, PolicyAuthor::Config, 1);
    let changed = by_operator(PolicyKind::Unsanctioned, 2);
    assert_eq!(history.record(declared.clone()), Recorded::Current);
    assert_eq!(history.record(changed.clone()), Recorded::Current);
    assert_eq!(history.entries(), &[declared, changed.clone()]);
    assert_eq!(history.current(), changed.policy());
}

#[test]
fn late_decision_is_kept_behind_newer_one() {
    let mut history = PolicyHistory::empty();
    let newer = by_operator(PolicyKind::Unsanctioned, 20);
    let older = by_operator(PolicyKind::Sanctioned, 10);
    assert_eq!(history.record(newer.clone()), Recorded::Current);
    assert_eq!(history.record(older.clone()), Recorded::Superseded);
    assert_eq!(history.entries(), &[older, newer.clone()]);
    assert_eq!(history.current(), newer.policy());
}

#[test]
fn recording_a_decision_twice_changes_nothing() {
    let mut history = PolicyHistory::empty();
    let first = by_operator(PolicyKind::Sanctioned, 10);
    let second = by_operator(PolicyKind::Unsanctioned, 20);
    history.record(first.clone());
    history.record(second.clone());
    let before = history.clone();
    assert_eq!(history.record(first), Recorded::Duplicate);
    assert_eq!(history.record(second), Recorded::Duplicate);
    assert_eq!(history, before);
}

#[test]
fn distinct_decisions_at_the_same_time_are_both_kept() {
    let mut history = PolicyHistory::empty();
    let one = decided(
        PolicyKind::Sanctioned,
        PolicyAuthor::Operator(operator(1)),
        10,
    );
    let other = decided(
        PolicyKind::Unsanctioned,
        PolicyAuthor::Operator(operator(2)),
        10,
    );
    assert_eq!(history.record(one.clone()), Recorded::Current);
    assert_eq!(history.record(other.clone()), Recorded::Current);
    assert_eq!(history.entries(), &[one, other.clone()]);
    assert_eq!(history.current(), other.policy());
}

/// Every order of applying the same decisions (distinct times) gives the
/// same history and current policy.
#[test]
fn history_is_independent_of_arrival_order() {
    let decisions = [
        decided(PolicyKind::Sanctioned, PolicyAuthor::Config, 1),
        by_operator(PolicyKind::Unsanctioned, 2),
        by_operator(PolicyKind::Unreviewed, 3),
        decided(PolicyKind::Sanctioned, PolicyAuthor::Config, 4),
    ];
    let expected = PolicyHistory::from_entries(decisions.to_vec()).expect("in time order");
    let orders = [
        [0, 1, 2, 3],
        [3, 2, 1, 0],
        [1, 3, 0, 2],
        [2, 0, 3, 1],
        [3, 0, 2, 1],
        [0, 3, 1, 2],
    ];
    for order in orders {
        let mut history = PolicyHistory::empty();
        for index in order {
            let decision = decisions.get(index).expect("index in range").clone();
            history.record(decision);
        }
        assert_eq!(history, expected, "order {order:?}");
        assert_eq!(history.current(), expected.current());
    }
}

#[test]
fn from_entries_rejects_out_of_order() {
    let entries = vec![
        by_operator(PolicyKind::Sanctioned, 10),
        by_operator(PolicyKind::Unsanctioned, 5),
    ];
    assert_eq!(
        PolicyHistory::from_entries(entries),
        Err(InvalidHistory::OutOfOrder { index: 1 })
    );
}

#[test]
fn from_entries_rejects_duplicates() {
    let entry = by_operator(PolicyKind::Sanctioned, 10);
    let other = by_operator(PolicyKind::Unsanctioned, 10);
    let entries = vec![entry.clone(), other, entry];
    assert_eq!(
        PolicyHistory::from_entries(entries),
        Err(InvalidHistory::Duplicate { index: 2 })
    );
}

#[test]
fn from_entries_accepts_ordered_history_with_ties() {
    let entries = vec![
        decided(PolicyKind::Sanctioned, PolicyAuthor::Config, 1),
        by_operator(PolicyKind::Unsanctioned, 5),
        by_operator(PolicyKind::Sanctioned, 5),
    ];
    let history = PolicyHistory::from_entries(entries.clone()).expect("ordered, no duplicates");
    assert_eq!(history.entries(), entries.as_slice());
}

#[test]
fn policy_in_force_is_latest_decision_at_or_before() {
    let declared = decided(PolicyKind::Sanctioned, PolicyAuthor::Config, 10);
    let revoked = by_operator(PolicyKind::Unsanctioned, 20);
    let history =
        PolicyHistory::from_entries(vec![declared.clone(), revoked.clone()]).expect("ordered");
    assert_eq!(history.in_force_at(at(9)), Policy::Unreviewed(None));
    assert_eq!(history.in_force_at(at(10)), declared.policy());
    assert_eq!(history.in_force_at(at(19)), declared.policy());
    assert_eq!(history.in_force_at(at(20)), revoked.policy());
    assert_eq!(history.in_force_at(at(1000)), revoked.policy());
}
