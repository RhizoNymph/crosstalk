//! An alert state's kind and whether it is active: what the inbox's tabs
//! and `AlertFilter::states` match on, and what acknowledging, resolving,
//! deduplication and suppression require.

use crate::aggregates::alert::{AlertState, AlertStateKind, SuppressReason};
use crate::ids::OperatorId;
use crate::tests::fixtures::at;

/// One state of every kind, behind an exhaustive match.
fn every_state() -> Vec<AlertState> {
    fn declared(state: AlertState) -> AlertState {
        match state {
            AlertState::Open
            | AlertState::Acknowledged { .. }
            | AlertState::Resolved { .. }
            | AlertState::Suppressed { .. } => state,
        }
    }
    let by = OperatorId::from_ulid(1);
    [
        AlertState::Open,
        AlertState::Acknowledged { by, at: at(2) },
        AlertState::Resolved {
            by,
            at: at(3),
            note: Some("rotated the key".into()),
        },
        AlertState::Suppressed {
            at: at(4),
            reason: SuppressReason::OperatorRejected,
        },
    ]
    .into_iter()
    .map(declared)
    .collect()
}

#[test]
fn each_state_has_its_own_kind() {
    let kinds: Vec<AlertStateKind> = every_state().iter().map(AlertState::kind).collect();
    assert_eq!(kinds, AlertStateKind::ALL.to_vec());
}

#[test]
fn only_open_and_acknowledged_alerts_are_active() {
    let active: Vec<bool> = every_state().iter().map(AlertState::is_active).collect();
    assert_eq!(active, vec![true, true, false, false]);
    for state in every_state() {
        assert_eq!(state.is_active(), state.kind().is_active(), "{state:?}");
    }
    for reason in [
        SuppressReason::ChannelSanctioned,
        SuppressReason::RuleDisabled,
        SuppressReason::OperatorRejected,
    ] {
        assert!(!AlertState::Suppressed { at: at(1), reason }.is_active());
    }
}
