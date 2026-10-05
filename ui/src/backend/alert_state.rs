//! What the spec's `AlertState` does not say itself: its
//! [`AlertStateKind`] (the inbox's tabs, `AlertFilter::states`) and
//! whether it is active (open or acknowledged).

use crosstalk_spec::aggregates::alert::AlertState;
use crosstalk_spec::interfaces::l8_surface::AlertStateKind;

/// The kind `AlertFilter::states` matches against.
pub fn kind(state: &AlertState) -> AlertStateKind {
    match state {
        AlertState::Open => AlertStateKind::Open,
        AlertState::Acknowledged { .. } => AlertStateKind::Acknowledged,
        AlertState::Resolved { .. } => AlertStateKind::Resolved,
        AlertState::Suppressed { .. } => AlertStateKind::Suppressed,
    }
}

/// Open or acknowledged: still waiting for someone. Only active alerts are
/// deduplicated into, suppressed, acknowledged or resolved.
pub fn is_active(state: &AlertState) -> bool {
    matches!(state, AlertState::Open | AlertState::Acknowledged { .. })
}

#[cfg(test)]
mod tests {
    use crosstalk_spec::aggregates::alert::SuppressReason;
    use crosstalk_spec::ids::OperatorId;
    use crosstalk_spec::support::Timestamp;

    use super::*;

    #[test]
    fn only_open_and_acknowledged_are_active() {
        let by = OperatorId::from_ulid(1);
        let at = Timestamp::from_micros(0);
        assert!(is_active(&AlertState::Open));
        assert!(is_active(&AlertState::Acknowledged { by, at }));
        assert!(!is_active(&AlertState::Resolved { by, at, note: None }));
        let rejected = AlertState::Suppressed {
            at,
            reason: SuppressReason::OperatorRejected,
        };
        assert!(!is_active(&rejected));
        assert_eq!(kind(&rejected), AlertStateKind::Suppressed);
        assert_eq!(kind(&AlertState::Open), AlertStateKind::Open);
    }
}
