//! Alerts and their states (items 1, 17 and 25).
//!
//! The same shapes as `crosstalk_spec::aggregates::alert`, with one more
//! suppress reason: a false-detection verdict suppresses the active alerts
//! on its transmission.

use crosstalk_spec::aggregates::alert::AlertSubject;
use crosstalk_spec::ids::{AlertId, AlertRuleId, OperatorId};
use crosstalk_spec::interfaces::l8_surface::AlertStateKind;
use crosstalk_spec::support::Timestamp;

/// Replaces `crosstalk_spec::aggregates::alert::Alert`, holding the
/// contract's [`AlertState`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Alert {
    pub id: AlertId,
    pub rule: AlertRuleId,
    pub subject: AlertSubject,
    pub raised_at: Timestamp,
    pub occurrences: u32,
    pub state: AlertState,
}

/// Replaces `crosstalk_spec::aggregates::alert::AlertState`, holding the
/// contract's [`SuppressReason`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AlertState {
    Open,
    Acknowledged {
        by: OperatorId,
        at: Timestamp,
    },
    Resolved {
        by: OperatorId,
        at: Timestamp,
        note: Option<String>,
    },
    /// The condition stopped being alert-worthy.
    Suppressed {
        at: Timestamp,
        reason: SuppressReason,
    },
}

impl AlertState {
    pub fn kind(&self) -> AlertStateKind {
        match self {
            Self::Open => AlertStateKind::Open,
            Self::Acknowledged { .. } => AlertStateKind::Acknowledged,
            Self::Resolved { .. } => AlertStateKind::Resolved,
            Self::Suppressed { .. } => AlertStateKind::Suppressed,
        }
    }

    /// Open or acknowledged: still waiting for someone.
    pub fn is_active(&self) -> bool {
        matches!(self, Self::Open | Self::Acknowledged { .. })
    }
}

/// Replaces `crosstalk_spec::aggregates::alert::SuppressReason`, adding
/// `OperatorRejected`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SuppressReason {
    /// The alert's channel was sanctioned.
    ChannelSanctioned,
    /// The alert's rule was disabled.
    RuleDisabled,
    /// An operator judged the alert's transmission a false detection.
    OperatorRejected,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_open_and_acknowledged_are_active() {
        let by = OperatorId::from_ulid(1);
        let at = Timestamp::from_micros(0);
        assert!(AlertState::Open.is_active());
        assert!(AlertState::Acknowledged { by, at }.is_active());
        assert!(!AlertState::Resolved { by, at, note: None }.is_active());
        let rejected = AlertState::Suppressed {
            at,
            reason: SuppressReason::OperatorRejected,
        };
        assert!(!rejected.is_active());
        assert_eq!(rejected.kind(), AlertStateKind::Suppressed);
    }
}
