//! Policy of a channel: what an operator or config says about it.
//!
//! Every confirmed transmission on a channel is routed through its policy.
//! A sanctioned channel drops it; the other two raise an alert.

use crate::aggregates::alert::AlertRuleKind;
use crate::ids::OperatorId;
use crate::support::Timestamp;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Policy {
    /// `None` when never reviewed; `Some` when an operator reset it.
    Unreviewed(Option<Decision>),
    Sanctioned(Decision),
    Unsanctioned(Decision),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Decision {
    pub by: PolicyAuthor,
    pub at: Timestamp,
    pub note: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PolicyAuthor {
    Config,
    Operator(OperatorId),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TrafficVerdict {
    Drop,
    Raise(AlertRuleKind),
}

impl Policy {
    pub fn on_traffic(&self) -> TrafficVerdict {
        match self {
            Self::Unreviewed(_) => TrafficVerdict::Raise(AlertRuleKind::UnreviewedTraffic),
            Self::Unsanctioned(_) => TrafficVerdict::Raise(AlertRuleKind::UnsanctionedTraffic),
            Self::Sanctioned(_) => TrafficVerdict::Drop,
        }
    }
}
