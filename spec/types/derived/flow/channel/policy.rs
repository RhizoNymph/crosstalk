//! Policy of a channel: what an operator or config says about it.
//!
//! Every confirmed transmission on a channel is routed through its policy.
//! A sanctioned channel drops it; the other two raise an alert.
//!
//! **History.** Every decision made about a channel, by config (a declared
//! channel's initial policy, or a config reload) or by an operator
//! (`PolicyChanged`), is kept in the channel's [`PolicyHistory`], ordered by
//! decision time. The channel's current [`Policy`] is always
//! [`PolicyHistory::current`]: the latest decision, or `Unreviewed(None)`
//! when nobody has decided anything. Flow detection (L5) records each
//! decision and updates the current policy in one step
//! (`ChannelRegistry::set_policy`).
//!
//! ```text
//! config declare/reload ─┐
//!                        ├─▶ PolicyDecision ─record─▶ PolicyHistory ─current─▶ Channel.policy
//! PolicyChanged (bus) ───┘                              (ordered by Decision::at)
//! ```

use serde::{Deserialize, Serialize};

use crate::aggregates::alert::AlertRuleKind;
use crate::ids::OperatorId;
use crate::support::Timestamp;
use crate::wire::Rejected;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "type",
    content = "data",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum Policy {
    /// `None` when never reviewed; `Some` when an operator reset it.
    Unreviewed(Option<Decision>),
    Sanctioned(Decision),
    Unsanctioned(Decision),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct Decision {
    pub by: PolicyAuthor,
    pub at: Timestamp,
    pub note: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "type",
    content = "data",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum PolicyAuthor {
    Config,
    Operator(OperatorId),
}

/// Which of the three policies, without the decision behind it. This is
/// what an operator asks for (`OperatorAction::SetPolicy`); the surface
/// stamps the author and time from the authenticated caller.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PolicyKind {
    Unreviewed,
    Sanctioned,
    Unsanctioned,
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

    pub fn kind(&self) -> PolicyKind {
        match self {
            Self::Unreviewed(_) => PolicyKind::Unreviewed,
            Self::Sanctioned(_) => PolicyKind::Sanctioned,
            Self::Unsanctioned(_) => PolicyKind::Unsanctioned,
        }
    }

    /// `None` only for a channel nobody has reviewed.
    pub fn decision(&self) -> Option<&Decision> {
        match self {
            Self::Unreviewed(decision) => decision.as_ref(),
            Self::Sanctioned(decision) | Self::Unsanctioned(decision) => Some(decision),
        }
    }
}

/// A policy with the decision that set it: one entry of a
/// [`PolicyHistory`]. Unlike [`Policy`], it cannot be `Unreviewed(None)`, so
/// every entry names its author and time.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct PolicyDecision {
    pub kind: PolicyKind,
    pub decision: Decision,
}

/// The policy carried no decision: `Policy::Unreviewed(None)` is the state
/// of a channel nobody has reviewed, not a decision anyone made.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NoDecision;

impl PolicyDecision {
    /// The policy this decision sets. An `Unreviewed` decision is a reset,
    /// `Unreviewed(Some(_))`.
    pub fn policy(&self) -> Policy {
        let decision = self.decision.clone();
        match self.kind {
            PolicyKind::Unreviewed => Policy::Unreviewed(Some(decision)),
            PolicyKind::Sanctioned => Policy::Sanctioned(decision),
            PolicyKind::Unsanctioned => Policy::Unsanctioned(decision),
        }
    }
}

impl TryFrom<Policy> for PolicyDecision {
    type Error = NoDecision;

    fn try_from(policy: Policy) -> Result<Self, NoDecision> {
        let kind = policy.kind();
        let decision = match policy {
            Policy::Unreviewed(None) => return Err(NoDecision),
            Policy::Unreviewed(Some(decision))
            | Policy::Sanctioned(decision)
            | Policy::Unsanctioned(decision) => decision,
        };
        Ok(Self { kind, decision })
    }
}

/// Every decision ever recorded for one channel, oldest first.
///
/// Built only through [`PolicyHistory::empty`],
/// [`PolicyHistory::from_entries`] and [`PolicyHistory::record`]: entries are
/// ordered by `Decision::at` (entries with equal times keep the order they
/// were recorded in), and no decision appears twice. Nothing is ever removed.
///
/// Ordering by decision time, not by arrival, makes the current policy
/// independent of bus delivery order: a decision that arrives after a later
/// one is kept in its place and does not become current.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", try_from = "RawPolicyHistory")]
pub struct PolicyHistory {
    entries: Vec<PolicyDecision>,
}

/// What [`PolicyHistory::record`] did with a decision.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Recorded {
    /// Appended as the latest entry: the channel's policy is now this one.
    Current,
    /// Kept in time order behind a later decision, which stays current.
    Superseded,
    /// Already in the history (a redelivered event): nothing changed.
    Duplicate,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InvalidHistory {
    /// The entry at `index` is older than the one before it.
    OutOfOrder { index: usize },
    /// The entry at `index` equals an earlier entry.
    Duplicate { index: usize },
}

/// [`PolicyHistory`]'s entries, decoded without the checks. Decoding goes
/// through [`PolicyHistory::from_entries`].
#[derive(Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
struct RawPolicyHistory {
    entries: Vec<PolicyDecision>,
}

impl TryFrom<RawPolicyHistory> for PolicyHistory {
    type Error = Rejected<InvalidHistory>;

    fn try_from(raw: RawPolicyHistory) -> Result<Self, Self::Error> {
        Self::from_entries(raw.entries).map_err(|error| Rejected::new("policy history", error))
    }
}

/// Whether `entries`, which are in time order, already hold `decision`.
/// Only entries with the same time can be equal to it.
fn holds(entries: &[PolicyDecision], decision: &PolicyDecision) -> bool {
    entries
        .iter()
        .rev()
        .skip_while(|entry| entry.decision.at > decision.decision.at)
        .take_while(|entry| entry.decision.at == decision.decision.at)
        .any(|entry| entry == decision)
}

impl PolicyHistory {
    /// The history of a channel nobody has made a decision about.
    pub fn empty() -> Self {
        Self::default()
    }

    /// Rebuild a stored history, checking its order.
    pub fn from_entries(entries: Vec<PolicyDecision>) -> Result<Self, InvalidHistory> {
        for index in 1..entries.len() {
            let (earlier, rest) = entries.split_at(index);
            let Some(entry) = rest.first() else {
                break;
            };
            if earlier
                .last()
                .is_some_and(|previous| entry.decision.at < previous.decision.at)
            {
                return Err(InvalidHistory::OutOfOrder { index });
            }
            if holds(earlier, entry) {
                return Err(InvalidHistory::Duplicate { index });
            }
        }
        Ok(Self { entries })
    }

    /// Record one decision in time order. Idempotent: recording a decision
    /// already present changes nothing.
    pub fn record(&mut self, decision: PolicyDecision) -> Recorded {
        if holds(&self.entries, &decision) {
            return Recorded::Duplicate;
        }
        let at = decision.decision.at;
        let position = self
            .entries
            .partition_point(|entry| entry.decision.at <= at);
        self.entries.insert(position, decision);
        if position + 1 == self.entries.len() {
            Recorded::Current
        } else {
            Recorded::Superseded
        }
    }

    /// The channel's policy: the latest decision, or `Unreviewed(None)` when
    /// there is none.
    pub fn current(&self) -> Policy {
        self.latest()
            .map_or(Policy::Unreviewed(None), PolicyDecision::policy)
    }

    /// The policy in force at `at`: the latest decision made at or before
    /// it, or `Unreviewed(None)` if there was none yet.
    pub fn in_force_at(&self, at: Timestamp) -> Policy {
        let position = self
            .entries
            .partition_point(|entry| entry.decision.at <= at);
        position
            .checked_sub(1)
            .and_then(|index| self.entries.get(index))
            .map_or(Policy::Unreviewed(None), PolicyDecision::policy)
    }

    pub fn latest(&self) -> Option<&PolicyDecision> {
        self.entries.last()
    }

    /// Oldest first.
    pub fn entries(&self) -> &[PolicyDecision] {
        &self.entries
    }
}
