//! The topic catalog after two re-fits: the first version dropped by
//! retention, the second retained but superseded, its lineage to the active
//! version leaving a topic unmapped, and a rule watching that topic stale.

use super::super::{RuleRole, Scenario, ScenarioError, StaleRuleFact, TopicHistoryFact};

pub const NAME: &str = "topics";

/// Watches a topic of the retained older version that the re-fit left
/// without a link at the remap threshold.
pub const STALE: RuleRole = RuleRole::new(NAME, "stale");

pub fn scenario() -> Result<Scenario, ScenarioError> {
    Scenario::build(NAME)
        .fact(TopicHistoryFact)
        .fact(StaleRuleFact { rule: STALE })
        .done()
}
