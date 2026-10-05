//! Dead letters in more than one consumer group, for the pipeline reads
//! and the replay action.

use std::num::NonZeroU8;

use super::super::{DeadLetterFact, Scenario, ScenarioError};

pub const NAME: &str = "pipeline";

pub const GROUPS: NonZeroU8 = NonZeroU8::MIN.saturating_add(1);

pub fn scenario() -> Result<Scenario, ScenarioError> {
    Scenario::build(NAME)
        .fact(DeadLetterFact { groups: GROUPS })
        .done()
}
