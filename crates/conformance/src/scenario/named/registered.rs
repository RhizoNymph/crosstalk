//! An agent config registered that never sent traffic: a row with no
//! claims, no last-seen time and no traffic.

use crosstalk_spec::observed::client::HarnessFamily;

use super::super::{AgentFact, AgentRole, Scenario, ScenarioError};

pub const NAME: &str = "registered";

pub const IDLE: AgentRole = AgentRole::new(NAME, "idle");
pub const LABEL: &str = "release-bot";

pub fn scenario() -> Result<Scenario, ScenarioError> {
    Scenario::build(NAME)
        .fact(
            AgentFact::new(IDLE, HarnessFamily::ClaudeCode)
                .labelled(LABEL)
                .registered_only(),
        )
        .done()
}
