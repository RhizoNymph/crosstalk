//! A pi agent whose traffic also claims Claude Code: claims are recorded
//! as claims, never trusted as identity.

use crosstalk_spec::observed::client::HarnessFamily;

use super::super::{
    AgentFact, AgentRole, Scenario, ScenarioError, TransmissionFact, TransmissionRole,
};

pub const NAME: &str = "impersonation";

/// Runs in pi; claims pi and Claude Code. Labelled `pi-scraper`.
pub const IMPERSONATOR: AgentRole = AgentRole::new(NAME, "impersonator");
pub const PEER: AgentRole = AgentRole::new(NAME, "peer");
/// From the impersonator, so it is a topology node.
pub const SENT: TransmissionRole = TransmissionRole::new(NAME, "sent");

pub const LABEL: &str = "pi-scraper";

pub fn scenario() -> Result<Scenario, ScenarioError> {
    Scenario::build(NAME)
        .fact(
            AgentFact::new(IMPERSONATOR, HarnessFamily::Pi)
                .claiming(HarnessFamily::ClaudeCode)
                .labelled(LABEL),
        )
        .fact(AgentFact::any(PEER))
        .fact(TransmissionFact::confirmed(SENT, IMPERSONATOR, PEER))
        .done()
}
