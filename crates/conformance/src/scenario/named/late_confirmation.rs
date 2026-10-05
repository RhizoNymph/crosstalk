//! A transmission confirmed in a later bucket than the one it opened in:
//! counting by confirmation time puts it in the later bucket only.

use super::super::{
    AgentFact, AgentRole, Evidence, Scenario, ScenarioError, Timing, TransmissionFact,
    TransmissionRole,
};

pub const NAME: &str = "late_confirmation";

pub const SENDER: AgentRole = AgentRole::new(NAME, "sender");
pub const RECEIVER: AgentRole = AgentRole::new(NAME, "receiver");
/// Opened in one bucket, confirmed in a later one.
pub const LATE: TransmissionRole = TransmissionRole::new(NAME, "late");

pub fn scenario() -> Result<Scenario, ScenarioError> {
    Scenario::build(NAME)
        .fact(AgentFact::any(SENDER))
        .fact(AgentFact::any(RECEIVER))
        .fact(TransmissionFact::with(
            LATE,
            RECEIVER,
            Evidence::Confirmed {
                writer: SENDER,
                timing: Timing::ConfirmedInLaterBucket,
            },
        ))
        .done()
}
