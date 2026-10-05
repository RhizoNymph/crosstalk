//! A confirmed transmission on every route that is not a channel: a
//! delegation from a parent to the sub-agent it spawned, text placed
//! straight into the reader's context, and an unobserved route.

use crosstalk_spec::derived::flow::transmission::DelegationDirection;

use super::super::{
    AgentFact, AgentRole, Scenario, ScenarioError, TransmissionFact, TransmissionRole, Via,
};

pub const NAME: &str = "routes";

pub const PARENT: AgentRole = AgentRole::new(NAME, "parent");
pub const CHILD: AgentRole = AgentRole::new(NAME, "child");
pub const DELEGATED: TransmissionRole = TransmissionRole::new(NAME, "delegated");
pub const DIRECT: TransmissionRole = TransmissionRole::new(NAME, "direct");
pub const UNOBSERVED: TransmissionRole = TransmissionRole::new(NAME, "unobserved");

const fn agent(name: &'static str) -> AgentRole {
    AgentRole::new(NAME, name)
}

pub fn scenario() -> Result<Scenario, ScenarioError> {
    let (direct_writer, direct_reader) = (agent("direct_writer"), agent("direct_reader"));
    let (hidden_writer, hidden_reader) = (agent("unobserved_writer"), agent("unobserved_reader"));
    Scenario::build(NAME)
        .fact(AgentFact::any(PARENT))
        .fact(AgentFact::any(CHILD).child_of(PARENT))
        .fact(
            TransmissionFact::confirmed(DELEGATED, PARENT, CHILD)
                .via(Via::Delegation(DelegationDirection::ParentToChild)),
        )
        .fact(AgentFact::any(direct_writer))
        .fact(AgentFact::any(direct_reader))
        .fact(TransmissionFact::confirmed(DIRECT, direct_writer, direct_reader).via(Via::Direct))
        .fact(AgentFact::any(hidden_writer))
        .fact(AgentFact::any(hidden_reader))
        .fact(
            TransmissionFact::confirmed(UNOBSERVED, hidden_writer, hidden_reader)
                .via(Via::Unobserved),
        )
        .done()
}
