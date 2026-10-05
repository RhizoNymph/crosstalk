//! Old confirmed transmissions whose sender's or reader's message body
//! content retention dropped: their evidence says so on that side only.

use super::super::{
    AgentFact, AgentRole, BodyDroppedFact, BodySide, Scenario, ScenarioError, TransmissionFact,
    TransmissionRole,
};

pub const NAME: &str = "dropped_bodies";

/// The sender's body is gone.
pub const SENDER_GONE: TransmissionRole = TransmissionRole::new(NAME, "sender_gone");
/// The reader's body is gone.
pub const READER_GONE: TransmissionRole = TransmissionRole::new(NAME, "reader_gone");
/// Both bodies kept.
pub const KEPT: TransmissionRole = TransmissionRole::new(NAME, "kept");

const fn agent(name: &'static str) -> AgentRole {
    AgentRole::new(NAME, name)
}

pub fn scenario() -> Result<Scenario, ScenarioError> {
    let mut builder = Scenario::build(NAME);
    for (role, writer, reader) in [
        (SENDER_GONE, agent("a_writer"), agent("a_reader")),
        (READER_GONE, agent("b_writer"), agent("b_reader")),
        (KEPT, agent("c_writer"), agent("c_reader")),
    ] {
        builder = builder
            .fact(AgentFact::any(writer))
            .fact(AgentFact::any(reader))
            .fact(TransmissionFact::confirmed(role, writer, reader));
    }
    builder
        .fact(BodyDroppedFact {
            transmission: SENDER_GONE,
            side: BodySide::Sender,
        })
        .fact(BodyDroppedFact {
            transmission: READER_GONE,
            side: BodySide::Reader,
        })
        .done()
}
