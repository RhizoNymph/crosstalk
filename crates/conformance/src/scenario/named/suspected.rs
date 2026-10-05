//! A channel whose every transmission is suspected: one agent writes an
//! object others read, and no content ever matched. Listed, drawn and
//! counted as unconfirmed (INV-753), and left out under "confirmed only".

use crosstalk_spec::derived::flow::resource::{Host, Locator};

use super::super::{
    AgentFact, AgentRole, ChannelFact, ChannelRole, ChannelSource, Evidence, ResourceFact,
    ResourceRole, Scenario, ScenarioError, TransmissionFact, TransmissionRole, Via,
};

pub const NAME: &str = "suspected";

pub const WRITER: AgentRole = AgentRole::new(NAME, "writer");
pub const READER: AgentRole = AgentRole::new(NAME, "reader");
/// `s3://agent-scratch/handoff/batch-0412.jsonl`.
pub const OBJECT: ResourceRole = ResourceRole::new(NAME, "object");
/// Discovered by the first suspected transmission through the object.
pub const S3: ChannelRole = ChannelRole::new(NAME, "s3");
/// Writer to reader through the object: access pattern only.
pub const SUSPECTED: TransmissionRole = TransmissionRole::new(NAME, "suspected");

pub fn scenario() -> Result<Scenario, ScenarioError> {
    Scenario::build(NAME)
        .fact(AgentFact::any(WRITER))
        .fact(AgentFact::any(READER))
        .fact(ResourceFact {
            role: OBJECT,
            locator: Locator::Url {
                scheme: "s3".to_owned(),
                host: Host("agent-scratch".to_owned()),
                path: "/handoff/batch-0412.jsonl".to_owned(),
                query: None,
            },
        })
        .fact(ChannelFact {
            role: S3,
            source: ChannelSource::Discovered { seed: OBJECT },
        })
        .fact(
            TransmissionFact::with(SUSPECTED, READER, Evidence::Suspected { writer: WRITER })
                .via(Via::Resource(OBJECT)),
        )
        .done()
}
