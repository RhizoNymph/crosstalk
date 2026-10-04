//! A discovered channel whose only cross-agent traffic was between two ids
//! an operator later merged: hidden while the merge stands, listed again
//! once it is reverted (INV-755).

use crosstalk_spec::derived::flow::resource::{Host, Locator};

use super::super::{
    AgentFact, AgentRole, ChannelFact, ChannelRole, ChannelSource, MergeBy, MergeFact, MergeRole,
    ResourceFact, ResourceRole, Scenario, ScenarioError, TransmissionFact, TransmissionRole, Via,
};

pub const NAME: &str = "hidden_channel";

/// The canonical agent.
pub const OWNER: AgentRole = AgentRole::new(NAME, "owner");
/// Another id of the same agent, merged into `OWNER` by an operator.
pub const OTHER_ID: AgentRole = AgentRole::new(NAME, "other_id");
pub const MERGE: MergeRole = MergeRole::new(NAME, "merge");
/// `devbox-7:/home/dev/.codex/handoff.md`.
pub const NOTES: ResourceRole = ResourceRole::new(NAME, "notes");
/// Discovered at `NOTES` while the two ids were separate.
pub const SELF_NOTES: ChannelRole = ChannelRole::new(NAME, "self_notes");
/// From one id to the other through the notes.
pub const BETWEEN: TransmissionRole = TransmissionRole::new(NAME, "between");

pub fn scenario() -> Result<Scenario, ScenarioError> {
    Scenario::build(NAME)
        .fact(AgentFact::any(OWNER))
        .fact(AgentFact::any(OTHER_ID))
        .fact(ResourceFact {
            role: NOTES,
            locator: Locator::File {
                host: Some(Host("devbox-7".to_owned())),
                path: "/home/dev/.codex/handoff.md".to_owned(),
            },
        })
        .fact(ChannelFact {
            role: SELF_NOTES,
            source: ChannelSource::Discovered { seed: NOTES },
        })
        .fact(TransmissionFact::confirmed(BETWEEN, OTHER_ID, OWNER).via(Via::Resource(NOTES)))
        .fact(MergeFact {
            role: MERGE,
            alias: OTHER_ID,
            into: OWNER,
            by: MergeBy::Operator,
            reverted: false,
        })
        .done()
}
