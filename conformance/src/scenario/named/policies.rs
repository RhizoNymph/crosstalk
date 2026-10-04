//! Operator policy decisions: a paste site judged unsanctioned, and an MCP
//! server sanctioned and later reset to unreviewed.

use crosstalk_spec::derived::flow::channel::policy::PolicyKind;
use crosstalk_spec::derived::flow::resource::Locator;
use crosstalk_spec::observed::message::ToolName;
use crosstalk_spec::support::NonEmpty;

use super::super::{
    AgentFact, AgentRole, ChannelFact, ChannelRole, ChannelSource, PolicyFact, ResourceFact,
    ResourceRole, Scenario, ScenarioError, TransmissionFact, TransmissionRole, Via,
};
use super::https;

pub const NAME: &str = "policies";

/// `https://paste.example.net/raw/q8Zt3LmK`.
pub const PASTE: ResourceRole = ResourceRole::new(NAME, "paste");
/// Discovered at the paste, judged unsanctioned.
pub const PASTEBIN: ChannelRole = ChannelRole::new(NAME, "pastebin");
/// `memory` MCP server, `create_entities` on `project-atlas`.
pub const ENTITIES: ResourceRole = ResourceRole::new(NAME, "entities");
/// Discovered at the entities, sanctioned, then reset.
pub const MEMORY: ChannelRole = ChannelRole::new(NAME, "memory");
pub const PASTER: AgentRole = AgentRole::new(NAME, "paster");
pub const PASTE_READER: AgentRole = AgentRole::new(NAME, "paste_reader");
pub const REMEMBERER: AgentRole = AgentRole::new(NAME, "rememberer");
pub const RECALLER: AgentRole = AgentRole::new(NAME, "recaller");
pub const PASTED: TransmissionRole = TransmissionRole::new(NAME, "pasted");
pub const REMEMBERED: TransmissionRole = TransmissionRole::new(NAME, "remembered");

pub fn scenario() -> Result<Scenario, ScenarioError> {
    Scenario::build(NAME)
        .fact(ResourceFact {
            role: PASTE,
            locator: https("paste.example.net", "/raw/q8Zt3LmK"),
        })
        .fact(ResourceFact {
            role: ENTITIES,
            locator: Locator::Mcp {
                server: "memory".to_owned(),
                tool: ToolName("create_entities".to_owned()),
                target: Some("project-atlas".to_owned()),
            },
        })
        .fact(ChannelFact {
            role: PASTEBIN,
            source: ChannelSource::Discovered { seed: PASTE },
        })
        .fact(ChannelFact {
            role: MEMORY,
            source: ChannelSource::Discovered { seed: ENTITIES },
        })
        .fact(AgentFact::any(PASTER))
        .fact(AgentFact::any(PASTE_READER))
        .fact(AgentFact::any(REMEMBERER))
        .fact(AgentFact::any(RECALLER))
        .fact(TransmissionFact::confirmed(PASTED, PASTER, PASTE_READER).via(Via::Resource(PASTE)))
        .fact(
            TransmissionFact::confirmed(REMEMBERED, REMEMBERER, RECALLER)
                .via(Via::Resource(ENTITIES)),
        )
        .fact(PolicyFact {
            channel: PASTEBIN,
            decisions: NonEmpty::new(PolicyKind::Unsanctioned),
        })
        .fact(PolicyFact {
            channel: MEMORY,
            decisions: {
                let mut decisions = NonEmpty::new(PolicyKind::Sanctioned);
                decisions.push(PolicyKind::Unreviewed);
                decisions
            },
        })
        .done()
}
