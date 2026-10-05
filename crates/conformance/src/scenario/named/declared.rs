//! Channels config declares: one carrying confirmed traffic, one nobody
//! has used yet. A declaration without cross-agent traffic is listed as a
//! declaration, counted as no channel and drawn nowhere (INV-857).

use crosstalk_spec::derived::flow::resource::{Host, ResourcePattern};

use super::super::{
    AgentFact, AgentRole, ChannelFact, ChannelRole, ChannelSource, ResourceFact, ResourceRole,
    Scenario, ScenarioError, TransmissionFact, TransmissionRole, Via,
};
use super::https;

pub const NAME: &str = "declared";

/// Declared over `wiki.corp.internal/eng`, with traffic.
pub const IN_USE: ChannelRole = ChannelRole::new(NAME, "in_use");
/// A page under it.
pub const PAGE: ResourceRole = ResourceRole::new(NAME, "page");
pub const WRITER: AgentRole = AgentRole::new(NAME, "writer");
pub const READER: AgentRole = AgentRole::new(NAME, "reader");
pub const ON_PAGE: TransmissionRole = TransmissionRole::new(NAME, "on_page");

/// Declared over `docs.corp.internal/design`, never used.
pub const UNUSED: ChannelRole = ChannelRole::new(NAME, "unused");

pub fn scenario() -> Result<Scenario, ScenarioError> {
    Scenario::build(NAME)
        .fact(ChannelFact {
            role: IN_USE,
            source: ChannelSource::Declared {
                pattern: ResourcePattern::UrlPrefix {
                    host: Host("wiki.corp.internal".to_owned()),
                    path_prefix: "/eng".to_owned(),
                },
            },
        })
        .fact(ResourceFact {
            role: PAGE,
            locator: https("wiki.corp.internal", "/eng/runbooks/deploy"),
        })
        .fact(AgentFact::any(WRITER))
        .fact(AgentFact::any(READER))
        .fact(TransmissionFact::confirmed(ON_PAGE, WRITER, READER).via(Via::Resource(PAGE)))
        .fact(ChannelFact {
            role: UNUSED,
            source: ChannelSource::Declared {
                pattern: ResourcePattern::UrlPrefix {
                    host: Host("docs.corp.internal".to_owned()),
                    path_prefix: "/design".to_owned(),
                },
            },
        })
        .done()
}
