//! A public wiki agents started coordinating through: discovered, never
//! reviewed, the busiest channel, with its talk page a second discovered
//! channel on the same host.

use crosstalk_spec::observed::client::HarnessFamily;

use super::super::{
    AgentFact, AgentRole, ChannelFact, ChannelRole, ChannelSource, ResourceFact, ResourceRole,
    Scenario, ScenarioError, TransmissionFact, TransmissionRole, Via,
};
use super::https;

pub const NAME: &str = "hijacked_wiki";

/// Writes the coordination page.
pub const WRITER: AgentRole = AgentRole::new(NAME, "writer");
/// Reads it, and takes in the writer's text.
pub const READER: AgentRole = AgentRole::new(NAME, "reader");
pub const TALK_WRITER: AgentRole = AgentRole::new(NAME, "talk_writer");
pub const TALK_READER: AgentRole = AgentRole::new(NAME, "talk_reader");

/// `https://wiki.example.org/wiki/Agent_Coordination`.
pub const PAGE: ResourceRole = ResourceRole::new(NAME, "page");
/// `https://wiki.example.org/wiki/Talk:Agent_Coordination`.
pub const TALK: ResourceRole = ResourceRole::new(NAME, "talk");

/// Discovered at the page.
pub const WIKI: ChannelRole = ChannelRole::new(NAME, "wiki");
/// Discovered at the talk page.
pub const TALK_PAGE: ChannelRole = ChannelRole::new(NAME, "talk_page");

/// Writer to reader through the page, confirmed.
pub const CONFIRMED: TransmissionRole = TransmissionRole::new(NAME, "confirmed");
/// Through the talk page, confirmed.
pub const ON_TALK: TransmissionRole = TransmissionRole::new(NAME, "on_talk");

/// The wiki's host, which promotion patterns name.
pub const HOST: &str = "wiki.example.org";

pub fn scenario() -> Result<Scenario, ScenarioError> {
    Scenario::build(NAME)
        .fact(AgentFact::new(WRITER, HarnessFamily::Pi))
        .fact(AgentFact::any(READER))
        .fact(AgentFact::any(TALK_WRITER))
        .fact(AgentFact::any(TALK_READER))
        .fact(ResourceFact {
            role: PAGE,
            locator: https(HOST, "/wiki/Agent_Coordination"),
        })
        .fact(ResourceFact {
            role: TALK,
            locator: https(HOST, "/wiki/Talk:Agent_Coordination"),
        })
        .fact(ChannelFact {
            role: WIKI,
            source: ChannelSource::Discovered { seed: PAGE },
        })
        .fact(ChannelFact {
            role: TALK_PAGE,
            source: ChannelSource::Discovered { seed: TALK },
        })
        .fact(TransmissionFact::confirmed(CONFIRMED, WRITER, READER).via(Via::Resource(PAGE)))
        .fact(
            TransmissionFact::confirmed(ON_TALK, TALK_WRITER, TALK_READER).via(Via::Resource(TALK)),
        )
        .done()
}
