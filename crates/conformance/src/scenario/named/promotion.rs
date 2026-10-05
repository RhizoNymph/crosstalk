//! A past promotion: a discovered notes page promoted under a `/team-a`
//! prefix keeps its id, becomes a declaration, and supersedes the
//! discovered channel of a sibling page, whose traffic now counts on it.

use crosstalk_spec::derived::flow::channel::policy::PolicyKind;
use crosstalk_spec::derived::flow::resource::{Host, ResourcePattern};

use super::super::{
    AgentFact, AgentRole, ChannelFact, ChannelRole, ChannelSource, PromotionFact, ResourceFact,
    ResourceRole, Scenario, ScenarioError, TransmissionFact, TransmissionRole, Via,
};
use super::https;

pub const NAME: &str = "promotion";

/// `notes.corp.internal/team-a/retro`: the promoted channel's seed.
pub const RETRO: ResourceRole = ResourceRole::new(NAME, "retro");
/// `notes.corp.internal/team-a/standup`.
pub const STANDUP: ResourceRole = ResourceRole::new(NAME, "standup");
/// Discovered at the retro page, then promoted.
pub const NOTES: ChannelRole = ChannelRole::new(NAME, "notes");
/// Discovered at the standup page, superseded by the promotion.
pub const OLD: ChannelRole = ChannelRole::new(NAME, "old");
pub const WRITER: AgentRole = AgentRole::new(NAME, "writer");
pub const READER: AgentRole = AgentRole::new(NAME, "reader");
pub const OLD_WRITER: AgentRole = AgentRole::new(NAME, "old_writer");
pub const OLD_READER: AgentRole = AgentRole::new(NAME, "old_reader");
pub const ON_RETRO: TransmissionRole = TransmissionRole::new(NAME, "on_retro");
pub const ON_STANDUP: TransmissionRole = TransmissionRole::new(NAME, "on_standup");

pub fn pattern() -> ResourcePattern {
    ResourcePattern::UrlPrefix {
        host: Host("notes.corp.internal".to_owned()),
        path_prefix: "/team-a".to_owned(),
    }
}

pub fn scenario() -> Result<Scenario, ScenarioError> {
    Scenario::build(NAME)
        .fact(ResourceFact {
            role: RETRO,
            locator: https("notes.corp.internal", "/team-a/retro"),
        })
        .fact(ResourceFact {
            role: STANDUP,
            locator: https("notes.corp.internal", "/team-a/standup"),
        })
        .fact(ChannelFact {
            role: NOTES,
            source: ChannelSource::Discovered { seed: RETRO },
        })
        .fact(ChannelFact {
            role: OLD,
            source: ChannelSource::Discovered { seed: STANDUP },
        })
        .fact(AgentFact::any(WRITER))
        .fact(AgentFact::any(READER))
        .fact(AgentFact::any(OLD_WRITER))
        .fact(AgentFact::any(OLD_READER))
        .fact(TransmissionFact::confirmed(ON_RETRO, WRITER, READER).via(Via::Resource(RETRO)))
        .fact(
            TransmissionFact::confirmed(ON_STANDUP, OLD_WRITER, OLD_READER)
                .via(Via::Resource(STANDUP)),
        )
        .fact(PromotionFact {
            channel: NOTES,
            pattern: pattern(),
            policy: PolicyKind::Sanctioned,
            supersedes: vec![OLD],
        })
        .done()
}
