//! Route selection: which route explains a content match, in the
//! precedence `Route` documents (`flow.route.precedence`): `Delegation`,
//! then `Channel`, then `Direct`, then `Unobserved`.

use crosstalk_spec::derived::flow::transmission::{
    DelegationDirection, DirectCarrier, NonChannelRoute,
};
use crosstalk_spec::derived::provenance::matching::{Carrier, ContentMatch};
use crosstalk_spec::observed::message::ToolName;
use serde::{Deserialize, Serialize};

use super::ids::Derive;
use super::key::MediumKey;
use super::kinship::Kinship;

/// Where a tool-result match stands toward the reader's tool call.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Carriage {
    /// The call yielded a read access, which carried the match.
    Read,
    /// The call yielded no access by the time the match's window closed.
    NoAccess,
}

/// The route a content match takes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RouteChoice {
    /// Through the channel transmission of the read that carried it, if
    /// one of the sender's writes explains it.
    Channel,
    /// Opened and confirmed at once.
    NonChannel(NonChannelRoute),
}

/// The route of `content`, whose carriage (for a tool-result match) is
/// `carriage`, with agents resolved through `kin`:
///
/// - `Delegation` when sender and reader are parent and child
///   (`flow.route.delegation-parent-link`), whatever carried it;
/// - `Channel` for a tool result whose call yielded a read access
///   (`flow.route.channel-requires-access`);
/// - `Direct` for a user turn, a system prompt, or a tool result whose call
///   yielded no access, named by `tool` (`flow.route.direct-carrier`);
/// - `Unobserved` for the reader's own output
///   (`flow.route.unobserved-reader-output`).
pub fn choose(
    content: &ContentMatch,
    carriage: Carriage,
    kin: &Kinship,
    tool: impl FnOnce() -> ToolName,
) -> RouteChoice {
    if let Some(direction) = kin.delegation(content.origin_agent(), content.reader()) {
        return RouteChoice::NonChannel(NonChannelRoute::Delegation(direction));
    }
    match content.carrier() {
        Carrier::ToolResult(_) => match carriage {
            Carriage::Read => RouteChoice::Channel,
            Carriage::NoAccess => {
                RouteChoice::NonChannel(NonChannelRoute::Direct(DirectCarrier::ToolResult(tool())))
            }
        },
        Carrier::UserTurn => {
            RouteChoice::NonChannel(NonChannelRoute::Direct(DirectCarrier::UserTurn))
        }
        Carrier::SystemPrompt => {
            RouteChoice::NonChannel(NonChannelRoute::Direct(DirectCarrier::SystemPrompt))
        }
        Carrier::ReaderOutput => RouteChoice::NonChannel(NonChannelRoute::Unobserved),
    }
}

/// A route as part of a transmission's identity, ordered so the
/// correlator's state iterates deterministically.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum RouteKey {
    /// A channel transmission, by the medium it opened in.
    Medium(MediumKey),
    Delegation(Direction),
    DirectUserTurn,
    DirectSystemPrompt,
    DirectToolResult(String),
    Unobserved,
}

/// `DelegationDirection`, ordered.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum Direction {
    ParentToChild,
    ChildToParent,
}

impl RouteKey {
    pub fn of(route: &NonChannelRoute) -> Self {
        match route {
            NonChannelRoute::Delegation(DelegationDirection::ParentToChild) => {
                Self::Delegation(Direction::ParentToChild)
            }
            NonChannelRoute::Delegation(DelegationDirection::ChildToParent) => {
                Self::Delegation(Direction::ChildToParent)
            }
            NonChannelRoute::Direct(DirectCarrier::UserTurn) => Self::DirectUserTurn,
            NonChannelRoute::Direct(DirectCarrier::SystemPrompt) => Self::DirectSystemPrompt,
            NonChannelRoute::Direct(DirectCarrier::ToolResult(name)) => {
                Self::DirectToolResult(name.0.clone())
            }
            NonChannelRoute::Unobserved => Self::Unobserved,
        }
    }

    /// Feed this key into an id derivation.
    pub(crate) fn write(&self, derive: Derive) -> Derive {
        match self {
            Self::Medium(MediumKey::Channel(channel)) => {
                derive.bytes(b"channel").ulid(channel.as_ulid())
            }
            Self::Medium(MediumKey::Resource(resource)) => {
                derive.bytes(b"resource").ulid(resource.as_ulid())
            }
            Self::Delegation(Direction::ParentToChild) => derive.bytes(b"parent_to_child"),
            Self::Delegation(Direction::ChildToParent) => derive.bytes(b"child_to_parent"),
            Self::DirectUserTurn => derive.bytes(b"user_turn"),
            Self::DirectSystemPrompt => derive.bytes(b"system_prompt"),
            Self::DirectToolResult(name) => derive.bytes(b"tool_result").bytes(name.as_bytes()),
            Self::Unobserved => derive.bytes(b"unobserved"),
        }
    }
}
