//! URL text for routes and route kinds.
//!
//! `ch.<channel ulid>`, `dl.p2c`, `dl.c2p`, `dr.user`, `dr.sys`,
//! `dr.tool.<tool name>`, `un`. Tool names are percent-encoded by the
//! query encoder like any other value.

use crosstalk_spec::aggregates::edge::RouteKind;
use crosstalk_spec::derived::flow::transmission::{DelegationDirection, DirectCarrier, Route};
use crosstalk_spec::ids::ChannelId;
use crosstalk_spec::observed::message::ToolName;

use super::ulid::{InvalidUlid, UlidId};

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum InvalidRoute {
    #[error("unknown route {0:?}")]
    Unknown(String),
    #[error("bad channel id: {0}")]
    Channel(#[from] InvalidUlid),
    #[error("empty tool name")]
    EmptyTool,
}

pub fn encode(route: &Route) -> String {
    match route {
        Route::Channel(id) => format!("ch.{}", id.to_ulid()),
        Route::Delegation(DelegationDirection::ParentToChild) => "dl.p2c".to_owned(),
        Route::Delegation(DelegationDirection::ChildToParent) => "dl.c2p".to_owned(),
        Route::Direct(DirectCarrier::UserTurn) => "dr.user".to_owned(),
        Route::Direct(DirectCarrier::SystemPrompt) => "dr.sys".to_owned(),
        Route::Direct(DirectCarrier::ToolResult(ToolName(name))) => format!("dr.tool.{name}"),
        Route::Unobserved => "un".to_owned(),
    }
}

pub fn decode(text: &str) -> Result<Route, InvalidRoute> {
    if let Some(id) = text.strip_prefix("ch.") {
        return Ok(Route::Channel(ChannelId::parse_ulid(id)?));
    }
    if let Some(name) = text.strip_prefix("dr.tool.") {
        if name.is_empty() {
            return Err(InvalidRoute::EmptyTool);
        }
        return Ok(Route::Direct(DirectCarrier::ToolResult(ToolName(
            name.to_owned(),
        ))));
    }
    match text {
        "dl.p2c" => Ok(Route::Delegation(DelegationDirection::ParentToChild)),
        "dl.c2p" => Ok(Route::Delegation(DelegationDirection::ChildToParent)),
        "dr.user" => Ok(Route::Direct(DirectCarrier::UserTurn)),
        "dr.sys" => Ok(Route::Direct(DirectCarrier::SystemPrompt)),
        "un" => Ok(Route::Unobserved),
        other => Err(InvalidRoute::Unknown(other.to_owned())),
    }
}

pub fn encode_kind(kind: RouteKind) -> &'static str {
    match kind {
        RouteKind::Channel => "channel",
        RouteKind::Delegation => "delegation",
        RouteKind::Direct => "direct",
        RouteKind::Unobserved => "unobserved",
    }
}

pub fn decode_kind(text: &str) -> Option<RouteKind> {
    match text {
        "channel" => Some(RouteKind::Channel),
        "delegation" => Some(RouteKind::Delegation),
        "direct" => Some(RouteKind::Direct),
        "unobserved" => Some(RouteKind::Unobserved),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_route_round_trips() {
        let routes = [
            Route::Channel(ChannelId::from_ulid(42)),
            Route::Delegation(DelegationDirection::ParentToChild),
            Route::Delegation(DelegationDirection::ChildToParent),
            Route::Direct(DirectCarrier::UserTurn),
            Route::Direct(DirectCarrier::SystemPrompt),
            Route::Direct(DirectCarrier::ToolResult(ToolName("mcp__wiki.read".into()))),
            Route::Unobserved,
        ];
        for route in routes {
            assert_eq!(decode(&encode(&route)), Ok(route));
        }
    }

    #[test]
    fn rejects_unknown_and_empty_tool() {
        assert_eq!(decode("dr.tool."), Err(InvalidRoute::EmptyTool));
        assert!(matches!(decode("zz"), Err(InvalidRoute::Unknown(_))));
        assert!(matches!(decode("ch.nope"), Err(InvalidRoute::Channel(_))));
    }

    #[test]
    fn kinds_round_trip() {
        for kind in [
            RouteKind::Channel,
            RouteKind::Delegation,
            RouteKind::Direct,
            RouteKind::Unobserved,
        ] {
            assert_eq!(decode_kind(encode_kind(kind)), Some(kind));
        }
    }
}
