//! The `<ct-topology>` selection value, parsed into typed ids.
//!
//! The grammar is the element's (`ui/elements/src/shared/selection.ts`):
//! `edge:<fromUlid>:<toUlid>:<routeCode>` | `agent:<ulid>` |
//! `channel:<ulid>` | `` (nothing selected). The route code is everything
//! after the third colon, since tool names may contain colons. The same
//! text is the topology page's `sel` key, so a selected edge is citeable.

use crosstalk_spec::derived::flow::transmission::Route;
use crosstalk_spec::ids::{AgentId, ChannelId};

use crate::url::route::{self, InvalidRoute};
use crate::url::ulid::{InvalidUlid, UlidId};

/// Longer values are rejected before parsing: the longest valid value is a
/// tool name away from 90 bytes.
pub const MAX_LEN: usize = 512;

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum Selection {
    #[default]
    None,
    Edge {
        from: AgentId,
        to: AgentId,
        route: Route,
    },
    Agent(AgentId),
    Channel(ChannelId),
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum InvalidSelection {
    #[error("longer than {MAX_LEN} bytes")]
    TooLong,
    #[error("unknown selection kind {0:?}")]
    Kind(String),
    #[error("{0}: expected one id")]
    Shape(&'static str),
    #[error("{part}: {source}")]
    Id {
        part: &'static str,
        source: InvalidUlid,
    },
    #[error("route: {0}")]
    Route(#[from] InvalidRoute),
}

fn id<T: UlidId>(text: &str, part: &'static str) -> Result<T, InvalidSelection> {
    T::parse_ulid(text).map_err(|source| InvalidSelection::Id { part, source })
}

impl Selection {
    pub fn parse(text: &str) -> Result<Self, InvalidSelection> {
        if text.len() > MAX_LEN {
            return Err(InvalidSelection::TooLong);
        }
        if text.is_empty() {
            return Ok(Self::None);
        }
        let (kind, rest) = text.split_once(':').unwrap_or((text, ""));
        match kind {
            "agent" | "channel" if rest.contains(':') || rest.is_empty() => {
                Err(InvalidSelection::Shape(if kind == "agent" {
                    "agent"
                } else {
                    "channel"
                }))
            }
            "agent" => Ok(Self::Agent(id(rest, "agent")?)),
            "channel" => Ok(Self::Channel(id(rest, "channel")?)),
            "edge" => {
                let mut parts = rest.splitn(3, ':');
                let from = id(parts.next().unwrap_or(""), "edge from")?;
                let to = id(parts.next().unwrap_or(""), "edge to")?;
                let route = route::decode(parts.next().unwrap_or(""))?;
                Ok(Self::Edge { from, to, route })
            }
            other => Err(InvalidSelection::Kind(other.to_owned())),
        }
    }

    /// The canonical text; `parse(encode(s)) == s`.
    pub fn encode(&self) -> String {
        match self {
            Self::None => String::new(),
            Self::Edge { from, to, route } => format!(
                "edge:{}:{}:{}",
                from.to_ulid(),
                to.to_ulid(),
                route::encode(route)
            ),
            Self::Agent(id) => format!("agent:{}", id.to_ulid()),
            Self::Channel(id) => format!("channel:{}", id.to_ulid()),
        }
    }

    pub fn edge(from: AgentId, to: AgentId, route: &Route) -> Self {
        Self::Edge {
            from,
            to,
            route: route.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use crosstalk_spec::derived::flow::transmission::{DelegationDirection, DirectCarrier};
    use crosstalk_spec::observed::message::ToolName;

    use super::*;

    const A: &str = "01J9ZQ3W8D0000000000000001";
    const B: &str = "01J9ZQ3W8D0000000000000002";

    #[test]
    fn every_kind_round_trips() {
        let a = AgentId::parse_ulid(A).expect("a");
        let b = AgentId::parse_ulid(B).expect("b");
        for selection in [
            Selection::None,
            Selection::Agent(a),
            Selection::Channel(ChannelId::from_ulid(7)),
            Selection::edge(a, b, &Route::Delegation(DelegationDirection::ParentToChild)),
            Selection::edge(a, b, &Route::Channel(ChannelId::from_ulid(9))),
            Selection::edge(a, b, &Route::Unobserved),
        ] {
            assert_eq!(Selection::parse(&selection.encode()), Ok(selection));
        }
    }

    #[test]
    fn route_codes_keep_their_colons() {
        let text = format!("edge:{A}:{B}:dr.tool.mcp__kv:put");
        let parsed = Selection::parse(&text).expect("parse");
        assert_eq!(
            parsed,
            Selection::Edge {
                from: AgentId::parse_ulid(A).expect("a"),
                to: AgentId::parse_ulid(B).expect("b"),
                route: Route::Direct(DirectCarrier::ToolResult(ToolName("mcp__kv:put".into()))),
            }
        );
        assert_eq!(parsed.encode(), text);
    }

    #[test]
    fn element_values_parse() {
        assert_eq!(
            Selection::parse(&format!("agent:{A}")),
            Ok(Selection::Agent(AgentId::parse_ulid(A).expect("a")))
        );
        assert_eq!(Selection::parse(""), Ok(Selection::None));
    }

    #[test]
    fn malformed_values_say_what_is_wrong() {
        assert_eq!(
            Selection::parse("node:x"),
            Err(InvalidSelection::Kind("node".into()))
        );
        assert_eq!(
            Selection::parse(&format!("agent:{A}:{B}")),
            Err(InvalidSelection::Shape("agent"))
        );
        assert_eq!(
            Selection::parse("channel"),
            Err(InvalidSelection::Shape("channel"))
        );
        assert!(matches!(
            Selection::parse(&format!("edge:{A}:nope:un")),
            Err(InvalidSelection::Id {
                part: "edge to",
                ..
            })
        ));
        assert!(matches!(
            Selection::parse(&format!("edge:{A}:{B}:teleport")),
            Err(InvalidSelection::Route(_))
        ));
        assert!(matches!(
            Selection::parse(&format!("edge:{A}:{B}")),
            Err(InvalidSelection::Route(_))
        ));
        assert_eq!(
            Selection::parse(&"x".repeat(MAX_LEN + 1)),
            Err(InvalidSelection::TooLong)
        );
    }
}
