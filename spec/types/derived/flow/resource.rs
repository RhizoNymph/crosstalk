//! Resources: concrete things agents touch through tools.
//!
//! A locator is normalized before it is stored, so the same resource reached
//! through different tools (a `web_fetch` of a URL, and a `curl` of the same
//! URL inside a bash command) gets the same [`ResourceId`]:
//! - URL scheme and host lowercased, default ports and fragments removed,
//!   query parameters sorted.
//! - File paths absolute and with `.`/`..` resolved. A relative path is
//!   resolved against the working directory the conversation states (harness
//!   system prompts carry it); when none is known, the access is keyed as
//!   `Locator::Opaque` on the tool and the path as written.

use serde::{Deserialize, Serialize};

use crate::ids::ResourceId;
use crate::observed::message::ToolName;
use crate::support::Timestamp;
use crate::wire::WireRequest;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Host(pub String);

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(
    tag = "type",
    content = "data",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum Locator {
    Url {
        scheme: String,
        host: Host,
        path: String,
        query: Option<String>,
    },
    File {
        host: Option<Host>,
        path: String,
    },
    /// A resource behind an MCP tool, keyed by the argument that names it.
    Mcp {
        server: String,
        tool: ToolName,
        target: Option<String>,
    },
    /// A tool the extractors understand only well enough to key it.
    Opaque {
        tool: ToolName,
        key: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct Resource {
    pub id: ResourceId,
    pub locator: Locator,
    pub first_seen: Timestamp,
}

/// Matches locators. Used by declared channels, which exist before any of
/// their resources have been seen. Prefixes match whole path segments only:
/// `/shared` matches `/shared` and `/shared/x`, not `/shared-other/x`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "type",
    content = "data",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum ResourcePattern {
    Exact(Locator),
    Host(Host),
    UrlPrefix { host: Host, path_prefix: String },
    PathPrefix { host: Option<Host>, prefix: String },
    McpServer(String),
}

/// A client names the pattern of a promotion and of its preview.
impl WireRequest for ResourcePattern {}

impl ResourcePattern {
    pub fn matches(&self, locator: &Locator) -> bool {
        match (self, locator) {
            (Self::Exact(expected), _) => expected == locator,
            (Self::Host(host), Locator::Url { host: h, .. }) => host == h,
            (Self::UrlPrefix { host, path_prefix }, Locator::Url { host: h, path, .. }) => {
                host == h && segment_prefix(path, path_prefix)
            }
            (Self::PathPrefix { host, prefix }, Locator::File { host: h, path }) => {
                host == h && segment_prefix(path, prefix)
            }
            (Self::McpServer(server), Locator::Mcp { server: s, .. }) => server == s,
            _ => false,
        }
    }

    /// Whether some locator matches both patterns. Two declared channels
    /// whose patterns overlap would make a lookup ambiguous, so declaring
    /// or promoting with an overlapping pattern is refused. Exact for
    /// segment prefixes: `/a` and `/a/b` overlap, `/a` and `/ab` do not.
    pub fn overlaps(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Exact(locator), pattern) | (pattern, Self::Exact(locator)) => {
                pattern.matches(locator)
            }
            (Self::Host(a), Self::Host(b))
            | (Self::Host(a), Self::UrlPrefix { host: b, .. })
            | (Self::UrlPrefix { host: b, .. }, Self::Host(a)) => a == b,
            (
                Self::UrlPrefix {
                    host: a,
                    path_prefix: p,
                },
                Self::UrlPrefix {
                    host: b,
                    path_prefix: q,
                },
            ) => a == b && nested(p, q),
            (Self::PathPrefix { host: a, prefix: p }, Self::PathPrefix { host: b, prefix: q }) => {
                a == b && nested(p, q)
            }
            (Self::McpServer(a), Self::McpServer(b)) => a == b,
            (
                Self::Host(_) | Self::UrlPrefix { .. },
                Self::PathPrefix { .. } | Self::McpServer(_),
            )
            | (
                Self::PathPrefix { .. } | Self::McpServer(_),
                Self::Host(_) | Self::UrlPrefix { .. },
            )
            | (Self::PathPrefix { .. }, Self::McpServer(_))
            | (Self::McpServer(_), Self::PathPrefix { .. }) => false,
        }
    }
}

/// Whether some path lies under both prefixes: one is a segment prefix of
/// the other.
fn nested(a: &str, b: &str) -> bool {
    segment_prefix(a, b) || segment_prefix(b, a)
}

/// Whether `prefix` is `path` or an ancestor of it, by whole `/` segments.
fn segment_prefix(path: &str, prefix: &str) -> bool {
    match path.strip_prefix(prefix) {
        Some(rest) => rest.is_empty() || prefix.ends_with('/') || rest.starts_with('/'),
        None => false,
    }
}
