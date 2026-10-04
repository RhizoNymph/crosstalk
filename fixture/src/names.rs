//! Locator and pattern text for `ChannelNode::locator_summary`, which the
//! spec defines as preformatted text and leaves the wording to the
//! implementation. The fixture words it as the UI names channels
//! (`ui/src/data/names.rs`), so the two read alike.

use crosstalk_spec::derived::flow::resource::{Host, Locator, ResourcePattern};

/// A locator as one line: host and path for URLs, the path for files, the
/// server and tool for MCP resources.
pub fn locator_name(locator: &Locator) -> String {
    match locator {
        Locator::Url {
            host, path, query, ..
        } => match query {
            Some(query) => format!("{}{path}?{query}", host.0),
            None => format!("{}{path}", host.0),
        },
        Locator::File { host, path } => with_host(host.as_ref(), path),
        Locator::Mcp {
            server,
            tool,
            target,
        } => match target {
            Some(target) => format!("mcp:{server}/{}:{target}", tool.0),
            None => format!("mcp:{server}/{}", tool.0),
        },
        Locator::Opaque { tool, key } => format!("{}:{key}", tool.0),
    }
}

/// A pattern as one line; a trailing `*` marks a prefix.
pub fn pattern_name(pattern: &ResourcePattern) -> String {
    match pattern {
        ResourcePattern::Exact(locator) => locator_name(locator),
        ResourcePattern::Host(host) => format!("{}/*", host.0),
        ResourcePattern::UrlPrefix { host, path_prefix } => format!("{}{path_prefix}*", host.0),
        ResourcePattern::PathPrefix { host, prefix } => {
            format!("{}*", with_host(host.as_ref(), prefix))
        }
        ResourcePattern::McpServer(server) => format!("mcp:{server}/*"),
    }
}

fn with_host(host: Option<&Host>, path: &str) -> String {
    match host {
        Some(host) => format!("{}:{path}", host.0),
        None => path.to_owned(),
    }
}
