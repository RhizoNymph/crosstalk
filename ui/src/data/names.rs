//! Display names for channels, from their pattern or seed locator.
//!
//! Locators identify resources, not message content, so these names are
//! shown with `View`. A trailing `*` marks a prefix pattern.

use crosstalk_spec::derived::flow::resource::{Host, Locator, ResourcePattern};

use crosstalk_spec::interfaces::l8_surface::channels::{ChannelName, ChannelShape};

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

/// A channel's name from its shape: the pattern or the seed locator.
pub fn shape_name(shape: &ChannelShape) -> String {
    match shape {
        ChannelShape::Pattern(pattern) => pattern_name(pattern),
        ChannelShape::Seed(locator) => locator_name(locator),
    }
}

/// A batch-looked-up channel's name: its pattern when declared, its seed
/// locator when discovered.
pub fn channel_name(name: &ChannelName) -> String {
    shape_name(name.shape())
}

#[cfg(test)]
mod tests {
    use crosstalk_spec::observed::message::ToolName;

    use super::*;

    fn host(name: &str) -> Host {
        Host(name.to_owned())
    }

    #[test]
    fn names_locators() {
        let url = Locator::Url {
            scheme: "https".to_owned(),
            host: host("wiki.internal"),
            path: "/pages/plan".to_owned(),
            query: Some("rev=3".to_owned()),
        };
        assert_eq!(locator_name(&url), "wiki.internal/pages/plan?rev=3");
        let file = Locator::File {
            host: None,
            path: "/srv/shared/notes.md".to_owned(),
        };
        assert_eq!(locator_name(&file), "/srv/shared/notes.md");
        let remote = Locator::File {
            host: Some(host("build-1")),
            path: "/tmp/x".to_owned(),
        };
        assert_eq!(locator_name(&remote), "build-1:/tmp/x");
        let mcp = Locator::Mcp {
            server: "linear".to_owned(),
            tool: ToolName("get_issue".to_owned()),
            target: Some("ENG-12".to_owned()),
        };
        assert_eq!(locator_name(&mcp), "mcp:linear/get_issue:ENG-12");
        let opaque = Locator::Opaque {
            tool: ToolName("redis".to_owned()),
            key: "queue:jobs".to_owned(),
        };
        assert_eq!(locator_name(&opaque), "redis:queue:jobs");
    }

    #[test]
    fn names_patterns_with_prefix_marker() {
        assert_eq!(
            pattern_name(&ResourcePattern::Host(host("pastebin.com"))),
            "pastebin.com/*"
        );
        assert_eq!(
            pattern_name(&ResourcePattern::UrlPrefix {
                host: host("github.com"),
                path_prefix: "/acme/ops".to_owned()
            }),
            "github.com/acme/ops*"
        );
        assert_eq!(
            pattern_name(&ResourcePattern::PathPrefix {
                host: None,
                prefix: "/srv/shared".to_owned()
            }),
            "/srv/shared*"
        );
        assert_eq!(
            pattern_name(&ResourcePattern::McpServer("slack".to_owned())),
            "mcp:slack/*"
        );
    }
}
