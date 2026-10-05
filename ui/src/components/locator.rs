//! Resource locators and patterns as text.

use crosstalk_spec::derived::flow::resource::{Locator, ResourcePattern};
use topcoat::Result;
use topcoat::view::{View, component, view};

/// A locator as one line: a URL, a path, an MCP or tool key, or a
/// repository's `host/owner/name`.
pub fn format_locator(locator: &Locator) -> String {
    match locator {
        Locator::Url {
            scheme,
            host,
            path,
            query,
        } => match query {
            Some(query) => format!("{scheme}://{}{path}?{query}", host.0),
            None => format!("{scheme}://{}{path}", host.0),
        },
        Locator::File {
            host: Some(host),
            path,
        } => format!("{}:{path}", host.0),
        Locator::File { host: None, path } => path.clone(),
        Locator::Mcp {
            server,
            tool,
            target: Some(target),
        } => format!("mcp:{server}/{} {target}", tool.0),
        Locator::Mcp {
            server,
            tool,
            target: None,
        } => format!("mcp:{server}/{}", tool.0),
        Locator::Opaque { tool, key } => format!("{}: {key}", tool.0),
        Locator::Repository { host, owner, name } => format!("{}/{owner}/{name}", host.0),
    }
}

/// What kind of resource a locator names.
pub fn locator_kind(locator: &Locator) -> &'static str {
    match locator {
        Locator::Url { .. } => "url",
        Locator::File { .. } => "file",
        Locator::Mcp { .. } => "mcp",
        Locator::Opaque { .. } => "tool",
        Locator::Repository { .. } => "repository",
    }
}

/// A pattern as one line; `…` stands for any rest of the path.
pub fn format_pattern(pattern: &ResourcePattern) -> String {
    match pattern {
        ResourcePattern::Exact(locator) => format_locator(locator),
        ResourcePattern::Host(host) => format!("{}/…", host.0),
        ResourcePattern::UrlPrefix { host, path_prefix } => {
            format!("{}{}/…", host.0, path_prefix.trim_end_matches('/'))
        }
        ResourcePattern::PathPrefix {
            host: Some(host),
            prefix,
        } => format!("{}:{}/…", host.0, prefix.trim_end_matches('/')),
        ResourcePattern::PathPrefix { host: None, prefix } => {
            format!("{}/…", prefix.trim_end_matches('/'))
        }
        ResourcePattern::McpServer(server) => format!("mcp:{server}/…"),
    }
}

pub fn pattern_kind(pattern: &ResourcePattern) -> &'static str {
    match pattern {
        ResourcePattern::Exact(_) => "exact",
        ResourcePattern::Host(_) => "host",
        ResourcePattern::UrlPrefix { .. } => "url prefix",
        ResourcePattern::PathPrefix { .. } => "path prefix",
        ResourcePattern::McpServer(_) => "mcp server",
    }
}

#[component]
pub async fn locator_text(locator: &Locator) -> Result<impl View> {
    let text = format_locator(locator);
    let kind = locator_kind(locator);
    Ok(view! {
        <span class="inline-flex min-w-0 items-baseline gap-1.5">
            <span class="text-[10px] uppercase tracking-wide text-zinc-400">(kind)</span>
            <span class="truncate font-mono text-xs" title=(text.clone())>(text)</span>
        </span>
    })
}

#[component]
pub async fn pattern_text(pattern: &ResourcePattern) -> Result<impl View> {
    let text = format_pattern(pattern);
    let kind = pattern_kind(pattern);
    Ok(view! {
        <span class="inline-flex min-w-0 items-baseline gap-1.5">
            <span class="text-[10px] uppercase tracking-wide text-zinc-400">(kind)</span>
            <span class="truncate font-mono text-xs" title=(text.clone())>(text)</span>
        </span>
    })
}

#[cfg(test)]
mod tests {
    use crosstalk_spec::derived::flow::resource::Host;
    use crosstalk_spec::observed::message::ToolName;

    use super::*;

    #[test]
    fn locators_read_like_their_source() {
        let url = Locator::Url {
            scheme: "https".into(),
            host: Host("wiki.example.org".into()),
            path: "/team/notes".into(),
            query: Some("a=1".into()),
        };
        assert_eq!(
            format_locator(&url),
            "https://wiki.example.org/team/notes?a=1"
        );
        let file = Locator::File {
            host: None,
            path: "/srv/shared/x.md".into(),
        };
        assert_eq!(format_locator(&file), "/srv/shared/x.md");
        let mcp = Locator::Mcp {
            server: "notion".into(),
            tool: ToolName("read_page".into()),
            target: Some("abc".into()),
        };
        assert_eq!(format_locator(&mcp), "mcp:notion/read_page abc");
    }

    #[test]
    fn patterns_mark_the_open_end() {
        let prefix = ResourcePattern::UrlPrefix {
            host: Host("wiki.example.org".into()),
            path_prefix: "/team/".into(),
        };
        assert_eq!(format_pattern(&prefix), "wiki.example.org/team/…");
        assert_eq!(
            format_pattern(&ResourcePattern::McpServer("notion".into())),
            "mcp:notion/…"
        );
        assert_eq!(pattern_kind(&prefix), "url prefix");
    }
}
