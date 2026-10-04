//! A channel's `locator_summary`: what it covers, as one line of text.
//!
//! `ChannelNode::locator_summary` is "the pattern of a channel declared
//! before traffic, otherwise its seed's locator, with the count of further
//! resources when there are any (`https://wiki.example/a (+12)`)". The spec
//! gives locators and patterns no text form, so this module is it:
//!
//! | Value | Text |
//! | --- | --- |
//! | `Locator::Url` | `scheme://host/path?query` |
//! | `Locator::File` | `path`, or `host:path` |
//! | `Locator::Mcp` | `mcp:server/tool`, `/target` when it has one |
//! | `Locator::Opaque` | `tool:key` |
//! | `ResourcePattern::Exact` | its locator's text |
//! | `ResourcePattern::Host` | `host/*` |
//! | `ResourcePattern::UrlPrefix` | `host/prefix*` |
//! | `ResourcePattern::PathPrefix` | `prefix*`, or `host:prefix*` |
//! | `ResourcePattern::McpServer` | `mcp:server/*` |

use crosstalk_spec::derived::flow::resource::{Locator, ResourcePattern};
use crosstalk_spec::ids::ChannelId;
use crosstalk_spec::support::NonBlank;

/// The text of a locator.
pub fn locator_text(locator: &Locator) -> String {
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
        Locator::File { host: None, path } => path.clone(),
        Locator::File {
            host: Some(host),
            path,
        } => format!("{}:{path}", host.0),
        Locator::Mcp {
            server,
            tool,
            target: None,
        } => format!("mcp:{server}/{}", tool.0),
        Locator::Mcp {
            server,
            tool,
            target: Some(target),
        } => format!("mcp:{server}/{}/{target}", tool.0),
        Locator::Opaque { tool, key } => format!("{}:{key}", tool.0),
    }
}

/// The text of a pattern.
pub fn pattern_text(pattern: &ResourcePattern) -> String {
    match pattern {
        ResourcePattern::Exact(locator) => locator_text(locator),
        ResourcePattern::Host(host) => format!("{}/*", host.0),
        ResourcePattern::UrlPrefix { host, path_prefix } => format!("{}{path_prefix}*", host.0),
        ResourcePattern::PathPrefix { host: None, prefix } => format!("{prefix}*"),
        ResourcePattern::PathPrefix {
            host: Some(host),
            prefix,
        } => format!("{}:{prefix}*", host.0),
        ResourcePattern::McpServer(server) => format!("mcp:{server}/*"),
    }
}

/// `text`, with ` (+n)` when `further` is not zero; the id summary the
/// edge store's defaults use when the text is blank.
pub fn summary(channel: ChannelId, text: &str, further: u64) -> NonBlank {
    let line = if further == 0 {
        text.to_owned()
    } else {
        format!("{text} (+{further})")
    };
    NonBlank::new(&line).unwrap_or_else(|_| id_summary(channel))
}

/// `channel <ULID>`: the summary of a channel whose text is unknown.
pub fn id_summary(channel: ChannelId) -> NonBlank {
    // Provably infallible: the text starts with "channel", so it is never
    // blank.
    #[allow(clippy::expect_used)]
    NonBlank::new(&format!("channel {}", channel.ulid_text()))
        .expect("a summary starting with \"channel\" is never blank")
}
