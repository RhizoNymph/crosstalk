//! Carrier and route of a hit, from where it sits in the reader's input.
//!
//! - A user turn is `Direct(UserTurn)`, a system prompt `Direct(SystemPrompt)`.
//! - A tool result is a `Channel` when the call that produced it names a
//!   resource the reference can extract (a URL, or an absolute file path in
//!   a path-like argument), and `Direct(ToolResult(name))` otherwise. A
//!   relative path is not extracted: with no known working directory it is
//!   not a canonical resource.

use crosstalk_spec::derived::flow::resource::{Host, Locator};
use crosstalk_spec::observed::message::{
    AssistantPart, Message, MessageBody, ToolArguments, ToolCall, ToolCallId,
};
use serde_json::Value;

/// Argument names whose string value is a file path.
const PATH_KEYS: &[&str] = &[
    "path",
    "file",
    "file_path",
    "filepath",
    "filename",
    "target",
    "source_path",
    "target_path",
];

/// The tool call with id `call` among `request`'s assistant messages.
pub fn find_call<'a>(
    request: impl Iterator<Item = &'a Message>,
    call: &ToolCallId,
) -> Option<&'a ToolCall> {
    let mut found = None;
    for message in request {
        if let MessageBody::Assistant(parts) = &message.body {
            for part in parts {
                if let AssistantPart::ToolCall(tool_call) = part
                    && &tool_call.id == call
                {
                    found = Some(tool_call);
                }
            }
        }
    }
    found
}

/// The resource a tool call's arguments name, if the reference can extract
/// one.
pub fn extract_resource(call: &ToolCall) -> Option<Locator> {
    let ToolArguments::Json(json) = &call.arguments else {
        return None;
    };
    let value: Value = serde_json::from_str(&json.0).ok()?;
    let Value::Object(members) = value else {
        return None;
    };
    // Members are sorted (canonical JSON), so the choice is deterministic.
    for (name, member) in &members {
        let Value::String(text) = member else {
            continue;
        };
        if let Some(url) = parse_url(text) {
            return Some(url);
        }
        if PATH_KEYS.contains(&name.as_str()) && text.starts_with('/') {
            return Some(Locator::File {
                host: None,
                path: normalize_path(text),
            });
        }
    }
    None
}

/// A URL with scheme and host lowercased, the fragment dropped and query
/// parameters sorted.
pub fn parse_url(text: &str) -> Option<Locator> {
    let (scheme, rest) = text.split_once("://")?;
    let scheme = scheme.to_ascii_lowercase();
    if scheme != "http" && scheme != "https" {
        return None;
    }
    if rest.chars().any(char::is_whitespace) {
        return None;
    }
    let rest = rest.split('#').next().unwrap_or(rest);
    let (authority, path_and_query) = match rest.find('/') {
        Some(at) => (&rest[..at], &rest[at..]),
        None => (rest, "/"),
    };
    if authority.is_empty() {
        return None;
    }
    let host = authority.to_ascii_lowercase();
    let host = match (scheme.as_str(), host.rsplit_once(':')) {
        ("http", Some((name, "80"))) | ("https", Some((name, "443"))) => name.to_owned(),
        _ => host,
    };
    let (path, query) = match path_and_query.split_once('?') {
        Some((path, query)) => {
            let mut params: Vec<&str> = query.split('&').filter(|p| !p.is_empty()).collect();
            params.sort_unstable();
            (path, (!params.is_empty()).then(|| params.join("&")))
        }
        None => (path_and_query, None),
    };
    Some(Locator::Url {
        scheme,
        host: Host(host),
        path: path.to_owned(),
        query,
    })
}

/// An absolute path with `.` and `..` resolved and repeated slashes merged.
pub fn normalize_path(path: &str) -> String {
    let mut parts: Vec<&str> = Vec::new();
    for segment in path.split('/') {
        match segment {
            "" | "." => {}
            ".." => {
                parts.pop();
            }
            other => parts.push(other),
        }
    }
    format!("/{}", parts.join("/"))
}
