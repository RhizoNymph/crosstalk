//! URLs: normalization and the URL locator
//! (`flow.resource.url-normalization`).
//!
//! The [`url`] crate parses (WHATWG): it lowercases the scheme and a special
//! scheme's host (IDNA to punycode), drops the default port and resolves
//! `.`/`..` path segments. On top of that, a locator:
//! - drops the fragment and any user info (credentials never become part of
//!   a resource's identity, nor get stored);
//! - lowercases a non-special scheme's host and drops a domain's trailing
//!   dot;
//! - normalizes percent-encoding in the path and query (unreserved
//!   characters decoded, hex digits upper-cased, RFC 3986 §6.2.2);
//! - sorts the query parameters, keeping each one's text, and drops empty
//!   ones; an empty query is no query.

use crosstalk_spec::derived::flow::resource::{Host, Locator};
use url::Url;

/// Why text is not a URL a locator can be made from.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum UrlError {
    #[error("not a URL: {0}")]
    Parse(#[from] url::ParseError),
    #[error("the URL has no host")]
    NoHost,
}

/// The canonical locator of `text`.
pub fn url_locator(text: &str) -> Result<Locator, UrlError> {
    let url = Url::parse(text.trim())?;
    let host = url
        .host_str()
        .filter(|host| !host.is_empty())
        .ok_or(UrlError::NoHost)?;
    let mut host = host.to_ascii_lowercase();
    if host.len() > 1 && host.ends_with('.') {
        host.pop();
    }
    if let Some(port) = url.port() {
        host = format!("{host}:{port}");
    }
    let path = match url.path() {
        "" => "/".to_owned(),
        path => normalize_percent(path),
    };
    Ok(Locator::Url {
        scheme: url.scheme().to_owned(),
        host: Host(host),
        path,
        query: url.query().and_then(sorted_query),
    })
}

/// The URL text a locator was made from, in canonical form: parsing it
/// again gives the same locator. `None` for a locator that is not a URL.
pub fn url_text(locator: &Locator) -> Option<String> {
    match locator {
        Locator::Url {
            scheme,
            host,
            path,
            query,
        } => {
            let mut text = format!("{scheme}://{}{path}", host.0);
            if let Some(query) = query {
                text.push('?');
                text.push_str(query);
            }
            Some(text)
        }
        Locator::File { .. }
        | Locator::Mcp { .. }
        | Locator::Opaque { .. }
        | Locator::Repository { .. } => None,
    }
}

fn sorted_query(query: &str) -> Option<String> {
    let mut parameters: Vec<String> = query
        .split('&')
        .filter(|parameter| !parameter.is_empty())
        .map(normalize_percent)
        .collect();
    if parameters.is_empty() {
        return None;
    }
    parameters.sort();
    Some(parameters.join("&"))
}

/// Percent-encoding in canonical form: an escaped unreserved character is
/// decoded, every other escape keeps its byte with upper-case hex. A `%` not
/// followed by two hex digits is kept as it is.
fn normalize_percent(text: &str) -> String {
    let bytes = text.as_bytes();
    let mut out = String::with_capacity(text.len());
    let mut index = 0;
    while index < bytes.len() {
        let escaped = bytes
            .get(index + 1..index + 3)
            .filter(|_| bytes[index] == b'%')
            .and_then(|hex| Some(hex_value(hex[0])? * 16 + hex_value(hex[1])?));
        match escaped {
            Some(byte) if is_unreserved(byte) => {
                out.push(char::from(byte));
                index += 3;
            }
            Some(byte) => {
                out.push_str(&format!("%{byte:02X}"));
                index += 3;
            }
            None => {
                // Copy the whole UTF-8 character starting here.
                let rest = &text[index..];
                let Some(character) = rest.chars().next() else {
                    break;
                };
                out.push(character);
                index += character.len_utf8();
            }
        }
    }
    out
}

fn hex_value(digit: u8) -> Option<u8> {
    match digit {
        b'0'..=b'9' => Some(digit - b'0'),
        b'a'..=b'f' => Some(digit - b'a' + 10),
        b'A'..=b'F' => Some(digit - b'A' + 10),
        _ => None,
    }
}

fn is_unreserved(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.' | b'_' | b'~')
}

/// URLs written in free text: every `http://` or `https://` run up to
/// whitespace or a delimiter, with trailing sentence punctuation removed.
pub fn scan_urls(text: &str) -> Vec<&str> {
    let mut found = Vec::new();
    let mut rest = text;
    while let Some(start) = find_scheme(rest) {
        let candidate = &rest[start..];
        let end = candidate
            .find(|c: char| c.is_whitespace() || matches!(c, '<' | '>' | '"' | '\'' | '`' | '|'))
            .unwrap_or(candidate.len());
        let url = candidate[..end].trim_end_matches(['.', ',', ';', ':', '!', '?', ')', ']', '}']);
        if url.len() > "http://".len() {
            found.push(url);
        }
        rest = &candidate[end.max(1)..];
    }
    found
}

fn find_scheme(text: &str) -> Option<usize> {
    let http = text.find("http://");
    let https = text.find("https://");
    match (http, https) {
        (Some(a), Some(b)) => Some(a.min(b)),
        (a, b) => a.or(b),
    }
}
