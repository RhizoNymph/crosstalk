//! HTTP requests: the one place URL accesses are decided, for every tool
//! that reaches a URL (fetch tools, `curl` and `wget`, MCP fetch servers,
//! URLs scanned from a prompt).
//!
//! A site rule ([`crate::extract::sites`]) that recognizes the request
//! decides its accesses (a wiki API edit writes the page it names, whatever
//! its URL). Otherwise a writing method (`POST`, `PUT`, `PATCH`, `DELETE`)
//! writes the URL, and any other request reads it when its body reaches the
//! tool result.
//!
//! **HTTP tools** (`tool_candidates`) are the contract for a tool whose
//! call names its `url` and `method` (`http_request {method, url, body?}`,
//! which the eval converter emits; a `url` with no scheme that names a host
//! is read as `https://`, `flow.extract.bare-host-url-is-https`). The
//! method alone decides the op: `GET`
//! and `HEAD` read (the tool result is the read part), `POST`, `PUT`,
//! `PATCH` and `DELETE` write (the body argument, the first of `body`,
//! `content`, `text` and `data`, is what is written), and any other method
//! is no access (`flow.extract.http-method-op`). The locator is the
//! canonical URL, never the tool's name, so one URL is one resource across
//! tools and methods (`flow.resource.http-url-tool-independent`); a site
//! rule names the page or file instead only when it agrees with the
//! method's op (a wiki API edit `POST` writes the page it names, so a later
//! `GET` of the article reads the same resource).

use serde_json::Value;

use crosstalk_spec::derived::flow::access::{AccessKind, Extraction};
use crosstalk_spec::derived::flow::resource::Locator;

use crate::extract::args::{ArgError, Args};
use crate::extract::catalog::HttpTool;
use crate::extract::fetch;
use crate::extract::op::Candidate;
use crate::extract::resource::tool_url_locator;
use crate::extract::sites::{SiteAccess, SitesConfig};

/// The arguments an HTTP tool's body may be in; the first present is the
/// body.
pub const BODY_KEYS: [&str; 4] = ["body", "content", "text", "data"];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Method {
    Get,
    Head,
    Post,
    Put,
    Patch,
    Delete,
    /// Any other method: neither reads nor writes are assumed.
    Other,
}

impl Method {
    pub fn parse(text: &str) -> Self {
        match text.to_ascii_uppercase().as_str() {
            "GET" => Self::Get,
            "HEAD" => Self::Head,
            "POST" => Self::Post,
            "PUT" => Self::Put,
            "PATCH" => Self::Patch,
            "DELETE" => Self::Delete,
            _ => Self::Other,
        }
    }

    pub fn writes(self) -> bool {
        matches!(self, Self::Post | Self::Put | Self::Patch | Self::Delete)
    }

    pub fn reads(self) -> bool {
        matches!(self, Self::Get | Self::Head)
    }
}

/// One request a tool makes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HttpRequest {
    /// A canonical `Locator::Url`.
    pub url: Locator,
    pub method: Method,
    /// Form fields sent in the body, decoded (`-d a=1&b=2`, `-F a=1`).
    pub form: Vec<(String, String)>,
}

impl HttpRequest {
    pub fn get(url: Locator) -> Self {
        Self {
            url,
            method: Method::Get,
            form: Vec::new(),
        }
    }

    /// The query's parameters followed by the form's, decoded.
    pub fn params(&self) -> Vec<(String, String)> {
        let mut params: Vec<(String, String)> = match &self.url {
            Locator::Url {
                query: Some(query), ..
            } => url::form_urlencoded::parse(query.as_bytes())
                .map(|(name, value)| (name.into_owned(), value.into_owned()))
                .collect(),
            _ => Vec::new(),
        };
        params.extend(self.form.iter().cloned());
        params
    }
}

/// The accesses `request` implies. `body_to_result`: the response body
/// reaches the tool result (it is not saved to a file).
pub(crate) fn candidates(
    request: &HttpRequest,
    body_to_result: bool,
    via: Extraction,
    sites: &SitesConfig,
) -> Vec<Candidate> {
    let (kind, locators) = match sites.apply(request) {
        Some(SiteAccess { kind, locators }) => (kind, locators),
        None if request.method.writes() => (AccessKind::Write, vec![request.url.clone()]),
        None if request.method.reads() => (AccessKind::Read, vec![request.url.clone()]),
        None => return Vec::new(),
    };
    match kind {
        AccessKind::Write => locators
            .into_iter()
            .map(|locator| Candidate::write(locator, via))
            .collect(),
        AccessKind::Read if body_to_result => locators
            .into_iter()
            .map(|locator| Candidate::read(locator, via))
            .collect(),
        AccessKind::Read => Vec::new(),
    }
}

/// Form fields of a URL-encoded body (`a=1&b=x+y`), decoded.
pub fn form_fields(body: &str) -> Vec<(String, String)> {
    url::form_urlencoded::parse(body.as_bytes())
        .map(|(name, value)| (name.into_owned(), value.into_owned()))
        .collect()
}

/// The access an HTTP tool call names: one access per locator, its op from
/// the method alone, `Structured`.
pub(crate) fn tool_candidates(
    tool: HttpTool,
    args: &Args,
    sites: &SitesConfig,
) -> Result<Vec<Candidate>, ArgError> {
    let (Some(url), Some(method)) = (args.opt_str("url")?, args.opt_str("method")?) else {
        return match tool.fallback {
            Some(fetch) => fetch::candidates(fetch, args, sites),
            None => Err(ArgError::Missing("url and method".to_owned())),
        };
    };
    let url = tool_url_locator(url).map_err(|error| ArgError::invalid("url", error))?;
    let method = Method::parse(method);
    let kind = if method.reads() {
        AccessKind::Read
    } else if method.writes() {
        AccessKind::Write
    } else {
        return Ok(Vec::new());
    };
    let request = HttpRequest {
        url,
        method,
        form: body_fields(args),
    };
    let locators = match sites.apply(&request) {
        Some(site) if site.kind == kind && !site.locators.is_empty() => site.locators,
        Some(_) | None => vec![request.url],
    };
    Ok(locators
        .into_iter()
        .map(|locator| match kind {
            AccessKind::Read => Candidate::read(locator, Extraction::Structured),
            AccessKind::Write => Candidate::write(locator, Extraction::Structured),
        })
        .collect())
}

/// The body's fields, for site rules: a URL-encoded string's fields, or a
/// JSON object's string and number members.
fn body_fields(args: &Args) -> Vec<(String, String)> {
    match BODY_KEYS.iter().find_map(|key| args.get(key)) {
        Some(Value::String(body)) => form_fields(body),
        Some(Value::Object(members)) => members
            .iter()
            .filter_map(|(name, value)| match value {
                Value::String(text) => Some((name.clone(), text.clone())),
                Value::Number(number) => Some((name.clone(), number.to_string())),
                _ => None,
            })
            .collect(),
        _ => Vec::new(),
    }
}
