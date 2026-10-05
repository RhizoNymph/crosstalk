//! The route an injection takes into the victim: the tool call that read it.
//!
//! - `get_webpage(url)` reads a web page: a channel through the URL.
//!   AgentDojo strips the scheme before looking a page up, so a scheme-less
//!   URL is the same page; it is read as `http://`, the scheme the dataset's
//!   calls almost always use.
//! - `read_file(file_path)` reads the banking suite's flat file system: a
//!   channel through the file, rooted at `/`.
//! - Every other tool is keyed by something the eval cannot resolve to a
//!   canonical resource (an email, an event, a channel name, a review), so
//!   the injection arrives `Direct` in the tool result.

use crosstalk_spec::derived::flow::resource::Locator;

use super::schema::RawCall;
use crate::reference::route::{normalize_path, parse_url};
use crate::truth::RouteExpectation;

/// The expected route of content read by `call`.
pub fn expected_route(call: Option<&RawCall>) -> RouteExpectation {
    let Some(call) = call else {
        return RouteExpectation::Direct;
    };
    let argument = |name: &str| call.args.get(name).and_then(|value| value.as_str());
    let resource = match call.function.as_str() {
        "get_webpage" => argument("url").and_then(url),
        "read_file" => argument("file_path")
            .filter(|path| !path.is_empty())
            .map(|path| Locator::File {
                host: None,
                path: normalize_path(&format!("/{path}")),
            }),
        _ => None,
    };
    match resource {
        Some(resource) => RouteExpectation::Channel { resource },
        None => RouteExpectation::Direct,
    }
}

fn url(text: &str) -> Option<Locator> {
    if text.contains("://") {
        parse_url(text)
    } else {
        parse_url(&format!("http://{text}"))
    }
}
