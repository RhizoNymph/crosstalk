//! The synthetic tool shape agents' reads and writes take.
//!
//! Centralised so the tool name and argument shape can change in one place.
//! An L5 extractor pulls the page URL from a string argument; the write's
//! inserted text and the read's page body carry the content that travels.
//!
//! Default shape (HTTP-style, resource in the `url` argument so a read and a
//! write of one page land on the same [`Locator::Url`]):
//!
//! - read: `http_request {"method":"GET","url":<page>}`;
//! - write: `http_request {"method":"POST","url":<page>,"body":<inserted text>}`.
//!
//! Never MCP-shaped: `Locator::Mcp` carries the tool name, so a read and a
//! write would resolve to different resources.
//!
//! [`Locator::Url`]: crosstalk_spec::derived::flow::resource::Locator

use serde_json::json;

/// The tool name both a read and a write use.
pub const TOOL: &str = "http_request";

/// The read call's arguments: a `GET` of the page URL. Its result is the
/// page body (INV-269: the matched text sits in the read's tool result).
pub fn read_args(url: &str) -> serde_json::Value {
    json!({ "method": "GET", "url": url })
}

/// The write call's arguments: a `POST` to the page URL whose body is the
/// lines this revision inserted.
pub fn write_args(url: &str, body: &str) -> serde_json::Value {
    json!({ "method": "POST", "url": url, "body": body })
}
