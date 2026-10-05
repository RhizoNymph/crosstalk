//! Running the model's tool calls: `http_request` against the wiki.
//!
//! A call is read as a [`WikiCall`] against the configured wiki base URL: a
//! GET of `<wiki>/pages/<name>` reads the page, a PUT with a `body` writes
//! it. Anything else (another tool, another method, another URL, a PUT
//! without a body) gets an error tool_result and touches nothing.
//!
//! A write the wiki accepted is reported at once ([`Event::WikiWrite`]). A
//! read is handed back as a [`PendingRead`]: the agent reports it with the
//! request that carries its result.

use bytes::Bytes;
use crosstalk_testkit::corpus::http::Headers;
use hyper::Method;
use hyper::header::{HeaderName, HeaderValue};
use serde_json::Value;
use tokio::sync::mpsc;

use crate::http::request;
use crate::protocol::{PageSlug, WikiCall, page_url};
use crate::wiki::{AUTHOR_HEADER, VERSION_HEADER};

use super::agent::{Agent, Shared};
use super::conversation::{PendingTools, ToolCall, ToolResult};
use super::stats::Event;
use super::truth::WriteRecord;

/// A read that returned, waiting for the request that carries its result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingRead {
    pub tool_use_id: String,
    pub page: PageSlug,
    pub url: String,
    /// The GET tool_use's input, as the model sent it.
    pub input: Value,
    pub at_ms: u64,
    pub read_at_unix_ms: u64,
    /// The writer (the wiki's author header) and version; `None` when the
    /// page did not exist.
    pub found: Option<(String, u64)>,
}

/// Runs every pending tool call, in order. `turn` is the turn of the
/// request whose answer made the calls, in conversation `session`.
pub async fn execute(
    agent: &Agent,
    shared: &Shared,
    events: &mpsc::Sender<Event>,
    session: &str,
    turn: u32,
    pending: &PendingTools,
) -> (Vec<ToolResult>, Vec<PendingRead>) {
    let mut results = Vec::with_capacity(pending.calls().len());
    let mut reads = Vec::new();
    for call in pending.calls() {
        let result = match WikiCall::parse(&call.name, &call.input, &shared.config.wiki) {
            Err(refused) => call.result(format!("Error: {refused}."), true),
            Ok(WikiCall::Write { page, body }) => {
                let origin = Origin {
                    session,
                    turn,
                    call,
                };
                write_page(agent, shared, events, origin, page, &body).await
            }
            Ok(WikiCall::Read { page }) => {
                let (result, read) = read_page(shared, events, call, page).await;
                reads.extend(read);
                result
            }
        };
        results.push(result);
    }
    (results, reads)
}

/// Where a write's tool call came from.
struct Origin<'a> {
    session: &'a str,
    turn: u32,
    call: &'a ToolCall,
}

async fn write_page(
    agent: &Agent,
    shared: &Shared,
    events: &mpsc::Sender<Event>,
    origin: Origin<'_>,
    page: PageSlug,
    content: &str,
) -> ToolResult {
    let call = origin.call;
    let mut headers = Headers::new();
    if let Ok(author) = HeaderValue::from_str(&agent.name) {
        headers.push(HeaderName::from_static(AUTHOR_HEADER), author);
    }
    headers.push(
        HeaderName::from_static("content-type"),
        HeaderValue::from_static("text/plain; charset=utf-8"),
    );
    let target = format!("/pages/{page}");
    let Ok(request) = request(Method::PUT, &target, headers, content.to_owned()) else {
        return call.result("Error: bad page name.".to_owned(), true);
    };
    let response = match shared.wiki.send(&request).await {
        Ok(response) if response.status.is_success() => response,
        Ok(response) => {
            let _ = events.send(Event::WikiError).await;
            return call.result(
                format!("Error: the wiki answered {}.", response.status),
                true,
            );
        }
        Err(error) => {
            tracing::debug!(%error, "wiki write failed");
            let _ = events.send(Event::WikiError).await;
            return call.result("Error: the wiki is unreachable.".to_owned(), true);
        }
    };
    let written_at = shared.clock.now();
    let Some(version) = serde_json::from_slice::<Value>(&response.body)
        .ok()
        .and_then(|v| v.get("version").and_then(Value::as_u64))
    else {
        // Accepted, but with no version the write cannot be paired with
        // its reads: count it as a wiki error, not a write.
        tracing::warn!(page = %page, "the wiki's answer to a write has no version");
        let _ = events.send(Event::WikiError).await;
        return call.result(format!("Saved `{page}`."), false);
    };
    let _ = events
        .send(Event::WikiWrite(WriteRecord {
            writer: agent.name.clone(),
            key_group: agent.key_group,
            session: origin.session.to_owned(),
            turn: origin.turn,
            tool_use_id: call.id.clone(),
            page: page.clone(),
            version,
            written_at_unix_ms: written_at.unix_ms,
        }))
        .await;
    call.result(
        format!(
            "Saved `{page}` (version {version}, {} bytes).",
            content.len()
        ),
        false,
    )
}

async fn read_page(
    shared: &Shared,
    events: &mpsc::Sender<Event>,
    call: &ToolCall,
    page: PageSlug,
) -> (ToolResult, Option<PendingRead>) {
    let target = format!("/pages/{page}");
    let Ok(request) = request(Method::GET, &target, Headers::new(), Bytes::new()) else {
        return (call.result("Error: bad page name.".to_owned(), true), None);
    };
    let response = match shared.wiki.send(&request).await {
        Ok(response) => response,
        Err(error) => {
            tracing::debug!(%error, "wiki read failed");
            let _ = events.send(Event::WikiError).await;
            let result = call.result("Error: the wiki is unreachable.".to_owned(), true);
            return (result, None);
        }
    };
    let read_at = shared.clock.now();
    let pending = |found| PendingRead {
        tool_use_id: call.id.clone(),
        url: page_url(&shared.config.wiki, &page),
        page: page.clone(),
        input: call.input.clone(),
        at_ms: read_at.at_ms,
        read_at_unix_ms: read_at.unix_ms,
        found,
    };
    if response.status.is_success() {
        let author = response
            .headers
            .get_str(AUTHOR_HEADER)
            .unwrap_or("anonymous")
            .to_owned();
        let version = response
            .headers
            .get_str(VERSION_HEADER)
            .and_then(|v| v.parse().ok())
            .unwrap_or(0);
        // The wiki stores UTF-8 only, so this is the page verbatim.
        let text = String::from_utf8_lossy(&response.body).into_owned();
        (
            call.result(text, false),
            Some(pending(Some((author, version)))),
        )
    } else if response.status == hyper::StatusCode::NOT_FOUND {
        let result = call.result(format!("Page `{page}` does not exist yet."), true);
        (result, Some(pending(None)))
    } else {
        let _ = events.send(Event::WikiError).await;
        let result = call.result(
            format!("Error: the wiki answered {}.", response.status),
            true,
        );
        (result, None)
    }
}
