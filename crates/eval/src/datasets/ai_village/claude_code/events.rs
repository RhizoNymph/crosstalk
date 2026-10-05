//! What the village MCP server's `get_events` tool returned: the events
//! the Claude Code agent had not yet seen, as pretty-printed JSON.
//!
//! ```json
//! {
//!   "events": [
//!     { "actionType": "AGENT_TALK", "agentName": "GPT-5.2",
//!       "content": "…", "createdAt": "3/11/2026, 10:31:38 AM PDT",
//!       "id": "8477d292-…" },
//!     …
//!   ],
//!   "hasMore": false,
//!   "agentStatus": { … }
//! }
//! ```
//!
//! Each `AGENT_TALK` event is another agent's chat message, keyed by its
//! `events.id`. Its content sits in the text JSON-escaped; [`talks`] finds
//! each one's escaped bytes, so a label can point at them exactly.

use serde_json::Value;

use super::super::text::{find, json_escape};

/// The village MCP server's name and the tool, as Claude Code names them.
pub const SERVER: &str = "village";
pub const GET_EVENTS: &str = "mcp__village__get_events";
pub const CHAT_MESSAGE: &str = "mcp__village__chat_message";

/// One chat message a `get_events` result delivered.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Talk {
    pub event: String,
    pub speaker: String,
    pub content: String,
    /// The content as it sits in the result: JSON-escaped.
    pub escaped: String,
    /// Its byte range in the result text, when found.
    pub range: Option<(usize, usize)>,
}

/// Why a result could not be read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Unreadable {
    NotJson,
    NoEvents,
}

/// The `AGENT_TALK` events in a `get_events` result text, with ids.
/// Events without an id cannot be keyed and are counted by the caller from
/// `without_id`.
pub fn talks(text: &str) -> Result<(Vec<Talk>, usize), Unreadable> {
    let value: Value = serde_json::from_str(text).map_err(|_| Unreadable::NotJson)?;
    let events = value
        .get("events")
        .and_then(Value::as_array)
        .ok_or(Unreadable::NoEvents)?;
    let mut out = Vec::new();
    let mut without_id = 0;
    for event in events {
        if event.get("actionType").and_then(Value::as_str) != Some("AGENT_TALK") {
            continue;
        }
        let string = |name: &str| event.get(name).and_then(Value::as_str);
        let (Some(id), Some(speaker), Some(content)) =
            (string("id"), string("agentName"), string("content"))
        else {
            without_id += 1;
            continue;
        };
        let escaped = json_escape(content);
        let anchor = format!("\"id\": \"{id}\"");
        let before = text.find(&anchor);
        out.push(Talk {
            event: id.to_owned(),
            speaker: speaker.to_owned(),
            content: content.to_owned(),
            range: find(text, &escaped, before),
            escaped,
        });
    }
    Ok((out, without_id))
}

/// The `content` argument of a `chat_message` call's canonical arguments.
pub fn chat_content(arguments: &str) -> Option<String> {
    let value: Value = serde_json::from_str(arguments).ok()?;
    value
        .get("content")
        .or_else(|| value.get("message"))
        .and_then(Value::as_str)
        .map(str::to_owned)
}
