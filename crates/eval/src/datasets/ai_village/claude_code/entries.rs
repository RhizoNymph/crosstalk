//! Claude Code's raw Agent SDK entries, read and ordered.
//!
//! `claude_code_messages` holds one row per SDK entry of the "Opus 4.5
//! (Claude Code)" agent. The rows are sorted by UUID, so [`load`] reads them
//! all (the table is small) and orders them by session, time and row id.
//! Each entry becomes an [`Entry`]:
//!
//! | `message_type` | Content | [`EntryKind`] |
//! | --- | --- | --- |
//! | `assistant` | one content block of a model message (`message.id` names the call) | `Assistant` |
//! | `user` | `tool_result` blocks | `ToolResults` |
//! | `user` | a text block (the compaction summary) or a string (a prompt) | `UserText` |
//! | `system` / `compact_boundary` | the context was compacted | `Boundary` |
//! | `result` | a query ended | `QueryEnd` |
//! | anything else (`init`, `status`) | | `Other` |

use std::path::Path;

use crosstalk_spec::observed::message::{AssistantPart, ToolResult};
use crosstalk_spec::support::Timestamp;
use serde_json::Value;

use super::super::AiVillageError;
use super::super::provider::anthropic;
use super::super::schema::ClaudeCodeRow;
use super::super::stream::{Table, decode};
use super::super::time::parse_timestamp;

/// Anthropic usage counts of one call, as the SDK reports them.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Usage {
    pub input: u64,
    pub output: u64,
    pub cache_read: u64,
    pub cache_write: u64,
}

impl Usage {
    fn read(raw: &Value) -> Option<Self> {
        let count = |name: &str| raw.get(name).and_then(Value::as_u64).unwrap_or(0);
        raw.is_object().then(|| Self {
            input: count("input_tokens"),
            output: count("output_tokens"),
            cache_read: count("cache_read_input_tokens"),
            cache_write: count("cache_creation_input_tokens"),
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum EntryKind {
    Assistant {
        message_id: String,
        model: String,
        parts: Vec<AssistantPart>,
        stop: Option<String>,
        usage: Option<Usage>,
    },
    /// Tool results, with each block's index in the entry's content.
    ToolResults(Vec<(usize, ToolResult)>),
    UserText(String),
    Boundary,
    QueryEnd,
    Other,
}

/// One SDK entry.
#[derive(Debug, Clone, PartialEq)]
pub struct Entry {
    /// The row id (`claude_code_messages.id`).
    pub row: String,
    pub session: String,
    pub at: Timestamp,
    pub kind: EntryKind,
}

/// One row as an entry; `None` for a row without a readable time.
pub fn entry(row: ClaudeCodeRow) -> Option<Entry> {
    let at = parse_timestamp(&row.created_at).ok()?;
    let message = row.content.get("message");
    let kind = match row.message_type.as_str() {
        "assistant" => {
            let message = message.unwrap_or(&Value::Null);
            let string = |name: &str| {
                message
                    .get(name)
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_owned()
            };
            EntryKind::Assistant {
                message_id: string("id"),
                model: string("model"),
                parts: message
                    .get("content")
                    .and_then(Value::as_array)
                    .into_iter()
                    .flatten()
                    .filter_map(anthropic::block)
                    .collect(),
                stop: message
                    .get("stop_reason")
                    .and_then(Value::as_str)
                    .map(str::to_owned),
                usage: message.get("usage").and_then(Usage::read),
            }
        }
        "user" => match message.and_then(|m| m.get("content")) {
            Some(Value::String(text)) => EntryKind::UserText(text.clone()),
            Some(Value::Array(blocks)) => {
                let results: Vec<(usize, ToolResult)> = blocks
                    .iter()
                    .enumerate()
                    .filter(|(_, block)| {
                        block.get("type").and_then(Value::as_str) == Some("tool_result")
                    })
                    .map(|(at, block)| (at, anthropic::tool_result(block)))
                    .collect();
                if results.is_empty() {
                    let text: Vec<&str> = blocks
                        .iter()
                        .filter_map(|block| block.get("text").and_then(Value::as_str))
                        .collect();
                    EntryKind::UserText(text.join("\n"))
                } else {
                    EntryKind::ToolResults(results)
                }
            }
            _ => EntryKind::Other,
        },
        "system" if row.message_subtype.as_deref() == Some("compact_boundary") => {
            EntryKind::Boundary
        }
        "result" => EntryKind::QueryEnd,
        _ => EntryKind::Other,
    };
    Some(Entry {
        row: row.id,
        session: row.sdk_session_id,
        at,
        kind,
    })
}

/// Every entry of the table, ordered by session, time and row id.
pub fn load(root: &Path) -> Result<Vec<Entry>, AiVillageError> {
    let mut entries = Vec::new();
    Table::ClaudeCodeMessages.scan::<AiVillageError>(root, |line| {
        let row: ClaudeCodeRow = decode(Table::ClaudeCodeMessages, line)?;
        entries.extend(entry(row));
        Ok(())
    })?;
    entries.sort_by(|a, b| (&a.session, a.at, &a.row).cmp(&(&b.session, b.at, &b.row)));
    Ok(entries)
}

/// The entries of each context: a session cut at its compaction
/// boundaries. Each range starts after a boundary (or at the session's
/// first entry) and ends at the next boundary (excluded).
pub fn contexts(entries: &[Entry]) -> Vec<std::ops::Range<usize>> {
    let mut out = Vec::new();
    let mut start = 0;
    for (at, entry) in entries.iter().enumerate() {
        let new_session = at > 0 && entries[at - 1].session != entry.session;
        if new_session && start < at {
            out.push(start..at);
            start = at;
        }
        if entry.kind == EntryKind::Boundary {
            if start < at {
                out.push(start..at);
            }
            start = at + 1;
        }
    }
    if start < entries.len() {
        out.push(start..entries.len());
    }
    out
}
