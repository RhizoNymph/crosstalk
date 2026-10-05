//! One Claude Code context as model calls with their requests.
//!
//! A call is every assistant entry sharing one `message.id` (the SDK writes
//! one entry per content block). Its request is the context's history when
//! its first block arrived: the compaction summary (if the context began
//! with one), then every earlier call's response and the tool results that
//! followed it. The system prompt and the per-query prompts the SDK sent
//! are not in the table, so requests carry neither (the one recorded
//! prompt string is kept where it occurs).
//!
//! With parallel tool use the SDK interleaves a message's blocks with the
//! results of the tools it already called; those results are held back and
//! placed after the message, where the API saw them.
//!
//! [`ResultRef`] remembers where each tool result sits (its message and
//! part) and which call first carried it: the next call opened after it
//! joined the history, or none when the context ended first.

use std::collections::HashMap;

use crosstalk_spec::observed::exchange::{StopReason, TokenCounts, TokenUsage};
use crosstalk_spec::observed::message::{
    AssistantPart, MessageBody, Text, ToolCallId, ToolResult, UserPart,
};
use crosstalk_spec::support::{NonEmpty, Timestamp};

use super::super::provider::anthropic::stop_reason;
use super::entries::{Entry, EntryKind, Usage};
use crate::corpus::HashedMessage;

/// One model call of the Claude Code agent.
#[derive(Debug, Clone)]
pub struct Call {
    pub message_id: String,
    /// The row of its first block.
    pub row: String,
    pub at: Timestamp,
    pub model: String,
    pub request: Vec<HashedMessage>,
    pub response: HashedMessage,
    pub stop: StopReason,
    pub usage: Option<TokenUsage>,
}

/// Where one tool result sits.
#[derive(Debug, Clone)]
pub struct ResultRef {
    pub row: String,
    /// Its block index in the row's content.
    pub block: usize,
    pub at: Timestamp,
    pub message: HashedMessage,
    pub part: u16,
    pub call_id: ToolCallId,
    /// The tool's name, from the call that made it.
    pub tool: String,
    /// Index (in [`Context::calls`]) of the first call carrying it.
    pub reader: Option<usize>,
}

/// One tool call the agent made: which call and the arguments.
#[derive(Debug, Clone)]
pub struct ToolUse {
    pub call: usize,
    pub part: u16,
    pub id: ToolCallId,
    pub name: String,
    pub arguments: String,
}

#[derive(Debug, Clone, Default)]
pub struct Context {
    pub calls: Vec<Call>,
    pub results: Vec<ResultRef>,
    pub tool_uses: Vec<ToolUse>,
}

struct Open {
    message_id: String,
    row: String,
    at: Timestamp,
    model: String,
    parts: Vec<AssistantPart>,
    stop: Option<String>,
    usage: Option<Usage>,
    request: Vec<HashedMessage>,
}

#[derive(Default)]
struct Walk {
    context: Context,
    history: Vec<HashedMessage>,
    open: Option<Open>,
    /// Tool messages that arrived while a call was open, with their refs.
    held: Vec<(HashedMessage, Vec<ResultRef>)>,
    /// Refs in the history not yet carried by a call.
    waiting: Vec<usize>,
    names: HashMap<String, String>,
}

impl Walk {
    fn close(&mut self) {
        let Some(open) = self.open.take() else {
            return;
        };
        let index = self.context.calls.len();
        for (part, assistant) in open.parts.iter().enumerate() {
            if let AssistantPart::ToolCall(call) = assistant {
                self.names.insert(call.id.0.clone(), call.name.0.clone());
                self.context.tool_uses.push(ToolUse {
                    call: index,
                    part: u16::try_from(part).unwrap_or(u16::MAX),
                    id: call.id.clone(),
                    name: call.name.0.clone(),
                    arguments: match &call.arguments {
                        crosstalk_spec::observed::message::ToolArguments::Json(json) => {
                            json.0.clone()
                        }
                        crosstalk_spec::observed::message::ToolArguments::Invalid(raw) => {
                            raw.clone()
                        }
                    },
                });
            }
        }
        let stop = stop_reason(open.stop.as_deref(), &open.parts);
        let response = HashedMessage::new(MessageBody::Assistant(open.parts));
        self.history.push(response.clone());
        self.context.calls.push(Call {
            message_id: open.message_id,
            row: open.row,
            at: open.at,
            model: open.model,
            request: open.request,
            response,
            stop,
            usage: open.usage.and_then(token_usage),
        });
        for (message, refs) in std::mem::take(&mut self.held) {
            self.push_results(message, refs);
        }
    }

    fn push_results(&mut self, message: HashedMessage, refs: Vec<ResultRef>) {
        self.history.push(message);
        for mut result in refs {
            if let Some(name) = self.names.get(&result.call_id.0) {
                result.tool = name.clone();
            }
            self.waiting.push(self.context.results.len());
            self.context.results.push(result);
        }
    }

    fn open(&mut self, entry: &Entry, message_id: &str, model: &str) {
        let index = self.context.calls.len();
        for waiting in std::mem::take(&mut self.waiting) {
            if let Some(result) = self.context.results.get_mut(waiting) {
                result.reader = Some(index);
            }
        }
        self.open = Some(Open {
            message_id: message_id.to_owned(),
            row: entry.row.clone(),
            at: entry.at,
            model: model.to_owned(),
            parts: Vec::new(),
            stop: None,
            usage: None,
            request: self.history.clone(),
        });
    }
}

fn token_usage(usage: Usage) -> Option<TokenUsage> {
    let clamp = |n: u64| u32::try_from(n).unwrap_or(u32::MAX);
    let input = usage
        .input
        .saturating_add(usage.cache_read)
        .saturating_add(usage.cache_write);
    TokenUsage::new(TokenCounts {
        input: clamp(input),
        output: clamp(usage.output),
        cache_read: clamp(usage.cache_read),
        cache_write: Some(clamp(usage.cache_write)),
        reasoning: None,
    })
    .ok()
}

/// The calls and tool results of one context's entries.
pub fn context(entries: &[Entry]) -> Context {
    let mut walk = Walk::default();
    for entry in entries {
        match &entry.kind {
            EntryKind::Assistant {
                message_id,
                model,
                parts,
                stop,
                usage,
            } => {
                let same = walk
                    .open
                    .as_ref()
                    .is_some_and(|open| &open.message_id == message_id);
                if !same {
                    walk.close();
                    walk.open(entry, message_id, model);
                }
                if let Some(open) = walk.open.as_mut() {
                    open.parts.extend(parts.iter().cloned());
                    if stop.is_some() {
                        open.stop.clone_from(stop);
                    }
                    if usage.is_some() {
                        open.usage = *usage;
                    }
                }
            }
            EntryKind::ToolResults(results) => {
                let parts: Vec<ToolResult> =
                    results.iter().map(|(_, result)| result.clone()).collect();
                let Some(body) = NonEmpty::from_vec(parts) else {
                    continue;
                };
                let message = HashedMessage::new(MessageBody::Tool(body));
                let refs = results
                    .iter()
                    .enumerate()
                    .map(|(part, (block, result))| ResultRef {
                        row: entry.row.clone(),
                        block: *block,
                        at: entry.at,
                        message: message.clone(),
                        part: u16::try_from(part).unwrap_or(u16::MAX),
                        call_id: result.call_id.clone(),
                        tool: String::new(),
                        reader: None,
                    })
                    .collect();
                if walk.open.is_some() {
                    walk.held.push((message, refs));
                } else {
                    walk.push_results(message, refs);
                }
            }
            EntryKind::UserText(text) => {
                walk.close();
                if !text.is_empty() {
                    walk.history
                        .push(HashedMessage::new(MessageBody::User(vec![UserPart::Text(
                            Text(text.clone()),
                        )])));
                }
            }
            EntryKind::QueryEnd | EntryKind::Boundary => walk.close(),
            EntryKind::Other => {}
        }
    }
    walk.close();
    // Results the context ended on were never carried by a call.
    walk.context
}
