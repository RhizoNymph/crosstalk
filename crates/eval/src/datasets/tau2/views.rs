//! Each agent's view of one simulation.
//!
//! A simulation records one conversation between the agent (`assistant`)
//! and the user simulator (`user`), with each side's tool calls and results
//! (`requestor`). Each side's model sees it differently:
//!
//! | record | agent sees | user simulator sees |
//! | --- | --- | --- |
//! | `assistant` | `Assistant` (text, tool calls) | `User` (its text only; nothing for a bare tool call) |
//! | `user` | `User` (its text only; nothing for a bare tool call) | `Assistant` (text, tool calls) |
//! | `tool`, requestor `assistant` | `Tool` | nothing |
//! | `tool`, requestor `user` | nothing | `Tool` |
//!
//! Each view starts with that side's system prompt.

use crosstalk_spec::observed::message::json::canonicalize;
use crosstalk_spec::observed::message::{
    AssistantPart, MessageBody, SystemPart, Text, ToolArguments, ToolCall, ToolCallId,
    ToolExecution, ToolName, ToolOutcome, ToolResult, ToolResultContent, UserPart,
};
use crosstalk_spec::support::NonEmpty;

use super::Tau2Error;
use super::schema::{RawCall, RawMessage};
use crate::corpus::HashedMessage;

/// Whose view.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Side {
    Agent,
    User,
}

impl Side {
    /// The record role this side's model writes.
    pub fn role(self) -> &'static str {
        match self {
            Self::Agent => "assistant",
            Self::User => "user",
        }
    }

    pub fn peer(self) -> Self {
        match self {
            Self::Agent => Self::User,
            Self::User => Self::Agent,
        }
    }
}

/// One message of a view and the record it came from.
#[derive(Debug, Clone)]
pub struct Entry {
    pub raw: usize,
    pub message: HashedMessage,
}

/// A side's system prompt and the records it sees, in order.
#[derive(Debug, Clone)]
pub struct View {
    pub system: HashedMessage,
    pub entries: Vec<Entry>,
}

impl View {
    /// The position of record `raw` in the view.
    pub fn position(&self, raw: usize) -> Option<usize> {
        self.entries.iter().position(|entry| entry.raw == raw)
    }

    pub fn entry(&self, raw: usize) -> Option<&Entry> {
        self.entries.iter().find(|entry| entry.raw == raw)
    }

    /// The request of the call whose response is the entry at `position`:
    /// the system prompt and every earlier entry.
    pub fn request(&self, position: usize) -> Vec<HashedMessage> {
        std::iter::once(self.system.clone())
            .chain(
                self.entries
                    .iter()
                    .take(position)
                    .map(|entry| entry.message.clone()),
            )
            .collect()
    }
}

/// `side`'s view of `messages`, after `system`.
pub fn view(side: Side, messages: &[RawMessage], system: String) -> Result<View, Tau2Error> {
    let mut entries = Vec::with_capacity(messages.len());
    for (raw, message) in messages.iter().enumerate() {
        let body = match message.role.as_str() {
            "assistant" | "user" if message.role == side.role() => Some(own(message)),
            "assistant" | "user" => message
                .text()
                .map(|text| MessageBody::User(vec![UserPart::Text(Text(text.to_owned()))])),
            "tool" => {
                let requestor = message.requestor.as_deref().unwrap_or("assistant");
                (requestor == side.role()).then(|| tool(message, raw))
            }
            other => return Err(Tau2Error::UnknownRole(other.to_owned())),
        };
        if let Some(body) = body {
            entries.push(Entry {
                raw,
                message: HashedMessage::new(body),
            });
        }
    }
    Ok(View {
        system: HashedMessage::new(MessageBody::System(vec![SystemPart::Text(Text(system))])),
        entries,
    })
}

/// A message the side's own model wrote: its text, then its tool calls.
fn own(message: &RawMessage) -> MessageBody {
    let mut parts = Vec::new();
    if let Some(text) = message.text() {
        parts.push(AssistantPart::Text(Text(text.to_owned())));
    }
    parts.extend(
        message
            .calls()
            .iter()
            .map(|call| AssistantPart::ToolCall(tool_call(call))),
    );
    MessageBody::Assistant(parts)
}

fn tool_call(call: &RawCall) -> ToolCall {
    let text = call.arguments.to_string();
    let arguments = match canonicalize(&text) {
        Ok(json) => ToolArguments::Json(json),
        Err(_) => ToolArguments::Invalid(text),
    };
    ToolCall {
        id: ToolCallId(call.id.clone()),
        name: ToolName(call.name.clone()),
        arguments,
        execution: ToolExecution::Client,
    }
}

fn tool(message: &RawMessage, raw: usize) -> MessageBody {
    MessageBody::Tool(NonEmpty::new(ToolResult {
        call_id: ToolCallId(
            message
                .id
                .clone()
                .unwrap_or_else(|| format!("tau2-result-{raw}")),
        ),
        content: vec![ToolResultContent::Text(Text(
            message.content.clone().unwrap_or_default(),
        ))],
        outcome: if message.error == Some(true) {
            ToolOutcome::Error
        } else {
            ToolOutcome::Success
        },
    }))
}
