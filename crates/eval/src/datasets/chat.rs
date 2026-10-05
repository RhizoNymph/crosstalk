//! OpenAI-chat messages as recorded by agent-trace datasets, and their
//! canonical form.
//!
//! | Recorded | Canonical |
//! | --- | --- |
//! | `system` | `System([Text])` |
//! | `user` | `User([Text])` |
//! | `assistant` | `Assistant`: visible reasoning (when not empty), the text (when not empty), then tool calls |
//! | `tool` | `Tool([ToolResult])` with the text as its content |
//!
//! **Tool-result pairing.** A tool message names its call by
//! `tool_call_id` when the dataset kept it. Open-SWE traces dropped it, so a
//! tool message without one answers the oldest unanswered call of the
//! assistant messages before it (calls are answered in order, parallel calls
//! included). A tool message with no call left to answer gets a synthetic id
//! naming its position, so its result still has a carrier.

use std::collections::VecDeque;

use crosstalk_spec::observed::message::json::canonicalize;
use crosstalk_spec::observed::message::{
    AssistantPart, MessageBody, Reasoning, SystemPart, Text, ToolArguments, ToolCall, ToolCallId,
    ToolExecution, ToolName, ToolOutcome, ToolResult, ToolResultContent, UserPart,
};
use crosstalk_spec::support::NonEmpty;
use serde::{Deserialize, Serialize};

use crate::corpus::HashedMessage;

/// One recorded message. Unknown fields are ignored; missing ones are
/// `None`.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct ChatMessage {
    pub role: String,
    #[serde(default)]
    pub content: Option<String>,
    #[serde(default)]
    pub reasoning_content: Option<String>,
    #[serde(default)]
    pub tool_calls: Option<Vec<ChatToolCall>>,
    #[serde(default)]
    pub tool_call_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct ChatToolCall {
    #[serde(default)]
    pub id: Option<String>,
    pub function: ChatFunction,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct ChatFunction {
    pub name: String,
    /// The arguments as the model wrote them: a JSON string, usually.
    #[serde(default)]
    pub arguments: Option<String>,
}

impl ChatMessage {
    pub fn text(&self) -> &str {
        self.content.as_deref().unwrap_or_default()
    }

    pub fn calls(&self) -> &[ChatToolCall] {
        self.tool_calls.as_deref().unwrap_or_default()
    }

    pub fn is_assistant(&self) -> bool {
        self.role == "assistant"
    }
}

impl ChatToolCall {
    pub fn arguments(&self) -> &str {
        self.function.arguments.as_deref().unwrap_or_default()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ChatError {
    #[error("message {index} has unknown role {role:?}")]
    UnknownRole { index: usize, role: String },
}

/// Deserializes `null` as the type's default (Parquet lists may be null).
pub fn null_as_default<'de, D, T>(deserializer: D) -> Result<T, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Default + Deserialize<'de>,
{
    Ok(Option::<T>::deserialize(deserializer)?.unwrap_or_default())
}

/// The id a call without one gets: its message and position.
pub fn synthetic_call_id(message: usize, call: usize) -> String {
    format!("call-{message}-{call}")
}

/// Arguments as canonical JSON when they parse, else kept verbatim.
pub fn arguments(raw: &str) -> ToolArguments {
    match canonicalize(raw) {
        Ok(json) => ToolArguments::Json(json),
        Err(_) => ToolArguments::Invalid(raw.to_owned()),
    }
}

/// The canonical bodies of `messages`, tool results paired with their
/// calls.
pub fn bodies(messages: &[ChatMessage]) -> Result<Vec<MessageBody>, ChatError> {
    let mut pending: VecDeque<String> = VecDeque::new();
    let mut out = Vec::with_capacity(messages.len());
    for (index, message) in messages.iter().enumerate() {
        let body = match message.role.as_str() {
            "system" => MessageBody::System(vec![SystemPart::Text(Text(message.text().into()))]),
            "user" => MessageBody::User(vec![UserPart::Text(Text(message.text().into()))]),
            "assistant" => {
                let parts = assistant_parts(index, message);
                for part in &parts {
                    if let AssistantPart::ToolCall(call) = part {
                        pending.push_back(call.id.0.clone());
                    }
                }
                MessageBody::Assistant(parts)
            }
            "tool" => {
                let id = match &message.tool_call_id {
                    Some(id) => {
                        if let Some(at) = pending.iter().position(|open| open == id) {
                            pending.remove(at);
                        }
                        id.clone()
                    }
                    None => pending
                        .pop_front()
                        .unwrap_or_else(|| format!("unpaired-{index}")),
                };
                MessageBody::Tool(NonEmpty::new(ToolResult {
                    call_id: ToolCallId(id),
                    content: vec![ToolResultContent::Text(Text(message.text().into()))],
                    outcome: ToolOutcome::Success,
                }))
            }
            other => {
                return Err(ChatError::UnknownRole {
                    index,
                    role: other.to_owned(),
                });
            }
        };
        out.push(body);
    }
    Ok(out)
}

/// [`bodies`], hashed.
pub fn convert(messages: &[ChatMessage]) -> Result<Vec<HashedMessage>, ChatError> {
    Ok(bodies(messages)?
        .into_iter()
        .map(HashedMessage::new)
        .collect())
}

fn assistant_parts(index: usize, message: &ChatMessage) -> Vec<AssistantPart> {
    let mut parts = Vec::new();
    if let Some(reasoning) = message.reasoning_content.as_deref()
        && !reasoning.is_empty()
    {
        parts.push(AssistantPart::Reasoning(Reasoning::Visible {
            text: Text(reasoning.into()),
            signature: None,
        }));
    }
    if !message.text().is_empty() {
        parts.push(AssistantPart::Text(Text(message.text().into())));
    }
    for (at, call) in message.calls().iter().enumerate() {
        let id = call
            .id
            .clone()
            .filter(|id| !id.is_empty())
            .unwrap_or_else(|| synthetic_call_id(index, at));
        parts.push(AssistantPart::ToolCall(ToolCall {
            id: ToolCallId(id),
            name: ToolName(call.function.name.clone()),
            arguments: arguments(call.arguments()),
            execution: ToolExecution::Client,
        }));
    }
    parts
}
