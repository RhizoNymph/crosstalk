//! A day's model calls per agent, before requests are rebuilt.
//!
//! A standard agent's calls are its computer-use turns: each turn row is one
//! model response (`agent_messages`), the action the scaffolding executed
//! (`agent_action`) and what the tool returned (`output`, `error`). A chat
//! message whose `AGENT_TALK` event matches no turn (it came from a call
//! outside computer use) adds a call made of the event's raw output.

use std::collections::{BTreeMap, HashMap};

use crosstalk_spec::observed::message::{
    MessageBody, Text, ToolCallId, ToolOutcome, ToolResult, ToolResultContent,
};
use crosstalk_spec::support::{NonEmpty, Timestamp};
use serde_json::Value;

use super::super::provider::{self, Response};
use super::super::schema::{EventRow, TurnRow};
use crate::corpus::HashedMessage;

/// Where a call came from.
#[derive(Debug, Clone, PartialEq)]
pub enum Origin {
    /// A computer-use turn.
    Turn {
        id: String,
        session: String,
        action: Option<Value>,
        /// `output`, then `error`, joined by a newline.
        output: String,
    },
    /// An `AGENT_TALK` event's own output.
    Talk { event: String },
}

/// One model call of an agent.
#[derive(Debug, Clone)]
pub struct Call {
    pub agent: String,
    pub at: Timestamp,
    pub origin: Origin,
    pub response: Response,
    pub message: HashedMessage,
    /// The tool results that answer it, when it called a tool or produced
    /// output.
    pub result: Option<HashedMessage>,
}

impl Call {
    pub fn session(&self) -> Option<&str> {
        match &self.origin {
            Origin::Turn { session, .. } => Some(session),
            Origin::Talk { .. } => None,
        }
    }

    pub fn turn_id(&self) -> Option<&str> {
        match &self.origin {
            Origin::Turn { id, .. } => Some(id),
            Origin::Talk { .. } => None,
        }
    }

    pub fn action(&self) -> Option<&Value> {
        match &self.origin {
            Origin::Turn { action, .. } => action.as_ref(),
            Origin::Talk { .. } => None,
        }
    }

    pub fn output(&self) -> &str {
        match &self.origin {
            Origin::Turn { output, .. } => output,
            Origin::Talk { .. } => "",
        }
    }

    /// The bash command it ran, if any.
    pub fn command(&self) -> Option<&str> {
        self.action()
            .and_then(|action| action.get("command"))
            .and_then(Value::as_str)
    }

    /// The chat message it sent, if any.
    pub fn chat_sent(&self) -> Option<&str> {
        let action = self.action()?;
        let kind = action.get("action").and_then(Value::as_str)?;
        if kind == "send_message_back_to_chat" || kind == "send_message_to_chat" {
            action.get("content").and_then(Value::as_str)
        } else {
            None
        }
    }
}

/// `output` and `error`, joined by a newline when both are present.
pub fn output_text(output: Option<&str>, error: Option<&str>) -> String {
    match (
        output.filter(|o| !o.is_empty()),
        error.filter(|e| !e.is_empty()),
    ) {
        (Some(output), Some(error)) => format!("{output}\n{error}"),
        (Some(output), None) => output.to_owned(),
        (None, Some(error)) => error.to_owned(),
        (None, None) => String::new(),
    }
}

/// The tool results answering `response`: the first call gets the turn's
/// output, the others an empty result; an output with no call to answer
/// gets the turn's own id.
pub fn results(response: &Response, turn: &str, output: &str) -> Option<HashedMessage> {
    let mut ids = response.call_ids();
    if ids.is_empty() {
        if output.is_empty() {
            return None;
        }
        ids.push(ToolCallId(format!("turn:{turn}")));
    }
    let results: Vec<ToolResult> = ids
        .into_iter()
        .enumerate()
        .map(|(at, call_id)| ToolResult {
            call_id,
            content: if at == 0 && !output.is_empty() {
                vec![ToolResultContent::Text(Text(output.to_owned()))]
            } else {
                Vec::new()
            },
            outcome: ToolOutcome::Success,
        })
        .collect();
    NonEmpty::from_vec(results).map(|body| HashedMessage::new(MessageBody::Tool(body)))
}

/// A turn as a call of `agent`.
pub fn turn_call(agent: &str, at: Timestamp, row: TurnRow) -> Call {
    let response = provider::response(&row.agent_messages, &row.id);
    let output = output_text(row.output.as_deref(), row.error.as_deref());
    let result = results(&response, &row.id, &output);
    Call {
        agent: agent.to_owned(),
        at,
        message: HashedMessage::new(response.body()),
        response,
        result,
        origin: Origin::Turn {
            id: row.id,
            session: row.session_id,
            action: row.agent_action,
            output,
        },
    }
}

/// An `AGENT_TALK` event's output as a call of its speaker.
pub fn talk_call(agent: &str, at: Timestamp, event: &EventRow) -> Option<Call> {
    let output = event.data.get("output").filter(|o| !o.is_null())?;
    let response = provider::response(output, &event.id);
    Some(Call {
        agent: agent.to_owned(),
        at,
        message: HashedMessage::new(response.body()),
        response,
        result: None,
        origin: Origin::Talk {
            event: event.id.clone(),
        },
    })
}

/// Calls grouped per agent, each agent's in time order (ties by turn id).
pub fn by_agent(calls: Vec<Call>) -> BTreeMap<String, Vec<Call>> {
    let mut out: BTreeMap<String, Vec<Call>> = BTreeMap::new();
    for call in calls {
        out.entry(call.agent.clone()).or_default().push(call);
    }
    for list in out.values_mut() {
        list.sort_by(|a, b| {
            (a.at, a.turn_id().unwrap_or("")).cmp(&(b.at, b.turn_id().unwrap_or("")))
        });
    }
    out
}

/// Index of the sending call of each chat message: the speaker's latest
/// call at or before the message that sent exactly its content, else the
/// first one within a minute after it.
pub fn senders(
    calls: &BTreeMap<String, Vec<Call>>,
    talks: &[(Timestamp, &EventRow)],
) -> HashMap<String, (String, usize)> {
    let mut out = HashMap::new();
    for (at, event) in talks {
        let (Some(speaker), Some(message), Some(content)) = (
            event.str("speakerId"),
            event.str("messageId"),
            event.str("content"),
        ) else {
            continue;
        };
        let Some(list) = calls.get(speaker) else {
            continue;
        };
        let matching = |call: &&Call| {
            call.chat_sent() == Some(content)
                || matches!(&call.origin, super::calls::Origin::Talk { event: id } if id == &event.id)
        };
        let before = list
            .iter()
            .enumerate()
            .rfind(|(_, call)| call.at <= *at && matching(call));
        let found = before.or_else(|| {
            list.iter().enumerate().find(|(_, call)| {
                call.at > *at
                    && call.at.as_micros() - at.as_micros() <= 60_000_000
                    && matching(call)
            })
        });
        if let Some((index, _)) = found {
            out.insert(message.to_owned(), (speaker.to_owned(), index));
        }
    }
    out
}
