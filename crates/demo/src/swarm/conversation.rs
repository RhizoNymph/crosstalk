//! One agent's conversation, as a harness keeps it: every user turn,
//! assistant turn and tool result in order, resent whole with every
//! request.
//!
//! The order is enforced: a prompt only when the last answer is in
//! ([`Conversation::ask`]), an answer only after a request
//! ([`Conversation::receive`]), and tool results only for the calls of the
//! answer before them, each exactly once ([`Conversation::resolve`]; a
//! [`ToolResult`] can only be made from its [`ToolCall`]).

use serde_json::{Value, json};

use crate::anthropic::{AssistantMessage, Block, Content, Message, ResponseBlock, Role};
use crate::protocol::tool_definitions;

/// The text of the `role: "system"` turn `--claude-code-shape` puts before
/// each prompt.
pub const SYSTEM_TURN: &str = "<system-reminder>The wiki is shared with the whole team; other agents may have changed pages since you last read them.</system-reminder>";

/// Who is sending: fixed for an agent's lifetime.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Profile {
    pub agent: String,
    pub system: String,
    pub model: String,
    pub max_tokens: u32,
}

/// A tool call from an answer, waiting for its result.
#[derive(Debug, Clone, PartialEq)]
pub struct ToolCall {
    pub id: String,
    pub name: String,
    pub input: Value,
}

/// The result of one tool call.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToolResult {
    tool_use_id: String,
    content: String,
    is_error: bool,
}

impl ToolCall {
    /// This call's result.
    pub fn result(&self, content: String, is_error: bool) -> ToolResult {
        ToolResult {
            tool_use_id: self.id.clone(),
            content,
            is_error,
        }
    }
}

impl ToolResult {
    pub fn content(&self) -> &str {
        &self.content
    }

    pub fn is_error(&self) -> bool {
        self.is_error
    }
}

/// The calls of the last answer; handed back with their results.
#[derive(Debug, PartialEq)]
pub struct PendingTools {
    calls: Vec<ToolCall>,
}

impl PendingTools {
    pub fn calls(&self) -> &[ToolCall] {
        &self.calls
    }
}

/// What an answer asks for next.
#[derive(Debug, PartialEq)]
pub enum Step {
    /// The turn is over; the next prompt may follow.
    Answered,
    /// Run these tools and send their results.
    Tools(PendingTools),
}

/// A call out of order.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum OrderError {
    #[error("a prompt while an answer or tool results are outstanding")]
    NotIdle,
    #[error("an answer without a request outstanding")]
    NoRequest,
    #[error("tool results when none are outstanding")]
    NoPendingTools,
    #[error("tool results do not answer the pending calls one for one, in order")]
    ResultMismatch,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Phase {
    Idle,
    AwaitingAnswer,
    AwaitingTools,
}

/// A conversation and where it is.
#[derive(Debug, Clone, PartialEq)]
pub struct Conversation {
    session: String,
    claude_code_shape: bool,
    messages: Vec<Message>,
    prompts: u32,
    phase: Phase,
}

impl Conversation {
    pub fn new(session: String, claude_code_shape: bool) -> Self {
        Self {
            session,
            claude_code_shape,
            messages: Vec::new(),
            prompts: 0,
            phase: Phase::Idle,
        }
    }

    /// The session id sent as `x-claude-code-session-id`.
    pub fn session(&self) -> &str {
        &self.session
    }

    pub fn messages(&self) -> &[Message] {
        &self.messages
    }

    /// User prompts so far (tool results not counted).
    pub fn prompts(&self) -> u32 {
        self.prompts
    }

    /// Whether the next request is a tool-result follow-up.
    pub fn is_followup(&self) -> bool {
        self.messages.last().is_some_and(|m| {
            matches!(&m.content, Content::Blocks(blocks)
                if blocks.iter().any(|b| matches!(b, Block::ToolResult { .. })))
        })
    }

    /// Adds a user prompt (after a system turn in the Claude Code shape).
    pub fn ask(&mut self, prompt: String) -> Result<(), OrderError> {
        if self.phase != Phase::Idle {
            return Err(OrderError::NotIdle);
        }
        if self.claude_code_shape {
            self.messages.push(Message {
                role: Role::System,
                content: Content::Text(SYSTEM_TURN.to_owned()),
            });
        }
        self.messages.push(Message {
            role: Role::User,
            content: Content::Blocks(vec![Block::Text { text: prompt }]),
        });
        self.prompts += 1;
        self.phase = Phase::AwaitingAnswer;
        Ok(())
    }

    /// Adds the assistant's answer; returns the tool calls it makes.
    pub fn receive(&mut self, answer: AssistantMessage) -> Result<Step, OrderError> {
        if self.phase != Phase::AwaitingAnswer {
            return Err(OrderError::NoRequest);
        }
        let calls: Vec<ToolCall> = answer
            .content
            .iter()
            .filter_map(|block| match block {
                ResponseBlock::ToolUse { id, name, input } => Some(ToolCall {
                    id: id.clone(),
                    name: name.clone(),
                    input: input.clone(),
                }),
                ResponseBlock::Text { .. } => None,
            })
            .collect();
        self.messages.push(Message {
            role: Role::Assistant,
            content: Content::Blocks(answer.content.into_iter().map(Block::from).collect()),
        });
        if calls.is_empty() {
            self.phase = Phase::Idle;
            Ok(Step::Answered)
        } else {
            self.phase = Phase::AwaitingTools;
            Ok(Step::Tools(PendingTools { calls }))
        }
    }

    /// Adds the results of `pending`'s calls as the next user turn.
    pub fn resolve(
        &mut self,
        pending: PendingTools,
        results: Vec<ToolResult>,
    ) -> Result<(), OrderError> {
        if self.phase != Phase::AwaitingTools {
            return Err(OrderError::NoPendingTools);
        }
        let matches = pending.calls.len() == results.len()
            && pending
                .calls
                .iter()
                .zip(&results)
                .all(|(call, result)| call.id == result.tool_use_id);
        if !matches {
            return Err(OrderError::ResultMismatch);
        }
        self.messages.push(Message {
            role: Role::User,
            content: Content::Blocks(
                results
                    .into_iter()
                    .map(|r| Block::ToolResult {
                        tool_use_id: r.tool_use_id,
                        content: r.content,
                        is_error: r.is_error,
                    })
                    .collect(),
            ),
        });
        self.phase = Phase::AwaitingAnswer;
        Ok(())
    }

    /// The request body for the next request: the whole conversation.
    pub fn body(&self, profile: &Profile, stream: bool) -> Value {
        json!({
            "model": profile.model,
            "max_tokens": profile.max_tokens,
            "system": [{"type": "text", "text": profile.system}],
            "tools": tool_definitions(),
            "messages": self.messages,
            "metadata": {"user_id": format!("{}_session_{}", profile.agent, self.session)},
            "stream": stream,
        })
    }
}
