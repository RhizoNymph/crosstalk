//! The messages only the wire traffic carries: system prompts, the
//! operator's prompts, the agents' own replies and tool calls, and the
//! summary turn a compacted session opens with. Each is a spec
//! [`MessageBody`] in the canonical encoding, kept in [`Bodies`] by hash.
//!
//! None of this text comes from the theme templates the transmissions'
//! paragraphs are cut from: prompts and replies are short, numbered status
//! lines, so the only text one agent's inputs share with another agent's
//! outputs is what the world's transmissions carried.

use std::collections::BTreeMap;

use crosstalk_spec::ids::MessageHash;
use crosstalk_spec::observed::message::encoding;
use crosstalk_spec::observed::message::{
    AssistantPart, CanonicalJson, MessageBody, SystemPart, Text, ToolArguments, ToolCall,
    ToolCallId, ToolExecution, ToolName, UserPart,
};

use crate::rng::Rng;
use crate::text::Theme;

/// The opening Claude Code gives a compacted session's summary turn
/// (L3 recognizes it as a summary).
pub const SUMMARY_PREAMBLE: &str = "This session is being continued from a previous conversation \
that ran out of context. The conversation is summarized below:";

const INSTRUCTIONS: &[&str] = &[
    "Pick up the next item on the list",
    "Check what changed since the last run",
    "Continue where you left off",
    "Go ahead with the next step",
    "Look into the open question",
    "Summarise where things stand",
    "Finish the current task",
    "Review the latest results",
];

const CLOSINGS: &[&str] = &[
    "next I will update the tracking notes",
    "nothing is blocked right now",
    "I will report back after the next pass",
    "two follow-ups are left for later",
    "the remaining items are small",
    "waiting on one more check before moving on",
];

/// Encoded wire bodies by hash.
#[derive(Debug, Clone, Default)]
pub struct Bodies {
    bodies: BTreeMap<MessageHash, Vec<u8>>,
}

impl Bodies {
    /// Encode `body`, keep it, and return its hash.
    pub fn keep(&mut self, body: &MessageBody) -> MessageHash {
        let bytes = encoding::encode(body);
        let hash = encoding::hash_bytes(&bytes);
        self.bodies.entry(hash).or_insert(bytes);
        hash
    }

    pub fn into_map(self) -> BTreeMap<MessageHash, Vec<u8>> {
        self.bodies
    }
}

fn text(content: String) -> Text {
    Text(content)
}

/// The system prompt an agent's harness sends.
pub fn system(harness: &str, agent: &str) -> MessageBody {
    MessageBody::System(vec![SystemPart::Text(text(format!(
        "You are {harness}, working as agent {agent} in this team's workspace. \
         Use the tools you are given and keep your answers short."
    )))])
}

/// The operator's next instruction.
pub fn prompt(rng: &mut Rng, theme: Theme, step: u32) -> MessageBody {
    let instruction = rng.pick(INSTRUCTIONS).copied().unwrap_or("Continue");
    MessageBody::User(vec![UserPart::Text(text(format!(
        "{instruction} ({}, step {step}).",
        theme.label()
    )))])
}

/// The agent's own reply: a numbered status line.
pub fn reply(rng: &mut Rng, step: u32) -> MessageBody {
    let closing = rng.pick(CLOSINGS).copied().unwrap_or("done");
    let files = 1 + rng.below(40);
    let tests = 2 + rng.below(300);
    MessageBody::Assistant(vec![AssistantPart::Text(text(format!(
        "Step {step} done: looked at {files} files, {tests} tests pass; {closing}."
    )))])
}

/// The agent's reply calling `tool` as `call`.
pub fn tool_call(call: &ToolCallId, tool: &ToolName, step: u32) -> MessageBody {
    // One integer field: already in canonical form.
    let arguments = CanonicalJson(format!("{{\"step\":{step}}}"));
    MessageBody::Assistant(vec![
        AssistantPart::Text(text(format!("Step {step}: checking with {}.", tool.0))),
        AssistantPart::ToolCall(ToolCall {
            id: call.clone(),
            name: tool.clone(),
            arguments: ToolArguments::Json(arguments),
            execution: ToolExecution::Client,
            signature: None,
        }),
    ])
}

/// The summary turn a compacted session opens with.
pub fn summary(theme: Theme, steps: u32, session: u32) -> MessageBody {
    MessageBody::User(vec![UserPart::Text(text(format!(
        "{SUMMARY_PREAMBLE}\nSession {session} covered {steps} steps of {} work; \
         the last two messages follow.",
        theme.label()
    )))])
}
