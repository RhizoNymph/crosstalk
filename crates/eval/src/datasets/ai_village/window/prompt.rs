//! The synthetic parts of a reconstructed request: the system prompt and
//! the chat user turn.
//!
//! The village's prompts are not public and `llm_calls` is withheld, so
//! these are assumptions, documented in docs/features/eval.md:
//!
//! - **System prompt** (one per computer-use session, from the session's
//!   first call): the agent's name, the village goal active then, the
//!   agent's own goals, and its latest memory written by then.
//! - **Chat user turn** (one per call, when non-empty): every chat message
//!   posted in the agent's room since its previous call, one per line, as
//!   `[YYYY-MM-DD HH:MM:SS UTC] #room speaker: content`. Where the
//!   scaffolding really placed chat (and memory) is not known.

use crosstalk_spec::observed::message::{MessageBody, SystemPart, Text, UserPart};
use crosstalk_spec::support::Timestamp;

use super::super::time::format_seconds;
use crate::corpus::HashedMessage;

/// The system prompt of a session.
pub fn system(
    name: &str,
    village_goal: Option<&str>,
    goals: &[&str],
    memory: Option<&str>,
) -> HashedMessage {
    let mut text = format!("You are {name}, an agent in the AI Village.\n");
    if let Some(goal) = village_goal {
        text.push_str("\n## Village goal\n");
        text.push_str(goal);
        text.push('\n');
    }
    if !goals.is_empty() {
        text.push_str("\n## Your goals\n");
        for goal in goals {
            text.push_str("- ");
            text.push_str(goal);
            text.push('\n');
        }
    }
    if let Some(memory) = memory {
        text.push_str("\n## Your memory\n");
        text.push_str(memory);
        text.push('\n');
    }
    HashedMessage::new(MessageBody::System(vec![SystemPart::Text(Text(text))]))
}

/// One chat message for a user turn.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChatLine<'a> {
    pub id: &'a str,
    pub at: Timestamp,
    pub room: &'a str,
    pub speaker: &'a str,
    pub content: &'a str,
}

/// A chat user turn and, per message id, the byte range of its content.
pub fn chat(lines: &[ChatLine<'_>]) -> (HashedMessage, Vec<(String, u32, u32)>) {
    let mut text = String::new();
    let mut ranges = Vec::with_capacity(lines.len());
    for (index, line) in lines.iter().enumerate() {
        if index > 0 {
            text.push('\n');
        }
        text.push_str(&format!(
            "[{} UTC] #{} {}: ",
            format_seconds(line.at),
            line.room,
            line.speaker
        ));
        let start = text.len();
        text.push_str(line.content);
        if let (Ok(start), Ok(end)) = (u32::try_from(start), u32::try_from(text.len())) {
            ranges.push((line.id.to_owned(), start, end));
        }
    }
    (
        HashedMessage::new(MessageBody::User(vec![UserPart::Text(Text(text))])),
        ranges,
    )
}
