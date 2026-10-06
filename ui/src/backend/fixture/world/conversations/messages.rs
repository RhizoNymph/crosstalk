//! The generated messages of a conversation: system prompts, user tasks,
//! replies (with tool calls and quotes of what the agent read), compaction
//! summaries and mid-conversation system turns.
//!
//! Every message is canonical ([`Message::new`]); each carries the id of
//! the exchange or conversation it belongs to in its text, so no two
//! generated messages share a hash and a conversation never repeats one.

use crosstalk_spec::ids::{ConversationId, ExchangeId};
use crosstalk_spec::observed::message::CanonicalJson;
use crosstalk_spec::observed::message::{
    AssistantPart, Message, MessageBody, SystemPart, Text, ToolArguments, ToolCall, ToolCallId,
    ToolExecution, ToolName, UserPart,
};

use crate::backend::fixture::rng::Rng;

const TASKS: &[&str] = &[
    "Triage the failing integration tests and summarise what broke.",
    "Update the deployment notes on the team wiki with today's changes.",
    "Review the open pull requests and leave comments on the risky ones.",
    "Collect the latest benchmark numbers and compare them with last week.",
    "Draft the incident write-up for the cache outage.",
    "Reconcile the config files across the three environments.",
];

const REPLIES: &[&str] = &[
    "I checked the relevant files and the change looks contained.",
    "The logs point at a timeout in the upstream call; retrying fixed it.",
    "I summarised the findings below and listed the open questions.",
    "Two of the steps are done; the third needs a decision from the team.",
    "The numbers are within noise of last week's run.",
];

/// The tools generated replies call.
const TOOLS: &[&str] = &["Read", "Bash", "WebFetch", "wiki_read", "kv_get", "Grep"];

pub fn system_prompt(family: &str, conversation: ConversationId) -> Message {
    Message::new(MessageBody::System(vec![SystemPart::Text(Text(format!(
        "You are a {family} coding agent working in a shared repository. \
         Follow the team's conventions and keep notes current. (session {})",
        conversation.ulid_text()
    )))]))
}

pub fn user_task(rng: &mut Rng, conversation: ConversationId) -> Message {
    let task = rng.pick(TASKS).copied().unwrap_or(TASKS[0]);
    Message::new(MessageBody::User(vec![UserPart::Text(Text(format!(
        "{task} (task {})",
        conversation.ulid_text()
    )))]))
}

/// A system turn the harness inserted mid-conversation.
pub fn system_turn(exchange: ExchangeId) -> Message {
    Message::new(MessageBody::System(vec![SystemPart::Text(Text(format!(
        "Reminder: the repository is frozen for the release; ask before \
         pushing. (inserted at {})",
        exchange.ulid_text()
    )))]))
}

/// The summary a harness starts a compacted conversation with.
pub fn compaction_summary(predecessor: ConversationId) -> Message {
    Message::new(MessageBody::User(vec![UserPart::Text(Text(format!(
        "This session is being continued from a previous conversation that \
         ran out of context. Summary: the agent triaged the failing tests, \
         fixed the flaky cache test and was updating the wiki notes. \
         (continued from {})",
        predecessor.ulid_text()
    )))]))
}

/// A reply: prose, an optional quote of text the agent read (whose byte
/// range in part 0's text is returned), and an optional tool call.
pub struct Reply {
    pub message: Message,
    /// Where the quote sits in part 0's text.
    pub quote: Option<(u32, u32)>,
}

pub fn reply(
    rng: &mut Rng,
    exchange: ExchangeId,
    quote: Option<&str>,
    call: Option<&ToolCallId>,
) -> Reply {
    let prose = rng.pick(REPLIES).copied().unwrap_or(REPLIES[0]);
    let mut text = format!("{prose} (reply {})", exchange.ulid_text());
    let quoted = quote.map(|quote| {
        text.push_str("\n\nFrom what I read: \"");
        let start = len(&text);
        text.push_str(quote);
        let end = len(&text);
        text.push('"');
        (start, end)
    });
    let mut parts = vec![AssistantPart::Text(Text(text))];
    if let Some(call) = call {
        let tool = rng.pick(TOOLS).copied().unwrap_or(TOOLS[0]);
        parts.push(AssistantPart::ToolCall(ToolCall {
            id: call.clone(),
            name: ToolName(tool.to_owned()),
            arguments: ToolArguments::Json(CanonicalJson(format!(
                "{{\"target\":\"step-{}\"}}",
                exchange.ulid_text()
            ))),
            execution: ToolExecution::Client,
            signature: None,
        }));
    }
    Reply {
        message: Message::new(MessageBody::Assistant(parts)),
        quote: quoted,
    }
}

/// A partial reply cut off by a failure.
pub fn partial(exchange: ExchangeId) -> Message {
    Message::new(MessageBody::Assistant(vec![AssistantPart::Text(Text(
        format!(
            "I started looking into the failing step and (truncated {})",
            exchange.ulid_text()
        ),
    ))]))
}

fn len(text: &str) -> u32 {
    u32::try_from(text.len()).unwrap_or(u32::MAX)
}
