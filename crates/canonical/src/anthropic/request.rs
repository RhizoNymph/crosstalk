//! An Anthropic Messages request body to canonical message bodies.
//!
//! - `system` (a string, or an array of blocks) becomes one `System`
//!   message first (`canonical.normalize.system-prompt-first`); absent or
//!   null, there is none.
//! - Each entry of `messages` normalizes on its own, in order
//!   (`canonical.normalize.request-is-concatenation`): an `assistant` turn
//!   is one `Assistant` message; a `system` turn (Claude Code sends one
//!   inside `messages`, besides the top-level `system`) is one `System`
//!   message at its own position, its content mapped like the top-level
//!   `system`; a `user` turn becomes one message per maximal run of blocks
//!   of one canonical role, so `[tool_result, tool_result, text]` is a
//!   `Tool` message then a `User` message
//!   (`canonical.normalize.split-mixed-roles`). A string `content` is one
//!   text part; an empty array is one message with no parts.
//!
//! Only a body that is not an Anthropic Messages request at all is an
//! error: not JSON, not an object, no `messages` array, a turn that is not
//! an object with a `user`, `assistant` or `system` role and string or
//! array content, or a `system` that is neither a string nor an array.
//! Blocks the normalizer does not know are kept as `Unknown` parts.

use crosstalk_spec::observed::message::{MessageBody, SystemPart, Text, UserPart};
use crosstalk_spec::support::NonEmpty;

use super::blocks::{self, Assembled, MediaSink, UserItem};
use crosstalk_spec::observed::message::json::{Json, JsonError};

/// Why a request body is not an Anthropic Messages request.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum RequestError {
    #[error("the body is not JSON: {0}")]
    NotJson(#[from] JsonError),
    #[error("the body is not a JSON object")]
    NotAnObject,
    #[error("`messages` is missing or not an array")]
    Messages,
    #[error("`system` is neither a string nor an array of blocks")]
    System,
    #[error("message {index} is not an object with a string `role`")]
    Message { index: usize },
    #[error("message {index} has role {role:?}, not `user`, `assistant` or `system`")]
    Role { index: usize, role: String },
    #[error("message {index}'s content is neither a string nor an array of blocks")]
    Content { index: usize },
}

/// The canonical messages of a request body, in order.
pub(crate) fn normalize(
    body: &[u8],
    sink: &mut MediaSink,
) -> Result<Vec<MessageBody>, RequestError> {
    let request = Json::parse_bytes(body)?;
    if !request.is_object() {
        return Err(RequestError::NotAnObject);
    }
    let turns = request
        .get("messages")
        .and_then(Json::as_array)
        .ok_or(RequestError::Messages)?;
    let mut messages = Vec::with_capacity(turns.len() + 1);
    match request.get("system") {
        None | Some(Json::Null) => {}
        Some(Json::String(text)) => messages.push(system_message(Content::Text(text))),
        Some(Json::Array(blocks)) => messages.push(system_message(Content::Blocks(blocks))),
        Some(_) => return Err(RequestError::System),
    }
    for (index, turn) in turns.iter().enumerate() {
        messages.extend(turn_messages(index, turn, sink)?);
    }
    Ok(messages)
}

/// The canonical messages one entry of `messages` becomes, in order.
pub(crate) fn turn_messages(
    index: usize,
    turn: &Json,
    sink: &mut MediaSink,
) -> Result<Vec<MessageBody>, RequestError> {
    let role = turn
        .get("role")
        .and_then(Json::as_str)
        .ok_or(RequestError::Message { index })?;
    let content = match turn.get("content") {
        Some(Json::String(text)) => Content::Text(text),
        Some(Json::Array(blocks)) => Content::Blocks(blocks),
        _ => return Err(RequestError::Content { index }),
    };
    match role {
        "user" => Ok(user_messages(content, sink)),
        "assistant" => {
            let parts = match content {
                Content::Text(text) => {
                    blocks::assistant_parts(vec![Assembled::Block(text_block(text))], sink)
                }
                Content::Blocks(items) => blocks::assistant_parts(
                    items.iter().cloned().map(Assembled::Block).collect(),
                    sink,
                ),
            };
            Ok(vec![MessageBody::Assistant(parts)])
        }
        "system" => Ok(vec![system_message(content)]),
        other => Err(RequestError::Role {
            index,
            role: other.to_owned(),
        }),
    }
}

enum Content<'a> {
    Text(&'a str),
    Blocks(&'a [Json]),
}

/// A system prompt, top-level or a `system` turn: a string is one text
/// part, an array one part per block (`blocks::system_part`).
fn system_message(content: Content<'_>) -> MessageBody {
    MessageBody::System(match content {
        Content::Text(text) => vec![SystemPart::Text(Text(text.to_owned()))],
        Content::Blocks(items) => items.iter().map(blocks::system_part).collect(),
    })
}

fn text_block(text: &str) -> Json {
    Json::Object(vec![
        ("type".to_owned(), Json::String("text".to_owned())),
        ("text".to_owned(), Json::String(text.to_owned())),
    ])
}

/// A user turn split into maximal runs of one canonical role.
fn user_messages(content: Content<'_>, sink: &mut MediaSink) -> Vec<MessageBody> {
    let blocks = match content {
        Content::Text(text) => {
            return vec![MessageBody::User(vec![UserPart::Text(Text(
                text.to_owned(),
            ))])];
        }
        Content::Blocks(blocks) => blocks,
    };
    if blocks.is_empty() {
        return vec![MessageBody::User(Vec::new())];
    }
    let mut messages = Vec::new();
    let mut run = Run::None;
    for block in blocks {
        run = match (run, blocks::user_item(block, sink)) {
            (Run::User(mut parts), UserItem::Part(part)) => {
                parts.push(part);
                Run::User(parts)
            }
            (Run::Tool(mut results), UserItem::Result(result)) => {
                results.push(result);
                Run::Tool(results)
            }
            (previous, item) => {
                if let Some(message) = previous.close() {
                    messages.push(message);
                }
                match item {
                    UserItem::Part(part) => Run::User(vec![part]),
                    UserItem::Result(result) => Run::Tool(NonEmpty::new(result)),
                }
            }
        };
    }
    if let Some(message) = run.close() {
        messages.push(message);
    }
    messages
}

/// The run of same-role blocks being collected.
enum Run {
    None,
    User(Vec<UserPart>),
    Tool(NonEmpty<crosstalk_spec::observed::message::ToolResult>),
}

impl Run {
    fn close(self) -> Option<MessageBody> {
        match self {
            Self::None => None,
            Self::User(parts) => Some(MessageBody::User(parts)),
            Self::Tool(results) => Some(MessageBody::Tool(results)),
        }
    }
}
