//! Canonical messages and their content hashes.
//!
//! [`content_hash`] is the production message hash, the BLAKE3 of the
//! body's canonical encoding (the spec's
//! `crosstalk_spec::observed::message::encoding`), so equal bodies always
//! hash equal, different bodies differ, and a built `NormalizedExchange`
//! passes its own check.

use crosstalk_spec::ids::MessageHash;
use crosstalk_spec::observed::message::encoding;
use crosstalk_spec::observed::message::{
    AssistantPart, CanonicalJson, Message, MessageBody, SystemPart, Text, ToolArguments, ToolCall,
    ToolCallId, ToolExecution, ToolName, ToolOutcome, ToolResult, ToolResultContent, UserPart,
};
use crosstalk_spec::support::NonEmpty;

/// The hash of `body`: the BLAKE3 of its canonical encoding.
pub fn content_hash(body: &MessageBody) -> MessageHash {
    encoding::hash(body)
}

/// `body` with its [`content_hash`].
pub fn message(body: MessageBody) -> Message {
    Message {
        hash: content_hash(&body),
        body,
    }
}

pub fn system_text(text: &str) -> MessageBody {
    MessageBody::System(vec![SystemPart::Text(Text(text.to_owned()))])
}

pub fn user_text(text: &str) -> MessageBody {
    MessageBody::User(vec![UserPart::Text(Text(text.to_owned()))])
}

pub fn assistant_text(text: &str) -> MessageBody {
    MessageBody::Assistant(vec![AssistantPart::Text(Text(text.to_owned()))])
}

/// A client-executed tool call. `arguments` is written compactly with
/// sorted keys, the canonical form for the JSON values tests use.
pub fn tool_call(id: &str, name: &str, arguments: &serde_json::Value) -> AssistantPart {
    AssistantPart::ToolCall(ToolCall {
        id: ToolCallId(id.to_owned()),
        name: ToolName(name.to_owned()),
        arguments: ToolArguments::Json(CanonicalJson(arguments.to_string())),
        execution: ToolExecution::Client,
    })
}

/// An assistant message holding `parts`, in order.
pub fn assistant(parts: Vec<AssistantPart>) -> MessageBody {
    MessageBody::Assistant(parts)
}

/// A tool message with one successful text result for `call_id`.
pub fn tool_result(call_id: &str, text: &str) -> MessageBody {
    MessageBody::Tool(NonEmpty::new(ToolResult {
        call_id: ToolCallId(call_id.to_owned()),
        content: vec![ToolResultContent::Text(Text(text.to_owned()))],
        outcome: ToolOutcome::Success,
    }))
}
