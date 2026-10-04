//! Canonical messages and their content hashes.
//!
//! [`content_hash`] stands in for the normalizer's BLAKE3 over the canonical
//! encoding (which L1 owns): it is a deterministic digest of the body, so
//! equal bodies always hash equal and different bodies differ, which is what
//! reconstruction and provenance tests rely on. It is not the production
//! hash and never needs to match it.

use crosstalk_spec::ids::MessageHash;
use crosstalk_spec::observed::message::{
    AssistantPart, CanonicalJson, Message, MessageBody, SystemPart, Text, ToolArguments, ToolCall,
    ToolCallId, ToolExecution, ToolName, ToolOutcome, ToolResult, ToolResultContent, UserPart,
};
use crosstalk_spec::support::{Blake3, NonEmpty};

use crate::ids::{expand, splitmix64};

/// A digest of `body`'s content: FNV-1a over its debug rendering, mixed out
/// to 32 bytes. Equal bodies give equal hashes.
pub fn content_hash(body: &MessageBody) -> MessageHash {
    let rendered = format!("{body:?}");
    let mut low: u64 = 0xcbf2_9ce4_8422_2325;
    let mut high: u64 = 0x8422_2325_cbf2_9ce4;
    for byte in rendered.bytes() {
        low = (low ^ u64::from(byte)).wrapping_mul(0x0000_0100_0000_01b3);
        high = splitmix64(high ^ u64::from(byte));
    }
    let raw = (u128::from(high) << 64) | u128::from(low);
    MessageHash::from_digest(Blake3::from_bytes(expand(raw)))
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
