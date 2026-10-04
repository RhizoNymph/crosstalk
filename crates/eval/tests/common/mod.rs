//! Shared helpers: small synthetic worlds built by hand.

#![allow(dead_code)]

use crosstalk_eval::corpus::{ExchangeDraft, Fidelity, HashedMessage, clock};
use crosstalk_eval::keys::{AgentKey, DatasetId, SourceRef};
use crosstalk_spec::observed::exchange::{StopReason, WireProtocol};
use crosstalk_spec::observed::message::json::canonicalize;
use crosstalk_spec::observed::message::{
    AssistantPart, MessageBody, Reasoning, Text, ToolArguments, ToolCall, ToolCallId,
    ToolExecution, ToolName,
};
use crosstalk_spec::support::Timestamp;
use crosstalk_testkit::build::message;

pub fn dataset() -> DatasetId {
    DatasetId::new("synthetic")
}

pub fn system(text: &str) -> HashedMessage {
    HashedMessage::new(message::system_text(text))
}

pub fn user(text: &str) -> HashedMessage {
    HashedMessage::new(message::user_text(text))
}

pub fn says(text: &str) -> HashedMessage {
    HashedMessage::new(message::assistant_text(text))
}

pub fn thinks_and_says(signature: &str, text: &str) -> HashedMessage {
    HashedMessage::new(MessageBody::Assistant(vec![
        AssistantPart::Reasoning(Reasoning::Opaque {
            signature: signature.into(),
        }),
        AssistantPart::Text(Text(text.into())),
    ]))
}

/// An assistant message making one client tool call with JSON arguments.
pub fn calls(id: &str, name: &str, arguments: &str) -> HashedMessage {
    let arguments = match canonicalize(arguments) {
        Ok(json) => ToolArguments::Json(json),
        Err(_) => ToolArguments::Invalid(arguments.into()),
    };
    HashedMessage::new(MessageBody::Assistant(vec![AssistantPart::ToolCall(
        ToolCall {
            id: ToolCallId(id.into()),
            name: ToolName(name.into()),
            arguments,
            execution: ToolExecution::Client,
        },
    )]))
}

pub fn result(call_id: &str, text: &str) -> HashedMessage {
    HashedMessage::new(message::tool_result(call_id, text))
}

pub fn tick(n: u64) -> Timestamp {
    match clock::ordinal(n) {
        Ok(at) => at,
        Err(error) => panic!("clock: {error}"),
    }
}

pub fn draft(
    agent: &AgentKey,
    at: u64,
    request: Vec<HashedMessage>,
    response: HashedMessage,
) -> ExchangeDraft {
    ExchangeDraft {
        agent: agent.clone(),
        at: tick(at),
        protocol: WireProtocol::OpenAiChat,
        model: "test/model".into(),
        request,
        response,
        stop: StopReason::EndTurn,
        usage: None,
        fidelity: Fidelity::Reconstructed,
        source: SourceRef::new("fixture.json", format!("/{}/{at}", agent.name)),
    }
}
