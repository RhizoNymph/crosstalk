//! The scenario's tool calls through L5's merged extractor, as the flow
//! consumer runs it: A's `Write` is a delivered write and B's `Read` a
//! read, both on the one file locator of the shared page.

use crosstalk_e2e::scenario::WIKI_PAGE;
use crosstalk_flow::extract::{ConversationContext, ExtractConfig, ToolExtractors};
use crosstalk_spec::derived::flow::access::WriteOutcome;
use crosstalk_spec::derived::flow::resource::Locator;
use crosstalk_spec::interfaces::l1_canonical::NormalizedExchange;
use crosstalk_spec::interfaces::l5_flow::{ExtractedAccess, ExtractedOp, ResourceExtractor};
use crosstalk_spec::observed::exchange::ExchangeOutcome;
use crosstalk_spec::observed::message::{
    AssistantPart, MessageBody, SystemPart, ToolCall, ToolResult,
};

use crate::support::{Failure, normalized, relay, unexpected};

fn body(exchange: &NormalizedExchange, index: usize) -> Option<&MessageBody> {
    let hash = *exchange.exchange.request.get(index)?;
    exchange
        .messages
        .iter()
        .find(|message| message.hash == hash)
        .map(|message| &message.body)
}

/// The conversation context the system prompt states.
fn context(exchange: &NormalizedExchange) -> ConversationContext {
    let text: String = exchange
        .messages
        .iter()
        .filter_map(|message| match &message.body {
            MessageBody::System(parts) => Some(parts),
            _ => None,
        })
        .flatten()
        .filter_map(|part| match part {
            SystemPart::Text(text) => Some(text.0.as_str()),
            SystemPart::Unknown(_) => None,
        })
        .collect::<Vec<_>>()
        .join("\n");
    ConversationContext::from_system_prompt(&text)
}

/// The tool call in `exchange`'s response.
fn call(exchange: &NormalizedExchange) -> Result<ToolCall, Failure> {
    let ExchangeOutcome::Completed { response, .. } = &exchange.exchange.outcome else {
        return Err(unexpected("the exchange failed"));
    };
    exchange
        .messages
        .iter()
        .filter(|message| message.hash == *response)
        .find_map(|message| match &message.body {
            MessageBody::Assistant(parts) => parts.iter().find_map(|part| match part {
                AssistantPart::ToolCall(call) => Some(call.clone()),
                _ => None,
            }),
            _ => None,
        })
        .ok_or_else(|| unexpected("no tool call in the response"))
}

/// The tool result `exchange`'s request ends with.
fn result(exchange: &NormalizedExchange) -> Result<ToolResult, Failure> {
    let last = exchange.exchange.request.len().saturating_sub(1);
    match body(exchange, last) {
        Some(MessageBody::Tool(results)) => Ok(results.first().clone()),
        _ => Err(unexpected("the request does not end in a tool result")),
    }
}

fn accesses(
    caller: &NormalizedExchange,
    returned: &NormalizedExchange,
) -> Result<Vec<ExtractedAccess>, Failure> {
    let config = ExtractConfig::default();
    let context = context(caller);
    let extractors = ToolExtractors::new(&config, &context);
    let call = call(caller)?;
    assert!(
        extractors.handles(&call),
        "{} is not a known tool",
        call.name.0
    );
    let result = result(returned)?;
    assert_eq!(result.call_id, call.id);
    extractors
        .extract(&call, Some(&result))
        .map_err(|error| unexpected(format!("extract: {error:?}")))
}

fn page() -> Locator {
    Locator::File {
        host: None,
        path: WIKI_PAGE.to_owned(),
    }
}

#[test]
fn a_writes_and_b_reads_one_file_locator() -> Result<(), Failure> {
    let scenario = relay();
    let exchanges = normalized(&scenario)?;
    let by_label = |label: &str| {
        exchanges
            .iter()
            .find(|(wire, _)| wire.label == label)
            .map(|(_, normalized)| normalized)
            .ok_or_else(|| unexpected(format!("no {label}")))
    };

    let written = accesses(by_label("a1-write")?, by_label("a2-ack")?)?;
    let [write] = written.as_slice() else {
        return Err(unexpected(format!(
            "{} accesses from A's Write",
            written.len()
        )));
    };
    assert_eq!(write.op, ExtractedOp::write(WriteOutcome::Delivered));
    assert_eq!(write.locator, page());

    let read = accesses(by_label("b1-read")?, by_label("b2-repeat")?)?;
    let [read] = read.as_slice() else {
        return Err(unexpected(format!("{} accesses from B's Read", read.len())));
    };
    assert_eq!(read.op, ExtractedOp::Read);
    assert_eq!(read.locator, write.locator);
    Ok(())
}
