//! The traffic, and what L0 and L1 make of it: two Claude Code sessions
//! under two API keys, full-history requests, the write call carrying the
//! sentence, the read result and B's answer repeating it.

use std::collections::BTreeSet;

use crosstalk_e2e::scenario::{SENTENCE, Scenario, WIKI_PAGE};
use crosstalk_spec::ids::MessageHash;
use crosstalk_spec::interfaces::l1_canonical::NormalizedExchange;
use crosstalk_spec::observed::client::{CredentialScheme, HarnessFamily, IngressMode};
use crosstalk_spec::observed::exchange::{ExchangeOutcome, Transport};
use crosstalk_spec::observed::message::{
    AssistantPart, MessageBody, ToolArguments, ToolResultContent,
};

use crate::support::{Failure, normalized, relay, unexpected};

/// The hash of the response message of a completed exchange.
fn response(exchange: &NormalizedExchange) -> Option<MessageHash> {
    match &exchange.exchange.outcome {
        ExchangeOutcome::Completed { response, .. } => Some(*response),
        ExchangeOutcome::Failed { .. } => None,
    }
}

fn body(exchange: &NormalizedExchange, hash: MessageHash) -> Option<&MessageBody> {
    exchange
        .messages
        .iter()
        .find(|message| message.hash == hash)
        .map(|message| &message.body)
}

#[test]
fn the_same_start_gives_the_same_traffic() {
    assert_eq!(relay(), relay());
}

#[test]
fn exchanges_are_in_time_order_with_distinct_ids() {
    let scenario = relay();
    let labels: Vec<&str> = scenario.exchanges.iter().map(|e| e.label).collect();
    assert_eq!(labels, ["a1-write", "a2-ack", "b1-read", "b2-repeat"]);
    let ids: BTreeSet<_> = scenario.exchanges.iter().map(|e| e.id).collect();
    assert_eq!(ids.len(), scenario.exchanges.len());
    for exchange in &scenario.exchanges {
        assert!(
            exchange.started_at < exchange.first_chunk_at,
            "{}",
            exchange.label
        );
        assert!(
            exchange.first_chunk_at < exchange.ended_at,
            "{}",
            exchange.label
        );
    }
    for pair in scenario.exchanges.windows(2) {
        assert!(
            pair[0].ended_at < pair[1].started_at,
            "{} overlaps {}",
            pair[0].label,
            pair[1].label
        );
    }
}

#[test]
fn a_later_start_shifts_every_time_and_nothing_else() {
    let early = relay();
    let shift = 3_600_000_000;
    let late = Scenario::wiki_relay(crosstalk_spec::support::Timestamp::from_micros(
        early.start.as_micros() + shift,
    ));
    for (a, b) in early.exchanges.iter().zip(&late.exchanges) {
        assert_eq!(a.started_at.as_micros() + shift, b.started_at.as_micros());
        assert_eq!(a.ended_at.as_micros() + shift, b.ended_at.as_micros());
        assert_eq!(a.request, b.request, "{}", a.label);
        assert_eq!(a.response, b.response, "{}", a.label);
    }
}

#[test]
fn l0_sees_two_claude_code_sessions_under_two_api_keys() -> Result<(), Failure> {
    let scenario = relay();
    let exchanges = normalized(&scenario)?;
    let mut credentials = BTreeSet::new();
    for (wire, normalized) in &exchanges {
        let meta = &normalized.exchange.meta;
        let agent = scenario
            .agents
            .iter()
            .find(|agent| agent.name == wire.agent)
            .ok_or_else(|| unexpected(format!("no agent {}", wire.agent)))?;
        assert_eq!(meta.id, wire.id);
        assert_eq!(meta.started_at, wire.started_at);
        assert_eq!(meta.transport, Transport::Sse);
        assert!(matches!(
            &meta.client.ingress,
            IngressMode::ReverseProxy { route } if route.0 == crosstalk_e2e::capture::ROUTE
        ));
        let credential = meta
            .client
            .credential
            .as_ref()
            .ok_or_else(|| unexpected(format!("{}: no credential", wire.label)))?;
        assert_eq!(
            credential.scheme,
            CredentialScheme::ApiKey,
            "{}",
            wire.label
        );
        credentials.insert((wire.agent, credential.hash));
        assert_eq!(
            meta.client.ids.session.as_deref(),
            Some(agent.headers.session_id.as_str()),
            "{}",
            wire.label
        );
        assert_eq!(meta.client.ids.agent, None);
        assert_eq!(meta.client.ids.parent_agent, None);
        let claim = meta
            .client
            .harness
            .as_ref()
            .ok_or_else(|| unexpected(format!("{}: no harness claim", wire.label)))?;
        assert_eq!(claim.family, HarnessFamily::ClaudeCode);
    }
    // One digest per agent, and the two differ.
    assert_eq!(credentials.len(), 2);
    let hashes: BTreeSet<_> = credentials.iter().map(|(_, hash)| *hash).collect();
    assert_eq!(hashes.len(), 2);
    Ok(())
}

/// L3 threads by message-hash prefix: each agent's second request replays
/// its first request and the response it got, then adds one tool result.
#[test]
fn each_follow_up_replays_the_history_and_adds_one_tool_result() -> Result<(), Failure> {
    let scenario = relay();
    let exchanges = normalized(&scenario)?;
    for agent in ["a", "b"] {
        let mine: Vec<&NormalizedExchange> = exchanges
            .iter()
            .filter(|(wire, _)| wire.agent == agent)
            .map(|(_, normalized)| normalized)
            .collect();
        let [first, second] = mine.as_slice() else {
            return Err(unexpected(format!("agent {agent}: not two exchanges")));
        };
        let mut expected = first.exchange.request.clone();
        expected.push(response(first).ok_or_else(|| unexpected("first failed"))?);
        let request = &second.exchange.request;
        assert_eq!(request.len(), expected.len() + 1, "agent {agent}");
        assert_eq!(request[..expected.len()], expected[..], "agent {agent}");
        let added = request[expected.len()];
        assert!(
            matches!(body(second, added), Some(MessageBody::Tool(_))),
            "agent {agent}: the new input is not a tool result"
        );
    }
    Ok(())
}

#[test]
fn a_writes_the_page_with_the_sentence_in_its_own_output() -> Result<(), Failure> {
    let scenario = relay();
    let exchanges = normalized(&scenario)?;
    let (_, write) = exchanges
        .iter()
        .find(|(wire, _)| wire.label == "a1-write")
        .ok_or_else(|| unexpected("no a1-write"))?;
    let output = response(write).ok_or_else(|| unexpected("a1-write failed"))?;
    let Some(MessageBody::Assistant(parts)) = body(write, output) else {
        return Err(unexpected(
            "a1-write's response is not an assistant message",
        ));
    };
    let call = parts
        .iter()
        .find_map(|part| match part {
            AssistantPart::ToolCall(call) => Some(call),
            _ => None,
        })
        .ok_or_else(|| unexpected("a1-write makes no tool call"))?;
    assert_eq!(call.name.0, "Write");
    let ToolArguments::Json(arguments) = &call.arguments else {
        return Err(unexpected("the Write arguments are not JSON"));
    };
    let arguments: serde_json::Value = serde_json::from_str(&arguments.0)?;
    assert_eq!(arguments["file_path"], WIKI_PAGE);
    let content = arguments["content"]
        .as_str()
        .ok_or_else(|| unexpected("no content argument"))?;
    assert!(content.contains(SENTENCE));
    // Canonical JSON keeps the sentence verbatim: L4 fingerprints it as
    // written.
    assert!(arguments.to_string().contains(SENTENCE));
    Ok(())
}

#[test]
fn b_reads_the_page_and_repeats_the_sentence() -> Result<(), Failure> {
    let scenario = relay();
    let exchanges = normalized(&scenario)?;
    let (_, read) = exchanges
        .iter()
        .find(|(wire, _)| wire.label == "b1-read")
        .ok_or_else(|| unexpected("no b1-read"))?;
    let output = response(read).ok_or_else(|| unexpected("b1-read failed"))?;
    let Some(MessageBody::Assistant(parts)) = body(read, output) else {
        return Err(unexpected("b1-read's response is not an assistant message"));
    };
    let call = parts
        .iter()
        .find_map(|part| match part {
            AssistantPart::ToolCall(call) => Some(call),
            _ => None,
        })
        .ok_or_else(|| unexpected("b1-read makes no tool call"))?;
    assert_eq!(call.name.0, "Read");

    let (_, repeat) = exchanges
        .iter()
        .find(|(wire, _)| wire.label == "b2-repeat")
        .ok_or_else(|| unexpected("no b2-repeat"))?;
    let last = *repeat
        .exchange
        .request
        .last()
        .ok_or_else(|| unexpected("b2-repeat sends nothing"))?;
    let Some(MessageBody::Tool(results)) = body(repeat, last) else {
        return Err(unexpected("b2-repeat's last input is not a tool result"));
    };
    let result = results.first();
    assert_eq!(result.call_id, call.id);
    let text: String = result
        .content
        .iter()
        .filter_map(|content| match content {
            ToolResultContent::Text(text) => Some(text.0.as_str()),
            _ => None,
        })
        .collect();
    assert!(
        text.contains(SENTENCE),
        "the read result lacks the sentence"
    );

    let output = response(repeat).ok_or_else(|| unexpected("b2-repeat failed"))?;
    let Some(MessageBody::Assistant(parts)) = body(repeat, output) else {
        return Err(unexpected(
            "b2-repeat's response is not an assistant message",
        ));
    };
    let said: String = parts
        .iter()
        .filter_map(|part| match part {
            AssistantPart::Text(text) => Some(text.0.as_str()),
            _ => None,
        })
        .collect();
    assert!(
        said.contains(SENTENCE),
        "B's answer does not repeat the sentence"
    );
    Ok(())
}

#[test]
fn the_sentence_is_long_plain_text() {
    // Plain ASCII without quotes, backslashes or newlines, so canonical
    // JSON leaves it as written, and long enough for any winnowing window.
    assert!(SENTENCE.len() >= 100);
    assert!(
        SENTENCE
            .chars()
            .all(|c| c.is_ascii() && !c.is_ascii_control() && c != '"' && c != '\\')
    );
}
