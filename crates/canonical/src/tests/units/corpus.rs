//! The recorded corpus: goldens, expectations, transports and echoes.

use std::collections::BTreeMap;

use crosstalk_spec::interfaces::l0_ingress::RawResponse;
use crosstalk_spec::observed::exchange::{ExchangeOutcome, Transport};
use crosstalk_spec::observed::message::{AssistantPart, Reasoning, ToolExecution};
use crosstalk_testkit::corpus::{BlockKind, Case, Endpoint, Expect};
use serde_json::{Value, json};

use crate::tests::golden::assert_normalization_golden;
use crate::tests::support::{case, corpus, normalize, raw_bytes, response, response_parts};

/// Every captured case normalizes to its golden.
pub fn corpus_cases_match_goldens() {
    let cases = corpus();
    assert!(cases.len() >= 10, "the generation cases load");
    for (case, raw) in cases {
        assert_normalization_golden("anthropic", &case.name, &normalize(&raw));
    }
}

fn block_kind(part: &AssistantPart) -> Option<BlockKind> {
    match part {
        AssistantPart::Text(_) => Some(BlockKind::Text),
        AssistantPart::Reasoning(Reasoning::Visible(_)) => Some(BlockKind::Thinking),
        AssistantPart::Reasoning(Reasoning::Opaque { .. }) => Some(BlockKind::RedactedThinking),
        AssistantPart::ToolCall(call) if call.execution == ToolExecution::Client => {
            Some(BlockKind::ToolUse)
        }
        AssistantPart::ToolCall(_) => Some(BlockKind::ServerToolUse),
        AssistantPart::ServerToolResult(_) | AssistantPart::Unknown(_) => None,
    }
}

/// Every captured case ends as its `meta.json` expects: the stop reason,
/// response id and block kinds, or the failure and the blocks started
/// before it.
pub fn corpus_cases_meet_their_expectations() {
    for (case, raw) in corpus() {
        let Endpoint::Generation { expect, .. } = &case.meta.endpoint else {
            continue;
        };
        let normalization = normalize(&raw);
        let exchange = &normalization.exchange;
        assert!(
            exchange.warnings.is_empty(),
            "{}: {:?}",
            case.name,
            exchange.warnings
        );
        let kinds = |exchange| -> Vec<BlockKind> {
            response(exchange)
                .map(|_| {
                    response_parts(exchange)
                        .iter()
                        .filter_map(block_kind)
                        .collect()
                })
                .unwrap_or_default()
        };
        match (expect, &exchange.exchange.outcome) {
            (
                Expect::Completed {
                    stop,
                    response_id,
                    blocks,
                },
                ExchangeOutcome::Completed {
                    stop: got,
                    response_id: got_id,
                    usage,
                    ..
                },
            ) => {
                assert_eq!(got, stop, "{}", case.name);
                assert_eq!(got_id.as_ref(), Some(response_id), "{}", case.name);
                assert_eq!(&kinds(exchange), blocks, "{}", case.name);
                assert!(usage.is_some(), "{}: usage", case.name);
            }
            (
                Expect::Failed {
                    failure,
                    partial_blocks,
                },
                ExchangeOutcome::Failed { failure: got, .. },
            ) => {
                assert_eq!(got, failure, "{}", case.name);
                assert_eq!(&kinds(exchange), partial_blocks, "{}", case.name);
            }
            (expect, outcome) => panic!("{}: expected {expect:?}, got {outcome:?}", case.name),
        }
    }
}

/// The token usage the corpus's cache cases map to: every prompt token in
/// `input`, cache reads in `cache_read`.
pub fn corpus_usage_maps_cache_tokens() {
    for (name, input, cache_read, output) in [
        ("system_cache_control", 6 + 3816, 0, 7),
        ("tool_use_streaming", 9 + 3810, 3810, 61),
        ("text_turn", 14 + 3810, 3810, 19),
    ] {
        let (_, raw) = case(name);
        let ExchangeOutcome::Completed { usage, .. } = normalize(&raw).exchange.exchange.outcome
        else {
            panic!("{name} completes");
        };
        let usage = usage.unwrap_or_else(|| panic!("{name}: usage"));
        assert_eq!(
            (usage.input, usage.cache_read, usage.output, usage.reasoning),
            (input, cache_read, output, None),
            "{name}"
        );
    }
}

/// A streamed case's whole-body form, rebuilt from its events with
/// `serde_json`: an independent reassembly.
fn whole_of(case: &Case) -> String {
    let stream = case
        .response
        .body
        .events()
        .unwrap_or_else(|| panic!("{} streams", case.name));
    let mut message = Value::Null;
    let mut blocks: BTreeMap<u64, Value> = BTreeMap::new();
    let mut inputs: BTreeMap<u64, String> = BTreeMap::new();
    for event in stream.dispatched() {
        let data = event.json().unwrap_or_else(|error| panic!("{error}"));
        let index = data["index"].as_u64().unwrap_or_default();
        match data["type"].as_str().unwrap_or_default() {
            "message_start" => message = data["message"].clone(),
            "content_block_start" => {
                blocks.insert(index, data["content_block"].clone());
            }
            "content_block_delta" => {
                let delta = &data["delta"];
                let block = blocks.entry(index).or_default();
                let mut append = |field: &str, from: &str| {
                    let text = block[field].as_str().unwrap_or_default().to_owned()
                        + delta[from].as_str().unwrap_or_default();
                    block[field] = Value::String(text);
                };
                match delta["type"].as_str().unwrap_or_default() {
                    "text_delta" => append("text", "text"),
                    "thinking_delta" => append("thinking", "thinking"),
                    "signature_delta" => append("signature", "signature"),
                    "input_json_delta" => inputs
                        .entry(index)
                        .or_default()
                        .push_str(delta["partial_json"].as_str().unwrap_or_default()),
                    other => panic!("an unexpected delta {other}"),
                }
            }
            "message_delta" => {
                message["stop_reason"] = data["delta"]["stop_reason"].clone();
                if let Some(usage) = data["usage"].as_object() {
                    for (name, value) in usage {
                        message["usage"][name] = value.clone();
                    }
                }
            }
            _ => {}
        }
    }
    for (index, text) in inputs {
        if !text.is_empty() {
            blocks.entry(index).or_default()["input"] =
                serde_json::from_str(&text).unwrap_or_else(|error| panic!("{error}"));
        }
    }
    message["content"] = Value::Array(blocks.into_values().collect());
    message.to_string()
}

/// A whole case's event stream: each block started empty and filled by one
/// delta.
fn stream_of(case: &Case) -> String {
    let body: Value =
        serde_json::from_slice(case.response_bytes()).unwrap_or_else(|e| panic!("{e}"));
    let mut start = body.clone();
    start["content"] = json!([]);
    start["stop_reason"] = Value::Null;
    let mut events = vec![json!({"type": "message_start", "message": start})];
    for (index, block) in body["content"].as_array().into_iter().flatten().enumerate() {
        let (first, delta) = match block["type"].as_str().unwrap_or_default() {
            "text" => (
                json!({"type": "text", "text": ""}),
                json!({"type": "text_delta", "text": block["text"]}),
            ),
            "tool_use" => {
                let mut first = block.clone();
                first["input"] = json!({});
                (
                    first,
                    json!({"type": "input_json_delta", "partial_json": block["input"].to_string()}),
                )
            }
            other => panic!("an unexpected block {other}"),
        };
        events.push(json!({"type": "content_block_start", "index": index, "content_block": first}));
        events.push(json!({"type": "content_block_delta", "index": index, "delta": delta}));
        events.push(json!({"type": "content_block_stop", "index": index}));
    }
    events.push(json!({"type": "message_delta", "delta": {"stop_reason": body["stop_reason"], "stop_sequence": null}, "usage": {"output_tokens": body["usage"]["output_tokens"]}}));
    events.push(json!({"type": "message_stop"}));
    events
        .iter()
        .map(|event| {
            format!(
                "event: {}\ndata: {event}\n\n",
                event["type"].as_str().unwrap_or_default()
            )
        })
        .collect()
}

/// Each completed case, streamed or not, normalizes to the same response
/// message and outcome when delivered the other way.
pub fn recorded_transports_normalize_equal() {
    let mut compared = 0;
    for (case, raw) in corpus() {
        let Endpoint::Generation {
            expect: Expect::Completed { .. },
            ..
        } = &case.meta.endpoint
        else {
            continue;
        };
        let (transport, body) = match raw.meta.transport {
            Transport::Sse => (Transport::Http, whole_of(&case)),
            _ => (Transport::Sse, stream_of(&case)),
        };
        let other = raw_bytes(
            &raw.request.body,
            transport,
            RawResponse::Complete {
                status: 200,
                body: body.into_bytes(),
            },
        );
        let (one, two) = (normalize(&raw), normalize(&other));
        assert_eq!(
            response(&one.exchange).map(|message| message.hash),
            response(&two.exchange).map(|message| message.hash),
            "{}",
            case.name
        );
        assert_eq!(
            one.exchange.exchange.outcome, two.exchange.exchange.outcome,
            "{}",
            case.name
        );
        compared += 1;
    }
    assert!(compared >= 8, "both directions are covered");
}

/// The assistant turn `tool_result_followup` echoes is the message
/// `tool_use_streaming` responded with: same hash.
pub fn recorded_echoes_hash_like_their_responses() {
    let (_, first) = case("tool_use_streaming");
    let (_, followup) = case("tool_result_followup");
    let response = match normalize(&first).exchange.exchange.outcome {
        ExchangeOutcome::Completed { response, .. } => response,
        other => panic!("completed: {other:?}"),
    };
    let request = normalize(&followup).exchange.exchange.request;
    assert_eq!(
        request.get(2),
        Some(&response),
        "system, user, then the echo"
    );
}
