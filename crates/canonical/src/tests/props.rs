//! Property bodies: each takes generated input and checks its oracle. The
//! `proptest!` entry points named by the invariants are in `tests/mod.rs`.

use crosstalk_spec::interfaces::l0_ingress::{ContentEncoding, RawExchange, RawResponse};
use crosstalk_spec::interfaces::l1_canonical::NormalizeWarning;
use crosstalk_spec::observed::client::Dialect;
use crosstalk_spec::observed::exchange::{
    Continuation, ExchangeFailure, ExchangeOutcome, Transport,
};
use crosstalk_spec::observed::message::{
    AssistantPart, MessageBody, Reasoning, SystemPart, Text, ToolArguments, ToolExecution,
    ToolResult, ToolResultContent, Unknown, UserPart,
};
use proptest::prelude::*;
use proptest::test_runner::TestCaseError;

use crate::encoding;
use crate::json::{Json, canonicalize};
use crate::tests::generate::Style;
use crate::tests::generate::anthropic::{
    GenBlock, GenRequest, GenResponse, GenSystem, GenSystemBlock, GenTurn, GenUserBlock,
    write_event,
};
use crate::tests::generate::json::{GenJson, GenNumber, string};
use crate::tests::support::{
    START, USER_TURN, normalize, ok, raw, request_bodies, response, response_parts, stop,
};

type Checked = Result<(), TestCaseError>;

/// How a generated exchange's response arrives.
#[derive(Debug, Clone)]
pub enum GenDelivery {
    Whole(GenResponse),
    Stream(GenResponse),
    /// The stream cut after `keep` bytes, failed by the proxy.
    Cut {
        response: GenResponse,
        keep: usize,
        failure: ExchangeFailure,
    },
    Status(u16, Vec<u8>),
    Garbage(Transport, Vec<u8>),
}

pub fn arb_failure() -> impl Strategy<Value = ExchangeFailure> {
    prop_oneof![
        Just(ExchangeFailure::StreamTruncated),
        any::<u64>().prop_map(|offset| ExchangeFailure::MalformedStream { offset }),
        Just(ExchangeFailure::UpstreamErrorEvent),
        Just(ExchangeFailure::ClientDisconnected),
        Just(ExchangeFailure::Timeout),
        Just(ExchangeFailure::UpstreamUnreachable),
    ]
}

pub fn arb_delivery() -> impl Strategy<Value = GenDelivery> {
    use crate::tests::generate::anthropic::arb_response;
    prop_oneof![
        arb_response().prop_map(GenDelivery::Whole),
        arb_response().prop_map(GenDelivery::Stream),
        (arb_response(), any::<usize>(), arb_failure()).prop_map(|(response, keep, failure)| {
            GenDelivery::Cut {
                response,
                keep,
                failure,
            }
        }),
        (400u16..600, proptest::collection::vec(any::<u8>(), 0..64))
            .prop_map(|(status, body)| GenDelivery::Status(status, body)),
        (
            prop_oneof![Just(Transport::Http), Just(Transport::Sse)],
            proptest::collection::vec(any::<u8>(), 0..64)
        )
            .prop_map(|(transport, body)| GenDelivery::Garbage(transport, body)),
    ]
}

/// The raw exchange of a generated request and delivery.
pub fn exchange(request: &GenRequest, delivery: &GenDelivery, seed: u64) -> RawExchange {
    let mut style = Style::new(seed);
    let body = request.render(&mut style);
    let (transport, response) = match delivery {
        GenDelivery::Whole(response) => (Transport::Http, ok(&response.whole(&mut style))),
        GenDelivery::Stream(response) => (Transport::Sse, ok(&response.stream(&mut style))),
        GenDelivery::Cut {
            response,
            keep,
            failure,
        } => {
            let bytes = response.stream(&mut style).into_bytes();
            let keep = keep % (bytes.len() + 1);
            (
                Transport::Sse,
                RawResponse::Failed {
                    failure: *failure,
                    partial_body: bytes[..keep].to_vec(),
                },
            )
        }
        GenDelivery::Status(status, body) => (
            Transport::Http,
            RawResponse::Complete {
                status: *status,
                body: body.clone(),
            },
        ),
        GenDelivery::Garbage(transport, body) => (
            *transport,
            RawResponse::Complete {
                status: 200,
                body: body.clone(),
            },
        ),
    };
    raw(&body, transport, response)
}

/// Every hash the exchange names resolves to one of its messages.
pub fn exchange_hashes_resolve(request: &GenRequest, delivery: &GenDelivery, seed: u64) -> Checked {
    let exchange = normalize(&exchange(request, delivery, seed)).exchange;
    let hashes: Vec<_> = exchange
        .messages
        .iter()
        .map(|message| message.hash)
        .collect();
    let mut named = exchange.exchange.request.clone();
    match &exchange.exchange.outcome {
        ExchangeOutcome::Completed { response, .. } => named.push(*response),
        ExchangeOutcome::Failed {
            partial_response, ..
        } => named.extend(*partial_response),
    }
    for hash in named {
        prop_assert!(hashes.contains(&hash), "{hash:?} names no message");
    }
    let mut distinct = hashes.clone();
    distinct.sort();
    distinct.dedup();
    prop_assert_eq!(distinct.len(), hashes.len(), "each body is kept once");
    Ok(())
}

/// Every message's hash is the BLAKE3 of its encoding.
pub fn hashes_match_encoding(request: &GenRequest, delivery: &GenDelivery, seed: u64) -> Checked {
    let normalization = normalize(&exchange(request, delivery, seed));
    for message in &normalization.exchange.messages {
        let bytes = encoding::encode(&message.body);
        prop_assert_eq!(
            *message.hash.digest().as_bytes(),
            *blake3::hash(&bytes).as_bytes()
        );
    }
    for media in &normalization.media {
        prop_assert_eq!(
            *media.hash.digest().as_bytes(),
            *blake3::hash(&media.bytes).as_bytes()
        );
    }
    Ok(())
}

/// The response or partial response is always an assistant message.
pub fn response_is_assistant(request: &GenRequest, delivery: &GenDelivery, seed: u64) -> Checked {
    let exchange = normalize(&exchange(request, delivery, seed)).exchange;
    if let Some(message) = response(&exchange) {
        prop_assert!(matches!(message.body, MessageBody::Assistant(_)));
    }
    Ok(())
}

/// Normalizing twice gives equal results.
pub fn deterministic(request: &GenRequest, delivery: &GenDelivery, seed: u64) -> Checked {
    let raw = exchange(request, delivery, seed);
    prop_assert_eq!(normalize(&raw), normalize(&raw.clone()));
    Ok(())
}

/// The upstream's dialect changes nothing: one normalizer, no dialect
/// branches.
pub fn dialect_independent(request: &GenRequest, delivery: &GenDelivery, seed: u64) -> Checked {
    let raw = exchange(request, delivery, seed);
    let reference = normalize(&raw);
    for dialect in [Dialect::Vllm, Dialect::Sglang, Dialect::Copilot] {
        let mut other = raw.clone();
        other.request.harness.dialect = dialect;
        prop_assert_eq!(&normalize(&other), &reference);
    }
    Ok(())
}

/// The request's content encoding changes nothing.
pub fn ignores_request_encoding(
    request: &GenRequest,
    delivery: &GenDelivery,
    seed: u64,
) -> Checked {
    let raw = exchange(request, delivery, seed);
    let reference = normalize(&raw);
    for encoding in [ContentEncoding::Gzip, ContentEncoding::Zstd] {
        let mut encoded = raw.clone();
        encoded.request.encoding = encoding;
        prop_assert_eq!(&normalize(&encoded), &reference);
    }
    Ok(())
}

/// The exchange's continuation is the decoded request's.
pub fn continuation_carried(
    request: &GenRequest,
    continuation: Continuation,
    seed: u64,
) -> Checked {
    let mut raw = exchange(
        request,
        &GenDelivery::Garbage(Transport::Http, Vec::new()),
        seed,
    );
    raw.request.harness.continuation = continuation.clone();
    prop_assert_eq!(normalize(&raw).exchange.exchange.continuation, continuation);
    Ok(())
}

/// Every Unknown part has an UnknownBlock warning of its kind: as many
/// warnings of each kind as parts.
pub fn unknown_parts_warned(request: &GenRequest, delivery: &GenDelivery, seed: u64) -> Checked {
    let exchange = normalize(&exchange(request, delivery, seed)).exchange;
    let mut parts: Vec<String> = Vec::new();
    let mut add = |unknown: &Unknown| parts.push(unknown.kind.clone());
    let mut bodies = request_bodies(&exchange);
    if let Some(message) = response(&exchange) {
        bodies.push(message.body.clone());
    }
    for body in &bodies {
        for_each_unknown(body, &mut add);
    }
    let mut warned: Vec<String> = exchange
        .warnings
        .iter()
        .filter_map(|warning| match warning {
            NormalizeWarning::UnknownBlock { kind } => Some(kind.clone()),
            NormalizeWarning::OrphanToolResult { .. } => None,
        })
        .collect();
    parts.sort();
    warned.sort();
    prop_assert_eq!(parts, warned);
    Ok(())
}

pub fn for_each_unknown(body: &MessageBody, visit: &mut impl FnMut(&Unknown)) {
    fn results(result: &ToolResult, visit: &mut impl FnMut(&Unknown)) {
        for content in &result.content {
            if let ToolResultContent::Unknown(unknown) = content {
                visit(unknown);
            }
        }
    }
    match body {
        MessageBody::System(parts) => {
            for part in parts {
                if let SystemPart::Unknown(unknown) = part {
                    visit(unknown);
                }
            }
        }
        MessageBody::User(parts) => {
            for part in parts {
                if let UserPart::Unknown(unknown) = part {
                    visit(unknown);
                }
            }
        }
        MessageBody::Assistant(parts) => {
            for part in parts {
                match part {
                    AssistantPart::Unknown(unknown) => visit(unknown),
                    AssistantPart::ServerToolResult(result) => results(result, visit),
                    _ => {}
                }
            }
        }
        MessageBody::Tool(items) => {
            for result in items.iter() {
                results(result, visit);
            }
        }
    }
}

/// The request after its System message is each provider message
/// normalized alone, concatenated in order.
pub fn request_message_by_message(request: &GenRequest, seed: u64) -> Checked {
    let empty = GenDelivery::Garbage(Transport::Http, Vec::new());
    let whole = request_bodies(&normalize(&exchange(request, &empty, seed)).exchange);
    let rest: Vec<MessageBody> = match (&request.system, whole.first()) {
        (GenSystem::Absent, _) => whole.clone(),
        (_, Some(MessageBody::System(_))) => whole[1..].to_vec(),
        (system, first) => {
            return Err(TestCaseError::fail(format!(
                "{system:?} gave first message {first:?}"
            )));
        }
    };
    let mut alone = Vec::new();
    for (at, turn) in request.turns.iter().enumerate() {
        let single = GenRequest {
            system: GenSystem::Absent,
            turns: vec![turn.clone()],
        };
        let seed = seed.wrapping_add(u64::try_from(at).unwrap_or(0) + 1);
        alone.extend(request_bodies(
            &normalize(&exchange(&single, &empty, seed)).exchange,
        ));
    }
    prop_assert_eq!(rest, alone);
    Ok(())
}

/// What a user block becomes: its role and identity.
#[derive(Debug, Clone, PartialEq)]
enum Item {
    Text(String),
    Media,
    Result(String),
    Unknown(String),
}

/// A user turn becomes maximal runs of one role, every block once, in
/// order.
pub fn split_preserves_blocks(blocks: &[GenUserBlock], seed: u64) -> Checked {
    let request = GenRequest {
        system: GenSystem::Absent,
        turns: vec![GenTurn::User(blocks.to_vec())],
    };
    let empty = GenDelivery::Garbage(Transport::Http, Vec::new());
    let bodies = request_bodies(&normalize(&exchange(&request, &empty, seed)).exchange);
    let expected: Vec<(bool, Item)> = blocks
        .iter()
        .map(|block| match block {
            GenUserBlock::Text(text) => (false, Item::Text(text.clone())),
            GenUserBlock::Image(_) => (false, Item::Media),
            GenUserBlock::ToolResult { id, .. } => (true, Item::Result(id.clone())),
            GenUserBlock::Unknown { kind, .. } => (false, Item::Unknown(kind.clone())),
        })
        .collect();
    let mut got: Vec<(bool, Item)> = Vec::new();
    let mut roles = Vec::new();
    for body in &bodies {
        match body {
            MessageBody::User(parts) => {
                roles.push(false);
                got.extend(parts.iter().map(|part| match part {
                    UserPart::Text(text) => (false, Item::Text(text.0.clone())),
                    UserPart::Media(_) => (false, Item::Media),
                    UserPart::Unknown(unknown) => (false, Item::Unknown(unknown.kind.clone())),
                }));
            }
            MessageBody::Tool(results) => {
                roles.push(true);
                got.extend(
                    results
                        .iter()
                        .map(|result| (true, Item::Result(result.call_id.0.clone()))),
                );
            }
            other => return Err(TestCaseError::fail(format!("a user turn gave {other:?}"))),
        }
    }
    prop_assert_eq!(got, expected);
    prop_assert!(
        roles.windows(2).all(|pair| pair[0] != pair[1]),
        "adjacent messages differ in role: runs are maximal"
    );
    if blocks.is_empty() {
        prop_assert_eq!(bodies, vec![MessageBody::User(Vec::new())]);
    }
    Ok(())
}

fn completed_response(raw: &RawExchange) -> Result<(MessageBody, ExchangeOutcome), TestCaseError> {
    let exchange = normalize(raw).exchange;
    let body = response(&exchange)
        .map(|message| message.body.clone())
        .ok_or_else(|| TestCaseError::fail("no response"))?;
    Ok((body, exchange.exchange.outcome))
}

/// A response sent whole and streamed (deltas cut anywhere, pings,
/// interleaved blocks) normalizes to the same message and outcome.
pub fn transport_independent(response: &GenResponse, seed: u64) -> Checked {
    let mut style = Style::new(seed);
    let whole = raw(USER_TURN, Transport::Http, ok(&response.whole(&mut style)));
    let streamed = raw(USER_TURN, Transport::Sse, ok(&response.stream(&mut style)));
    let (whole_body, whole_outcome) = completed_response(&whole)?;
    let (stream_body, stream_outcome) = completed_response(&streamed)?;
    prop_assert_eq!(whole_body, stream_body);
    prop_assert_eq!(whole_outcome, stream_outcome);
    Ok(())
}

/// A streamed response and the same blocks echoed in the next request (re-
/// serialized, members reordered, cache markers added) hash the same.
pub fn echo_hashes_equal(response: &GenResponse, seed: u64) -> Checked {
    let mut style = Style::new(seed);
    let streamed = raw(USER_TURN, Transport::Sse, ok(&response.stream(&mut style)));
    let exchange = normalize(&streamed).exchange;
    let ExchangeOutcome::Completed { response: hash, .. } = exchange.exchange.outcome else {
        return Err(TestCaseError::fail("the stream completes"));
    };
    let next = GenRequest {
        system: GenSystem::Str("system".to_owned()),
        turns: vec![
            GenTurn::UserText("hi".to_owned()),
            GenTurn::Assistant(response.blocks.clone()),
        ],
    };
    let echo = exchange_of(&next, &mut style);
    let echoed = normalize(&echo).exchange.exchange.request;
    prop_assert_eq!(echoed.get(2), Some(&hash));
    Ok(())
}

fn exchange_of(request: &GenRequest, style: &mut Style) -> RawExchange {
    raw(&request.render(style), Transport::Http, ok("{}"))
}

/// An unrecognized block's `Unknown` holds its type and its canonical JSON,
/// in every part list.
pub fn unknown_blocks_canonical(request: &GenRequest, seed: u64) -> Checked {
    let mut style = Style::new(seed);
    let exchange = normalize(&exchange_of(request, &mut style)).exchange;
    let mut got: Vec<Unknown> = Vec::new();
    for body in request_bodies(&exchange) {
        for_each_unknown(&body, &mut |unknown| got.push(unknown.clone()));
    }
    let mut expected: Vec<Unknown> = Vec::new();
    let mut push = |kind: &str, payload: &GenJson| {
        let block = GenJson::Object(vec![
            ("type".to_owned(), GenJson::Str(kind.to_owned())),
            ("payload".to_owned(), payload.clone()),
        ]);
        expected.push(Unknown {
            kind: kind.to_owned(),
            raw: block.value().canonical(),
        });
    };
    if let GenSystem::Blocks(blocks) = &request.system {
        for block in blocks {
            if let GenSystemBlock::Unknown { kind, payload } = block {
                push(kind, payload);
            }
        }
    }
    for turn in &request.turns {
        match turn {
            GenTurn::UserText(_) => {}
            GenTurn::User(blocks) => {
                for block in blocks {
                    match block {
                        GenUserBlock::Unknown { kind, payload } => push(kind, payload),
                        GenUserBlock::ToolResult {
                            content:
                                crate::tests::generate::anthropic::GenResultContent::Items(items),
                            ..
                        } => {
                            for item in items {
                                if let crate::tests::generate::anthropic::GenResultItem::Unknown {
                                    kind,
                                    payload,
                                } = item
                                {
                                    push(kind, payload);
                                }
                            }
                        }
                        _ => {}
                    }
                }
            }
            GenTurn::Assistant(blocks) => {
                for block in blocks {
                    if let GenBlock::Unknown { kind, payload } = block {
                        push(kind, payload);
                    }
                }
            }
        }
    }
    // Only the generated unknown types: an unpaired server result is kept
    // as `Unknown` too, with whatever marker its echo carried.
    got.retain(|unknown| unknown.kind.starts_with("zz_"));
    // A raw block parses back to the value it was generated from.
    for unknown in &got {
        let reparsed =
            Json::parse(&unknown.raw.0).map_err(|error| TestCaseError::fail(error.to_string()))?;
        prop_assert_eq!(reparsed.canonical(), unknown.raw.clone());
    }
    prop_assert_eq!(got, expected);
    Ok(())
}

/// Opaque reasoning payloads are carried byte for byte, in responses and
/// requests.
pub fn opaque_preserved(data: &str, seed: u64) -> Checked {
    let mut style = Style::new(seed);
    let block = GenBlock::Redacted(data.to_owned());
    let response = GenResponse {
        id: "msg_1".to_owned(),
        blocks: vec![block.clone()],
        stop_reason: "end_turn",
        usage: (1, 0, 0, 1),
    };
    let expected = vec![AssistantPart::Reasoning(Reasoning::Opaque {
        signature: data.to_owned(),
    })];
    for raw in [
        raw(USER_TURN, Transport::Sse, ok(&response.stream(&mut style))),
        raw(USER_TURN, Transport::Http, ok(&response.whole(&mut style))),
    ] {
        prop_assert_eq!(response_parts(&normalize(&raw).exchange), expected.clone());
    }
    let echo = GenRequest {
        system: GenSystem::Absent,
        turns: vec![GenTurn::Assistant(vec![block])],
    };
    prop_assert_eq!(
        request_bodies(&normalize(&exchange_of(&echo, &mut style)).exchange),
        vec![MessageBody::Assistant(expected)]
    );
    Ok(())
}

/// Text parts hold the decoded strings exactly, wherever text appears.
pub fn text_preserved(text: &str, seed: u64) -> Checked {
    let mut style = Style::new(seed);
    let request = GenRequest {
        system: GenSystem::Str(text.to_owned()),
        turns: vec![
            GenTurn::User(vec![
                GenUserBlock::Text(text.to_owned()),
                GenUserBlock::ToolResult {
                    id: "t".to_owned(),
                    content: crate::tests::generate::anthropic::GenResultContent::Str(
                        text.to_owned(),
                    ),
                    is_error: false,
                },
            ]),
            GenTurn::Assistant(vec![GenBlock::Text(text.to_owned())]),
        ],
    };
    let bodies = request_bodies(&normalize(&exchange_of(&request, &mut style)).exchange);
    let want = Text(text.to_owned());
    prop_assert_eq!(
        &bodies[0],
        &MessageBody::System(vec![SystemPart::Text(want.clone())])
    );
    prop_assert_eq!(
        &bodies[1],
        &MessageBody::User(vec![UserPart::Text(want.clone())])
    );
    let MessageBody::Tool(results) = &bodies[2] else {
        return Err(TestCaseError::fail("a tool message"));
    };
    prop_assert_eq!(
        &results.first().content,
        &vec![ToolResultContent::Text(want.clone())]
    );
    prop_assert_eq!(
        &bodies[3],
        &MessageBody::Assistant(vec![AssistantPart::Text(want.clone())])
    );
    let response = GenResponse {
        id: "msg_1".to_owned(),
        blocks: vec![
            GenBlock::Text(text.to_owned()),
            GenBlock::Thinking {
                text: text.to_owned(),
                signature: String::new(),
            },
        ],
        stop_reason: "end_turn",
        usage: (1, 0, 0, 1),
    };
    let raw = raw(USER_TURN, Transport::Sse, ok(&response.stream(&mut style)));
    prop_assert_eq!(
        response_parts(&normalize(&raw).exchange),
        vec![
            AssistantPart::Text(want.clone()),
            AssistantPart::Reasoning(Reasoning::Visible(want)),
        ]
    );
    Ok(())
}

/// Streamed argument text is `Json` exactly when it is JSON (judged by
/// serde_json, independently), its canonical form; otherwise `Invalid`,
/// verbatim.
pub fn arguments_by_parse(text: &str, seed: u64) -> Checked {
    let mut style = Style::new(seed);
    let mut events = String::new();
    let mut event = |name: &str, data: String, style: &mut Style| {
        write_event(&mut events, name, &data, false, style);
    };
    event(START.0, START.1.to_owned(), &mut style);
    event(
        "content_block_start",
        r#"{"type":"content_block_start","index":0,"content_block":{"type":"tool_use","id":"t","name":"n","input":{}}}"#.to_owned(),
        &mut style,
    );
    for piece in style.clone().split(text) {
        let data = format!(
            r#"{{"type":"content_block_delta","index":0,"delta":{{"type":"input_json_delta","partial_json":{}}}}}"#,
            string(&piece, &mut style)
        );
        event("content_block_delta", data, &mut style);
    }
    for (name, data) in stop("tool_use") {
        event(name, data, &mut style);
    }
    let parts = response_parts(&normalize(&raw(USER_TURN, Transport::Sse, ok(&events))).exchange);
    let [AssistantPart::ToolCall(call)] = parts.as_slice() else {
        return Err(TestCaseError::fail(format!("one call: {parts:?}")));
    };
    let valid = serde_json::from_str::<serde::de::IgnoredAny>(text).is_ok();
    let expected = if text.is_empty() {
        ToolArguments::Json(crosstalk_spec::observed::message::CanonicalJson(
            "{}".to_owned(),
        ))
    } else if valid {
        ToolArguments::Json(
            canonicalize(text).map_err(|error| TestCaseError::fail(error.to_string()))?,
        )
    } else {
        ToolArguments::Invalid(text.to_owned())
    };
    prop_assert_eq!(&call.arguments, &expected);
    Ok(())
}

/// Every server result follows a server call with its id in the same
/// assistant message, in requests and responses.
pub fn server_results_paired(blocks: &[GenBlock], seed: u64) -> Checked {
    let mut style = Style::new(seed);
    let response = GenResponse {
        id: "msg_1".to_owned(),
        blocks: blocks.to_vec(),
        stop_reason: "end_turn",
        usage: (1, 0, 0, 1),
    };
    let request = GenRequest {
        system: GenSystem::Absent,
        turns: vec![GenTurn::Assistant(blocks.to_vec())],
    };
    let mut messages = request_bodies(&normalize(&exchange_of(&request, &mut style)).exchange);
    for raw in [
        raw(USER_TURN, Transport::Sse, ok(&response.stream(&mut style))),
        raw(USER_TURN, Transport::Http, ok(&response.whole(&mut style))),
    ] {
        messages.push(MessageBody::Assistant(response_parts(
            &normalize(&raw).exchange,
        )));
    }
    let expected_results = blocks
        .iter()
        .enumerate()
        .filter(|(at, block)| {
            match block {
            GenBlock::ServerResult { tool_use_id, .. } => blocks[..*at].iter().any(|earlier| {
                matches!(earlier, GenBlock::ServerToolUse { id, .. } if id == tool_use_id)
            }),
            _ => false,
        }
        })
        .count();
    for body in messages {
        let MessageBody::Assistant(parts) = body else {
            return Err(TestCaseError::fail("an assistant message"));
        };
        let mut results = 0;
        for (at, part) in parts.iter().enumerate() {
            if let AssistantPart::ServerToolResult(result) = part {
                results += 1;
                prop_assert!(parts[..at].iter().any(|earlier| matches!(
                    earlier,
                    AssistantPart::ToolCall(call)
                        if call.execution == ToolExecution::Server && call.id == result.call_id
                )));
            }
        }
        prop_assert_eq!(results, expected_results);
    }
    Ok(())
}

/// A failed exchange normalizes, with its whole request, whatever its
/// partial bytes.
pub fn failed_bytes_never_fail(
    request: &GenRequest,
    transport: Transport,
    failure: ExchangeFailure,
    partial: Vec<u8>,
    seed: u64,
) -> Checked {
    let mut style = Style::new(seed);
    let body = request.render(&mut style);
    let reference = request_bodies(&normalize(&raw(&body, Transport::Http, ok("{}"))).exchange);
    let failed = raw(
        &body,
        transport,
        RawResponse::Failed {
            failure,
            partial_body: partial,
        },
    );
    let normalization = crate::anthropic::normalize(&failed)
        .map_err(|error| TestCaseError::fail(format!("{error:?}")))?;
    prop_assert_eq!(request_bodies(&normalization.exchange), reference);
    let ExchangeOutcome::Failed { failure: got, .. } = normalization.exchange.exchange.outcome
    else {
        return Err(TestCaseError::fail("failed"));
    };
    prop_assert_eq!(got, failure);
    Ok(())
}

/// A complete 200 body that does not parse is an unparseable failure with
/// no response, and the request is kept.
pub fn unparseable_keeps_request(transport: Transport, body: &[u8]) -> Checked {
    let text = String::from_utf8_lossy(body);
    prop_assume!(!text.contains("message_start") && !text.contains("error"));
    prop_assume!(
        Json::parse_bytes(body)
            .map(|json| json.get("content").is_none())
            .unwrap_or(true)
    );
    let normalization = normalize(&crate::tests::support::raw_bytes(
        USER_TURN.as_bytes(),
        transport,
        RawResponse::Complete {
            status: 200,
            body: body.to_vec(),
        },
    ));
    let unparseable = matches!(
        normalization.exchange.exchange.outcome,
        ExchangeOutcome::Failed {
            partial_response: None,
            failure: ExchangeFailure::UnparseableResponse,
            ..
        }
    );
    prop_assert!(
        unparseable,
        "outcome {:?}",
        normalization.exchange.exchange.outcome
    );
    prop_assert_eq!(normalization.exchange.exchange.request.len(), 1);
    Ok(())
}

/// Any spelling of a value (member order, whitespace, escapes, number
/// forms) has one canonical text: the value's.
pub fn canonical_ignores_formatting(value: &GenJson, seeds: (u64, u64)) -> Checked {
    let one = value.render(&mut Style::new(seeds.0));
    let two = value.render(&mut Style::new(seeds.1));
    let canonical =
        canonicalize(&one).map_err(|error| TestCaseError::fail(format!("{one}: {error}")))?;
    prop_assert_eq!(
        &canonical,
        &canonicalize(&two).map_err(|error| TestCaseError::fail(format!("{two}: {error}")))?
    );
    prop_assert_eq!(&canonical, &value.value().canonical());
    // Canonical text is a fixed point.
    prop_assert_eq!(&canonicalize(&canonical.0).ok(), &Some(canonical.clone()));
    Ok(())
}

/// Integers far beyond 2^53, either sign, keep their exact value through
/// canonical text, and their digits while they have at most 21.
pub fn large_integer_exact(negative: bool, digits: &str, seed: u64) -> Checked {
    let number = GenNumber {
        negative,
        digits: digits.to_owned(),
        exponent: 0,
    };
    let spelled = number.spell(&mut Style::new(seed));
    let canonical =
        canonicalize(&spelled).map_err(|error| TestCaseError::fail(error.to_string()))?;
    let reparsed =
        Json::parse(&canonical.0).map_err(|error| TestCaseError::fail(error.to_string()))?;
    prop_assert_eq!(reparsed, Json::Number(number.value()));
    if digits.len() <= 21 {
        let sign = if negative { "-" } else { "" };
        prop_assert_eq!(canonical.0, format!("{sign}{digits}"));
    }
    Ok(())
}

/// Decoding a body's encoding gives the body back.
pub fn encoding_round_trip(body: &MessageBody) -> Checked {
    let bytes = encoding::encode(body);
    let decoded = encoding::decode(&bytes);
    prop_assert_eq!(decoded.as_ref(), Ok(body));
    Ok(())
}
