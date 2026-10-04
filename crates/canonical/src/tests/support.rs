//! Raw exchanges for tests, from bodies or from corpus cases, and lookups
//! into a normalization.

use crosstalk_spec::interfaces::l0_ingress::{
    ContentEncoding, DecodedRequest, HarnessRequest, RawExchange, RawResponse,
};
use crosstalk_spec::interfaces::l1_canonical::NormalizedExchange;
use crosstalk_spec::observed::client::Dialect;
use crosstalk_spec::observed::exchange::{
    Continuation, ExchangeFailure, ExchangeMeta, ExchangeOutcome, ModelName, Transport,
    WireProtocol,
};
use crosstalk_spec::observed::message::{AssistantPart, Message, MessageBody};
use crosstalk_testkit::build::exchange::claude_code_client;
use crosstalk_testkit::corpus::{Case, Endpoint, Expect};
use crosstalk_testkit::ids::Ids;
use crosstalk_testkit::time::{T0, millis, secs};

use crate::Normalization;
use crate::anthropic;

pub const MODEL: &str = "claude-opus-5-5";

/// A complete 200 response.
pub fn ok(body: &str) -> RawResponse {
    RawResponse::Complete {
        status: 200,
        body: body.as_bytes().to_vec(),
    }
}

/// A Claude Code exchange through the reverse proxy to the Anthropic API,
/// started at the testkit epoch: first chunk 400 ms later, ended at 3 s.
pub fn raw(request: &str, transport: Transport, response: RawResponse) -> RawExchange {
    raw_bytes(request.as_bytes(), transport, response)
}

pub fn raw_bytes(request: &[u8], transport: Transport, response: RawResponse) -> RawExchange {
    let mut ids = Ids::seeded(7);
    let client = claude_code_client(&mut ids);
    RawExchange {
        meta: ExchangeMeta {
            id: ids.exchange(),
            protocol: WireProtocol::AnthropicMessages,
            transport,
            model: ModelName(MODEL.to_owned()),
            client,
            started_at: T0,
        },
        request: DecodedRequest {
            harness: HarnessRequest {
                protocol: WireProtocol::AnthropicMessages,
                dialect: Dialect::Reference,
                model: ModelName(MODEL.to_owned()),
                stream: transport == Transport::Sse,
                continuation: Continuation::FullHistory,
            },
            body: request.to_vec(),
            encoding: ContentEncoding::Identity,
        },
        response,
        first_chunk_at: Some(millis(400)),
        ended_at: secs(3),
    }
}

/// The raw exchange the proxy hands L1 for a generation case: the
/// response complete with its status, or, for a case whose expected
/// failure is the proxy's to detect (an error event, a truncated or
/// malformed stream), failed with that failure and the bytes received.
/// `None` for a case that is not captured.
pub fn corpus_raw(case: &Case) -> Option<RawExchange> {
    let Endpoint::Generation { expect, .. } = &case.meta.endpoint else {
        return None;
    };
    let transport = case.response.framing().transport();
    let body = case.response_bytes().to_vec();
    let response = match expect {
        Expect::Failed {
            failure:
                failure @ (ExchangeFailure::UpstreamErrorEvent
                | ExchangeFailure::StreamTruncated
                | ExchangeFailure::MalformedStream { .. }),
            ..
        } => RawResponse::Failed {
            failure: *failure,
            partial_body: body,
        },
        _ => RawResponse::Complete {
            status: case.response.status.as_u16(),
            body,
        },
    };
    let mut raw = raw_bytes(case.request.body.as_ref(), transport, response);
    raw.request.harness.stream = transport == Transport::Sse;
    Some(raw)
}

/// Every captured corpus case with its raw exchange.
pub fn corpus() -> Vec<(Case, RawExchange)> {
    let cases = crosstalk_testkit::corpus::anthropic::cases()
        .unwrap_or_else(|error| panic!("the corpus loads: {error}"));
    cases
        .into_iter()
        .filter_map(|case| corpus_raw(&case).map(|raw| (case, raw)))
        .collect()
}

pub fn case(name: &str) -> (Case, RawExchange) {
    corpus()
        .into_iter()
        .find(|(case, _)| case.name == name)
        .unwrap_or_else(|| panic!("no captured corpus case {name}"))
}

pub fn normalize(raw: &RawExchange) -> Normalization {
    anthropic::normalize(raw).unwrap_or_else(|error| panic!("normalizes: {error:?}"))
}

pub fn message<'a>(
    exchange: &'a NormalizedExchange,
    hash: &crosstalk_spec::ids::MessageHash,
) -> &'a Message {
    exchange
        .messages
        .iter()
        .find(|message| message.hash == *hash)
        .unwrap_or_else(|| panic!("no message {hash:?}"))
}

/// The request's message bodies, in order.
pub fn request_bodies(exchange: &NormalizedExchange) -> Vec<MessageBody> {
    exchange
        .exchange
        .request
        .iter()
        .map(|hash| message(exchange, hash).body.clone())
        .collect()
}

/// The response or partial response message, if any.
pub fn response(exchange: &NormalizedExchange) -> Option<&Message> {
    match &exchange.exchange.outcome {
        ExchangeOutcome::Completed { response, .. } => Some(message(exchange, response)),
        ExchangeOutcome::Failed {
            partial_response, ..
        } => partial_response
            .as_ref()
            .map(|hash| message(exchange, hash)),
    }
}

/// The response's assistant parts; panics when there is none.
pub fn response_parts(exchange: &NormalizedExchange) -> Vec<AssistantPart> {
    match response(exchange).map(|message| &message.body) {
        Some(MessageBody::Assistant(parts)) => parts.clone(),
        other => panic!("no assistant response: {other:?}"),
    }
}

/// A one-turn streamed request whose response is `stream`.
pub fn streamed(stream: &str) -> RawExchange {
    raw(USER_TURN, Transport::Sse, ok(stream))
}

/// A minimal valid request.
pub const USER_TURN: &str =
    r#"{"model":"claude-opus-5-5","max_tokens":1024,"messages":[{"role":"user","content":"hi"}]}"#;

/// An event stream from `(name, data)` pairs.
pub fn sse(events: &[(&str, &str)]) -> String {
    events
        .iter()
        .map(|(name, data)| format!("event: {name}\ndata: {data}\n\n"))
        .collect()
}

/// The events that open a stream: `message_start` with id `msg_1`.
pub const START: (&str, &str) = (
    "message_start",
    r#"{"type":"message_start","message":{"id":"msg_1","type":"message","role":"assistant","model":"claude-opus-5-5","content":[],"stop_reason":null,"stop_sequence":null,"usage":{"input_tokens":5,"output_tokens":1}}}"#,
);

pub fn stop(reason: &str) -> [(&'static str, String); 2] {
    [
        (
            "message_delta",
            format!(
                r#"{{"type":"message_delta","delta":{{"stop_reason":"{reason}","stop_sequence":null}},"usage":{{"output_tokens":9}}}}"#
            ),
        ),
        ("message_stop", r#"{"type":"message_stop"}"#.to_owned()),
    ]
}
