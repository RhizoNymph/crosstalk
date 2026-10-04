//! Responses on fixed inputs: outcomes, stop reasons, tool calls, server
//! tools, opaque reasoning and arguments.

use crosstalk_spec::interfaces::l0_ingress::RawResponse;
use crosstalk_spec::observed::exchange::{ExchangeFailure, ExchangeOutcome, StopReason, Transport};
use crosstalk_spec::observed::message::{
    AssistantPart, CanonicalJson, MessageBody, Reasoning, Text, ToolArguments, ToolCallId,
    ToolExecution, ToolOutcome, ToolResult, ToolResultContent,
};

use crate::tests::support::{
    START, USER_TURN, case, normalize, ok, raw, raw_bytes, request_bodies, response,
    response_parts, sse, stop, streamed,
};

fn whole(content: &str, stop_reason: &str) -> RawResponse {
    ok(&format!(
        r#"{{"id":"msg_1","type":"message","role":"assistant","model":"m","content":{content},"stop_reason":"{stop_reason}","stop_sequence":null,"usage":{{"input_tokens":3,"output_tokens":4}}}}"#
    ))
}

fn stop_of(raw: &crosstalk_spec::interfaces::l0_ingress::RawExchange) -> StopReason {
    match normalize(raw).exchange.exchange.outcome {
        ExchangeOutcome::Completed { stop, .. } => stop,
        other => panic!("completed: {other:?}"),
    }
}

/// A complete non-2xx response is `Upstream` with its status and no
/// response message; the request is kept.
pub fn error_status_normalizes_to_upstream_failure() {
    for (name, status) in [("rate_limited", 429), ("unauthorized", 401)] {
        let (_, raw) = case(name);
        let normalization = normalize(&raw);
        let ExchangeOutcome::Failed {
            partial_response,
            failure,
            ..
        } = &normalization.exchange.exchange.outcome
        else {
            panic!("{name} failed");
        };
        assert_eq!(*failure, ExchangeFailure::Upstream { status });
        assert_eq!(*partial_response, None);
        assert!(!normalization.exchange.exchange.request.is_empty());
    }
    // Even an error status whose body looks like a message.
    let body = whole(r#"[{"type":"text","text":"not output"}]"#, "end_turn");
    let RawResponse::Complete { body, .. } = body else {
        panic!("complete");
    };
    let normalization = normalize(&raw(
        USER_TURN,
        Transport::Http,
        RawResponse::Complete { status: 503, body },
    ));
    assert!(matches!(
        normalization.exchange.exchange.outcome,
        ExchangeOutcome::Failed {
            partial_response: None,
            failure: ExchangeFailure::Upstream { status: 503 },
            ..
        }
    ));
    assert_eq!(normalization.exchange.messages.len(), 1, "only the request");
}

/// A failed exchange keeps its whole request whatever its partial bytes
/// hold: garbage, cut events, invalid UTF-8.
pub fn failed_exchange_keeps_request_with_bad_partial_body() {
    let (_, reference) = case("tool_result_followup");
    let expected = request_bodies(&normalize(&reference).exchange);
    let partials: [&[u8]; 4] = [
        b"",
        b"\xff\xfe garbage",
        b"event: message_start\ndata: {\"type\":\"message_start\",\"mess",
        b"event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":9}\n\n",
    ];
    for transport in [Transport::Sse, Transport::Http] {
        for partial in partials {
            for failure in [
                ExchangeFailure::StreamTruncated,
                ExchangeFailure::MalformedStream { offset: 3 },
                ExchangeFailure::ClientDisconnected,
                ExchangeFailure::Timeout,
            ] {
                let mut raw = reference.clone();
                raw.meta.transport = transport;
                raw.response = RawResponse::Failed {
                    failure,
                    partial_body: partial.to_vec(),
                };
                let normalization = normalize(&raw);
                assert_eq!(request_bodies(&normalization.exchange), expected);
                assert!(matches!(
                    normalization.exchange.exchange.outcome,
                    ExchangeOutcome::Failed { partial_response: None, failure: f, .. } if f == failure
                ));
            }
        }
    }
}

/// A complete 2xx body that does not parse is `UnparseableResponse`, with
/// no response message and the request kept.
pub fn garbled_200_body_is_unparseable_failure() {
    let cases: [(Transport, &str); 6] = [
        (Transport::Http, "not json"),
        (Transport::Http, r#"{"id":"msg_1","content":"#),
        (
            Transport::Http,
            r#"{"type":"message","content":"text, not blocks"}"#,
        ),
        (Transport::Sse, "garbage without events"),
        (Transport::Sse, "data: {not json}\n\n"),
        (
            Transport::Sse,
            "data: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\n",
        ),
    ];
    for (transport, body) in cases {
        let normalization = normalize(&raw(USER_TURN, transport, ok(body)));
        assert_eq!(
            normalization.exchange.exchange.outcome,
            ExchangeOutcome::Failed {
                partial_response: None,
                first_chunk_at: Some(crosstalk_testkit::time::millis(400)),
                failed_at: crosstalk_testkit::time::secs(3),
                failure: ExchangeFailure::UnparseableResponse,
            },
            "{body}"
        );
        assert_eq!(normalization.exchange.exchange.request.len(), 1);
    }
}

/// A response with a client tool call stops with `ToolUse` unless a token
/// limit cut it, whatever reason the dialect gave.
pub fn tool_call_responses_stop_with_tool_use() {
    let call = r#"[{"type":"text","text":"Running it."},{"type":"tool_use","id":"t","name":"Bash","input":{"command":"ls"}}]"#;
    for reason in [
        "end_turn",
        "tool_use",
        "stop_sequence",
        "pause_turn",
        "refusal",
    ] {
        assert_eq!(
            stop_of(&raw(USER_TURN, Transport::Http, whole(call, reason))),
            StopReason::ToolUse,
            "{reason}"
        );
    }
    for reason in ["max_tokens", "model_context_window_exceeded"] {
        assert_eq!(
            stop_of(&raw(USER_TURN, Transport::Http, whole(call, reason))),
            StopReason::MaxTokens
        );
    }
    let server_only =
        r#"[{"type":"server_tool_use","id":"s","name":"web_search","input":{"query":"q"}}]"#;
    assert_eq!(
        stop_of(&raw(
            USER_TURN,
            Transport::Http,
            whole(server_only, "end_turn")
        )),
        StopReason::EndTurn
    );
    assert_eq!(
        stop_of(&raw(USER_TURN, Transport::Http, whole("[]", "pause_turn"))),
        StopReason::Other
    );
    let (_, streamed) = case("tool_use_streaming");
    assert_eq!(stop_of(&streamed), StopReason::ToolUse);
}

/// A redacted thinking block's payload is carried byte for byte, in a
/// response and in its echo.
pub fn redacted_thinking_kept_verbatim() {
    let payload = "EmwKAhgBEgy3va3pzix/LafPsn4aDFIT2Xlxh0L5L8rLVyIwxtE3rAFBa8cr3qpP\n+==  ";
    let escaped = payload.replace('\n', "\\n");
    let block = format!(r#"{{"type":"redacted_thinking","data":"{escaped}"}}"#);
    let start = format!(r#"{{"type":"content_block_start","index":0,"content_block":{block}}}"#);
    let [delta, end] = stop("end_turn");
    let stream = sse(&[
        START,
        ("content_block_start", &start),
        (delta.0, &delta.1),
        (end.0, &end.1),
    ]);
    let normalization = normalize(&streamed(&stream));
    let expected = vec![AssistantPart::Reasoning(Reasoning::Opaque {
        signature: payload.to_owned(),
    })];
    assert_eq!(response_parts(&normalization.exchange), expected);
    let echo = format!(
        r#"{{"model":"m","messages":[{{"role":"user","content":"hi"}},{{"role":"assistant","content":[{block}]}}]}}"#
    );
    let echoed = request_bodies(&normalize(&raw(&echo, Transport::Http, ok("{}"))).exchange);
    assert_eq!(echoed[1], MessageBody::Assistant(expected));
}

const SEARCH: &str = r#"[
    {"type":"text","text":"Searching."},
    {"type":"server_tool_use","id":"srvtoolu_01","name":"web_search","input":{"query":"rust blake3"}},
    {"type":"web_search_tool_result","tool_use_id":"srvtoolu_01","content":[{"type":"web_search_result","url":"https://example.com","title":"t","encrypted_content":"e"}]},
    {"type":"mcp_tool_use","id":"mcptoolu_01","name":"lookup","server_name":"docs","input":{}},
    {"type":"mcp_tool_result","tool_use_id":"mcptoolu_01","is_error":true,"content":[{"type":"text","text":"denied"}]},
    {"type":"tool_use","id":"toolu_01","name":"Read","input":{"file_path":"/a"}}
]"#;

/// Provider-hosted tools (server tools, MCP connector tools) are `Server`;
/// a harness's tool is `Client`.
pub fn server_tool_use_marked_server() {
    let normalization = normalize(&raw(USER_TURN, Transport::Http, whole(SEARCH, "tool_use")));
    let executions: Vec<(String, ToolExecution)> = response_parts(&normalization.exchange)
        .into_iter()
        .filter_map(|part| match part {
            AssistantPart::ToolCall(call) => Some((call.id.0, call.execution)),
            _ => None,
        })
        .collect();
    assert_eq!(
        executions,
        vec![
            ("srvtoolu_01".to_owned(), ToolExecution::Server),
            ("mcptoolu_01".to_owned(), ToolExecution::Server),
            ("toolu_01".to_owned(), ToolExecution::Client),
        ]
    );
}

/// A web search result follows its call as a `ServerToolResult` naming it;
/// a result with no earlier server call is kept as `Unknown`.
pub fn anthropic_web_search_result_follows_its_call() {
    let parts = response_parts(
        &normalize(&raw(USER_TURN, Transport::Http, whole(SEARCH, "tool_use"))).exchange,
    );
    let AssistantPart::ServerToolResult(search) = &parts[2] else {
        panic!("a server result: {parts:?}");
    };
    assert_eq!(search.call_id, ToolCallId("srvtoolu_01".to_owned()));
    assert_eq!(search.outcome, ToolOutcome::Success);
    assert!(matches!(
        search.content.as_slice(),
        [ToolResultContent::Unknown(unknown)] if unknown.kind == "web_search_result"
    ));
    assert_eq!(
        parts[4],
        AssistantPart::ServerToolResult(ToolResult {
            call_id: ToolCallId("mcptoolu_01".to_owned()),
            content: vec![ToolResultContent::Text(Text("denied".to_owned()))],
            outcome: ToolOutcome::Error,
        })
    );
    let unpaired = r#"[
        {"type":"web_search_tool_result","tool_use_id":"srvtoolu_01","content":{"type":"web_search_tool_result_error","error_code":"max_uses_exceeded"}},
        {"type":"server_tool_use","id":"srvtoolu_01","name":"web_search","input":{}},
        {"type":"tool_use","id":"toolu_02","name":"Read","input":{}},
        {"type":"web_search_tool_result","tool_use_id":"toolu_02","content":[]},
        {"type":"web_search_tool_result","tool_use_id":"srvtoolu_01","content":{"type":"web_search_tool_result_error","error_code":"unavailable"}}
    ]"#;
    let parts = response_parts(
        &normalize(&raw(
            USER_TURN,
            Transport::Http,
            whole(unpaired, "end_turn"),
        ))
        .exchange,
    );
    assert!(
        matches!(&parts[0], AssistantPart::Unknown(unknown) if unknown.kind == "web_search_tool_result")
    );
    assert!(
        matches!(&parts[3], AssistantPart::Unknown(unknown) if unknown.kind == "web_search_tool_result")
    );
    assert!(matches!(
        &parts[4],
        AssistantPart::ServerToolResult(result) if result.outcome == ToolOutcome::Error
    ));
}

/// Streamed argument text that does not parse is kept verbatim as
/// `Invalid`; text that parses is canonical JSON.
pub fn malformed_arguments_kept_as_invalid() {
    let tool = |fragments: &[&str]| {
        let mut events = vec![
            START.0.to_owned(),
            START.1.to_owned(),
            "content_block_start".to_owned(),
            r#"{"type":"content_block_start","index":0,"content_block":{"type":"tool_use","id":"t","name":"Read","input":{}}}"#.to_owned(),
        ];
        for fragment in fragments {
            events.push("content_block_delta".to_owned());
            events.push(format!(
                r#"{{"type":"content_block_delta","index":0,"delta":{{"type":"input_json_delta","partial_json":{}}}}}"#,
                serde_json::to_string(fragment).unwrap_or_default()
            ));
        }
        for (name, data) in stop("tool_use") {
            events.push(name.to_owned());
            events.push(data);
        }
        let pairs: Vec<(&str, &str)> = events
            .chunks(2)
            .map(|pair| (pair[0].as_str(), pair[1].as_str()))
            .collect();
        let parts = response_parts(&normalize(&streamed(&sse(&pairs))).exchange);
        match parts.as_slice() {
            [AssistantPart::ToolCall(call)] => call.arguments.clone(),
            other => panic!("one call: {other:?}"),
        }
    };
    assert_eq!(
        tool(&["{\"file_path\": \"/wo", "rkspace/a.md\""]),
        ToolArguments::Invalid("{\"file_path\": \"/workspace/a.md\"".to_owned())
    );
    assert_eq!(
        tool(&["", "not json at all "]),
        ToolArguments::Invalid("not json at all ".to_owned())
    );
    assert_eq!(
        tool(&["", "{\"b\": 1, ", "\"a\": [true]}"]),
        ToolArguments::Json(CanonicalJson(r#"{"a":[true],"b":1}"#.to_owned()))
    );
    assert_eq!(
        tool(&[""]),
        ToolArguments::Json(CanonicalJson("{}".to_owned())),
        "no input text: the start block's input"
    );
}

/// An `error` event inside a 200 stream fails the exchange with the blocks
/// before it as the partial response, whether the proxy flagged it or
/// handed the stream over complete; a stream that stops early is
/// truncated.
pub fn error_event_keeps_partial_response() {
    let (case, flagged) = case("overloaded_mid_stream");
    let complete = raw_bytes(
        &flagged.request.body,
        Transport::Sse,
        RawResponse::Complete {
            status: 200,
            body: case.response_bytes().to_vec(),
        },
    );
    for raw in [flagged, complete] {
        let normalization = normalize(&raw);
        assert!(matches!(
            normalization.exchange.exchange.outcome,
            ExchangeOutcome::Failed {
                failure: ExchangeFailure::UpstreamErrorEvent,
                partial_response: Some(_),
                ..
            }
        ));
        assert_eq!(
            response(&normalization.exchange).map(|message| &message.body),
            Some(&MessageBody::Assistant(vec![AssistantPart::Text(Text(
                "The build pipeline has three stages:".to_owned()
            ))]))
        );
    }
    let cut = sse(&[
        START,
        (
            "content_block_start",
            r#"{"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}"#,
        ),
        (
            "content_block_delta",
            r#"{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"half"}}"#,
        ),
    ]);
    let normalization = normalize(&streamed(&cut));
    assert!(matches!(
        normalization.exchange.exchange.outcome,
        ExchangeOutcome::Failed {
            failure: ExchangeFailure::StreamTruncated,
            partial_response: Some(_),
            ..
        }
    ));
}
