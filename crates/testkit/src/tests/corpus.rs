//! The corpus loads, every case is consistent with its metadata, and the
//! loader refuses malformed recordings.

use bytes::Bytes;
use crosstalk_spec::interfaces::l0_ingress::ResponseFraming;
use crosstalk_spec::observed::client::{CredentialScheme, EndpointKind, RequestClass};
use crosstalk_spec::observed::exchange::{ExchangeFailure, StopReason};
use hyper::{Method, StatusCode};

use crate::corpus::anthropic::{self, CorpusError, Inconsistency};
use crate::corpus::http::{self, Malformed, NOT_RECORDED};
use crate::corpus::{BlockKind, Case, Endpoint, Expect, Provenance, ResponseBody};

fn cases() -> Vec<Case> {
    anthropic::cases().expect("every corpus case loads and checks")
}

fn case(name: &str) -> Case {
    anthropic::case(name).expect("the case exists")
}

const NAMES: [&str; 15] = [
    "count_tokens",
    "hello_probe",
    "models_list",
    "multi_block_assistant",
    "overloaded_mid_stream",
    "rate_limited",
    "subagent_oauth_streaming",
    "system_cache_control",
    "system_turn_streaming",
    "text_turn",
    "text_turn_streaming",
    "thinking_streaming",
    "tool_result_followup",
    "tool_use_streaming",
    "unauthorized",
];

#[test]
fn every_case_loads_sorted_by_name() {
    let names: Vec<String> = cases().into_iter().map(|case| case.name).collect();
    assert_eq!(names, NAMES);
}

#[test]
fn every_case_is_marked_synthetic_and_names_its_sources() {
    for case in cases() {
        let Provenance::Synthetic { sources } = &case.meta.provenance else {
            panic!("{} claims to be captured", case.name);
        };
        assert!(!sources.is_empty(), "{}", case.name);
        assert!(!case.meta.description.is_empty(), "{}", case.name);
    }
}

#[test]
fn every_recording_omits_framing_headers() {
    for case in cases() {
        for headers in [&case.request.headers, &case.response.headers] {
            for (name, _) in headers.iter() {
                assert!(
                    !NOT_RECORDED.contains(&name.as_str()),
                    "{}: {name}",
                    case.name
                );
            }
        }
    }
}

#[test]
fn every_event_stream_reassembles_exactly_and_ends_cleanly() {
    let mut streams = 0;
    for case in cases() {
        if let ResponseBody::EventStream(stream) = &case.response.body {
            streams += 1;
            assert_eq!(
                &stream.chunks().concat()[..],
                &stream.raw()[..],
                "{}",
                case.name
            );
            assert!(stream.trailing().is_empty(), "{}", case.name);
            for event in stream.events() {
                assert!(event.json().is_ok(), "{}: {}", case.name, event.kind());
            }
        }
    }
    assert_eq!(streams, 8);
}

#[test]
fn generation_requests_are_claude_code_shaped() {
    for case in cases() {
        let Endpoint::Generation { .. } = case.meta.endpoint else {
            continue;
        };
        let headers = &case.request.headers;
        assert_eq!(headers.get_str("anthropic-version"), Some("2023-06-01"));
        assert!(
            headers
                .get_str("anthropic-beta")
                .is_some_and(|betas| betas.contains("claude-code-20250219")),
            "{}",
            case.name
        );
        assert_eq!(headers.get_str("x-app"), Some("cli"));
        assert!(headers.get_str("x-claude-code-session-id").is_some());
        assert_eq!(case.request.method, Method::POST);
        assert_eq!(case.request.target.as_str(), "/v1/messages?beta=true");
        let body = case.request.json().expect("JSON body");
        let user_id = body
            .pointer("/metadata/user_id")
            .and_then(serde_json::Value::as_str)
            .expect("metadata.user_id");
        assert!(user_id.starts_with("user_") && user_id.contains("_session_"));
        let system = body.get("system").and_then(serde_json::Value::as_array);
        assert!(
            system.is_some_and(|blocks| blocks.len() == 3),
            "{}",
            case.name
        );
        for claim in &case.meta.harness.claim_headers {
            assert!(headers.get(claim).is_some());
        }
    }
}

/// The stream flag and expected outcome of generation case `name`.
fn generation(name: &str) -> (bool, Expect) {
    match case(name).meta.endpoint {
        Endpoint::Generation { stream, expect, .. } => (stream, expect),
        Endpoint::TokenCount | Endpoint::ModelList | Endpoint::Probe => {
            panic!("{name} is a generation case")
        }
    }
}

/// Whether `expect` completes with `stop` and exactly `blocks`.
fn completes(expect: &Expect, stop: StopReason, blocks: &[BlockKind]) -> bool {
    match expect {
        Expect::Completed {
            stop: found,
            blocks: found_blocks,
            ..
        } => *found == stop && found_blocks == blocks,
        Expect::Failed { .. } => false,
    }
}

/// The failure `expect` names, if it fails.
fn failure(expect: &Expect) -> Option<ExchangeFailure> {
    match expect {
        Expect::Failed { failure, .. } => Some(*failure),
        Expect::Completed { .. } => None,
    }
}

#[test]
fn the_required_shapes_are_covered() {
    use BlockKind::{Text, Thinking, ToolUse};
    use StopReason::{EndTurn, ToolUse as ToolStop};
    let shapes: [(&str, bool, StopReason, &[BlockKind]); 8] = [
        ("text_turn", false, EndTurn, &[Text]),
        ("text_turn_streaming", true, EndTurn, &[Text]),
        ("tool_use_streaming", true, ToolStop, &[Text, ToolUse]),
        ("tool_result_followup", true, EndTurn, &[Text]),
        (
            "multi_block_assistant",
            false,
            ToolStop,
            &[Text, ToolUse, ToolUse],
        ),
        ("system_cache_control", true, EndTurn, &[Text]),
        ("thinking_streaming", true, EndTurn, &[Thinking, Text]),
        ("system_turn_streaming", true, EndTurn, &[Text]),
    ];
    for (name, stream, stop, blocks) in shapes {
        let (streams, expect) = generation(name);
        assert_eq!(streams, stream, "{name}");
        assert!(completes(&expect, stop, blocks), "{name}: {expect:?}");
    }
    let failures = [
        ("overloaded_mid_stream", ExchangeFailure::UpstreamErrorEvent),
        ("rate_limited", ExchangeFailure::Upstream { status: 429 }),
        ("unauthorized", ExchangeFailure::Upstream { status: 401 }),
    ];
    for (name, expected) in failures {
        assert_eq!(failure(&generation(name).1), Some(expected), "{name}");
    }
    assert_eq!(
        case("count_tokens").meta.endpoint.kind(),
        EndpointKind::TokenCount
    );
    assert_eq!(
        case("models_list").meta.endpoint.kind(),
        EndpointKind::ModelList
    );
    assert_eq!(
        case("hello_probe").meta.endpoint.kind(),
        EndpointKind::Probe
    );
}

/// Claude Code's system turn: a top-level `system` array, and a `system`
/// role inside `messages`, after the first user turn.
#[test]
fn the_system_turn_case_carries_both_system_prompts() {
    let body = case("system_turn_streaming").request.json().expect("JSON");
    assert!(body["system"].is_array(), "the top-level system prompt");
    assert_eq!(
        body.pointer("/messages/0/role")
            .and_then(|role| role.as_str()),
        Some("user")
    );
    assert_eq!(
        body.pointer("/messages/1/role")
            .and_then(|role| role.as_str()),
        Some("system")
    );
    assert!(
        body.pointer("/messages/1/content")
            .is_some_and(|content| content.is_array()),
        "the system turn's content is blocks"
    );
}

#[test]
fn the_follow_up_echoes_the_tool_use_it_answers() {
    let first = case("tool_use_streaming");
    let follow_up = case("tool_result_followup");
    assert_eq!(
        follow_up.meta.follows.as_deref(),
        Some("tool_use_streaming")
    );
    let started = first
        .response
        .body
        .events()
        .expect("streamed")
        .events()
        .iter()
        .find_map(|event| {
            let data = event.json().ok()?;
            (data.pointer("/content_block/type")? == "tool_use").then(|| {
                data.pointer("/content_block/id")?
                    .as_str()
                    .map(str::to_owned)
            })?
        })
        .expect("a tool_use block");
    let body = follow_up.request.json().expect("JSON");
    let echoed = body
        .pointer("/messages/1/content/1/id")
        .and_then(|id| id.as_str());
    let answered = body
        .pointer("/messages/2/content/0/tool_use_id")
        .and_then(|id| id.as_str());
    assert_eq!(echoed, Some(started.as_str()));
    assert_eq!(answered, Some(started.as_str()));
}

#[test]
fn cache_control_and_retry_headers_are_recorded() {
    let cached = case("system_cache_control");
    let body = cached.request.json().expect("JSON");
    assert_eq!(body.pointer("/system/0/cache_control"), None);
    assert_eq!(
        body.pointer("/system/1/cache_control/ttl")
            .and_then(|ttl| ttl.as_str()),
        Some("1h")
    );
    assert!(
        cached
            .request
            .headers
            .get_str("anthropic-beta")
            .is_some_and(|betas| betas.contains("extended-cache-ttl"))
    );
    let limited = case("rate_limited");
    assert_eq!(limited.response.status, StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(limited.response.headers.get_str("retry-after"), Some("17"));
    assert_eq!(limited.response.framing(), ResponseFraming::Whole);
    assert_eq!(limited.request.json().expect("JSON")["stream"], true);
}

#[test]
fn credentials_and_claims_are_declared() {
    let oauth = case("subagent_oauth_streaming");
    let credential = oauth.meta.credential.as_ref().expect("a credential");
    assert_eq!(credential.scheme, CredentialScheme::OauthAccessToken);
    assert_eq!(oauth.meta.harness.class, RequestClass::Subagent);
    assert!(oauth.meta.harness.ids.agent.is_some());
    assert!(
        oauth
            .request
            .headers
            .get_str("anthropic-beta")
            .is_some_and(|betas| betas.contains("oauth-2025-04-20"))
    );
    assert!(case("hello_probe").meta.credential.is_none());

    let request = case("text_turn")
        .request_with_credential("sk-ant-api03-test-key-1")
        .expect("a valid header value");
    assert_eq!(
        request.headers.get_str("x-api-key"),
        Some("sk-ant-api03-test-key-1")
    );
    let bearer = oauth
        .request_with_credential("sk-ant-oat01-test")
        .expect("valid");
    assert_eq!(
        bearer.headers.get_str("authorization"),
        Some("Bearer sk-ant-oat01-test")
    );
    assert!(oauth.request_with_credential("bad\nvalue").is_err());
}

#[test]
fn heads_convert_to_the_spec_heads() {
    let case = case("text_turn_streaming");
    let head = case.request.head();
    assert_eq!(head.method, "POST");
    assert_eq!(head.path, "/v1/messages");
    assert_eq!(head.query.as_deref(), Some("beta=true"));
    assert_eq!(head.headers.len(), case.request.headers.len());
    let response = case.response.head();
    assert_eq!(response.status, 200);
    assert_eq!(response.framing(), ResponseFraming::EventStream);
}

// ── Refusals ────────────────────────────────────────────────────────────

#[test]
fn malformed_recordings_are_refused() {
    let request = |text: &'static str| http::parse_request(&Bytes::from_static(text.as_bytes()));
    assert_eq!(request("GET / HTTP/1.1\nx: y"), Err(Malformed::NoBlankLine));
    assert!(matches!(
        request("GET /\n\n"),
        Err(Malformed::StartLine { .. })
    ));
    assert!(matches!(
        request("GET nopath HTTP/1.1\n\n"),
        Err(Malformed::Target { .. })
    ));
    assert!(matches!(
        request("GET / HTTP/1.1\nNoColon\n\n"),
        Err(Malformed::HeaderLine { .. })
    ));
    assert!(matches!(
        request("GET / HTTP/1.1\nX-Up: 1\n\n"),
        Err(Malformed::HeaderName { .. })
    ));
    assert!(matches!(
        request("GET / HTTP/1.1\ncontent-length: 0\n\n"),
        Err(Malformed::NotRecorded { .. })
    ));
    let parsed = request("POST /v1/x?a=b HTTP/1.1\nx-a: 1\n\nbody\n").expect("valid");
    assert_eq!(parsed.body, Bytes::from_static(b"body\n"));
    assert_eq!(parsed.query(), Some("a=b"));

    let response = |text: &'static str| http::parse_response(&Bytes::from_static(text.as_bytes()));
    assert!(matches!(
        response("HTTP/1.1 abc OK\n\n"),
        Err(Malformed::Status { .. })
    ));
    assert!(matches!(
        response("HTTP/2 200 OK\n\n"),
        Err(Malformed::StartLine { .. })
    ));
    let streamed =
        response("HTTP/1.1 200 OK\ncontent-type: text/event-stream\n\ndata: 1\n\n").expect("valid");
    assert_eq!(streamed.framing(), ResponseFraming::EventStream);
}

/// A copy of `case` in a fresh directory, with `edit` applied to its files.
fn edited(name: &str, edit: impl FnOnce(&std::path::Path)) -> Result<Case, CorpusError> {
    let source = anthropic::dir().join(name);
    let scratch = std::env::temp_dir().join(format!(
        "crosstalk-testkit-{}-{name}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|elapsed| elapsed.as_nanos())
            .unwrap_or_default()
    ));
    let dir = scratch.join(name);
    std::fs::create_dir_all(&dir).expect("scratch dir");
    for file in ["request.http", "response.http", "meta.json"] {
        std::fs::copy(source.join(file), dir.join(file)).expect("copy");
    }
    edit(&dir);
    let loaded = anthropic::load(&dir);
    std::fs::remove_dir_all(&scratch).expect("clean up");
    loaded
}

fn replace(path: &std::path::Path, from: &str, to: &str) {
    let text = std::fs::read_to_string(path).expect("read");
    assert!(text.contains(from), "{from} not in {}", path.display());
    std::fs::write(path, text.replacen(from, to, 1)).expect("write");
}

fn inconsistency(result: Result<Case, CorpusError>) -> Inconsistency {
    match result {
        Err(CorpusError::Inconsistent { problem, .. }) => problem,
        other => panic!("expected an inconsistency, got {other:?}"),
    }
}

#[test]
fn cases_inconsistent_with_their_metadata_are_refused() {
    assert!(edited("text_turn", |_| {}).is_ok());
    let wrong_model = edited("text_turn", |dir| {
        replace(&dir.join("meta.json"), "claude-opus-5-5", "claude-other");
    });
    assert!(matches!(
        inconsistency(wrong_model),
        Inconsistency::Model { .. }
    ));
    let wrong_stream = edited("text_turn", |dir| {
        replace(
            &dir.join("meta.json"),
            "\"stream\": false",
            "\"stream\": true",
        );
    });
    assert!(matches!(
        inconsistency(wrong_stream),
        Inconsistency::Stream { .. }
    ));
    let leaked = edited("text_turn", |dir| {
        replace(
            &dir.join("request.http"),
            "sk-ant-api03-REDACTED",
            "sk-ant-api03-real",
        );
    });
    assert!(matches!(
        inconsistency(leaked),
        Inconsistency::MissingCredential { .. }
    ));
    let wrong_stop = edited("text_turn_streaming", |dir| {
        replace(&dir.join("meta.json"), "\"end_turn\"", "\"tool_use\"");
    });
    assert!(matches!(
        inconsistency(wrong_stop),
        Inconsistency::Stop { .. }
    ));
    let truncated = edited("text_turn_streaming", |dir| {
        let path = dir.join("response.http");
        let text = std::fs::read_to_string(&path).expect("read");
        let cut = text.find("event: message_stop").expect("a message_stop");
        std::fs::write(&path, &text[..cut]).expect("write");
    });
    assert!(matches!(
        inconsistency(truncated),
        Inconsistency::Terminal { .. }
    ));
    let wrong_blocks = edited("tool_use_streaming", |dir| {
        replace(&dir.join("meta.json"), "\"tool_use\"\n", "\"text\"\n");
    });
    assert!(matches!(
        inconsistency(wrong_blocks),
        Inconsistency::Blocks { .. }
    ));
    let missing_claim = edited("text_turn", |dir| {
        replace(&dir.join("request.http"), "x-app: cli\n", "");
    });
    assert!(matches!(
        inconsistency(missing_claim),
        Inconsistency::MissingClaimHeader { .. }
    ));
    let unknown_field = edited("text_turn", |dir| {
        replace(
            &dir.join("meta.json"),
            "\"notes\"",
            "\"extra\": 1,\n  \"notes\"",
        );
    });
    assert!(matches!(unknown_field, Err(CorpusError::Meta { .. })));
}
