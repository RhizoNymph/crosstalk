//! Exchanges on the wire (`IngestEvent::ExchangeCaptured`): the record, its
//! continuation and outcome, the client context it was captured with, and
//! the message references (`PartRef`, `ToolCallId`, `ToolName`) other
//! areas carry.

use super::super::harness::{assert_golden, assert_rejected, assert_round_trips};
use super::super::{ULID_A, id, ts};
use super::{
    ACCOUNT_HEX, AREA, CREDENTIAL_HEX, PARTIAL_HEX, PREVIOUS_ACCOUNT_HEX, PREVIOUS_CREDENTIAL_HEX,
    REQUEST_HEX, RESPONSE_HEX, SYSTEM_HEX, TOOL_RESULT_HEX, ULID_E, digest, message,
};
use crate::derived::flow::resource::Host;
use crate::ids::{AccountHash, CredentialHash, ExchangeId, SecretVersion};
use crate::observed::client::{
    ClientContext, CredentialRef, CredentialScheme, HarnessClaim, HarnessFamily, HarnessIds,
    InferenceServer, IngressMode, PreviousDigests, RequestClass, RouteName, Upstream, UpstreamId,
    UpstreamKind, Vendor,
};
use crate::observed::exchange::{
    ConnectionId, Continuation, Exchange, ExchangeFailure, ExchangeMeta, ExchangeOutcome,
    ModelName, ResponseId, StopReason, TokenUsage, Transport, WireProtocol,
};
use crate::observed::message::{PartRef, ToolCallId, ToolName};

fn credential(hex: &str, key: u16) -> CredentialHash {
    CredentialHash::from_keyed_digest(SecretVersion(key), digest(hex))
}

fn account(hex: &str, key: u16) -> AccountHash {
    AccountHash::from_keyed_digest(SecretVersion(key), digest(hex))
}

fn connection() -> ConnectionId {
    id(ConnectionId::from_ulid_text, ULID_E)
}

/// Claude Code on a Claude subscription, through a reverse-proxy route.
fn claude_code_client() -> ClientContext {
    ClientContext {
        ingress: IngressMode::ReverseProxy {
            route: RouteName("anthropic".into()),
        },
        upstream: Upstream {
            id: UpstreamId("claude-max".into()),
            kind: UpstreamKind::Subscription(Vendor::Anthropic),
        },
        credential: Some(CredentialRef {
            scheme: CredentialScheme::OauthAccessToken,
            hash: credential(CREDENTIAL_HEX, 2),
        }),
        account: None,
        previous_digests: None,
        harness: Some(HarnessClaim {
            family: HarnessFamily::ClaudeCode,
            version: Some("2.0.14".into()),
            user_agent: "claude-cli/2.0.14 (external, cli)".into(),
        }),
        ids: HarnessIds {
            session: Some("6f1c2a9e-4b7d-4e2a-9c3f-1d8e5b0a7c42".into()),
            agent: Some("planner".into()),
            parent_agent: None,
        },
        class: RequestClass::Main,
    }
}

/// Codex through the forward proxy, during a secret rotation overlap.
fn codex_client() -> ClientContext {
    ClientContext {
        ingress: IngressMode::ForwardProxy {
            host: Host("chatgpt.com".into()),
        },
        upstream: Upstream {
            id: UpstreamId("chatgpt-codex".into()),
            kind: UpstreamKind::Subscription(Vendor::OpenAi),
        },
        credential: Some(CredentialRef {
            scheme: CredentialScheme::OauthAccessToken,
            hash: credential(CREDENTIAL_HEX, 3),
        }),
        account: Some(account(ACCOUNT_HEX, 3)),
        previous_digests: Some(PreviousDigests {
            credential: Some(credential(PREVIOUS_CREDENTIAL_HEX, 2)),
            account: Some(account(PREVIOUS_ACCOUNT_HEX, 2)),
        }),
        harness: Some(HarnessClaim {
            family: HarnessFamily::Codex,
            version: Some("0.46.0".into()),
            user_agent: "codex_cli_rs/0.46.0".into(),
        }),
        ids: HarnessIds {
            session: Some("0199b0f2-6c1e-7d3a-8f42-5e9a1c7b3d20".into()),
            agent: Some("0199b0f2-7a44-7c19-b0de-2f6e8d1a9c53".into()),
            parent_agent: Some("0199b0f2-6c1e-7d3a-8f42-5e9a1c7b3d20".into()),
        },
        class: RequestClass::Subagent,
    }
}

/// An unauthenticated self-hosted server that no harness names.
fn vllm_client() -> ClientContext {
    ClientContext {
        ingress: IngressMode::ReverseProxy {
            route: RouteName("local-vllm".into()),
        },
        upstream: Upstream {
            id: UpstreamId("vllm".into()),
            kind: UpstreamKind::InferenceServer(InferenceServer::Vllm),
        },
        credential: None,
        account: None,
        previous_digests: None,
        harness: None,
        ids: HarnessIds {
            session: None,
            agent: None,
            parent_agent: None,
        },
        class: RequestClass::Unknown,
    }
}

fn meta(
    protocol: WireProtocol,
    transport: Transport,
    model: &str,
    client: ClientContext,
) -> ExchangeMeta {
    ExchangeMeta {
        id: id(ExchangeId::from_ulid_text, ULID_A),
        protocol,
        transport,
        model: ModelName(model.into()),
        client,
        started_at: ts("2026-10-04T12:34:56.789012Z"),
    }
}

/// A full-history SSE exchange that ended its turn on a tool call.
pub(super) fn completed() -> Exchange {
    Exchange {
        meta: meta(
            WireProtocol::AnthropicMessages,
            Transport::Sse,
            "claude-sonnet-4-5",
            claude_code_client(),
        ),
        continuation: Continuation::FullHistory,
        request: vec![message(SYSTEM_HEX), message(REQUEST_HEX)],
        outcome: ExchangeOutcome::Completed {
            response: message(RESPONSE_HEX),
            response_id: Some(ResponseId("msg_01XFDUDYJgAACzvnptvVoYEL".into())),
            first_chunk_at: ts("2026-10-04T12:34:57.402118Z"),
            finished_at: ts("2026-10-04T12:35:03.950031Z"),
            stop: StopReason::ToolUse,
            usage: Some(TokenUsage {
                input: 18_342,
                output: 611,
                cache_read: 16_384,
                reasoning: None,
            }),
        },
    }
}

/// A WebSocket turn carrying only its increment, cut off mid-stream.
pub(super) fn increment_truncated() -> Exchange {
    Exchange {
        meta: meta(
            WireProtocol::OpenAiResponses,
            Transport::WebSocket,
            "gpt-5-codex",
            codex_client(),
        ),
        continuation: Continuation::Increment {
            previous: ResponseId("resp_68e1a2b3c4d5e6f708192a3b".into()),
            connection: Some(connection()),
        },
        request: vec![message(TOOL_RESULT_HEX)],
        outcome: ExchangeOutcome::Failed {
            partial_response: Some(message(PARTIAL_HEX)),
            first_chunk_at: Some(ts("2026-10-04T12:34:58.000250Z")),
            failed_at: ts("2026-10-04T12:35:10.500000Z"),
            failure: ExchangeFailure::StreamTruncated,
        },
    }
}

/// A plain HTTP request a self-hosted server refused before any content.
fn upstream_refused() -> Exchange {
    Exchange {
        meta: meta(
            WireProtocol::OpenAiChat,
            Transport::Http,
            "Qwen/Qwen3-Coder-30B-A3B-Instruct",
            vllm_client(),
        ),
        continuation: Continuation::FullHistory,
        request: vec![message(REQUEST_HEX)],
        outcome: ExchangeOutcome::Failed {
            partial_response: None,
            first_chunk_at: None,
            failed_at: ts("2026-10-04T12:34:56.912000Z"),
            failure: ExchangeFailure::Upstream { status: 429 },
        },
    }
}

#[test]
fn exchanges_golden() {
    assert_golden(AREA, "exchange_completed", &completed());
    assert_golden(AREA, "exchange_increment_truncated", &increment_truncated());
    assert_golden(AREA, "exchange_upstream_refused", &upstream_refused());
}

#[test]
fn exchange_enums_golden_with_every_variant() {
    fn protocol(value: WireProtocol) -> WireProtocol {
        match value {
            WireProtocol::AnthropicMessages
            | WireProtocol::OpenAiChat
            | WireProtocol::OpenAiResponses
            | WireProtocol::GeminiGenerate
            | WireProtocol::GeminiCodeAssist => value,
        }
    }
    let protocols = [
        WireProtocol::AnthropicMessages,
        WireProtocol::OpenAiChat,
        WireProtocol::OpenAiResponses,
        WireProtocol::GeminiGenerate,
        WireProtocol::GeminiCodeAssist,
    ]
    .map(protocol);
    assert_golden(AREA, "wire_protocols", &protocols.to_vec());

    fn transport(value: Transport) -> Transport {
        match value {
            Transport::Http | Transport::Sse | Transport::WebSocket => value,
        }
    }
    let transports = [Transport::Http, Transport::Sse, Transport::WebSocket].map(transport);
    assert_golden(AREA, "transports", &transports.to_vec());

    fn stop(value: StopReason) -> StopReason {
        match value {
            StopReason::EndTurn
            | StopReason::ToolUse
            | StopReason::MaxTokens
            | StopReason::StopSequence
            | StopReason::Refusal
            | StopReason::Aborted
            | StopReason::Other => value,
        }
    }
    let stops = [
        StopReason::EndTurn,
        StopReason::ToolUse,
        StopReason::MaxTokens,
        StopReason::StopSequence,
        StopReason::Refusal,
        StopReason::Aborted,
        StopReason::Other,
    ]
    .map(stop);
    assert_golden(AREA, "stop_reasons", &stops.to_vec());

    fn failure(value: ExchangeFailure) -> ExchangeFailure {
        match value {
            ExchangeFailure::Upstream { .. }
            | ExchangeFailure::UpstreamUnreachable
            | ExchangeFailure::StreamTruncated
            | ExchangeFailure::MalformedStream { .. }
            | ExchangeFailure::UpstreamErrorEvent
            | ExchangeFailure::UnparseableResponse
            | ExchangeFailure::ClientDisconnected
            | ExchangeFailure::Timeout => value,
        }
    }
    let failures = [
        ExchangeFailure::Upstream { status: 529 },
        ExchangeFailure::UpstreamUnreachable,
        ExchangeFailure::StreamTruncated,
        ExchangeFailure::MalformedStream { offset: 40_961 },
        ExchangeFailure::UpstreamErrorEvent,
        ExchangeFailure::UnparseableResponse,
        ExchangeFailure::ClientDisconnected,
        ExchangeFailure::Timeout,
    ]
    .map(failure);
    assert_golden(AREA, "exchange_failures", &failures.to_vec());

    fn continuation(value: Continuation) -> Continuation {
        match value {
            Continuation::FullHistory | Continuation::Increment { .. } => value,
        }
    }
    let continuations = [
        Continuation::FullHistory,
        Continuation::Increment {
            previous: ResponseId("resp_68e1a2b3c4d5e6f708192a3b".into()),
            connection: Some(connection()),
        },
        // An increment over HTTP: `previous_response_id` without a socket.
        Continuation::Increment {
            previous: ResponseId("resp_68e1a2b3c4d5e6f708192a3c".into()),
            connection: None,
        },
    ]
    .map(continuation);
    assert_golden(AREA, "continuations", &continuations.to_vec());

    fn outcome(value: ExchangeOutcome) -> ExchangeOutcome {
        match value {
            ExchangeOutcome::Completed { .. } | ExchangeOutcome::Failed { .. } => value,
        }
    }
    let outcomes = [completed().outcome, increment_truncated().outcome].map(outcome);
    assert_golden(AREA, "exchange_outcomes", &outcomes.to_vec());
}

#[test]
fn client_enums_golden_with_every_variant() {
    fn ingress(value: IngressMode) -> IngressMode {
        match value {
            IngressMode::ReverseProxy { .. } | IngressMode::ForwardProxy { .. } => value,
        }
    }
    let modes = [claude_code_client().ingress, codex_client().ingress].map(ingress);
    assert_golden(AREA, "ingress_modes", &modes.to_vec());

    fn vendor(value: Vendor) -> Vendor {
        match value {
            Vendor::Anthropic
            | Vendor::OpenAi
            | Vendor::Google
            | Vendor::GithubCopilot
            | Vendor::Other(_) => value,
        }
    }
    let vendors = [
        Vendor::Anthropic,
        Vendor::OpenAi,
        Vendor::Google,
        Vendor::GithubCopilot,
        Vendor::Other("mistral".into()),
    ]
    .map(vendor);
    assert_golden(AREA, "vendors", &vendors.to_vec());

    fn server(value: InferenceServer) -> InferenceServer {
        match value {
            InferenceServer::Vllm | InferenceServer::Sglang => value,
        }
    }
    let servers = [InferenceServer::Vllm, InferenceServer::Sglang].map(server);
    assert_golden(AREA, "inference_servers", &servers.to_vec());

    fn kind(value: UpstreamKind) -> UpstreamKind {
        match value {
            UpstreamKind::VendorApi(_)
            | UpstreamKind::Subscription(_)
            | UpstreamKind::InferenceServer(_) => value,
        }
    }
    let kinds = [
        UpstreamKind::VendorApi(Vendor::Google),
        UpstreamKind::Subscription(Vendor::GithubCopilot),
        UpstreamKind::InferenceServer(InferenceServer::Sglang),
    ]
    .map(kind);
    assert_golden(AREA, "upstream_kinds", &kinds.to_vec());

    fn scheme(value: CredentialScheme) -> CredentialScheme {
        match value {
            CredentialScheme::ApiKey
            | CredentialScheme::OauthAccessToken
            | CredentialScheme::ExchangedToken
            | CredentialScheme::ServerKey => value,
        }
    }
    let schemes = [
        CredentialScheme::ApiKey,
        CredentialScheme::OauthAccessToken,
        CredentialScheme::ExchangedToken,
        CredentialScheme::ServerKey,
    ]
    .map(scheme);
    assert_golden(AREA, "credential_schemes", &schemes.to_vec());

    fn family(value: HarnessFamily) -> HarnessFamily {
        match value {
            HarnessFamily::ClaudeCode
            | HarnessFamily::Codex
            | HarnessFamily::Pi
            | HarnessFamily::OhMyPi
            | HarnessFamily::Unknown => value,
        }
    }
    let families = [
        HarnessFamily::ClaudeCode,
        HarnessFamily::Codex,
        HarnessFamily::Pi,
        HarnessFamily::OhMyPi,
        HarnessFamily::Unknown,
    ]
    .map(family);
    assert_golden(AREA, "harness_families", &families.to_vec());

    fn class(value: RequestClass) -> RequestClass {
        match value {
            RequestClass::Main
            | RequestClass::Subagent
            | RequestClass::Compaction
            | RequestClass::Auxiliary
            | RequestClass::Unknown => value,
        }
    }
    let classes = [
        RequestClass::Main,
        RequestClass::Subagent,
        RequestClass::Compaction,
        RequestClass::Auxiliary,
        RequestClass::Unknown,
    ]
    .map(class);
    assert_golden(AREA, "request_classes", &classes.to_vec());
}

/// The message references other areas carry: a part of a message (spans,
/// accesses), a tool call's id (a content match's carrier) and a tool's
/// name (a direct route).
#[test]
fn message_references_golden() {
    assert_golden(
        AREA,
        "part_ref",
        &PartRef {
            message: message(TOOL_RESULT_HEX),
            index: 2,
        },
    );
    assert_golden(
        AREA,
        "tool_call_id",
        &ToolCallId("toolu_01A09q90qw90lq917835lq9".into()),
    );
    assert_golden(AREA, "tool_name", &ToolName("web_fetch".into()));
}

/// A connection id is ULID text, never the bare number.
#[test]
fn connection_ids_are_ulid_text() {
    assert_golden(AREA, "connection_id", &connection());
    assert_eq!(connection().ulid_text(), ULID_E);
    assert_round_trips(&ConnectionId(u128::MAX));
    assert_round_trips(&ConnectionId(0));
    assert_rejected::<ConnectionId>("42", "invalid type: integer `42`, expected a string");
    assert_rejected::<ConnectionId>(
        "340282366920938463463374607431768211455",
        "expected a string",
    );
    assert_rejected::<ConnectionId>(
        &format!(r#""{}""#, ULID_E.to_lowercase()),
        "invalid ULID text",
    );
    assert_rejected::<ConnectionId>(r#""81J9Z3Q8R9S0T1V2W3X4Y5Z6A7""#, "Overflow");
}

#[test]
fn exchanges_refuse_unknown_fields_and_variants() {
    let full = serde_json::to_value(completed()).expect("an exchange encodes");

    let mut extra = full.clone();
    extra["meta"]["region"] = "eu-west-1".into();
    assert_rejected::<Exchange>(&extra.to_string(), "unknown field `region`");

    let mut extra = full.clone();
    extra["outcome"]["data"]["latency_ms"] = 6548.into();
    assert_rejected::<Exchange>(&extra.to_string(), "unknown field `latency_ms`");

    let mut extra = full.clone();
    extra["meta"]["client"]["ids"]["trace"] = "abc".into();
    assert_rejected::<Exchange>(&extra.to_string(), "unknown field `trace`");

    let mut missing = full;
    missing["meta"]["client"]
        .as_object_mut()
        .expect("the client is an object")
        .remove("class");
    assert_rejected::<Exchange>(&missing.to_string(), "missing field `class`");

    assert_rejected::<WireProtocol>(
        r#""bedrock_converse""#,
        "unknown variant `bedrock_converse`",
    );
    assert_rejected::<Transport>(r#""grpc""#, "unknown variant `grpc`");
    assert_rejected::<StopReason>(r#""pause_turn""#, "unknown variant `pause_turn`");
    assert_rejected::<ExchangeFailure>(
        r#"{"type": "rate_limited"}"#,
        "unknown variant `rate_limited`",
    );
    assert_rejected::<ExchangeFailure>(
        r#"{"type": "upstream", "data": {"status": 503, "body": "overloaded"}}"#,
        "unknown field `body`",
    );
    assert_rejected::<Continuation>(
        r#"{"type": "increment", "data": {"previous": "resp_1", "connection": 7}}"#,
        "expected a string",
    );
    assert_rejected::<Continuation>(r#"{"type": "partial"}"#, "unknown variant `partial`");
    assert_rejected::<TokenUsage>(
        r#"{"input": 1, "output": 2, "cache_read": 0, "reasoning": null, "cache_write": 0}"#,
        "unknown field `cache_write`",
    );
    assert_rejected::<TokenUsage>(
        r#"{"input": -1, "output": 2, "cache_read": 0, "reasoning": null}"#,
        "invalid value",
    );
}

#[test]
fn client_context_refuses_unknown_fields_and_variants() {
    assert_rejected::<IngressMode>(
        r#"{"type": "transparent_proxy", "data": {"host": "api.openai.com"}}"#,
        "unknown variant `transparent_proxy`",
    );
    assert_rejected::<IngressMode>(
        r#"{"type": "reverse_proxy", "data": {"route": "anthropic", "path": "/v1"}}"#,
        "unknown field `path`",
    );
    assert_rejected::<Vendor>(r#"{"type": "mistral"}"#, "unknown variant `mistral`");
    assert_rejected::<UpstreamKind>(
        r#"{"type": "inference_server", "data": "ollama"}"#,
        "unknown variant `ollama`",
    );
    assert_rejected::<CredentialScheme>(r#""session_cookie""#, "unknown variant `session_cookie`");
    assert_rejected::<HarnessFamily>(r#""cursor""#, "unknown variant `cursor`");
    assert_rejected::<RequestClass>(r#""title""#, "unknown variant `title`");
    assert_rejected::<CredentialRef>(
        &format!(
            r#"{{"scheme": "api_key", "hash": {{"key": 1, "digest": "{CREDENTIAL_HEX}"}}, "raw": "sk-ant"}}"#
        ),
        "unknown field `raw`",
    );
    assert_rejected::<HarnessClaim>(
        r#"{"family": "pi", "version": null, "user_agent": "pi/0.9", "verified": true}"#,
        "unknown field `verified`",
    );
    assert_rejected::<PreviousDigests>(
        r#"{"credential": null, "account": null, "key": 1}"#,
        "unknown field `key`",
    );
}

/// `ToolCallId` and `ToolName` are plain text; `PartRef` is strict.
#[test]
fn message_references_refuse_unknown_fields() {
    assert_rejected::<PartRef>(
        &format!(r#"{{"message": "{REQUEST_HEX}", "index": 0, "role": "tool"}}"#),
        "unknown field `role`",
    );
    assert_rejected::<PartRef>(
        &format!(r#"{{"message": "{REQUEST_HEX}", "index": 65536}}"#),
        "invalid value",
    );
    assert_rejected::<PartRef>(
        &format!(
            r#"{{"message": "{}", "index": 0}}"#,
            REQUEST_HEX.to_uppercase()
        ),
        "invalid",
    );
    assert_rejected::<ToolCallId>("7", "expected a string");
    assert_round_trips(&ToolName(String::new()));
}
