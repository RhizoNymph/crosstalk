//! The fake upstream, the wiki and a short swarm run, over real sockets.

use std::net::SocketAddr;
use std::num::NonZeroU32;
use std::time::Duration;

use bytes::Bytes;
use crosstalk_testkit::client::{BodyEnd, HarnessClient};
use crosstalk_testkit::corpus::http::Headers;
use hyper::header::{HeaderName, HeaderValue};
use hyper::{Method, StatusCode};
use serde_json::{Value, json};
use tokio::net::TcpListener;
use tokio::sync::oneshot;

use crate::anthropic::assemble::assemble_stream;
use crate::anthropic::sse::Split;
use crate::anthropic::{AssistantMessage, ResponseBlock, StopReason};
use crate::http::{BaseUrl, healthcheck, request};
use crate::knobs::{Fraction, PositiveSpan, Span};
use crate::protocol::{HTTP_TOOL, Task, tool_definitions};
use crate::swarm::config::{SwarmConfig, TaskMix};
use crate::upstream::generate::GenConfig;
use crate::upstream::{UpstreamConfig, frame_offset};
use crate::wiki::WikiConfig;

/// A running server; dropping it stops it.
pub(super) struct Running {
    pub(super) addr: SocketAddr,
    _stop: oneshot::Sender<()>,
}

pub(super) async fn listener() -> (TcpListener, SocketAddr) {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    (listener, addr)
}

async fn upstream(first_byte_ms: Span, stream_ms: Span) -> Running {
    let (listener, addr) = listener().await;
    let (stop, stopped) = oneshot::channel::<()>();
    let config = UpstreamConfig {
        listen: addr,
        generation: GenConfig {
            seed: 5,
            words: Span::ordered(20, 40),
            first_byte_ms,
            stream_ms,
        },
        split: Split::default(),
    };
    tokio::spawn(crate::upstream::serve_on(listener, config, async {
        let _ = stopped.await;
    }));
    Running { addr, _stop: stop }
}

pub(super) async fn wiki() -> Running {
    let (listener, addr) = listener().await;
    let (stop, stopped) = oneshot::channel::<()>();
    let config = WikiConfig {
        listen: addr,
        max_page_bytes: 1024,
        max_pages: 100,
    };
    tokio::spawn(crate::wiki::serve_on(listener, config, async {
        let _ = stopped.await;
    }));
    Running { addr, _stop: stop }
}

fn headers(pairs: &[(&'static str, &str)]) -> Headers {
    let mut headers = Headers::new();
    for (name, value) in pairs {
        headers.push(
            HeaderName::from_static(name),
            HeaderValue::from_str(value).expect("value"),
        );
    }
    headers
}

fn messages_body(stream: bool) -> Vec<u8> {
    let task = Task::Write {
        page: "release-plan-8".parse().expect("slug"),
        topic: 8,
        base: "http://wiki:8090".parse().expect("url"),
    };
    serde_json::to_vec(&json!({
        "model": "claude-opus-5-5",
        "max_tokens": 1024,
        "tools": tool_definitions(),
        "messages": [{"role": "user", "content": task.prompt()}],
        "stream": stream,
    }))
    .expect("encode")
}

fn api_headers() -> Headers {
    headers(&[
        ("x-api-key", "sk-ant-demo0000-test"),
        ("anthropic-version", "2023-06-01"),
        ("content-type", "application/json"),
    ])
}

#[tokio::test]
async fn upstream_streams_a_paced_tool_call() {
    let server = upstream(Span::ordered(50, 50), Span::ordered(300, 300)).await;
    let client = HarnessClient::new(server.addr).idle_timeout(Duration::from_secs(5));
    let req = request(
        Method::POST,
        "/v1/messages?beta=true",
        api_headers(),
        messages_body(true),
    )
    .expect("request");
    let response = client.send(&req).await.expect("response");
    assert_eq!(response.status, StatusCode::OK);
    assert_eq!(response.end, BodyEnd::Complete);
    assert_eq!(
        response.headers.get_str("content-type"),
        Some("text/event-stream; charset=utf-8")
    );
    assert!(
        response
            .headers
            .get_str("request-id")
            .is_some_and(|id| id.starts_with("req_01"))
    );
    // One frame per chunk, the first after the wait, spread over the stream.
    assert!(response.chunks.len() > 5);
    let first = response.chunks[0].after;
    let last = response.chunks[response.chunks.len() - 1].after;
    assert!(
        first >= Duration::from_millis(45),
        "first byte at {first:?}"
    );
    assert!(
        last - first >= Duration::from_millis(250),
        "stream took {:?}",
        last - first
    );
    let message = assemble_stream(&response.events().expect("sse")).expect("message");
    assert_eq!(message.stop_reason, StopReason::ToolUse);
    assert!(
        matches!(&message.content[1], ResponseBlock::ToolUse { name, input, .. }
            if name == HTTP_TOOL && input["method"] == "PUT"
                && input["url"] == "http://wiki:8090/pages/release-plan-8")
    );
    // The same request again: the same bytes.
    let again = client.send(&req).await.expect("response");
    assert_eq!(again.body, response.body);
}

#[tokio::test]
async fn upstream_answers_whole_documents_and_errors() {
    let server = upstream(Span::ordered(0, 0), Span::ordered(0, 0)).await;
    let client = HarnessClient::new(server.addr).idle_timeout(Duration::from_secs(5));
    let send = |method: Method, target: &str, headers: Headers, body: Vec<u8>| {
        let client = client.clone();
        let req = request(method, target, headers, body).expect("request");
        async move { client.send(&req).await.expect("response") }
    };
    let whole = send(
        Method::POST,
        "/v1/messages",
        api_headers(),
        messages_body(false),
    )
    .await;
    assert_eq!(whole.status, StatusCode::OK);
    assert_eq!(
        whole.headers.get_str("content-type"),
        Some("application/json")
    );
    let message = AssistantMessage::from_document(&whole.body).expect("document");
    assert_eq!(message.stop_reason, StopReason::ToolUse);

    let unauthorized = send(
        Method::POST,
        "/v1/messages",
        Headers::new(),
        messages_body(false),
    )
    .await;
    assert_eq!(unauthorized.status, StatusCode::UNAUTHORIZED);
    let error: Value = serde_json::from_slice(&unauthorized.body).expect("json");
    assert_eq!(error["error"]["type"], "authentication_error");

    let bad = send(Method::POST, "/v1/messages", api_headers(), b"{}".to_vec()).await;
    assert_eq!(bad.status, StatusCode::BAD_REQUEST);
    let error: Value = serde_json::from_slice(&bad.body).expect("json");
    assert_eq!(error["type"], "error");
    assert_eq!(error["error"]["type"], "invalid_request_error");

    let count = send(
        Method::POST,
        "/v1/messages/count_tokens",
        api_headers(),
        messages_body(false),
    )
    .await;
    assert_eq!(count.status, StatusCode::OK);
    let counted: Value = serde_json::from_slice(&count.body).expect("json");
    assert!(counted["input_tokens"].as_u64().is_some_and(|n| n > 0));

    let missing = send(Method::GET, "/v1/models", api_headers(), Vec::new()).await;
    assert_eq!(missing.status, StatusCode::NOT_FOUND);

    let url: BaseUrl = format!("http://{}/healthz", server.addr)
        .parse()
        .expect("url");
    assert_eq!(healthcheck(&url).await.expect("healthy"), StatusCode::OK);
    let url: BaseUrl = format!("http://{}/nope", server.addr).parse().expect("url");
    assert!(healthcheck(&url).await.is_err());
}

#[test]
fn frames_are_spread_evenly_over_the_stream() {
    let first = Duration::from_millis(100);
    let stream = Duration::from_millis(1000);
    assert_eq!(frame_offset(0, 11, first, stream), first);
    assert_eq!(
        frame_offset(5, 11, first, stream),
        Duration::from_millis(600)
    );
    assert_eq!(
        frame_offset(10, 11, first, stream),
        Duration::from_millis(1100)
    );
    assert_eq!(frame_offset(0, 1, first, stream), first);
}

#[tokio::test]
async fn wiki_stores_pages_with_versions_and_authors() {
    let server = wiki().await;
    let client = HarnessClient::new(server.addr).idle_timeout(Duration::from_secs(5));
    let send = |method: Method, target: &str, headers: Headers, body: &str| {
        let client = client.clone();
        let req = request(method, target, headers, Bytes::from(body.to_owned())).expect("request");
        async move { client.send(&req).await.expect("response") }
    };
    let missing = send(Method::GET, "/pages/a-1", Headers::new(), "").await;
    assert_eq!(missing.status, StatusCode::NOT_FOUND);
    let created = send(
        Method::PUT,
        "/pages/a-1",
        headers(&[("x-wiki-author", "agent-000")]),
        "first text",
    )
    .await;
    assert_eq!(created.status, StatusCode::CREATED);
    let replaced = send(
        Method::PUT,
        "/pages/a-1",
        headers(&[("x-wiki-author", "agent-001")]),
        "second text",
    )
    .await;
    assert_eq!(replaced.status, StatusCode::OK);
    let written: Value = serde_json::from_slice(&replaced.body).expect("json");
    assert_eq!(written["version"], 2);
    let page = send(Method::GET, "/pages/a-1", Headers::new(), "").await;
    assert_eq!(page.status, StatusCode::OK);
    assert_eq!(&page.body[..], b"second text");
    assert_eq!(page.headers.get_str("x-wiki-author"), Some("agent-001"));
    assert_eq!(page.headers.get_str("x-wiki-version"), Some("2"));
    let list = send(Method::GET, "/pages", Headers::new(), "").await;
    let list: Value = serde_json::from_slice(&list.body).expect("json");
    assert_eq!(
        list["pages"],
        json!([{"page": "a-1", "version": 2, "bytes": 11, "author": "agent-001"}])
    );
    let anonymous = send(Method::PUT, "/pages/b", Headers::new(), "x").await;
    assert_eq!(anonymous.status, StatusCode::CREATED);
    let bad_name = send(Method::PUT, "/pages/Bad", Headers::new(), "x").await;
    assert_eq!(bad_name.status, StatusCode::BAD_REQUEST);
    let too_large = send(Method::PUT, "/pages/c", Headers::new(), &"x".repeat(1025)).await;
    assert_eq!(too_large.status, StatusCode::PAYLOAD_TOO_LARGE);
    let wrong = send(Method::DELETE, "/pages/a-1", Headers::new(), "").await;
    assert_eq!(wrong.status, StatusCode::METHOD_NOT_ALLOWED);
}

#[tokio::test]
async fn a_short_swarm_writes_reads_and_reports() {
    let model = upstream(Span::ordered(5, 20), Span::ordered(20, 60)).await;
    let pages = wiki().await;
    let truth =
        std::env::temp_dir().join(format!("crosstalk-demo-truth-{}.jsonl", std::process::id()));
    let base = |addr: SocketAddr| format!("http://{addr}").parse::<BaseUrl>().expect("url");
    let mut config = SwarmConfig::new(base(model.addr), base(pages.addr));
    config.agents = NonZeroU32::MIN.saturating_add(5);
    config.agents_per_key = NonZeroU32::MIN.saturating_add(1);
    config.think_ms = Span::ordered(5, 20);
    config.turns = PositiveSpan::ordered(2, 4);
    config.mix = TaskMix::new(
        Fraction::new(0.5).expect("fraction"),
        Fraction::new(0.4).expect("fraction"),
    )
    .expect("mix");
    config.pages = NonZeroU32::MIN.saturating_add(3);
    config.duration = Duration::from_millis(2500);
    config.ramp = Duration::from_millis(100);
    config.stream_fraction = Fraction::new(0.5).expect("fraction");
    config.grace = Duration::from_secs(5);
    config.ground_truth = Some(truth.clone());
    let report = crate::swarm::run(config, std::future::pending())
        .await
        .expect("report");
    assert_eq!(report.agents, 6);
    assert_eq!(report.keys, 3);
    assert!(report.requests > 10, "{report}");
    assert_eq!(report.ok, report.requests, "{report}");
    assert!(report.followups > 0, "{report}");
    assert!(report.wiki_writes > 0 && report.wiki_reads > 0, "{report}");
    assert!(report.expected_transmissions > 0, "{report}");
    assert!(
        report.streaming.count > 0 && report.non_streaming.count > 0,
        "{report}"
    );
    assert!(report.streaming.ttfb.is_some());
    let text = report.to_string();
    assert!(text.contains("expected transmissions"));
    let lines = tokio::fs::read_to_string(&truth)
        .await
        .expect("ground truth");
    let rows: Vec<Value> = lines
        .lines()
        .map(|line| serde_json::from_str(line).expect("json"))
        .collect();
    assert_eq!(rows[0]["kind"], "header");
    assert_eq!(rows[0]["version"], 2);
    let transmissions: Vec<&Value> = rows
        .iter()
        .filter(|row| row["kind"] == "transmission")
        .collect();
    assert_eq!(transmissions.len() as u64, report.expected_transmissions);
    let first = transmissions.first().expect("a transmission");
    assert_ne!(first["writer"], first["reader"]);
    let _ = tokio::fs::remove_file(&truth).await;
}
