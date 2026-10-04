//! Faithful forwarding (`ingress.passthrough.request-unchanged`,
//! `ingress.passthrough.response-unchanged`): over real sockets, through
//! testkit's fake upstream and fake harness, compared with testkit's
//! `differences_from`.

use std::io::Write;

use bytes::Bytes;
use crosstalk_testkit::corpus::http::{Headers, ResponseBody};
use crosstalk_testkit::corpus::{CorpusRequest, CorpusResponse};
use crosstalk_testkit::upstream::{FakeUpstream, Framing, Pacing, Reply, Script};
use hyper::header::{HeaderName, HeaderValue};
use hyper::http::uri::PathAndQuery;
use hyper::{Method, StatusCode};
use proptest::prelude::*;
use proptest::test_runner::{Config, TestCaseError, TestRunner};

use super::support::{Options, cases, start};

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("a runtime")
}

fn header_value() -> impl Strategy<Value = String> {
    // Printable ASCII without leading or trailing whitespace, which HTTP
    // parsers trim.
    "[!-~]([ -~]{0,30}[!-~])?"
}

fn gzip(body: &[u8]) -> Vec<u8> {
    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
    let _ = encoder.write_all(body);
    encoder.finish().unwrap_or_default()
}

#[derive(Debug, Clone)]
struct Generated {
    request: CorpusRequest,
}

fn request_strategy() -> impl Strategy<Value = Generated> {
    let method = prop_oneof![
        Just(Method::POST),
        Just(Method::GET),
        Just(Method::PUT),
        Just(Method::DELETE),
        Just(Method::PATCH),
    ];
    let path = prop_oneof![
        Just("/v1/messages".to_owned()),
        Just("/v1/messages/count_tokens".to_owned()),
        Just("/v1/models".to_owned()),
        Just("/api/hello".to_owned()),
        "/[a-z]{1,8}(/[a-z0-9_.-]{1,8}){0,3}",
    ];
    let query = proptest::option::of(prop_oneof![
        Just("beta=true".to_owned()),
        "[a-z]{1,5}=[a-z0-9]{0,5}(&[a-z]{1,5}=[a-z0-9]{0,5}){0,2}",
    ]);
    let headers = proptest::collection::vec(("x-test-[a-z]{1,6}", header_value()), 0..6);
    let credential = prop_oneof![
        Just(None),
        "sk-ant-api03-[A-Za-z0-9]{8}".prop_map(|key| Some(("x-api-key".to_owned(), key))),
        "sk-ant-oat01-[A-Za-z0-9]{8}"
            .prop_map(|token| Some(("authorization".to_owned(), format!("Bearer {token}")))),
    ];
    let body = prop_oneof![
        proptest::collection::vec(any::<u8>(), 0..512),
        Just(
            br#"{"model":"claude-opus-5-5","messages":[],"max_tokens":8,"stream":false}"#.to_vec()
        ),
    ];
    (
        method,
        path,
        query,
        headers,
        credential,
        body,
        any::<bool>(),
    )
        .prop_map(|(method, path, query, extra, credential, body, compress)| {
            let mut headers = Headers::new();
            let mut push = |name: &str, value: &str| {
                if let (Ok(name), Ok(value)) = (
                    HeaderName::from_bytes(name.as_bytes()),
                    HeaderValue::from_str(value),
                ) {
                    headers.push(name, value);
                }
            };
            push("anthropic-version", "2023-06-01");
            push("anthropic-beta", "claude-code-20250219,oauth-2025-04-20");
            push("user-agent", "claude-cli/2.1.282 (external, cli)");
            if let Some((name, value)) = &credential {
                push(name, value);
            }
            for (name, value) in &extra {
                push(name, value);
            }
            let body = if method == Method::GET {
                Vec::new()
            } else if compress && !body.is_empty() {
                push("content-encoding", "gzip");
                gzip(&body)
            } else {
                body
            };
            if !body.is_empty() {
                push("content-type", "application/json");
            }
            let target = match query {
                Some(query) => format!("{path}?{query}"),
                None => path,
            };
            Generated {
                request: CorpusRequest {
                    method,
                    target: PathAndQuery::try_from(target.as_str())
                        .unwrap_or_else(|_| PathAndQuery::from_static("/")),
                    headers,
                    body: Bytes::from(body),
                },
            }
        })
}

/// Every request the proxy routes reaches the upstream with the client's
/// method, path, query, end-to-end headers (credentials and anthropic-*
/// included) and body bytes (still compressed), whatever its endpoint kind
/// and whether or not it decodes: the corpus, then generated requests.
#[test]
fn request_forwarded_unchanged() {
    let runtime = runtime();
    let (upstream, proxy) = runtime.block_on(async {
        let upstream = FakeUpstream::start(Script::new()).await.expect("upstream");
        let proxy = start(&upstream.base_url(), Options::default()).await;
        (upstream, proxy)
    });
    let client = proxy.client();
    runtime.block_on(async {
        for case in cases() {
            let _ = client.send(&case.request).await.expect("the proxy answers");
            let received = upstream.received().await.expect("log");
            let last = received.last().expect("the upstream got the request");
            assert_eq!(
                last.differences_from(&case.request),
                Vec::new(),
                "case {} was changed on the way upstream",
                case.name
            );
        }
    });
    let mut runner = TestRunner::new(Config::with_cases(64));
    runner
        .run(&request_strategy(), |generated| {
            runtime.block_on(async {
                let _ = client
                    .send(&generated.request)
                    .await
                    .map_err(|error| TestCaseError::fail(error.to_string()))?;
                let received = upstream
                    .received()
                    .await
                    .map_err(|error| TestCaseError::fail(error.to_string()))?;
                let last = received
                    .last()
                    .ok_or_else(|| TestCaseError::fail("nothing reached the upstream"))?;
                let differences = last.differences_from(&generated.request);
                prop_assert!(differences.is_empty(), "{differences:?}");
                Ok(())
            })
        })
        .expect("every request is forwarded unchanged");
}

#[derive(Debug, Clone)]
struct GeneratedReply {
    reply: Reply,
    generation: bool,
}

fn sse_event() -> impl Strategy<Value = Vec<u8>> {
    prop_oneof![
        Just(b"event: ping\ndata: {\"type\": \"ping\"}\n\n".to_vec()),
        Just(b": keep-alive\n\n".to_vec()),
        "[a-z ]{0,20}".prop_map(|text| format!(
            "event: content_block_delta\ndata: {{\"type\":\"content_block_delta\",\"index\":0,\"delta\":{{\"type\":\"text_delta\",\"text\":\"{text}\"}}}}\n\n"
        )
        .into_bytes()),
        proptest::collection::vec(any::<u8>(), 1..40),
    ]
}

fn reply_strategy() -> impl Strategy<Value = GeneratedReply> {
    let status = prop_oneof![
        Just(200u16),
        Just(201),
        Just(400),
        Just(401),
        Just(429),
        Just(500),
        Just(529)
    ];
    let headers = proptest::collection::vec(
        prop_oneof![
            ("x-test-[a-z]{1,6}", header_value()),
            Just(("retry-after".to_owned(), "17".to_owned())),
            Just(("x-should-retry".to_owned(), "true".to_owned())),
            ("anthropic-ratelimit-[a-z-]{1,12}", "[0-9]{1,7}"),
            Just((
                "request-id".to_owned(),
                "req_011CJ78QzmNdRLxeV4daIp6u".to_owned()
            )),
        ],
        0..6,
    );
    let stream = proptest::collection::vec(sse_event(), 0..8);
    let whole = proptest::collection::vec(any::<u8>(), 0..512);
    (status, headers, any::<bool>(), stream, whole, any::<bool>()).prop_map(
        |(status, extra, streamed, events, whole, generation)| {
            let mut headers = Headers::new();
            let content_type = if streamed {
                "text/event-stream; charset=utf-8"
            } else {
                "application/json"
            };
            headers.push(
                hyper::header::CONTENT_TYPE,
                HeaderValue::from_static(content_type),
            );
            for (name, value) in &extra {
                if let (Ok(name), Ok(value)) = (
                    HeaderName::from_bytes(name.as_bytes()),
                    HeaderValue::from_str(value),
                ) {
                    headers.push(name, value);
                }
            }
            let (chunks, framing) = if streamed {
                (
                    events.into_iter().map(Bytes::from).collect(),
                    Framing::Streamed,
                )
            } else {
                (vec![Bytes::from(whole)], Framing::Whole)
            };
            GeneratedReply {
                reply: Reply {
                    status: StatusCode::from_u16(status).unwrap_or(StatusCode::OK),
                    headers,
                    chunks,
                    framing,
                    pacing: Pacing::IMMEDIATE,
                    fault: None,
                },
                generation,
            }
        },
    )
}

/// The client receives the upstream's status, end-to-end headers (retry
/// and rate-limit ones included) and body bytes (pings and comments
/// included), on captured and uncaptured routes: the corpus, then
/// generated responses.
#[test]
fn response_relayed_unchanged() {
    let runtime = runtime();
    let corpus = cases();
    let (upstream, proxy) = runtime.block_on(async {
        let upstream = FakeUpstream::start(Script::new().cases(&corpus))
            .await
            .expect("upstream");
        let proxy = start(&upstream.base_url(), Options::default()).await;
        (upstream, proxy)
    });
    let client = proxy.client();
    runtime.block_on(async {
        for case in &corpus {
            upstream
                .reply_next(Reply::from_case(case))
                .await
                .expect("command");
            let response = client.send(&case.request).await.expect("the proxy answers");
            assert_eq!(
                response.differences_from(&case.response),
                Vec::new(),
                "case {} was changed on the way back",
                case.name
            );
        }
    });
    let generation = corpus
        .iter()
        .find(|case| case.name == "text_turn")
        .expect("text_turn")
        .request
        .clone();
    let other = corpus
        .iter()
        .find(|case| case.name == "count_tokens")
        .expect("count_tokens")
        .request
        .clone();
    let mut runner = TestRunner::new(Config::with_cases(64));
    runner
        .run(&reply_strategy(), |generated| {
            runtime.block_on(async {
                upstream
                    .reply_next(generated.reply.clone())
                    .await
                    .map_err(|error| TestCaseError::fail(error.to_string()))?;
                let request = if generated.generation {
                    &generation
                } else {
                    &other
                };
                let response = client
                    .send(request)
                    .await
                    .map_err(|error| TestCaseError::fail(error.to_string()))?;
                let expected = CorpusResponse {
                    status: generated.reply.status,
                    headers: generated.reply.headers.clone(),
                    body: ResponseBody::Whole(generated.reply.body()),
                };
                let differences = response.differences_from(&expected);
                prop_assert!(differences.is_empty(), "{differences:?}");
                Ok(())
            })
        })
        .expect("every response is relayed unchanged");
}

/// Hop-by-hop fields, and the fields `Connection` names, are not
/// forwarded; `Host` is the upstream's.
#[tokio::test]
async fn hop_by_hop_headers_dropped() {
    let upstream = FakeUpstream::start(Script::new()).await.expect("upstream");
    let proxy = start(&upstream.base_url(), Options::default()).await;
    let mut request = super::support::case("count_tokens").request;
    for (name, value) in [
        ("connection", "x-private-hop"),
        ("x-private-hop", "1"),
        ("keep-alive", "timeout=5"),
        ("proxy-connection", "keep-alive"),
        ("upgrade", "websocket"),
    ] {
        request.headers.push(
            HeaderName::from_static(name),
            HeaderValue::from_static(value),
        );
    }
    let _ = proxy.client().send(&request).await.expect("answered");
    let received = upstream.received().await.expect("log");
    let last = received.last().expect("forwarded");
    for name in ["x-private-hop", "keep-alive", "proxy-connection", "upgrade"] {
        assert!(last.headers.get(name).is_none(), "{name} was forwarded");
    }
    let host = last.headers.get_str("host").expect("a host");
    assert_eq!(host, upstream.addr().to_string());
}
