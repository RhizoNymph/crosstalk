//! What a RawExchange holds, and what is counted when there is none.

use std::io::Write;
use std::time::Duration;

use bytes::Bytes;
use crosstalk_spec::interfaces::l0_ingress::{ContentEncoding, RawResponse};
use crosstalk_spec::observed::exchange::ExchangeFailure;
use crosstalk_testkit::upstream::{FakeUpstream, Pacing, Reply, Script};
use hyper::header::{HeaderName, HeaderValue};
use proptest::prelude::*;
use proptest::test_runner::{Config, TestCaseError, TestRunner};

use super::support::{Options, case, limits_with, non_zero, start};
use crate::capture::UncapturedReason;

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("a runtime")
}

/// A request no adapter claims is forwarded and counted `unclassified`.
#[tokio::test]
async fn unclassified_request_counted() {
    let upstream = FakeUpstream::start(Script::new()).await.expect("upstream");
    let proxy = start(&upstream.base_url(), Options::default()).await;
    let mut request = case("hello_probe").request;
    request.target = "/telemetry/v2?x=1".parse().expect("target");
    let _ = proxy.client().send(&request).await.expect("answered");
    assert_eq!(upstream.received().await.expect("log").len(), 1);
    let counts = proxy.stats.snapshot();
    assert_eq!(counts.uncaptured(UncapturedReason::Unclassified), 1);
    assert_eq!(counts.captured, 0);
}

/// A generation request whose body does not decode is forwarded, answered,
/// and counted `decode_error`.
#[tokio::test]
async fn decode_error_counted() {
    let text = case("text_turn");
    let upstream = FakeUpstream::start(Script::new().case(&text))
        .await
        .expect("upstream");
    let proxy = start(&upstream.base_url(), Options::default()).await;
    let mut request = text.request.clone();
    request.body = Bytes::from_static(b"{\"messages\": []}");
    let response = proxy.client().send(&request).await.expect("answered");
    assert!(response.differences_from(&text.response).is_empty());
    proxy.settle(1).await;
    assert_eq!(
        proxy
            .stats
            .snapshot()
            .uncaptured(UncapturedReason::DecodeError),
        1
    );
}

/// With the capture channel full, a finished exchange is dropped and
/// counted `channel_full`.
#[tokio::test]
async fn channel_full_counted() {
    let text = case("text_turn");
    let upstream = FakeUpstream::start(Script::new().case(&text))
        .await
        .expect("upstream");
    let proxy = start(
        &upstream.base_url(),
        Options {
            capacity: 1,
            ..Options::default()
        },
    )
    .await;
    for _ in 0..3 {
        let _ = proxy.client().send(&text.request).await.expect("answered");
    }
    proxy.settle(3).await;
    let counts = proxy.stats.snapshot();
    assert_eq!(counts.captured, 1);
    assert_eq!(counts.uncaptured(UncapturedReason::ChannelFull), 2);
}

/// A response larger than the response capture bound reaches the client
/// whole and unchanged, and its exchange is counted `response_too_large`
/// instead of captured with a cut body.
#[tokio::test]
async fn oversized_response_counted_and_relayed() {
    let streaming = case("text_turn_streaming");
    let upstream = FakeUpstream::start(Script::new().case(&streaming))
        .await
        .expect("upstream");
    let limits = limits_with(|limits| limits.response_capture_bytes = non_zero(512));
    let mut proxy = start(
        &upstream.base_url(),
        Options {
            limits,
            ..Options::default()
        },
    )
    .await;
    assert!(streaming.response_bytes().len() > 512);
    let response = proxy
        .client()
        .send(&streaming.request)
        .await
        .expect("answered");
    assert!(response.differences_from(&streaming.response).is_empty());
    proxy.settle(1).await;
    assert_eq!(
        proxy
            .stats
            .snapshot()
            .uncaptured(UncapturedReason::ResponseTooLarge),
        1
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(200), proxy.captured.recv())
            .await
            .is_err()
    );
}

fn encode(body: &[u8], encoding: ContentEncoding) -> Vec<u8> {
    match encoding {
        ContentEncoding::Identity => body.to_vec(),
        ContentEncoding::Gzip => {
            let mut encoder =
                flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
            let _ = encoder.write_all(body);
            encoder.finish().unwrap_or_default()
        }
        ContentEncoding::Zstd => zstd::stream::encode_all(body, 3).unwrap_or_default(),
    }
}

/// A captured request's body is the forwarded body decoded with its
/// content-encoding, and the encoding recorded is that content-encoding;
/// the upstream gets the encoded bytes.
#[test]
fn request_body_is_decoded_forwarded_body() {
    let runtime = runtime();
    let text = case("text_turn");
    let (upstream, proxy) = runtime.block_on(async {
        let upstream = FakeUpstream::start(Script::new().case(&text))
            .await
            .expect("upstream");
        let proxy = start(&upstream.base_url(), Options::default()).await;
        (upstream, proxy)
    });
    let proxy = tokio::sync::Mutex::new(proxy);
    let strategy = (
        "[ -~]{0,200}",
        prop_oneof![
            Just(ContentEncoding::Identity),
            Just(ContentEncoding::Gzip),
            Just(ContentEncoding::Zstd)
        ],
        any::<bool>(),
    );
    let mut runner = TestRunner::new(Config::with_cases(48));
    runner
        .run(&strategy, |(text_content, encoding, stream)| {
            runtime.block_on(async {
                let mut proxy = proxy.lock().await;
                let plain = serde_json::json!({
                    "model": "claude-opus-5-5",
                    "max_tokens": 32,
                    "stream": stream,
                    "messages": [{"role": "user", "content": text_content}],
                })
                .to_string()
                .into_bytes();
                let mut request = text.request.clone();
                request.body = Bytes::from(encode(&plain, encoding));
                let name = match encoding {
                    ContentEncoding::Identity => None,
                    ContentEncoding::Gzip => Some("gzip"),
                    ContentEncoding::Zstd => Some("zstd"),
                };
                if let Some(name) = name {
                    request.headers.push(
                        HeaderName::from_static("content-encoding"),
                        HeaderValue::from_static(name),
                    );
                }
                let _ = proxy
                    .client()
                    .send(&request)
                    .await
                    .map_err(|error| TestCaseError::fail(error.to_string()))?;
                let received = upstream
                    .received()
                    .await
                    .map_err(|error| TestCaseError::fail(error.to_string()))?;
                let forwarded = received
                    .last()
                    .ok_or_else(|| TestCaseError::fail("not forwarded"))?;
                prop_assert_eq!(&forwarded.body, &request.body);
                let raw = proxy
                    .next_capture()
                    .await
                    .ok_or_else(|| TestCaseError::fail("not captured"))?;
                prop_assert_eq!(raw.request.encoding, encoding);
                prop_assert_eq!(&raw.request.body, &plain);
                prop_assert_eq!(raw.request.harness.stream, stream);
                Ok(())
            })
        })
        .expect("captured bodies are the decoded forwarded bodies");
}

/// An SSE stream built from content events with random text.
fn stream_strategy() -> impl Strategy<Value = Vec<Bytes>> {
    proptest::collection::vec("[ -~&&[^\"\\\\]]{0,30}", 0..8).prop_map(|texts| {
        let mut events = vec![Bytes::from_static(b"event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_1\"}}\n\n")];
        for text in texts {
            events.push(Bytes::from(format!(
                "event: content_block_delta\ndata: {{\"type\":\"content_block_delta\",\"index\":0,\"delta\":{{\"type\":\"text_delta\",\"text\":\"{text}\"}}}}\n\n"
            )));
        }
        events.push(Bytes::from_static(b"event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n"));
        events
    })
}

/// Re-split `events` at `cuts` (fractions of the total length).
fn resplit(events: &[Bytes], cuts: &[u16]) -> Vec<Bytes> {
    let all: Bytes = events.concat().into();
    let mut points: Vec<usize> = cuts
        .iter()
        .map(|cut| usize::from(*cut) * all.len() / usize::from(u16::MAX))
        .collect();
    points.push(all.len());
    points.sort_unstable();
    points.dedup();
    let mut start = 0;
    let mut chunks = Vec::new();
    for point in points {
        if point > start {
            chunks.push(all.slice(start..point));
            start = point;
        }
    }
    chunks
}

/// The captured response body is exactly the bytes received, in order,
/// however the upstream chunks them.
#[test]
fn response_body_matches_received() {
    let runtime = runtime();
    let streaming = case("text_turn_streaming");
    let (upstream, proxy) = runtime.block_on(async {
        let upstream = FakeUpstream::start(Script::new()).await.expect("upstream");
        let proxy = start(&upstream.base_url(), Options::default()).await;
        (upstream, proxy)
    });
    let proxy = tokio::sync::Mutex::new(proxy);
    let strategy = (
        stream_strategy(),
        proptest::collection::vec(any::<u16>(), 0..12),
    );
    let mut runner = TestRunner::new(Config::with_cases(48));
    runner
        .run(&strategy, |(events, cuts)| {
            runtime.block_on(async {
                let mut proxy = proxy.lock().await;
                let mut reply = Reply::from_case(&streaming);
                reply.chunks = resplit(&events, &cuts);
                reply.pacing = Pacing::IMMEDIATE;
                let expected = reply.body();
                upstream
                    .reply_next(reply)
                    .await
                    .map_err(|error| TestCaseError::fail(error.to_string()))?;
                let response = proxy
                    .client()
                    .send(&streaming.request)
                    .await
                    .map_err(|error| TestCaseError::fail(error.to_string()))?;
                prop_assert_eq!(&response.body, &expected);
                let raw = proxy
                    .next_capture()
                    .await
                    .ok_or_else(|| TestCaseError::fail("not captured"))?;
                prop_assert_eq!(
                    raw.response,
                    RawResponse::Complete {
                        status: 200,
                        body: expected.to_vec()
                    }
                );
                Ok(())
            })
        })
        .expect("captured bodies are the received bodies");
}

/// After a malformed frame the stream keeps flowing to the client, and the
/// captured partial body holds every byte, those after the error included.
#[test]
fn partial_body_includes_bytes_after_frame_error() {
    let runtime = runtime();
    let streaming = case("text_turn_streaming");
    let (upstream, proxy) = runtime.block_on(async {
        let upstream = FakeUpstream::start(Script::new()).await.expect("upstream");
        let proxy = start(&upstream.base_url(), Options::default()).await;
        (upstream, proxy)
    });
    let proxy = tokio::sync::Mutex::new(proxy);
    let strategy = (
        stream_strategy(),
        any::<prop::sample::Index>(),
        proptest::collection::vec(any::<u16>(), 0..12),
    );
    let mut runner = TestRunner::new(Config::with_cases(48));
    runner
        .run(&strategy, |(mut events, at, cuts)| {
            runtime.block_on(async {
                let mut proxy = proxy.lock().await;
                let index = 1 + at.index(events.len() - 1);
                events.insert(
                    index,
                    Bytes::from_static(b"event: content_block_delta\ndata: {broken\n\n"),
                );
                let offset = events[..index].iter().map(Bytes::len).sum::<usize>() as u64;
                let mut reply = Reply::from_case(&streaming);
                reply.chunks = resplit(&events, &cuts);
                let expected = reply.body();
                upstream
                    .reply_next(reply)
                    .await
                    .map_err(|error| TestCaseError::fail(error.to_string()))?;
                let response = proxy
                    .client()
                    .send(&streaming.request)
                    .await
                    .map_err(|error| TestCaseError::fail(error.to_string()))?;
                prop_assert_eq!(&response.body, &expected);
                let raw = proxy
                    .next_capture()
                    .await
                    .ok_or_else(|| TestCaseError::fail("not captured"))?;
                prop_assert_eq!(
                    raw.response,
                    RawResponse::Failed {
                        failure: ExchangeFailure::MalformedStream { offset },
                        partial_body: expected.to_vec()
                    }
                );
                Ok(())
            })
        })
        .expect("partial bodies hold every byte received");
}
