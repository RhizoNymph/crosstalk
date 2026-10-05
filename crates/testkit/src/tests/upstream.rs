//! The fake harness and the fake upstream round-trip every corpus case
//! byte for byte, and the upstream's commands do what they say.

use std::time::Duration;

use hyper::header::{HeaderName, HeaderValue};
use hyper::{Method, StatusCode};

use crate::client::{BodyEnd, HarnessClient, Next};
use crate::corpus::anthropic;
use crate::corpus::{Case, ResponseBody};
use crate::upstream::{FakeUpstream, Fault, Pacing, Reply, Route, Script};

fn case(name: &str) -> Case {
    anthropic::case(name).expect("the case exists")
}

/// How long a stalled read waits before the test calls it stalled.
const IDLE: Duration = Duration::from_millis(300);

#[tokio::test]
async fn every_case_round_trips_unchanged() {
    for case in anthropic::cases().expect("the corpus loads") {
        let upstream = FakeUpstream::replay(&case).await.expect("starts");
        let response = HarnessClient::new(upstream.addr())
            .send(&case.request)
            .await
            .expect("answered");
        assert_eq!(response.end, BodyEnd::Complete, "{}", case.name);
        assert_eq!(
            response.differences_from(&case.response),
            Vec::new(),
            "{}",
            case.name
        );
        let received = upstream.received().await.expect("running");
        assert_eq!(received.len(), 1, "{}", case.name);
        assert_eq!(
            received[0].differences_from(&case.request),
            Vec::new(),
            "{}",
            case.name
        );
        if let ResponseBody::EventStream(recorded) = &case.response.body {
            let events = response.events().expect("an event stream");
            assert_eq!(events.events(), recorded.events(), "{}", case.name);
        }
    }
}

#[tokio::test]
async fn framing_follows_the_reply() {
    let streamed = case("text_turn_streaming");
    let upstream = FakeUpstream::replay(&streamed).await.expect("starts");
    let response = HarnessClient::new(upstream.addr())
        .send(&streamed.request)
        .await
        .expect("answered");
    assert_eq!(
        response.headers.get_str("transfer-encoding"),
        Some("chunked")
    );
    assert!(response.headers.get("content-length").is_none());
    assert!(response.headers.get("date").is_none());

    let whole = case("text_turn");
    let upstream = FakeUpstream::replay(&whole).await.expect("starts");
    let response = HarnessClient::new(upstream.addr())
        .send(&whole.request)
        .await
        .expect("answered");
    let length = whole.response_bytes().len().to_string();
    assert_eq!(
        response.headers.get_str("content-length"),
        Some(length.as_str())
    );
}

#[tokio::test]
async fn paced_streams_arrive_one_event_per_chunk() {
    let case = case("thinking_streaming");
    let events = case
        .response
        .body
        .events()
        .expect("streamed")
        .events()
        .len();
    let delay = Duration::from_millis(15);
    let script = Script::new().route(
        Route::of(&case.request),
        Reply::from_case(&case).paced(Pacing::every(delay)),
    );
    let upstream = FakeUpstream::start(script).await.expect("starts");
    let response = HarnessClient::new(upstream.addr())
        .send(&case.request)
        .await
        .expect("answered");
    assert_eq!(response.differences_from(&case.response), Vec::new());
    assert_eq!(response.chunks.len(), events);
    let last = response.chunks.last().expect("chunks").after;
    let spread = delay * u32::try_from(events).expect("few events");
    assert!(last >= spread, "{last:?} < {spread:?}");
    for (chunk, event) in response
        .chunks
        .iter()
        .zip(case.response.body.events().iter().flat_map(|s| s.events()))
    {
        assert_eq!(chunk.bytes, event.raw);
    }
}

#[tokio::test]
async fn a_slow_reader_holds_the_stream_back() {
    let case = case("tool_use_streaming");
    let upstream = FakeUpstream::replay(&case).await.expect("starts");
    let mut stream = HarnessClient::new(upstream.addr())
        .open(&case.request)
        .await
        .expect("head");
    assert_eq!(stream.status(), StatusCode::OK);
    let mut body = Vec::new();
    loop {
        match stream.next().await {
            Next::Chunk(chunk) => {
                body.extend_from_slice(&chunk.bytes);
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
            Next::End(end) => {
                assert_eq!(end, BodyEnd::Complete);
                break;
            }
        }
    }
    assert_eq!(body, case.response_bytes().to_vec());
    assert!(matches!(stream.next().await, Next::End(BodyEnd::Complete)));
}

#[tokio::test]
async fn stall_holds_the_body_open_after_its_chunks() {
    let case = case("text_turn_streaming");
    let upstream = FakeUpstream::replay(&case).await.expect("starts");
    upstream.stall_next(2).await.expect("running");
    let response = HarnessClient::new(upstream.addr())
        .idle_timeout(IDLE)
        .send(&case.request)
        .await
        .expect("head arrives");
    assert_eq!(response.status, StatusCode::OK);
    assert_eq!(response.chunks.len(), 2);
    assert_eq!(response.end, BodyEnd::Stalled { idle: IDLE });
    let chunks = case.response.body.chunks();
    assert_eq!(
        response.body,
        [chunks[0].clone(), chunks[1].clone()].concat()
    );

    // The command applied once: the next request completes.
    let again = HarnessClient::new(upstream.addr())
        .send(&case.request)
        .await
        .expect("answered");
    assert_eq!(again.end, BodyEnd::Complete);
}

#[tokio::test]
async fn disconnect_cuts_the_body_mid_stream() {
    let case = case("thinking_streaming");
    let upstream = FakeUpstream::replay(&case).await.expect("starts");
    upstream.disconnect_next(3).await.expect("running");
    let response = HarnessClient::new(upstream.addr())
        .idle_timeout(IDLE)
        .send(&case.request)
        .await
        .expect("head arrives");
    assert_eq!(response.chunks.len(), 3);
    assert!(
        matches!(response.end, BodyEnd::Aborted { .. }),
        "{:?}",
        response.end
    );
}

#[tokio::test]
async fn disconnect_cuts_a_whole_body_short_of_its_length() {
    let case = case("text_turn");
    let upstream = FakeUpstream::replay(&case).await.expect("starts");
    upstream.disconnect_next(0).await.expect("running");
    let response = HarnessClient::new(upstream.addr())
        .idle_timeout(IDLE)
        .send(&case.request)
        .await
        .expect("head arrives");
    assert!(response.body.is_empty());
    assert!(
        matches!(response.end, BodyEnd::Aborted { .. }),
        "{:?}",
        response.end
    );
}

#[tokio::test]
async fn fail_next_answers_with_an_anthropic_error() {
    let case = case("text_turn_streaming");
    let upstream = FakeUpstream::replay(&case).await.expect("starts");
    upstream
        .fail_next(StatusCode::from_u16(529).expect("a status"))
        .await
        .expect("running");
    let response = HarnessClient::new(upstream.addr())
        .send(&case.request)
        .await
        .expect("answered");
    assert_eq!(response.status.as_u16(), 529);
    let body: serde_json::Value = serde_json::from_slice(&response.body).expect("JSON");
    assert_eq!(body["type"], "error");
    assert_eq!(body["error"]["type"], "overloaded_error");
    let next = HarnessClient::new(upstream.addr())
        .send(&case.request)
        .await
        .expect("answered");
    assert_eq!(next.status, StatusCode::OK);
}

#[tokio::test]
async fn no_response_never_sends_a_head() {
    let case = case("text_turn");
    let upstream = FakeUpstream::replay(&case).await.expect("starts");
    upstream
        .fault_next(Fault::NoResponse)
        .await
        .expect("running");
    let result = HarnessClient::new(upstream.addr())
        .idle_timeout(IDLE)
        .send(&case.request)
        .await;
    assert!(matches!(
        result,
        Err(crate::client::ClientError::HeadTimeout(_))
    ));
    assert_eq!(upstream.received().await.expect("running").len(), 1);
}

#[tokio::test]
async fn routes_answer_in_order_then_repeat_and_unknown_routes_get_404() {
    let first = case("tool_use_streaming");
    let second = case("tool_result_followup");
    let upstream = FakeUpstream::start(Script::new().cases([&first, &second]))
        .await
        .expect("starts");
    let client = HarnessClient::new(upstream.addr());
    for expected in [&first, &second, &second] {
        let response = client.send(&expected.request).await.expect("answered");
        assert_eq!(response.differences_from(&expected.response), Vec::new());
    }
    let models = case("models_list");
    let missing = client.send(&models.request).await.expect("answered");
    assert_eq!(missing.status, StatusCode::NOT_FOUND);
    let body: serde_json::Value = serde_json::from_slice(&missing.body).expect("JSON");
    assert_eq!(body["error"]["type"], "not_found_error");

    upstream
        .mount(Route::of(&models.request), Reply::from_case(&models))
        .await
        .expect("running");
    let found = client.send(&models.request).await.expect("answered");
    assert_eq!(found.differences_from(&models.response), Vec::new());
    let received = upstream.received().await.expect("running");
    assert_eq!(received.len(), 5);
    assert_eq!(received[3].method, Method::GET);
}

#[tokio::test]
async fn received_requests_show_what_a_proxy_changed() {
    let case = case("text_turn");
    let upstream = FakeUpstream::replay(&case).await.expect("starts");
    let mut altered = case.request.clone();
    altered.headers.push(
        HeaderName::from_static("x-added"),
        HeaderValue::from_static("1"),
    );
    altered.body = bytes::Bytes::from_static(b"{}");
    HarnessClient::new(upstream.addr())
        .send(&altered)
        .await
        .expect("answered");
    let received = upstream.received().await.expect("running");
    let differences = received[0].differences_from(&case.request);
    assert_eq!(differences.len(), 2, "{differences:?}");
}

#[tokio::test]
async fn a_base_url_prefix_is_prepended_and_replies_take_extra_headers() {
    let case = case("text_turn");
    let reply = Reply::from_case(&case).header(
        HeaderName::from_static("x-should-retry"),
        HeaderValue::from_static("false"),
    );
    let script = Script::new().route(Route::new(Method::POST, "/anthropic/v1/messages"), reply);
    let upstream = FakeUpstream::start(script).await.expect("starts");
    let response = HarnessClient::new(upstream.addr())
        .prefix("/anthropic/")
        .send(&case.request)
        .await
        .expect("answered");
    assert_eq!(response.status, StatusCode::OK);
    assert_eq!(response.headers.get_str("x-should-retry"), Some("false"));
    let received = upstream.received().await.expect("running");
    assert_eq!(received[0].target, "/anthropic/v1/messages?beta=true");
    assert!(upstream.base_url().starts_with("http://127.0.0.1:"));
}

#[tokio::test]
async fn dropping_the_upstream_closes_its_port() {
    let case = case("text_turn");
    let upstream = FakeUpstream::replay(&case).await.expect("starts");
    let addr = upstream.addr();
    drop(upstream);
    tokio::task::yield_now().await;
    let result = HarnessClient::new(addr)
        .idle_timeout(IDLE)
        .send(&case.request)
        .await;
    assert!(result.is_err());
}
