//! Only generation is captured; everything else is forwarded unchanged and
//! not captured, and not counted as a loss.

use std::time::Duration;

use crosstalk_testkit::upstream::{FakeUpstream, Script};
use hyper::header::{HeaderName, HeaderValue};

use super::support::{Options, case, cases, start};

/// count_tokens, model listing and the HEAD /api/hello probe (and another
/// Anthropic route) are relayed unchanged both ways and produce no
/// RawExchange; a generation request on the same proxy does.
#[tokio::test]
async fn non_generation_routes_not_captured() {
    let corpus = cases();
    let upstream = FakeUpstream::start(Script::new().cases(&corpus))
        .await
        .expect("upstream");
    let mut proxy = start(&upstream.base_url(), Options::default()).await;
    let client = proxy.client();
    let mut other = case("count_tokens").request;
    other.target = "/v1/files?limit=5".parse().expect("a target");
    other.method = hyper::Method::GET;
    other.body = bytes::Bytes::new();
    for name in ["count_tokens", "models_list", "hello_probe"] {
        let case = case(name);
        let response = client.send(&case.request).await.expect("answered");
        assert!(
            response.differences_from(&case.response).is_empty(),
            "{name}"
        );
        let received = upstream.received().await.expect("log");
        let last = received.last().expect("forwarded");
        assert!(last.differences_from(&case.request).is_empty(), "{name}");
    }
    let _ = client.send(&other).await.expect("answered");
    let received = upstream.received().await.expect("log");
    assert!(
        received
            .last()
            .expect("forwarded")
            .differences_from(&other)
            .is_empty()
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(300), proxy.captured.recv())
            .await
            .is_err(),
        "a non-generation request was captured"
    );
    assert_eq!(proxy.stats.snapshot(), Default::default());

    let mut generation = case("text_turn").request;
    generation.headers.push(
        HeaderName::from_static("x-test-control"),
        HeaderValue::from_static("1"),
    );
    let _ = client.send(&generation).await.expect("answered");
    assert!(
        proxy.next_capture().await.is_some(),
        "the generation request was not captured"
    );
}
