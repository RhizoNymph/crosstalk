//! Each way an exchange fails, over real sockets, is recorded with its
//! cause; the proxy never assigns UnparseableResponse.

use bytes::Bytes;
use crosstalk_spec::interfaces::l0_ingress::RawResponse;
use crosstalk_spec::observed::exchange::ExchangeFailure;
use crosstalk_testkit::client::{BodyEnd, Next};
use crosstalk_testkit::upstream::{FakeUpstream, Fault, Reply, Script};
use hyper::StatusCode;

use super::support::{Options, TestProxy, case, cases, limits_with, non_zero, start};

fn failure(response: &RawResponse) -> Option<ExchangeFailure> {
    match response {
        RawResponse::Complete { .. } => None,
        RawResponse::Failed { failure, .. } => Some(*failure),
    }
}

async fn expect(proxy: &mut TestProxy, cause: Option<ExchangeFailure>, what: &str) {
    let raw = proxy
        .next_capture()
        .await
        .unwrap_or_else(|| panic!("{what}: nothing captured"));
    let got = failure(&raw.response);
    assert_ne!(got, Some(ExchangeFailure::UnparseableResponse), "{what}");
    assert_eq!(got, cause, "{what}");
}

/// Each failure cause: a non-2xx status before content (429, 401), no
/// response head (connection refused), an error event in a 2xx stream, a
/// stream cut before its end, a malformed frame, the idle timeout, and the
/// client leaving; and a clean completion.
#[tokio::test]
async fn each_cause_classified() {
    let corpus = cases();
    let upstream = FakeUpstream::start(Script::new().cases(&corpus))
        .await
        .expect("upstream");
    let limits = limits_with(|limits| limits.upstream_idle_timeout_ms = Some(non_zero(300)));
    let mut proxy = start(
        &upstream.base_url(),
        Options {
            limits,
            ..Options::default()
        },
    )
    .await;
    let client = proxy.client();
    let streaming = case("text_turn_streaming");

    for (name, status) in [("rate_limited", 429), ("unauthorized", 401)] {
        let case = case(name);
        upstream
            .reply_next(Reply::from_case(&case))
            .await
            .expect("command");
        let response = client.send(&case.request).await.expect("answered");
        assert!(
            response.differences_from(&case.response).is_empty(),
            "{name}"
        );
        expect(&mut proxy, Some(ExchangeFailure::Upstream { status }), name).await;
    }

    let overloaded = case("overloaded_mid_stream");
    upstream
        .reply_next(Reply::from_case(&overloaded))
        .await
        .expect("command");
    let _ = client.send(&overloaded.request).await.expect("answered");
    expect(
        &mut proxy,
        Some(ExchangeFailure::UpstreamErrorEvent),
        "error event",
    )
    .await;

    upstream
        .reply_next(Reply::from_case(&streaming).with_fault(Fault::Disconnect { after_chunks: 3 }))
        .await
        .expect("command");
    let response = client.send(&streaming.request).await.expect("answered");
    assert!(
        matches!(response.end, BodyEnd::Aborted { .. }),
        "the cut did not reach the client"
    );
    expect(
        &mut proxy,
        Some(ExchangeFailure::StreamTruncated),
        "disconnect",
    )
    .await;

    let frames = streaming.response.body.chunks();
    let malformed = Bytes::from_static(b"event: content_block_delta\ndata: {\"type\": oops}\n\n");
    let offset = (frames[0].len() + frames[1].len()) as u64;
    let mut reply = Reply::from_case(&streaming);
    reply.chunks = [frames[..2].to_vec(), vec![malformed], frames[2..].to_vec()].concat();
    upstream.reply_next(reply).await.expect("command");
    let _ = client.send(&streaming.request).await.expect("answered");
    expect(
        &mut proxy,
        Some(ExchangeFailure::MalformedStream { offset }),
        "malformed",
    )
    .await;

    upstream
        .reply_next(Reply::from_case(&streaming).with_fault(Fault::Stall { after_chunks: 2 }))
        .await
        .expect("command");
    let response = client.send(&streaming.request).await.expect("answered");
    assert!(
        matches!(response.end, BodyEnd::Aborted { .. }),
        "the timeout did not end the client's body: {:?}",
        response.end
    );
    expect(&mut proxy, Some(ExchangeFailure::Timeout), "stall").await;

    // A proxy without the idle timeout: the client is the one who leaves.
    let mut patient = start(&upstream.base_url(), Options::default()).await;
    upstream
        .reply_next(Reply::from_case(&streaming).with_fault(Fault::Stall { after_chunks: 2 }))
        .await
        .expect("command");
    let mut stream = patient
        .client()
        .open(&streaming.request)
        .await
        .expect("head");
    for _ in 0..2 {
        assert!(matches!(stream.next().await, Next::Chunk(_)));
    }
    drop(stream);
    expect(
        &mut patient,
        Some(ExchangeFailure::ClientDisconnected),
        "client left",
    )
    .await;

    upstream
        .reply_next(Reply::from_case(&streaming))
        .await
        .expect("command");
    let _ = client.send(&streaming.request).await.expect("answered");
    expect(&mut proxy, None, "complete").await;

    let closed = tokio::net::TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("bind");
    let dead = format!("http://{}", closed.local_addr().expect("address"));
    drop(closed);
    let mut unreachable = start(&dead, Options::default()).await;
    let response = unreachable
        .client()
        .send(&streaming.request)
        .await
        .expect("answered");
    assert_eq!(response.status, StatusCode::BAD_GATEWAY);
    expect(
        &mut unreachable,
        Some(ExchangeFailure::UpstreamUnreachable),
        "unreachable",
    )
    .await;
}
