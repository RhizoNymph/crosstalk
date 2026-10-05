//! ExchangeMeta.transport follows the response actually received.

use crosstalk_spec::observed::exchange::Transport;
use crosstalk_testkit::upstream::{FakeUpstream, Reply, Script};
use hyper::StatusCode;

use super::support::{Options, case, start};

/// A streamed request answered with a JSON 429 is Http; answered with an
/// event stream, Sse. A non-streamed request answered with JSON is Http,
/// and answered with an event stream, Sse.
#[tokio::test]
async fn transport_follows_response() {
    let upstream = FakeUpstream::start(Script::new()).await.expect("upstream");
    let mut proxy = start(&upstream.base_url(), Options::default()).await;
    let client = proxy.client();
    let streamed = case("text_turn_streaming");
    let whole = case("text_turn");
    let mut whole_as_stream = Reply::from_case(&streamed);
    whole_as_stream.chunks = vec![streamed.response_bytes().clone()];
    let table = [
        (
            &streamed,
            Reply::error(StatusCode::TOO_MANY_REQUESTS, "slow down"),
            Transport::Http,
        ),
        (&streamed, Reply::from_case(&streamed), Transport::Sse),
        (&whole, Reply::from_case(&whole), Transport::Http),
        (&whole, whole_as_stream, Transport::Sse),
    ];
    for (case, reply, transport) in table {
        upstream.reply_next(reply).await.expect("command");
        let _ = client.send(&case.request).await.expect("answered");
        let raw = proxy.next_capture().await.expect("captured");
        assert_eq!(raw.meta.transport, transport, "{}", case.name);
        assert_eq!(
            raw.request.harness.stream,
            case.request
                .json()
                .ok()
                .and_then(|json| json["stream"].as_bool())
                .unwrap_or(false)
        );
    }
}
