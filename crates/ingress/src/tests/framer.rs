//! The response framers: chosen from the response head, independent of
//! chunking and of the request, and reporting events in order.

use std::time::Duration;

use bytes::Bytes;
use crosstalk_spec::interfaces::l0_ingress::{
    FrameError, FrameEvent, ProviderAdapter, ResponseFramer, ResponseHead,
};
use crosstalk_spec::observed::exchange::{ExchangeStage, WireProtocol};
use crosstalk_testkit::upstream::{FakeUpstream, Reply, Script};
use proptest::prelude::*;
use proptest::test_runner::{Config, TestCaseError, TestRunner};

use super::support::{Options, adapter, case, cases};
use crate::config::LimitsConfig;
use crate::framer::{AnthropicFramer, FramerKind};

fn head(status: u16, content_type: Option<&str>) -> ResponseHead {
    ResponseHead {
        status,
        headers: content_type
            .map(|value| vec![("Content-Type".to_owned(), value.to_owned())])
            .unwrap_or_default(),
    }
}

/// Push `chunks` the way the relay does: each chunk, then an empty push
/// after any push that returned events. Returns the events and the error.
fn run(head: &ResponseHead, chunks: &[Bytes]) -> (Vec<FrameEvent>, Option<FrameError>) {
    let mut framer = adapter(&LimitsConfig::default()).framer(head);
    let mut events = Vec::new();
    let mut error = None;
    let mut record =
        |result: Result<Vec<FrameEvent>, FrameError>, events: &mut Vec<FrameEvent>| match result {
            Ok(more) => {
                let any = !more.is_empty();
                events.extend(more);
                any
            }
            Err(found) => {
                error.get_or_insert(found);
                false
            }
        };
    for chunk in chunks {
        if record(framer.push(chunk), &mut events) {
            record(framer.push(&[]), &mut events);
        }
    }
    (events, error)
}

fn split(bytes: &Bytes, cuts: &[usize]) -> Vec<Bytes> {
    let mut points: Vec<usize> = cuts.iter().map(|cut| cut % (bytes.len() + 1)).collect();
    points.push(bytes.len());
    points.sort_unstable();
    points.dedup();
    let mut start = 0;
    let mut chunks = Vec::new();
    for point in points {
        if point > start {
            chunks.push(bytes.slice(start..point));
            start = point;
        }
    }
    chunks
}

const SSE: &str = "text/event-stream; charset=utf-8";

/// Inputs: every corpus body, each in its own framing, and mutations of
/// the streams (a malformed event inserted, a cut, CRLF or CR line ends, an
/// error event appended).
fn bodies() -> Vec<(ResponseHead, Bytes)> {
    let mut bodies = Vec::new();
    for case in cases() {
        let response = &case.response;
        let head = ResponseHead {
            status: response.status.as_u16(),
            headers: response.headers.to_pairs(),
        };
        let bytes = response.body.bytes().clone();
        if head.framing() == crosstalk_spec::interfaces::l0_ingress::ResponseFraming::EventStream {
            let text = String::from_utf8_lossy(&bytes).into_owned();
            let middle = text.len() / 2;
            let at = text[..middle].rfind("\n\n").map_or(0, |at| at + 2);
            let mut malformed = text.clone();
            malformed.insert_str(at, "event: content_block_delta\ndata: {nope\n\n");
            for variant in [
                malformed,
                text[..middle].to_owned(),
                text.replace('\n', "\r\n"),
                text.replace('\n', "\r"),
                format!(
                    "{}event: error\ndata: {{\"type\":\"error\",\"error\":{{\"type\":\"overloaded_error\",\"message\":\"Overloaded\"}}}}\n\n",
                    &text[..at]
                ),
            ] {
                bodies.push((head.clone(), Bytes::from(variant)));
            }
        }
        bodies.push((head, bytes));
    }
    bodies.push((
        head(200, Some("application/json")),
        Bytes::from_static(b"  {\"a\": \"}\\\"{\", \"b\": [1, {\"c\": []}]}\n"),
    ));
    bodies.push((
        head(200, Some("application/json")),
        Bytes::from_static(b"{\"a\": ["),
    ));
    bodies.push((
        head(200, Some("application/json")),
        Bytes::from_static(b"<html>bad gateway</html>"),
    ));
    bodies
}

proptest! {
    #![proptest_config(Config::with_cases(64))]

    /// Splitting the same bytes into other chunks gives the same events and
    /// the same error, offset included.
    #[test]
    fn events_independent_of_chunking(
        pick in any::<prop::sample::Index>(),
        cuts in proptest::collection::vec(any::<usize>(), 0..40),
    ) {
        let bodies = bodies();
        let (head, bytes) = &bodies[pick.index(bodies.len())];
        let whole = run(head, std::slice::from_ref(bytes));
        let bytewise: Vec<Bytes> = (0..bytes.len()).map(|at| bytes.slice(at..at + 1)).collect();
        prop_assert_eq!(&run(head, &split(bytes, &cuts)), &whole);
        prop_assert_eq!(&run(head, &bytewise), &whole);
    }
}

fn piece() -> impl Strategy<Value = String> {
    prop_oneof![
        Just("event: message_start\ndata: {\"type\":\"message_start\"}\n\n".to_owned()),
        Just("event: content_block_start\ndata: {}\n\n".to_owned()),
        Just("event: content_block_delta\ndata: {\"delta\":{}}\n\n".to_owned()),
        Just("event: content_block_stop\ndata: {}\n\n".to_owned()),
        Just("event: message_delta\ndata: {}\n\n".to_owned()),
        Just("event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n".to_owned()),
        Just("event: ping\ndata: {\"type\": \"ping\"}\n\n".to_owned()),
        Just("event: future_event\ndata: {}\n\n".to_owned()),
        Just(": comment\n\n".to_owned()),
        Just("\n".to_owned()),
        Just("event: error\ndata: {\"error\":{\"message\":\"x\"}}\n\n".to_owned()),
        Just("data: [DONE]\n\n".to_owned()),
        "[ -~]{0,12}\n",
    ]
}

proptest! {
    #![proptest_config(Config::with_cases(256))]

    /// Across all pushes, the events form a prefix of FirstContent,
    /// Finished, whatever the stream holds (repeated starts and stops,
    /// trailing events, errors) and however it is chunked.
    #[test]
    fn events_form_ordered_prefix(
        pieces in proptest::collection::vec(piece(), 0..16),
        cuts in proptest::collection::vec(any::<usize>(), 0..20),
        json in any::<bool>(),
    ) {
        let bytes = Bytes::from(pieces.concat());
        let head = if json { head(200, Some("application/json")) } else { head(200, Some(SSE)) };
        let (events, _) = run(&head, &split(&bytes, &cuts));
        let order = [FrameEvent::FirstContent, FrameEvent::Finished];
        prop_assert!(events.len() <= 2 && events[..] == order[..events.len()], "{:?}", events);
    }
}

/// The framer follows the documented table: an event stream for a 2xx
/// `text/event-stream` (parameters and case aside), one JSON body for any
/// other 2xx, and an error document for any other status, from the
/// adapter of the classifying protocol.
#[test]
fn framer_chosen_from_protocol_and_response_head() {
    let adapter = adapter(&LimitsConfig::default());
    assert_eq!(adapter.protocol(), WireProtocol::AnthropicMessages);
    let table = [
        (head(200, Some(SSE)), FramerKind::EventStream),
        (
            head(200, Some("Text/Event-Stream")),
            FramerKind::EventStream,
        ),
        (
            head(201, Some("text/event-stream;charset=UTF-8")),
            FramerKind::EventStream,
        ),
        (head(200, Some("application/json")), FramerKind::SingleBody),
        (head(200, None), FramerKind::SingleBody),
        (head(204, Some("text/plain")), FramerKind::SingleBody),
        (
            head(429, Some("application/json")),
            FramerKind::ErrorDocument,
        ),
        (head(529, Some(SSE)), FramerKind::ErrorDocument),
        (head(302, None), FramerKind::ErrorDocument),
    ];
    for (head, kind) in table {
        assert_eq!(adapter.framer(&head).kind(), kind, "{head:?}");
        assert_eq!(FramerKind::for_head(&head), kind);
    }
    let mut error = AnthropicFramer::for_response(
        &head(429, Some("application/json")),
        LimitsConfig::default().sse_event_bytes,
    );
    assert_eq!(error.push(br#"{"type":"error"}"#), Ok(Vec::new()));
    let mut whole =
        AnthropicFramer::for_response(&head(200, None), LimitsConfig::default().sse_event_bytes);
    assert_eq!(
        whole.push(br#"{"id":"msg_1"}"#),
        Ok(vec![FrameEvent::FirstContent, FrameEvent::Finished])
    );
}

/// The stages a response drives (and so its FrameEvents and FrameError)
/// are the same whatever the request body: valid, not JSON, compressed
/// garbage or empty; decoded, failed or still decoding.
#[test]
fn events_independent_of_request_body() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("runtime");
    let streaming = case("text_turn_streaming");
    let (upstream, proxy) = runtime.block_on(async {
        let upstream = FakeUpstream::start(Script::new()).await.expect("upstream");
        let proxy = super::support::start(&upstream.base_url(), Options::default()).await;
        (upstream, proxy)
    });
    let proxy = tokio::sync::Mutex::new(proxy);
    let frames = streaming.response.body.chunks();
    let mut malformed = Reply::from_case(&streaming);
    malformed.chunks = [
        frames[..2].to_vec(),
        vec![Bytes::from_static(b"event: message_delta\ndata: {x\n\n")],
        frames[2..].to_vec(),
    ]
    .concat();
    let replies = [Reply::from_case(&streaming), malformed];
    let strategy = (
        prop_oneof![
            Just(streaming.request.body.to_vec()),
            proptest::collection::vec(any::<u8>(), 0..200),
            Just(b"{\"model\": 1}".to_vec()),
            Just(Vec::new()),
        ],
        0usize..2,
    );
    let expected: std::cell::RefCell<[Option<Vec<String>>; 2]> =
        std::cell::RefCell::new([None, None]);
    let mut runner = TestRunner::new(Config::with_cases(32));
    runner
        .run(&strategy, |(body, which)| {
            runtime.block_on(async {
                let mut proxy = proxy.lock().await;
                let mut request = streaming.request.clone();
                request.body = Bytes::from(body);
                upstream
                    .reply_next(replies[which].clone())
                    .await
                    .map_err(|error| TestCaseError::fail(error.to_string()))?;
                let _ = proxy
                    .client()
                    .send(&request)
                    .await
                    .map_err(|error| TestCaseError::fail(error.to_string()))?;
                let mut stages = Vec::new();
                while let Ok(Some(event)) =
                    tokio::time::timeout(Duration::from_millis(100), proxy.stages.recv()).await
                {
                    stages.push(match event.stage {
                        ExchangeStage::Forwarded { .. } => "forwarded".to_owned(),
                        ExchangeStage::Responding { .. } => "responding".to_owned(),
                        other => format!("{other:?}"),
                    });
                    if stages.len() == 3 {
                        break;
                    }
                }
                let mut expected = expected.borrow_mut();
                let seen = expected[which].get_or_insert_with(|| stages.clone());
                prop_assert_eq!(&stages, seen);
                Ok(())
            })
        })
        .expect("the request body never changes the framing");
    assert_eq!(
        expected.borrow()[0].as_deref(),
        Some(
            &[
                "forwarded".to_owned(),
                "responding".to_owned(),
                "Completed".to_owned()
            ][..]
        )
    );
}
