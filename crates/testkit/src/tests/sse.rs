//! Event-stream framing keeps every byte and reads fields per the spec.

use bytes::Bytes;

use crate::corpus::sse::{EventStream, SseError};

fn parse(text: &'static str) -> EventStream {
    EventStream::parse(Bytes::from_static(text.as_bytes())).expect("UTF-8")
}

#[test]
fn frames_split_on_blank_lines_and_rejoin_exactly() {
    let text = "event: ping\ndata: {\"type\": \"ping\"}\n\nevent: message_stop\ndata: {}\n\n";
    let stream = parse(text);
    assert_eq!(stream.events().len(), 2);
    assert_eq!(stream.events()[0].kind(), "ping");
    assert_eq!(
        stream.events()[0].data.as_deref(),
        Some("{\"type\": \"ping\"}")
    );
    assert_eq!(
        stream.events()[0].raw,
        "event: ping\ndata: {\"type\": \"ping\"}\n\n"
    );
    assert_eq!(&stream.chunks().concat()[..], text.as_bytes());
    assert!(stream.trailing().is_empty());
}

#[test]
fn crlf_comments_multiline_data_and_defaults() {
    let stream =
        parse(": keep-alive\r\n\r\nid: 7\r\nretry: 300\r\ndata: a\r\ndata:b\r\nfield\r\n\r\n");
    assert_eq!(stream.events().len(), 2);
    let keep_alive = &stream.events()[0];
    assert!(!keep_alive.dispatches());
    assert_eq!(keep_alive.comments, ["keep-alive"]);
    assert_eq!(stream.dispatched().count(), 1);
    let event = &stream.events()[1];
    assert_eq!(event.kind(), "message");
    assert_eq!(event.data.as_deref(), Some("a\nb"));
    assert_eq!(event.id.as_deref(), Some("7"));
    assert_eq!(event.retry, Some(300));
}

#[test]
fn a_cut_stream_keeps_its_partial_frame() {
    let text = "event: a\ndata: 1\n\nevent: b\ndata: 2";
    let stream = parse(text);
    assert_eq!(stream.events().len(), 1);
    assert_eq!(stream.trailing(), &Bytes::from_static(b"event: b\ndata: 2"));
    assert_eq!(&stream.chunks().concat()[..], text.as_bytes());
}

#[test]
fn non_utf8_frames_are_refused_with_their_offset() {
    let bytes = Bytes::from_static(b"data: ok\n\ndata: \xff\n\n");
    assert_eq!(
        EventStream::parse(bytes),
        Err(SseError::NotUtf8 { offset: 16 })
    );
}
