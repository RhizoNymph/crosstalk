//! The event stream parser, as the WHATWG standard reads a stream, fed in
//! every split a network may produce.

use crate::live::sse::{SseError, SseEvent, SseParser};

fn event(name: &str, id: Option<&str>, data: &str) -> SseEvent {
    SseEvent {
        name: name.to_owned(),
        id: id.map(str::to_owned),
        data: data.to_owned(),
    }
}

fn parse_in(chunks: &[&[u8]]) -> Vec<SseEvent> {
    let mut parser = SseParser::new(1024);
    let mut events = Vec::new();
    for chunk in chunks {
        parser.feed(chunk).unwrap_or_else(|error| panic!("{error}"));
        while let Some(event) = parser.next_event() {
            events.push(event);
        }
    }
    events
}

/// The binding's two frames, whole and split at every byte.
#[test]
fn binding_frames_parse_however_they_are_split() {
    let stream = concat!(
        "event: heartbeat\nid: 7-1042\ndata: {\"type\":\"heartbeat\",\"data\":{\"cursor\":\"7-1042\"}}\n\n",
        "event: end\ndata: \"lagged\"\n\n",
    )
    .as_bytes();
    let expected = vec![
        event(
            "heartbeat",
            Some("7-1042"),
            r#"{"type":"heartbeat","data":{"cursor":"7-1042"}}"#,
        ),
        event("end", None, "\"lagged\""),
    ];
    assert_eq!(parse_in(&[stream]), expected);
    for at in 0..=stream.len() {
        let (head, tail) = stream.split_at(at);
        assert_eq!(parse_in(&[head, tail]), expected, "split at {at}");
    }
    let bytes: Vec<&[u8]> = stream.chunks(1).collect();
    assert_eq!(parse_in(&bytes), expected);
}

/// `\r\n`, `\r` and `\n` all end a line, a `\r\n` pair split across chunks
/// included; a leading byte-order mark and comments are dropped.
#[test]
fn line_ends_bom_and_comments() {
    let crlf: &[u8] = b"\xEF\xBB\xBFdata: a\r\n: comment\r\ndata:b\r\rdata: c\n\n";
    let expected = vec![event("message", None, "a\nb"), event("message", None, "c")];
    assert_eq!(parse_in(&[crlf]), expected);
    for at in 0..=crlf.len() {
        let (head, tail) = crlf.split_at(at);
        assert_eq!(parse_in(&[head, tail]), expected, "split at {at}");
    }
}

/// One leading space is dropped from a value; a line with no colon is a
/// field with an empty value; an empty `data` line still dispatches; an
/// event with no `data` does not; unknown fields and `retry` are ignored;
/// an id with a NUL is ignored.
#[test]
fn fields_follow_the_standard() {
    let stream = b"data:  two spaces\n\ndata\n\nevent: x\nid: 1\n\nretry: 10\nfoo: bar\nid: a\0b\ndata: y\n\n";
    assert_eq!(
        parse_in(&[stream]),
        vec![
            event("message", None, " two spaces"),
            event("message", None, ""),
            event("message", None, "y"),
        ]
    );
}

/// A partial event at the end of a stream is never dispatched.
#[test]
fn an_unfinished_event_is_not_dispatched() {
    assert_eq!(parse_in(&[b"event: end\ndata: \"lagged\"\n"]), vec![]);
}

/// A line or an event's data over the limit, or bytes that are not UTF-8,
/// are refused.
#[test]
fn oversized_and_invalid_input_is_refused() {
    let mut parser = SseParser::new(8);
    assert_eq!(
        parser.feed(b"data: 123456789"),
        Err(SseError::LineTooLong { limit: 8 })
    );
    let mut parser = SseParser::new(8);
    assert_eq!(
        parser.feed(b"data: 1234\ndata: 5678\n"),
        Err(SseError::LineTooLong { limit: 8 })
    );
    let mut parser = SseParser::new(64);
    assert_eq!(parser.feed(b"data: \xFF\n"), Err(SseError::NotUtf8));
}
