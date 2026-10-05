//! The SSE encoder against the Anthropic event sequence, and reassembly.

use bytes::{Bytes, BytesMut};
use crosstalk_testkit::corpus::sse::EventStream;
use serde_json::json;

use crate::anthropic::assemble::{AssembleError, assemble, assemble_stream};
use crate::anthropic::sse::{Split, char_pieces, encode, word_pieces};
use crate::anthropic::{AssistantMessage, ResponseBlock, StopReason, Usage};

fn message() -> AssistantMessage {
    AssistantMessage {
        id: "msg_01Test".to_owned(),
        model: "claude-opus-5-5".to_owned(),
        content: vec![
            ResponseBlock::Text {
                text: "I'll update the wiki page `rate-limiting-3` now.".to_owned(),
            },
            ResponseBlock::ToolUse {
                id: "toolu_01Test".to_owned(),
                name: "wiki_write".to_owned(),
                input: json!({"page": "rate-limiting-3", "content": "Token bucket notes: \"quoted\" and üñíçødé."}),
            },
        ],
        stop_reason: StopReason::ToolUse,
        usage: Usage {
            input_tokens: 120,
            output_tokens: 42,
        },
    }
}

fn stream_bytes(message: &AssistantMessage, split: Split) -> Bytes {
    let mut bytes = BytesMut::new();
    for frame in encode(message, split) {
        bytes.extend_from_slice(&frame.to_bytes());
    }
    bytes.freeze()
}

#[test]
fn event_sequence_is_anthropics() {
    let frames = encode(&message(), Split::default());
    let names: Vec<&str> = frames.iter().map(|f| f.event).collect();
    assert_eq!(names.first(), Some(&"message_start"));
    assert_eq!(names.get(1), Some(&"ping"));
    assert_eq!(
        &names[names.len() - 2..],
        &["message_delta", "message_stop"]
    );
    // Between ping and message_delta: per block, start, deltas, stop.
    let mut index = 2;
    for block in 0..2 {
        assert_eq!(names[index], "content_block_start", "block {block}");
        assert_eq!(frames[index].data["index"], block);
        index += 1;
        let mut deltas = 0;
        while names[index] == "content_block_delta" {
            assert_eq!(frames[index].data["index"], block);
            deltas += 1;
            index += 1;
        }
        assert!(deltas > 0);
        assert_eq!(names[index], "content_block_stop");
        assert_eq!(frames[index].data["index"], block);
        index += 1;
    }
    assert_eq!(index, names.len() - 2);
    // The type field of every data object equals its event name.
    for frame in &frames {
        assert_eq!(frame.data["type"], frame.event);
    }
}

#[test]
fn message_start_and_delta_carry_the_right_fields() {
    let frames = encode(&message(), Split::default());
    let start = &frames[0].data["message"];
    assert_eq!(start["id"], "msg_01Test");
    assert_eq!(start["role"], "assistant");
    assert_eq!(start["type"], "message");
    assert_eq!(start["content"], json!([]));
    assert!(start["stop_reason"].is_null());
    assert_eq!(start["usage"]["input_tokens"], 120);
    let delta = &frames[frames.len() - 2].data;
    assert_eq!(delta["delta"]["stop_reason"], "tool_use");
    assert!(delta["delta"]["stop_sequence"].is_null());
    assert_eq!(delta["usage"]["output_tokens"], 42);
}

#[test]
fn tool_block_starts_with_empty_input_and_an_empty_json_delta() {
    let frames = encode(&message(), Split::default());
    let start = frames
        .iter()
        .find(|f| f.event == "content_block_start" && f.data["index"] == 1)
        .expect("tool block start");
    assert_eq!(start.data["content_block"]["type"], "tool_use");
    assert_eq!(start.data["content_block"]["input"], json!({}));
    let first_delta = frames
        .iter()
        .find(|f| f.event == "content_block_delta" && f.data["index"] == 1)
        .expect("tool delta");
    assert_eq!(first_delta.data["delta"]["type"], "input_json_delta");
    assert_eq!(first_delta.data["delta"]["partial_json"], "");
}

#[test]
fn deltas_concatenate_to_the_block() {
    let original = message();
    let frames = encode(
        &original,
        Split {
            words_per_delta: 2,
            json_chars_per_delta: 5,
        },
    );
    let text: String = frames
        .iter()
        .filter(|f| f.event == "content_block_delta" && f.data["index"] == 0)
        .map(|f| f.data["delta"]["text"].as_str().expect("text").to_owned())
        .collect();
    let json_text: String = frames
        .iter()
        .filter(|f| f.event == "content_block_delta" && f.data["index"] == 1)
        .map(|f| {
            f.data["delta"]["partial_json"]
                .as_str()
                .expect("json")
                .to_owned()
        })
        .collect();
    let ResponseBlock::Text { text: want } = &original.content[0] else {
        panic!("text first")
    };
    assert_eq!(&text, want);
    let ResponseBlock::ToolUse { input, .. } = &original.content[1] else {
        panic!("tool second")
    };
    assert_eq!(
        &serde_json::from_str::<serde_json::Value>(&json_text).expect("json"),
        input
    );
}

#[test]
fn frames_are_sse_and_reassemble_through_testkits_parser() {
    for split in [
        Split::default(),
        Split {
            words_per_delta: 1,
            json_chars_per_delta: 1,
        },
        Split {
            words_per_delta: 1000,
            json_chars_per_delta: 1000,
        },
    ] {
        let bytes = stream_bytes(&message(), split);
        let stream = EventStream::parse(bytes.clone()).expect("event stream");
        assert!(stream.trailing().is_empty());
        assert_eq!(stream.chunks().concat(), bytes.to_vec());
        for event in stream.events() {
            assert!(event.raw.starts_with(b"event: "));
            assert!(event.raw.ends_with(b"\n\n"));
        }
        assert_eq!(assemble_stream(&stream).expect("assembles"), message());
    }
}

#[test]
fn a_text_only_message_round_trips() {
    let original = AssistantMessage {
        content: vec![ResponseBlock::Text {
            text: "  leading spaces,\nnew lines\tand tabs  ".to_owned(),
        }],
        stop_reason: StopReason::EndTurn,
        ..message()
    };
    let stream = EventStream::parse(stream_bytes(&original, Split::default())).expect("sse");
    assert_eq!(assemble_stream(&stream).expect("assembles"), original);
}

#[test]
fn word_and_char_pieces_concatenate() {
    for text in [
        "",
        "one",
        "a b c d e f g",
        "  lead and trail  ",
        "multi\n\nline text here",
    ] {
        for words in 1..5 {
            assert_eq!(word_pieces(text, words).concat(), text);
        }
        for chars in 1..7 {
            assert_eq!(char_pieces(text, chars).concat(), text);
        }
    }
    assert_eq!(word_pieces("a b c d e", 2), vec!["a b ", "c d ", "e"]);
    assert_eq!(char_pieces("ü✓x", 2), vec!["ü✓", "x"]);
}

fn events(names_data: &[(&str, serde_json::Value)]) -> Vec<(String, serde_json::Value)> {
    names_data
        .iter()
        .map(|(name, data)| ((*name).to_owned(), data.clone()))
        .collect()
}

#[test]
fn assembly_refuses_broken_streams() {
    let start = (
        "message_start",
        json!({"type": "message_start", "message": {"id": "m", "model": "x", "usage": {"input_tokens": 1}}}),
    );
    let stop = ("message_stop", json!({"type": "message_stop"}));
    // Unfinished.
    assert!(matches!(
        assemble(events(std::slice::from_ref(&start))),
        Err(AssembleError::Unfinished)
    ));
    // A delta before the start.
    assert!(matches!(
        assemble(events(&[("content_block_delta", json!({"index": 0}))])),
        Err(AssembleError::BeforeStart(_))
    ));
    // No stop reason.
    assert!(matches!(
        assemble(events(&[start.clone(), stop.clone()])),
        Err(AssembleError::NoStopReason)
    ));
    // A delta for a block never started.
    assert!(matches!(
        assemble(events(&[
            start.clone(),
            (
                "content_block_delta",
                json!({"index": 3, "delta": {"type": "text_delta", "text": "x"}})
            ),
        ])),
        Err(AssembleError::BadIndex { index: 3 })
    ));
    // A JSON delta for a text block.
    assert!(matches!(
        assemble(events(&[
            start.clone(),
            (
                "content_block_start",
                json!({"index": 0, "content_block": {"type": "text", "text": ""}})
            ),
            (
                "content_block_delta",
                json!({"index": 0, "delta": {"type": "input_json_delta", "partial_json": "{"}})
            ),
        ])),
        Err(AssembleError::DeltaKind { index: 0, .. })
    ));
    // Tool input that is not JSON.
    assert!(matches!(
        assemble(events(&[
            start.clone(),
            (
                "content_block_start",
                json!({"index": 0, "content_block": {"type": "tool_use", "id": "t", "name": "n", "input": {}}})
            ),
            (
                "content_block_delta",
                json!({"index": 0, "delta": {"type": "input_json_delta", "partial_json": "{nope"}})
            ),
            ("content_block_stop", json!({"index": 0})),
        ])),
        Err(AssembleError::ToolInput { index: 0, .. })
    ));
    // An error event.
    assert!(matches!(
        assemble(events(&[
            start,
            ("error", json!({"type": "error", "error": {"type": "overloaded_error", "message": "Overloaded"}})),
        ])),
        Err(AssembleError::ErrorEvent { kind, .. }) if kind == "overloaded_error"
    ));
}

#[test]
fn non_streaming_document_round_trips() {
    let original = message();
    let document = original.to_document();
    assert_eq!(document["type"], "message");
    assert_eq!(document["role"], "assistant");
    assert!(document["stop_sequence"].is_null());
    let bytes = serde_json::to_vec(&document).expect("encode");
    assert_eq!(
        AssistantMessage::from_document(&bytes).expect("decode"),
        original
    );
    assert!(AssistantMessage::from_document(br#"{"type":"error"}"#).is_err());
}
