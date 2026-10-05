//! The normalized exchange a normalizer hands the capture task: its
//! exchange, messages (each body in its encoding's shape), warnings and
//! media blobs. In process only; the goldens pin the shape the
//! normalizers' own goldens use.

use serde_json::Value;

use super::AREA;
use super::exchange::{completed, increment_truncated};
use crate::interfaces::l1_canonical::{
    InvalidNormalizedExchange, NormalizeWarning, NormalizedExchange,
};
use crate::observed::exchange::ExchangeOutcome;
use crate::observed::message::{
    AssistantPart, CanonicalJson, Media, MediaBlob, MediaKind, Message, MessageBody, Reasoning,
    SystemPart, Text, ToolArguments, ToolCall, ToolCallId, ToolExecution, ToolName, ToolOutcome,
    ToolResult, ToolResultContent, Unknown, UserPart,
};
use crate::support::NonEmpty;
use crate::tests::wire::harness::{assert_golden, assert_rejected};

fn text(value: &str) -> Text {
    Text(value.to_owned())
}

fn png() -> MediaBlob {
    MediaBlob::new(b"\x89PNG\r\n\x1a\n".to_vec())
}

fn pdf() -> MediaBlob {
    MediaBlob::new(b"%PDF-1.7\n".to_vec())
}

fn media(kind: MediaKind, blob: &MediaBlob) -> Media {
    Media {
        kind,
        blob: blob.hash(),
    }
}

/// Media in ascending hash order, as a normalized exchange holds it.
fn sorted(mut blobs: Vec<MediaBlob>) -> Vec<MediaBlob> {
    blobs.sort_by_key(MediaBlob::hash);
    blobs
}

fn system() -> Message {
    Message::new(MessageBody::System(vec![SystemPart::Text(text(
        "You are Claude Code, Anthropic's official CLI for Claude.",
    ))]))
}

fn user() -> Message {
    Message::new(MessageBody::User(vec![
        UserPart::Text(text("What does this screenshot show?")),
        UserPart::Media(media(MediaKind::Image, &png())),
    ]))
}

fn tool_results() -> Message {
    let results = NonEmpty::from_vec(vec![ToolResult {
        call_id: ToolCallId("toolu_01A09q90qw90lq917835lq9".into()),
        content: vec![
            ToolResultContent::Text(text("     1\t# Plan\n")),
            ToolResultContent::Media(media(MediaKind::Document, &pdf())),
        ],
        outcome: ToolOutcome::Success,
    }])
    .unwrap_or_else(|| panic!("one result"));
    Message::new(MessageBody::Tool(results))
}

fn response() -> Message {
    Message::new(MessageBody::Assistant(vec![
        AssistantPart::Reasoning(Reasoning::Visible {
            text: text("The image is a terminal; read the plan next."),
            signature: Some("EqNrZpUTd52EaHrgdvgOqGkUPnUywNXrBhd6Pd1kulGi==".into()),
        }),
        AssistantPart::Text(text("It shows a failing test. Reading the plan.")),
        AssistantPart::ToolCall(ToolCall {
            id: ToolCallId("toolu_01A09q90qw90lq917835lq9".into()),
            name: ToolName("Read".into()),
            arguments: ToolArguments::Json(CanonicalJson(
                r#"{"file_path":"/workspace/notes/plan.md","limit":1234567890123456789}"#.into(),
            )),
            execution: ToolExecution::Client,
            signature: None,
        }),
        AssistantPart::Unknown(Unknown {
            kind: "zz_widget".into(),
            raw: CanonicalJson(r#"{"type":"zz_widget"}"#.into()),
        }),
    ]))
}

/// A completed full-history exchange with media in a user turn and a tool
/// result, a signed thinking block, and both warnings.
fn normalized_completed() -> NormalizedExchange {
    let mut exchange = completed();
    let messages = vec![system(), user(), tool_results(), response()];
    exchange.request = messages[..3].iter().map(|message| message.hash).collect();
    if let ExchangeOutcome::Completed { response, .. } = &mut exchange.outcome {
        *response = messages[3].hash;
    }
    NormalizedExchange {
        exchange,
        messages,
        warnings: vec![
            NormalizeWarning::UnknownBlock {
                kind: "zz_widget".into(),
            },
            NormalizeWarning::OrphanToolResult {
                call_id: "toolu_01A09q90qw90lq917835lq9".into(),
            },
        ],
        media: sorted(vec![png(), pdf()]),
    }
}

/// A truncated increment: its tool results, and the partial response.
fn normalized_failed() -> NormalizedExchange {
    let mut exchange = increment_truncated();
    let partial = Message::new(MessageBody::Assistant(vec![AssistantPart::Text(text(
        "The plan has three",
    ))]));
    let request = Message::new(MessageBody::Tool(
        NonEmpty::from_vec(vec![ToolResult {
            call_id: ToolCallId("call_0123456789abcdef01234567".into()),
            content: vec![ToolResultContent::Text(text("exit 0"))],
            outcome: ToolOutcome::Success,
        }])
        .unwrap_or_else(|| panic!("one result")),
    ));
    exchange.request = vec![request.hash];
    if let ExchangeOutcome::Failed {
        partial_response, ..
    } = &mut exchange.outcome
    {
        *partial_response = Some(partial.hash);
    }
    NormalizedExchange {
        exchange,
        messages: vec![request, partial],
        warnings: Vec::new(),
        media: Vec::new(),
    }
}

#[test]
fn normalized_exchanges_golden() {
    for (name, normalized) in [
        ("normalized_exchange_completed", normalized_completed()),
        ("normalized_exchange_failed", normalized_failed()),
    ] {
        assert_eq!(normalized.check(), Ok(()), "{name}");
        assert_golden(AREA, name, &normalized);
    }
}

fn json_of(normalized: &NormalizedExchange) -> Value {
    serde_json::to_value(normalized).unwrap_or_else(|error| panic!("encodes: {error}"))
}

fn edited(edit: impl FnOnce(&mut Value)) -> String {
    let mut json = json_of(&normalized_completed());
    edit(&mut json);
    json.to_string()
}

fn array(value: &mut Value, key: &str) -> Vec<Value> {
    value[key]
        .as_array()
        .cloned()
        .unwrap_or_else(|| panic!("{key} is an array"))
}

/// Decoding checks what `check` checks, and each message's and blob's hash
/// against its content.
#[test]
fn normalized_exchanges_refuse_what_check_refuses() {
    let unknown = edited(|json| json["extra"] = Value::Null);
    assert_rejected::<NormalizedExchange>(&unknown, "unknown field `extra`");

    // A message whose hash is not its body's.
    let forged = edited(|json| json["messages"][0]["hash"] = json["messages"][1]["hash"].clone());
    assert_rejected::<NormalizedExchange>(&forged, "MessageHashMismatch");

    // A body no message has: a tool message with no result.
    let empty = edited(|json| json["messages"][2]["body"]["data"] = Value::Array(Vec::new()));
    assert_rejected::<NormalizedExchange>(&empty, "EmptyTool");

    let dropped = edited(|json| {
        let mut messages = array(json, "messages");
        messages.remove(1);
        json["messages"] = Value::Array(messages);
    });
    assert_rejected::<NormalizedExchange>(&dropped, "UnresolvedMessage");

    let twice = edited(|json| {
        let mut messages = array(json, "messages");
        messages.push(messages[0].clone());
        json["messages"] = Value::Array(messages);
    });
    assert_rejected::<NormalizedExchange>(&twice, "DuplicateMessage");

    let stray = edited(|json| {
        let extra = Message::new(MessageBody::User(vec![UserPart::Text(text("stray"))]));
        let mut messages = array(json, "messages");
        messages.push(serde_json::to_value(&extra).unwrap_or(Value::Null));
        json["messages"] = Value::Array(messages);
    });
    assert_rejected::<NormalizedExchange>(&stray, "UnreferencedMessage");

    let missing = edited(|json| {
        let mut media = array(json, "media");
        media.remove(0);
        json["media"] = Value::Array(media);
    });
    assert_rejected::<NormalizedExchange>(&missing, "MissingMedia");

    let unordered = edited(|json| {
        let mut media = array(json, "media");
        media.reverse();
        json["media"] = Value::Array(media);
    });
    assert_rejected::<NormalizedExchange>(&unordered, "MediaOutOfOrder");

    let unnamed = edited(|json| {
        let mut media = array(json, "media");
        media.push(serde_json::to_value(MediaBlob::new(b"GIF89a".to_vec())).unwrap_or(Value::Null));
        media.sort_by_key(|blob| blob["hash"].as_str().unwrap_or_default().to_owned());
        json["media"] = Value::Array(media);
    });
    assert_rejected::<NormalizedExchange>(&unnamed, "UnreferencedMedia");

    // A blob whose bytes are not what its hash names, or not lower-case hex.
    let altered = edited(|json| json["media"][0]["bytes"] = Value::String("00".into()));
    assert_rejected::<NormalizedExchange>(&altered, "HashMismatch");
    let upper = edited(|json| {
        let bytes = json["media"][0]["bytes"]
            .as_str()
            .unwrap_or_default()
            .to_uppercase();
        json["media"][0]["bytes"] = Value::String(bytes);
    });
    assert_rejected::<NormalizedExchange>(&upper, "Bytes(Character");
    let odd = edited(|json| json["media"][0]["bytes"] = Value::String("abc".into()));
    assert_rejected::<NormalizedExchange>(&odd, "Bytes(Length");

    // A signature that is not a string or null.
    let signature = edited(|json| {
        json["messages"][3]["body"]["data"][0]["data"]["data"]["signature"] = Value::Bool(true);
    });
    assert_rejected::<NormalizedExchange>(&signature, "invalid type");
}

/// `check` refuses the same in memory, where the fields are public.
#[test]
fn check_refuses_a_message_under_another_hash() {
    let mut normalized = normalized_completed();
    let other = normalized.messages[1].hash;
    normalized.messages[0].hash = other;
    assert_eq!(
        normalized.check(),
        Err(InvalidNormalizedExchange::HashMismatch { hash: other })
    );
}

/// Media blobs print their hash and length, never their bytes.
#[test]
fn media_blobs_do_not_print_their_bytes() {
    let blob = MediaBlob::new(b"secret-ish bytes".to_vec());
    let printed = format!("{blob:?}");
    assert!(printed.contains("len: 16"), "{printed}");
    assert!(!printed.contains("115, 101, 99"), "{printed}");
}
