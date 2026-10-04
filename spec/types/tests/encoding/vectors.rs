//! The pinned encodings: one body per shape, its canonical bytes and its
//! hash, in `golden/encoding/vectors.json`.

use serde_json::json;

use crate::observed::message::encoding::{self, DecodeError};
use crate::observed::message::{
    AssistantPart, CanonicalJson, Media, MediaKind, MessageBody, Reasoning, SystemPart, Text,
    ToolArguments, ToolCall, ToolCallId, ToolExecution, ToolName, ToolOutcome, ToolResult,
    ToolResultContent, Unknown, UserPart,
};
use crate::support::NonEmpty;
use crate::tests::wire::harness::assert_golden;

fn text(value: &str) -> Text {
    Text(value.to_owned())
}

fn unknown(kind: &str, raw: &str) -> Unknown {
    Unknown {
        kind: kind.to_owned(),
        raw: CanonicalJson(raw.to_owned()),
    }
}

fn media(kind: MediaKind, bytes: &[u8]) -> Media {
    Media {
        kind,
        blob: encoding::hash_bytes(bytes),
    }
}

/// One body per shape: every body variant, every part variant, an
/// `Unknown` in every part list, both argument variants, a server result,
/// arguments with integers beyond 2^53, text that needs escaping, a tool
/// call with and without a signature, and every tool outcome.
pub fn vectors() -> Vec<(&'static str, MessageBody)> {
    let call = |id: &str, arguments: ToolArguments, execution: ToolExecution| {
        AssistantPart::ToolCall(ToolCall {
            id: ToolCallId(id.to_owned()),
            name: ToolName("Read".to_owned()),
            arguments,
            execution,
            signature: None,
        })
    };
    vec![
        ("system_empty", MessageBody::System(Vec::new())),
        (
            "system_text_and_unknown",
            MessageBody::System(vec![
                SystemPart::Text(text("You are Claude Code.\n")),
                SystemPart::Unknown(unknown(
                    "zz_policy",
                    r#"{"rules":[1,2],"type":"zz_policy"}"#,
                )),
            ]),
        ),
        (
            "user_every_part",
            MessageBody::User(vec![
                UserPart::Text(text(" e\u{301} \"quoted\" \\ \t\u{1}\u{1F600} ")),
                UserPart::Media(media(MediaKind::Image, b"\x89PNG")),
                UserPart::Media(media(MediaKind::Audio, b"RIFF")),
                UserPart::Media(media(MediaKind::Document, b"%PDF-1.7")),
                UserPart::Unknown(unknown(
                    "search_result",
                    r#"{"source":"s","type":"search_result"}"#,
                )),
            ]),
        ),
        (
            "assistant_every_part",
            MessageBody::Assistant(vec![
                AssistantPart::Reasoning(Reasoning::Visible {
                    text: text("Think first."),
                    signature: Some("EqNrZpUTd52EaHrgdvgOqGkUPnUywNXrBhd6Pd1kulGi==".to_owned()),
                }),
                AssistantPart::Reasoning(Reasoning::Visible {
                    text: text("Unsigned."),
                    signature: None,
                }),
                AssistantPart::Reasoning(Reasoning::Opaque {
                    signature: "EqNrZpUTd52EaHrg+/==".to_owned(),
                }),
                AssistantPart::Text(text("I'll look.")),
                call(
                    "toolu_01",
                    ToolArguments::Json(CanonicalJson(
                        r#"{"after":-9223372036854775809,"id":1234567890123456789,"path":"/a"}"#
                            .to_owned(),
                    )),
                    ToolExecution::Client,
                ),
                call(
                    "srvtoolu_01",
                    ToolArguments::Invalid(r#"{"query": "rust"#.to_owned()),
                    ToolExecution::Server,
                ),
                AssistantPart::ToolCall(ToolCall {
                    id: ToolCallId("__thought__CiQB0e2Kb7=".to_owned()),
                    name: ToolName("read_file".to_owned()),
                    arguments: ToolArguments::Json(CanonicalJson(r#"{"path":"/b"}"#.to_owned())),
                    execution: ToolExecution::Client,
                    signature: Some("CiQB0e2Kb7Zg+u1kQx/==".to_owned()),
                }),
                AssistantPart::ServerToolResult(ToolResult {
                    call_id: ToolCallId("srvtoolu_01".to_owned()),
                    content: vec![
                        ToolResultContent::Text(text("result")),
                        ToolResultContent::Media(media(MediaKind::Image, b"img")),
                        ToolResultContent::Unknown(unknown(
                            "web_search_result",
                            r#"{"type":"web_search_result","url":"https://example.com"}"#,
                        )),
                    ],
                    outcome: ToolOutcome::Error,
                }),
                AssistantPart::Unknown(unknown("zz_widget", r#"{"type":"zz_widget"}"#)),
            ]),
        ),
        (
            "tool_results",
            MessageBody::Tool(
                NonEmpty::from_vec(vec![
                    ToolResult {
                        call_id: ToolCallId("toolu_01".to_owned()),
                        content: vec![ToolResultContent::Text(text("     1\t# Plan\n"))],
                        outcome: ToolOutcome::Success,
                    },
                    ToolResult {
                        call_id: ToolCallId("call_0123456789abcdef01234567".to_owned()),
                        content: Vec::new(),
                        outcome: ToolOutcome::Error,
                    },
                    ToolResult {
                        call_id: ToolCallId("call_unflagged".to_owned()),
                        content: vec![ToolResultContent::Text(text("sent"))],
                        outcome: ToolOutcome::Unknown,
                    },
                ])
                .unwrap_or_else(|| panic!("two results")),
            ),
        ),
    ]
}

/// Every vector encodes to its pinned bytes and hash, and the pinned
/// bytes decode back to it.
pub fn golden_encodings_match_pinned_bytes() {
    let pinned: Vec<serde_json::Value> = vectors()
        .iter()
        .map(|(name, body)| {
            let bytes = encoding::encode(body);
            let text = String::from_utf8(bytes).unwrap_or_else(|error| panic!("{error}"));
            json!({"name": name, "encoding": text, "hash": encoding::hash(body)})
        })
        .collect();
    let pinned = serde_json::Value::Array(pinned);
    assert_golden("encoding", "vectors", &pinned);
    let golden = pinned.as_array().unwrap_or_else(|| panic!("an array"));
    assert_eq!(golden.len(), vectors().len());
    for ((name, body), pinned) in vectors().iter().zip(golden) {
        let bytes = pinned["encoding"]
            .as_str()
            .unwrap_or_else(|| panic!("{name}: an encoding"))
            .as_bytes();
        assert_eq!(&encoding::decode(bytes).as_ref(), &Ok(body), "{name}");
        assert_eq!(
            serde_json::to_value(encoding::hash_bytes(bytes))
                .ok()
                .as_ref(),
            Some(&pinned["hash"]),
            "{name}: BLAKE3 of the pinned bytes"
        );
    }
    // Only the canonical bytes decode.
    let canonical = encoding::encode(&vectors()[1].1);
    let spaced = String::from_utf8_lossy(&canonical).replacen(',', ", ", 1);
    assert_eq!(
        encoding::decode(spaced.as_bytes()),
        Err(DecodeError::NotCanonical)
    );
    assert_eq!(
        encoding::decode(br#"{"data":[],"type":"tool"}"#),
        Err(DecodeError::EmptyTool)
    );
    assert_eq!(
        encoding::decode(
            br#"{"data":[{"data":{"kind":"x","raw":"{ }"},"type":"unknown"}],"type":"user"}"#
        ),
        Err(DecodeError::NonCanonicalJson)
    );
    assert!(matches!(
        encoding::decode(br#"{"data":[],"extra":1,"type":"user"}"#),
        Err(DecodeError::Shape { .. })
    ));
}

/// `decode` is the inverse of `encode` and nothing more: every vector's
/// bytes decode to it, and bytes that hold the same value in another
/// spelling, or a shape `encode` never writes, are refused.
pub fn decode_accepts_only_encodings() {
    for (name, body) in vectors() {
        let bytes = encoding::encode(&body);
        let decoded = encoding::decode(&bytes).unwrap_or_else(|error| panic!("{name}: {error}"));
        assert_eq!(decoded, body, "{name}");
        assert_eq!(encoding::encode(&decoded), bytes, "{name}");
    }
    let image = "87da8eea1705a3b5da471f2e9ff924794004aef45804b611c8b0323c1049eae7";
    let refused: [(&[u8], &str); 12] = [
        // The same value, spelled another way.
        (br#"{"type":"user","data":[]}"#, "not in canonical form"),
        (br#"{"data":[], "type":"user"}"#, "not in canonical form"),
        (
            br#"{"data":[],"type":"user","type":"user"}"#,
            "not in canonical form",
        ),
        (
            br#"{"data":[{"data":"\u0062","type":"text"}],"type":"user"}"#,
            "not in canonical form",
        ),
        // Shapes no body has.
        (br#"{"data":[],"type":"User"}"#, "not a message body"),
        (
            br#"{"data":[],"extra":null,"type":"user"}"#,
            "not a message body",
        ),
        (br#"{"type":"user"}"#, "not a message body"),
        (
            br#"{"data":[{"data":"hi","type":"text"}],"type":"tool"}"#,
            "not a message body",
        ),
        (
            br#"{"data":[],"type":"tool"}"#,
            "a tool message with no result",
        ),
        (
            br#"{"data":[{"data":{"kind":"x","raw":"{ }"},"type":"unknown"}],"type":"user"}"#,
            "canonical JSON inside the body is not canonical",
        ),
        // Not JSON at all.
        (b"{\"data\":[],\"type\":\"user\"", "not JSON"),
        (b"\xff", "not JSON"),
    ];
    for (bytes, reason) in refused {
        let error = encoding::decode(bytes)
            .err()
            .unwrap_or_else(|| panic!("{} decoded", String::from_utf8_lossy(bytes)));
        assert!(
            error.to_string().starts_with(reason),
            "{}: refused for `{error}`, not `{reason}`",
            String::from_utf8_lossy(bytes)
        );
    }
    let upper = format!(
        r#"{{"data":[{{"data":{{"blob":"{}","kind":"image"}},"type":"media"}}],"type":"user"}}"#,
        image.to_uppercase()
    );
    assert!(matches!(
        encoding::decode(upper.as_bytes()),
        Err(DecodeError::Shape { .. })
    ));
}
