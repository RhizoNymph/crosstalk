//! The canonical encoding and the blob store on fixed inputs.

use crosstalk_spec::interfaces::l2_transport::BlobStore;
use crosstalk_spec::observed::exchange::Transport;
use crosstalk_spec::observed::message::{
    AssistantPart, CanonicalJson, Media, MediaKind, MessageBody, Reasoning, SystemPart, Text,
    ToolArguments, ToolCall, ToolCallId, ToolExecution, ToolName, ToolOutcome, ToolResult,
    ToolResultContent, Unknown, UserPart,
};
use crosstalk_spec::support::NonEmpty;
use crosstalk_transport::blob::MemoryBlobStore;
use serde_json::json;

use crate::encoding::{self, DecodeError};
use crate::tests::golden::assert_golden;
use crate::tests::support::{case, normalize, ok, raw, request_bodies};

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
/// arguments with integers beyond 2^53, and text that needs escaping.
pub fn vectors() -> Vec<(&'static str, MessageBody)> {
    let call = |id: &str, arguments: ToolArguments, execution: ToolExecution| {
        AssistantPart::ToolCall(ToolCall {
            id: ToolCallId(id.to_owned()),
            name: ToolName("Read".to_owned()),
            arguments,
            execution,
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
                AssistantPart::Reasoning(Reasoning::Visible(text("Think first."))),
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
    let golden = assert_golden("encoding", "vectors", &serde_json::Value::Array(pinned));
    let golden = golden.as_array().unwrap_or_else(|| panic!("an array"));
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

/// L1 stores each message as its canonical encoding, under its hash, and
/// never as the provider's wire bytes; media as its decoded bytes.
pub fn capture_puts_canonical_encoding_under_message_hash() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap_or_else(|error| panic!("a runtime: {error}"));
    runtime.block_on(async {
        let (_, followup) = case("tool_result_followup");
        let image = r#"{"model":"m","messages":[{"role":"user","content":[{"type":"image","source":{"type":"base64","media_type":"image/png","data":"iVBORw0KGgo="}}]}]}"#;
        for raw in [followup, raw(image, Transport::Http, ok("{}"))] {
            let normalization = normalize(&raw);
            let blobs = MemoryBlobStore::new();
            crate::store(&blobs, &normalization)
                .await
                .unwrap_or_else(|error| panic!("stored: {error}"));
            for message in &normalization.exchange.messages {
                let stored = blobs
                    .get(message.hash)
                    .await
                    .unwrap_or_else(|error| panic!("{error:?}"))
                    .unwrap_or_else(|| panic!("a body under {:?}", message.hash));
                assert_eq!(stored, encoding::encode(&message.body));
                assert_eq!(encoding::decode(&stored).as_ref(), Ok(&message.body));
            }
            let wire = blobs
                .get(encoding::hash_bytes(&raw.request.body))
                .await
                .unwrap_or_else(|error| panic!("{error:?}"));
            assert_eq!(wire, None, "the provider's bytes are not stored");
            for media in &normalization.media {
                let stored = blobs
                    .get(media.hash)
                    .await
                    .unwrap_or_else(|error| panic!("{error:?}"));
                assert_eq!(stored.as_ref(), Some(&media.bytes));
            }
            let count = normalization.exchange.messages.len() + normalization.media.len();
            assert_eq!(blobs.len().ok(), Some(count));
        }
    });
}

/// A media part's blob is the BLAKE3 of the decoded bytes, not of the
/// base64 text, wherever the media sits.
pub fn media_hash_is_of_decoded_bytes() {
    let png = b"\x89PNG\r\n\x1a\n";
    let data = "iVBORw0KGgo=";
    let request = format!(
        r#"{{"model":"m","messages":[{{"role":"user","content":[{{"type":"image","source":{{"type":"base64","media_type":"image/png","data":"{data}"}}}},{{"type":"tool_result","tool_use_id":"t","content":[{{"type":"document","source":{{"type":"base64","media_type":"application/pdf","data":"{data}"}}}}]}}]}}]}}"#
    );
    let normalization = normalize(&raw(&request, Transport::Http, ok("{}")));
    let decoded = encoding::hash_bytes(png);
    assert_ne!(decoded, encoding::hash_bytes(data.as_bytes()));
    let bodies = request_bodies(&normalization.exchange);
    assert_eq!(
        bodies[0],
        MessageBody::User(vec![UserPart::Media(Media {
            kind: MediaKind::Image,
            blob: decoded
        })])
    );
    let MessageBody::Tool(results) = &bodies[1] else {
        panic!("a tool message: {bodies:?}");
    };
    assert_eq!(
        results.first().content,
        vec![ToolResultContent::Media(Media {
            kind: MediaKind::Document,
            blob: decoded
        })]
    );
    assert_eq!(normalization.media.len(), 1, "one blob for the one file");
    assert_eq!(normalization.media[0].hash, decoded);
    assert_eq!(normalization.media[0].bytes, png.to_vec());
}
