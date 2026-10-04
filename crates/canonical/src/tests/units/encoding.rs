//! The normalizer's use of the canonical encoding and the blob store, on
//! fixed inputs. The encoding's own vectors are the spec's.

use crosstalk_spec::interfaces::l2_transport::BlobStore;
use crosstalk_spec::observed::exchange::Transport;
use crosstalk_spec::observed::message::encoding;
use crosstalk_spec::observed::message::{
    Media, MediaKind, MessageBody, ToolResultContent, UserPart,
};
use crosstalk_transport::blob::MemoryBlobStore;

use crate::tests::support::{case, normalize, ok, raw, request_bodies};

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
            for message in &normalization.messages {
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
                    .get(media.hash())
                    .await
                    .unwrap_or_else(|error| panic!("{error:?}"));
                assert_eq!(stored.as_ref(), Some(&media.bytes().to_vec()));
            }
            let count = normalization.messages.len() + normalization.media.len();
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
    let bodies = request_bodies(&normalization);
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
    assert_eq!(normalization.media[0].hash(), decoded);
    assert_eq!(normalization.media[0].bytes(), png.to_vec());
}
