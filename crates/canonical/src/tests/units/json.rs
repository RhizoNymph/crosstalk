//! Canonical JSON through the normalizer, on fixed inputs. The JSON
//! module's own vectors are the spec's (`crosstalk_spec::tests::encoding`).

use crosstalk_spec::observed::message::{AssistantPart, ToolArguments};

use crate::tests::support::{START, normalize, response_parts, sse, stop, streamed};

/// A snowflake id beyond 2^53 in streamed tool arguments keeps every digit,
/// where a double would round it.
pub fn snowflake_id_survives_canonicalization() {
    let id = "1234567890123456789";
    #[allow(clippy::cast_precision_loss)]
    let rounded = 1_234_567_890_123_456_789_u64 as f64;
    assert_ne!(format!("{rounded}"), id, "a double cannot hold the id");
    let delta = format!(
        r#"{{"type":"content_block_delta","index":0,"delta":{{"type":"input_json_delta","partial_json":"{{\"channel\": {id}, \"after\": -9223372036854775809, \"limit\": 1.50}}"}}}}"#
    );
    let [delta_stop, message_stop] = stop("tool_use");
    let stream = sse(&[
        START,
        (
            "content_block_start",
            r#"{"type":"content_block_start","index":0,"content_block":{"type":"tool_use","id":"toolu_1","name":"fetch","input":{}}}"#,
        ),
        ("content_block_delta", &delta),
        (
            "content_block_stop",
            r#"{"type":"content_block_stop","index":0}"#,
        ),
        (delta_stop.0, &delta_stop.1),
        (message_stop.0, &message_stop.1),
    ]);
    let normalization = normalize(&streamed(&stream));
    let parts = response_parts(&normalization);
    let Some(AssistantPart::ToolCall(call)) = parts.first() else {
        panic!("a tool call: {parts:?}");
    };
    assert_eq!(
        call.arguments,
        ToolArguments::Json(crosstalk_spec::observed::message::CanonicalJson(format!(
            r#"{{"after":-9223372036854775809,"channel":{id},"limit":1.5}}"#
        )))
    );
}
