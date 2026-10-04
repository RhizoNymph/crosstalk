//! Golden files: every captured corpus case's normalization, and the pinned
//! canonical encodings of message bodies.
//!
//! A golden is pretty-printed JSON with one trailing newline under
//! `crates/canonical/tests/golden/`. A test fails when the value no longer
//! encodes to its file; to rewrite the files after an intended change, run
//! the tests with `CROSSTALK_BLESS=1` and review the diff:
//!
//! ```sh
//! CROSSTALK_BLESS=1 cargo test -p crosstalk-canonical golden
//! git diff crates/canonical/tests/golden
//! ```
//!
//! A normalization's golden is
//! `{"exchange": <Exchange in the spec's wire JSON>, "messages": [{"hash", "body"}], "warnings": [..], "media": [{"hash", "base64"}]}`,
//! each `body` the message's canonical encoding as JSON. Checking also
//! decodes the file: the exchange through the spec's serde, each body
//! through [`crate::encoding::decode`], each hash recomputed.

use std::path::PathBuf;

use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use crosstalk_spec::ids::MessageHash;
use crosstalk_spec::interfaces::l1_canonical::NormalizeWarning;
use crosstalk_spec::observed::exchange::Exchange;
use crosstalk_spec::observed::message::MessageBody;
use serde_json::{Value, json};

use crate::{Normalization, encoding, json as canonical_json};

pub const BLESS: &str = "CROSSTALK_BLESS";

fn golden_path(area: &str, name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/golden")
        .join(area)
        .join(format!("{name}.json"))
}

fn blessing() -> bool {
    std::env::var(BLESS).is_ok_and(|value| value == "1")
}

/// `value` is the golden `area/name`, byte for byte (or, when blessing, is
/// written there). Returns the file's value.
pub fn assert_golden(area: &str, name: &str, value: &Value) -> Value {
    let mut text = serde_json::to_string_pretty(value)
        .unwrap_or_else(|error| panic!("a golden value encodes: {error}"));
    text.push('\n');
    let path = golden_path(area, name);
    if blessing() {
        let dir = path
            .parent()
            .unwrap_or_else(|| panic!("{path:?} has a parent"));
        std::fs::create_dir_all(dir).unwrap_or_else(|error| panic!("create {dir:?}: {error}"));
        std::fs::write(&path, &text).unwrap_or_else(|error| panic!("write {path:?}: {error}"));
    }
    let golden = std::fs::read_to_string(&path).unwrap_or_else(|error| {
        panic!("golden {path:?} unreadable ({error}); run with {BLESS}=1 to write it")
    });
    assert_eq!(
        text, golden,
        "{area}/{name}: differs from its golden; if the change is intended, run with \
         {BLESS}=1 and review the diff"
    );
    serde_json::from_str(&golden).unwrap_or_else(|error| panic!("{area}/{name} parses: {error}"))
}

fn warning_json(warning: &NormalizeWarning) -> Value {
    match warning {
        NormalizeWarning::UnknownBlock { kind } => {
            json!({"type": "unknown_block", "data": {"kind": kind}})
        }
        NormalizeWarning::OrphanToolResult { call_id } => {
            json!({"type": "orphan_tool_result", "data": {"call_id": call_id}})
        }
    }
}

/// The body's canonical encoding, as a JSON value.
pub fn body_json(body: &MessageBody) -> Value {
    serde_json::from_slice(&encoding::encode(body))
        .unwrap_or_else(|error| panic!("an encoding is JSON: {error}"))
}

/// The body a golden's JSON value encodes, checked against its hash.
pub fn body_from_json(value: &Value, hash: MessageHash) -> MessageBody {
    let text = serde_json::to_string(value).unwrap_or_else(|error| panic!("{error}"));
    let canonical = canonical_json::canonicalize(&text)
        .unwrap_or_else(|error| panic!("a golden body is JSON: {error}"));
    let bytes = canonical.0.into_bytes();
    assert_eq!(
        encoding::hash_bytes(&bytes),
        hash,
        "a golden body hashes to its hash"
    );
    encoding::decode(&bytes).unwrap_or_else(|error| panic!("a golden body decodes: {error}"))
}

pub fn normalization_json(normalization: &Normalization) -> Value {
    let exchange = &normalization.exchange;
    json!({
        "exchange": serde_json::to_value(&exchange.exchange)
            .unwrap_or_else(|error| panic!("an exchange encodes: {error}")),
        "messages": exchange.messages.iter().map(|message| json!({
            "hash": message.hash,
            "body": body_json(&message.body),
        })).collect::<Vec<_>>(),
        "warnings": exchange.warnings.iter().map(warning_json).collect::<Vec<_>>(),
        "media": normalization.media.iter().map(|media| json!({
            "hash": media.hash,
            "base64": STANDARD.encode(&media.bytes),
        })).collect::<Vec<_>>(),
    })
}

/// `normalization` matches its golden, and the golden decodes back to it.
pub fn assert_normalization_golden(area: &str, name: &str, normalization: &Normalization) {
    let golden = assert_golden(area, name, &normalization_json(normalization));
    let exchange: Exchange = serde_json::from_value(golden["exchange"].clone())
        .unwrap_or_else(|error| panic!("{name}: the golden exchange decodes: {error}"));
    assert_eq!(
        exchange, normalization.exchange.exchange,
        "{name}: exchange"
    );
    let messages = golden["messages"]
        .as_array()
        .unwrap_or_else(|| panic!("{name}: messages is an array"));
    assert_eq!(messages.len(), normalization.exchange.messages.len());
    for (golden, message) in messages.iter().zip(&normalization.exchange.messages) {
        let hash: MessageHash = serde_json::from_value(golden["hash"].clone())
            .unwrap_or_else(|error| panic!("{name}: a hash decodes: {error}"));
        assert_eq!(hash, message.hash, "{name}: message hash");
        assert_eq!(
            body_from_json(&golden["body"], hash),
            message.body,
            "{name}: body"
        );
    }
}
