//! The golden-file harness for the wire contract.
//!
//! A golden file holds the exact JSON of one value:
//! `spec/types/tests/golden/<area>/<name>.json`, written by
//! `serde_json::to_string_pretty` with a trailing newline. [`assert_golden`]
//! checks that the value encodes to the file byte for byte and that the
//! file decodes back to an equal value, so any change to a type's JSON
//! fails a test until its golden is rewritten, and the rewrite is a
//! reviewable diff.
//!
//! To write or rewrite goldens after an intended format change, run the
//! tests with `CROSSTALK_BLESS=1`:
//!
//! ```sh
//! CROSSTALK_BLESS=1 cargo test -p crosstalk-spec wire
//! git diff spec/types/tests/golden
//! ```
//!
//! Blessing writes every golden its tests reach and still checks that each
//! decodes back to its value. Review the diff before committing it.

use std::fmt::Debug;
use std::path::PathBuf;

use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value;
use serde_json::error::Category;

use crate::wire::{WireRequest, decode_request};

/// The variable that switches the harness from checking goldens to writing
/// them.
pub const BLESS: &str = "CROSSTALK_BLESS";

/// Keys a request must never carry: who sent it and when are the server's
/// to stamp (`crate::wire::authority`).
pub const AUTHORITY_KEYS: [&str; 8] = [
    "by",
    "at",
    "author",
    "caller",
    "permissions",
    "requested_by",
    "created",
    "accepted_at",
];

pub fn golden_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("types/tests/golden")
}

fn golden_path(area: &str, name: &str) -> PathBuf {
    golden_root().join(area).join(format!("{name}.json"))
}

fn blessing() -> bool {
    std::env::var(BLESS).is_ok_and(|value| value == "1")
}

/// The golden text of `value`: pretty-printed JSON and a newline.
pub fn encode<T: Serialize>(value: &T) -> String {
    let mut text = serde_json::to_string_pretty(value)
        .unwrap_or_else(|error| panic!("a golden value must encode: {error}"));
    text.push('\n');
    text
}

/// `value` encodes to the golden file `area/name`, byte for byte. For a type
/// that only serializes; prefer [`assert_golden`].
pub fn assert_encodes<T: Serialize>(area: &str, name: &str, value: &T) {
    let text = encode(value);
    let path = golden_path(area, name);
    if blessing() {
        let dir = path
            .parent()
            .unwrap_or_else(|| panic!("{path:?} has a parent"));
        std::fs::create_dir_all(dir).unwrap_or_else(|error| panic!("create {dir:?}: {error}"));
        std::fs::write(&path, &text).unwrap_or_else(|error| panic!("write {path:?}: {error}"));
        return;
    }
    let golden = std::fs::read_to_string(&path).unwrap_or_else(|error| {
        panic!("golden {path:?} unreadable ({error}); run with {BLESS}=1 to write it")
    });
    assert_eq!(
        text, golden,
        "{area}/{name}: the encoding differs from its golden; if the change is intended, \
         run with {BLESS}=1 and review the diff"
    );
}

/// `value` encodes to the golden file `area/name`, and the file decodes
/// back to `value`.
pub fn assert_golden<T>(area: &str, name: &str, value: &T)
where
    T: Serialize + DeserializeOwned + PartialEq + Debug,
{
    assert_encodes(area, name, value);
    let path = golden_path(area, name);
    let golden =
        std::fs::read_to_string(&path).unwrap_or_else(|error| panic!("read {path:?}: {error}"));
    let decoded: T = serde_json::from_str(&golden)
        .unwrap_or_else(|error| panic!("{area}/{name}: the golden does not decode: {error}"));
    assert_eq!(&decoded, value, "{area}/{name}: decoding the golden");
}

/// [`assert_golden`] for a request type, which also decodes through
/// [`decode_request`] (the HTTP layer's only decoder) and carries no key a
/// client must not supply ([`AUTHORITY_KEYS`]), at any depth.
pub fn assert_request_golden<T>(area: &str, name: &str, value: &T)
where
    T: WireRequest + PartialEq + Debug,
{
    assert_request_golden_allowing(area, name, value, &[]);
}

/// [`assert_request_golden`] for a request with a field that shares a name
/// with a stamp but is the client's to choose, such as `AuditFilter::by`
/// (which authors to list, not who is asking). Each allowed key needs that
/// justification beside the call.
pub fn assert_request_golden_allowing<T>(area: &str, name: &str, value: &T, allowed: &[&str])
where
    T: WireRequest + PartialEq + Debug,
{
    assert_golden(area, name, value);
    let text = encode(value);
    let decoded: T = decode_request(text.as_bytes())
        .unwrap_or_else(|error| panic!("{area}/{name}: decode_request refused it: {error:?}"));
    assert_eq!(&decoded, value, "{area}/{name}: decode_request");
    let json: Value = serde_json::from_str(&text)
        .unwrap_or_else(|error| panic!("{area}/{name}: not JSON: {error}"));
    if let Some(key) = authority_key_except(&json, allowed) {
        panic!("{area}/{name}: a request carries `{key}`, which the server stamps");
    }
}

/// The first authority key anywhere in `json`.
pub fn authority_key(json: &Value) -> Option<&str> {
    authority_key_except(json, &[])
}

fn authority_key_except<'a>(json: &'a Value, allowed: &[&str]) -> Option<&'a str> {
    match json {
        Value::Object(map) => map.iter().find_map(|(key, value)| {
            if AUTHORITY_KEYS.contains(&key.as_str()) && !allowed.contains(&key.as_str()) {
                Some(key.as_str())
            } else {
                authority_key_except(value, allowed)
            }
        }),
        Value::Array(items) => items
            .iter()
            .find_map(|item| authority_key_except(item, allowed)),
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => None,
    }
}

/// `json` is valid JSON that does not decode as `T`: a data error (never a
/// value, and not a syntax error, which would test nothing), whose message
/// contains `reason`.
pub fn assert_rejected<T: DeserializeOwned + Debug>(json: &str, reason: &str) {
    match serde_json::from_str::<T>(json) {
        Ok(value) => panic!("{json} decoded as {value:?}; it must be refused"),
        Err(error) => {
            assert_eq!(
                error.classify(),
                Category::Data,
                "{json} must be refused for its content, not its syntax: {error}"
            );
            let message = error.to_string();
            assert!(
                message.contains(reason),
                "{json} was refused, but for `{message}`, not `{reason}`"
            );
        }
    }
}

/// `value` encodes to JSON that decodes back to it (for values with no
/// golden of their own).
pub fn assert_round_trips<T>(value: &T)
where
    T: Serialize + DeserializeOwned + PartialEq + Debug,
{
    let text = serde_json::to_string(value)
        .unwrap_or_else(|error| panic!("{value:?} must encode: {error}"));
    let decoded: T =
        serde_json::from_str(&text).unwrap_or_else(|error| panic!("{text} must decode: {error}"));
    assert_eq!(&decoded, value, "{text}");
}
