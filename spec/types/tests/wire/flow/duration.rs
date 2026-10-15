//! The duration convention (`crate::wire::duration`): whole microseconds as
//! a number, in a field named `<what>_micros`.

use std::path::Path;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use super::super::harness::{assert_rejected, assert_round_trips};
use crate::wire::duration::{UnfitDuration, micros};

/// A wire type with one duration field, as every such field is declared.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
struct Lagged {
    #[serde(with = "crate::wire::duration")]
    lag_micros: Duration,
}

#[test]
fn durations_encode_as_whole_microseconds() {
    let cases = [
        (Duration::ZERO, "0"),
        (Duration::from_micros(1), "1"),
        (Duration::from_millis(30_250), "30250000"),
        (Duration::from_secs(86_400), "86400000000"),
        (Duration::from_micros(u64::MAX), "18446744073709551615"),
    ];
    for (duration, text) in cases {
        let value = Lagged {
            lag_micros: duration,
        };
        assert_eq!(
            serde_json::to_string(&value).ok(),
            Some(format!(r#"{{"lag_micros":{text}}}"#))
        );
        assert_round_trips(&value);
    }
}

#[test]
fn durations_that_do_not_fit_fail_to_encode() {
    let fraction = Duration::new(1, 500);
    assert_eq!(
        micros(fraction),
        Err(UnfitDuration::SubMicrosecond(fraction))
    );
    let too_long = Duration::from_micros(u64::MAX) + Duration::from_micros(1);
    assert_eq!(micros(too_long), Err(UnfitDuration::TooLong(too_long)));
    for duration in [fraction, too_long, Duration::MAX] {
        let error = serde_json::to_string(&Lagged {
            lag_micros: duration,
        })
        .expect_err("no exact microsecond count");
        assert!(error.to_string().contains("invalid duration"), "{error}");
    }
}

#[test]
fn durations_refuse_every_other_shape() {
    assert_rejected::<Lagged>(r#"{"lag_micros": -1}"#, "invalid value");
    assert_rejected::<Lagged>(r#"{"lag_micros": 1.5}"#, "invalid type");
    assert_rejected::<Lagged>(r#"{"lag_micros": "30250000"}"#, "invalid type");
    assert_rejected::<Lagged>(r#"{"lag_micros": 18446744073709551616}"#, "invalid type");
    assert_rejected::<Lagged>(
        r#"{"lag_micros": {"secs": 30, "nanos": 250000000}}"#,
        "invalid type",
    );
    assert_rejected::<Lagged>(
        r#"{"lag": {"secs": 30, "nanos": 250000000}}"#,
        "unknown field `lag`",
    );
}

/// Every `Duration` field of a type that derives `Serialize` or
/// `Deserialize` uses the
/// convention, and every field using it is named `<what>_micros`. A source
/// scan: serde cannot see field names, and a `Duration` field without the
/// attribute would silently encode as `{"secs", "nanos"}`.
#[test]
fn every_wire_duration_field_uses_the_convention() {
    const ATTRIBUTE: &str = r#"#[serde(with = "crate::wire::duration")]"#;

    fn field_name(line: &str) -> Option<&str> {
        let field = line.trim().strip_suffix(": Duration,")?;
        let name = field
            .strip_prefix("pub(crate) ")
            .or_else(|| field.strip_prefix("pub "))
            .unwrap_or(field);
        name.chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
            .then_some(name)
    }

    fn scan(path: &Path, fields: &mut Vec<String>) {
        let text =
            std::fs::read_to_string(path).unwrap_or_else(|error| panic!("read {path:?}: {error}"));
        let mut derive_serialize = false;
        let mut in_serialized_item = false;
        let mut previous = "";
        for (number, line) in text.lines().enumerate() {
            let trimmed = line.trim_start();
            if trimmed.starts_with("#[derive(") {
                derive_serialize = trimmed.contains("Serialize") || trimmed.contains("Deserialize");
            } else if ["pub struct ", "struct ", "pub enum ", "enum "]
                .iter()
                .any(|item| trimmed.starts_with(item))
            {
                in_serialized_item = derive_serialize;
                derive_serialize = false;
            } else if [
                "impl",
                "fn ",
                "pub fn ",
                "pub(crate) fn ",
                "async fn ",
                "pub trait ",
            ]
            .iter()
            .any(|item| trimmed.starts_with(item))
            {
                in_serialized_item = false;
            }
            let at = format!("{}:{}", path.display(), number + 1);
            if previous.trim() == ATTRIBUTE {
                let name = field_name(line)
                    .unwrap_or_else(|| panic!("{at}: the attribute must sit on a Duration field"));
                assert!(
                    name.ends_with("_micros"),
                    "{at}: `{name}` must end in `_micros`"
                );
            }
            if let Some(name) = field_name(line)
                && in_serialized_item
            {
                assert_eq!(
                    previous.trim(),
                    ATTRIBUTE,
                    "{at}: `{name}` needs {ATTRIBUTE}"
                );
                fields.push(at);
            }
            previous = line;
        }
    }

    fn visit(dir: &Path, fields: &mut Vec<String>) {
        let entries =
            std::fs::read_dir(dir).unwrap_or_else(|error| panic!("read {dir:?}: {error}"));
        for entry in entries {
            let path = entry
                .unwrap_or_else(|error| panic!("entry of {dir:?}: {error}"))
                .path();
            if path.is_dir() {
                if path.file_name().is_some_and(|name| name != "tests") {
                    visit(&path, fields);
                }
            } else if path.extension().is_some_and(|ext| ext == "rs") {
                scan(&path, fields);
            }
        }
    }

    let mut fields = Vec::new();
    visit(
        &Path::new(env!("CARGO_MANIFEST_DIR")).join("types"),
        &mut fields,
    );
    // `CoAccess` and its raw mirror. The config types holding durations
    // (`CorrelationTiming`, `RetryPolicy`, `LiveConfig`) are not wire types.
    assert_eq!(fields.len(), 2, "{fields:?}");
}
