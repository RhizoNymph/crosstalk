//! `QueryApi::present` on the wire: the gateway's clock and the config a
//! client needs before it builds a request (`Present`), with its export
//! formats (`ExportFormats`) and frame retention (`FrameRetention`).

use std::num::{NonZeroU16, NonZeroU64};

use serde_json::{Value, json};

use super::super::harness::{assert_golden, assert_rejected};
use super::super::ts;
use crate::aggregates::alert::RULE_QUERY_MAX_CHARS;
use crate::aggregates::projection::FrameRetention;
use crate::aggregates::series::BucketWidth;
use crate::aggregates::topic::TopicModelVersion;
use crate::interfaces::l8_surface::Present;
use crate::interfaces::l8_surface::export::{ExportFormat, ExportFormats};
use crate::support::Similarity;

const AREA: &str = "surface_reads/present";

/// Five minutes.
fn bucket() -> BucketWidth {
    BucketWidth::from_micros(NonZeroU64::new(300_000_000).expect("non-zero"))
}

fn present(formats: Vec<ExportFormat>) -> Present {
    Present {
        now: ts("2026-10-04T12:58:30.000000Z"),
        bucket_width: bucket(),
        export_formats: ExportFormats::new(formats).expect("distinct, non-empty"),
        current_rule_version: TopicModelVersion(4),
        default_remap_threshold: Similarity::new(0.8).expect("in range"),
        frame_retention_micros: FrameRetention::default(),
    }
}

fn json_of(present: &Present) -> Value {
    serde_json::to_value(present).expect("present encodes")
}

/// A gateway writing JSONL only (the UI's fixture), and one writing both,
/// JSONL offered first.
#[test]
fn present_goldens() {
    assert_golden(
        AREA,
        "present_jsonl_only",
        &present(vec![ExportFormat::Jsonl]),
    );
    assert_golden(
        AREA,
        "present_every_format",
        &present(vec![ExportFormat::Jsonl, ExportFormat::Parquet]),
    );
}

/// The retention is whole microseconds in a `_micros` field: 180 days.
#[test]
fn frame_retention_is_microseconds_in_a_micros_field() {
    let json = json_of(&present(vec![ExportFormat::Jsonl]));
    assert_eq!(
        json["frame_retention_micros"],
        json!(180_u64 * 86_400 * 1_000_000)
    );
    assert_eq!(json["bucket_width"], json!(300_000_000_u64));
    assert_eq!(json["export_formats"], json!(["jsonl"]));
}

#[test]
fn present_refuses_what_its_fields_refuse() {
    let edited = |edit: &dyn Fn(&mut Value)| {
        let mut json = json_of(&present(vec![ExportFormat::Jsonl]));
        edit(&mut json);
        json.to_string()
    };
    assert_rejected::<Present>(
        &edited(&|json| json["export_formats"] = json!([])),
        "invalid export formats: Empty",
    );
    assert_rejected::<Present>(
        &edited(&|json| json["export_formats"] = json!(["jsonl", "parquet", "jsonl"])),
        "invalid export formats: Duplicate(Jsonl)",
    );
    assert_rejected::<Present>(
        &edited(&|json| json["export_formats"] = json!(["csv"])),
        "unknown variant `csv`",
    );
    assert_rejected::<Present>(
        &edited(&|json| json["frame_retention_micros"] = json!(0)),
        "nonzero",
    );
    assert_rejected::<Present>(&edited(&|json| json["bucket_width"] = json!(0)), "nonzero");
    assert_rejected::<Present>(
        &edited(&|json| json["default_remap_threshold"] = json!(1.5)),
        "invalid similarity",
    );
    assert_rejected::<Present>(
        &edited(&|json| json["now"] = json!("2026-10-04T12:58:30Z")),
        "timestamp",
    );
    // The clock and the config are the server's: a client field is unknown.
    assert_rejected::<Present>(
        &edited(&|json| json["rule_query_max_chars"] = json!(RULE_QUERY_MAX_CHARS)),
        "unknown field `rule_query_max_chars`",
    );
    let mut renamed = json_of(&present(vec![ExportFormat::Jsonl]));
    if let Some(object) = renamed.as_object_mut()
        && let Some(retention) = object.remove("frame_retention_micros")
    {
        object.insert("frame_retention".into(), retention);
    }
    assert_rejected::<Present>(&renamed.to_string(), "unknown field `frame_retention`");
}

#[test]
fn frame_retention_counts_whole_days() {
    let days = |n: u16| FrameRetention::from_days(NonZeroU16::new(n).expect("non-zero"));
    assert_eq!(days(1).as_micros().get(), 86_400_000_000);
    assert_eq!(
        FrameRetention::default(),
        days(FrameRetention::DEFAULT_DAYS)
    );
    let fitted = ts("2026-10-04T00:00:00.000000Z");
    assert_eq!(
        days(2).expires_at(fitted),
        ts("2026-10-06T00:00:00.000000Z")
    );
    assert_eq!(
        days(2).as_duration(),
        std::time::Duration::from_secs(2 * 86_400)
    );
}
