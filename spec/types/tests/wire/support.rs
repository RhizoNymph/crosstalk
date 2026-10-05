//! The shared building blocks on the wire, and the checked ones refusing
//! what their constructors refuse.

use std::num::{NonZeroU16, NonZeroU32};

use super::harness::{assert_golden, assert_rejected, assert_round_trips};
use super::{ULID_A, ULID_B, id, ts};
use crate::aggregates::watermark::Watermarked;
use crate::ids::AgentId;
use crate::support::{
    ByteRange, Capped, Change, DisplayText, NonBlank, NonEmpty, Share, Similarity, TimeWindow,
    Watermark,
};

const AREA: &str = "support";

fn window() -> TimeWindow {
    TimeWindow::new(
        ts("2026-10-04T12:00:00.000000Z"),
        ts("2026-10-04T13:00:00.000000Z"),
    )
    .expect("the window is not empty")
}

#[test]
fn support_goldens() {
    assert_golden(AREA, "time_window", &window());
    assert_golden(
        AREA,
        "byte_range",
        &ByteRange::new(128, 4096).expect("not empty"),
    );
    assert_golden(
        AREA,
        "similarity",
        &Similarity::new(0.875).expect("in range"),
    );
    assert_golden(AREA, "share", &Share::new(0.25).expect("in range"));
    let agents = NonEmpty::from_vec(vec![
        id(AgentId::from_ulid_text, ULID_A),
        id(AgentId::from_ulid_text, ULID_B),
    ])
    .expect("two agents");
    assert_golden(AREA, "non_empty", &agents);
    assert_golden(
        AREA,
        "non_blank",
        &NonBlank::new("agents writing to the wiki").expect("not blank"),
    );
    assert_golden(
        AREA,
        "display_text",
        &DisplayText::<64>::new("research lead").expect("valid"),
    );
    let capped: Capped<u32, 3> = Capped::new(vec![7, 8, 9], 41).expect("within the cap");
    assert_golden(AREA, "capped", &capped);
    assert_golden(AREA, "changes", &[Change::Applied, Change::Unchanged]);
    assert_golden(
        AREA,
        "watermark",
        &Watermark(ts("2026-10-04T12:00:00.000000Z")),
    );
    assert_golden(
        AREA,
        "watermarked",
        &Watermarked {
            watermark: Watermark(ts("2026-10-04T12:00:00.000000Z")),
            value: 42u64,
        },
    );
}

#[test]
fn non_zero_numbers_refuse_zero() {
    assert_round_trips(&NonZeroU32::MIN);
    assert_rejected::<NonZeroU32>("0", "invalid value");
    assert_rejected::<NonZeroU16>("0", "invalid value");
    assert_rejected::<NonZeroU16>("65536", "invalid value");
}

#[test]
fn time_windows_refuse_empty_and_unknown_shapes() {
    assert_rejected::<TimeWindow>(
        r#"{"start": "2026-10-04T13:00:00.000000Z", "end": "2026-10-04T13:00:00.000000Z"}"#,
        "invalid time window: EmptyWindow",
    );
    assert_rejected::<TimeWindow>(
        r#"{"start": "2026-10-04T13:00:00.000000Z", "end": "2026-10-04T12:00:00.000000Z"}"#,
        "invalid time window: EmptyWindow",
    );
    assert_rejected::<TimeWindow>(
        r#"{"start": "2026-10-04T12:00:00.000000Z", "end": "2026-10-04T13:00:00.000000Z", "step": 1}"#,
        "unknown field `step`",
    );
    assert_rejected::<TimeWindow>(
        r#"{"start": "2026-10-04T12:00:00.000000Z"}"#,
        "missing field `end`",
    );
}

#[test]
fn byte_ranges_refuse_empty() {
    assert_rejected::<ByteRange>(
        r#"{"start": 64, "end": 64}"#,
        "invalid byte range: EmptyRange",
    );
    assert_rejected::<ByteRange>(r#"{"start": -1, "end": 64}"#, "invalid value");
}

#[test]
fn similarities_and_shares_refuse_out_of_range() {
    assert_rejected::<Similarity>("1.5", "invalid similarity");
    assert_rejected::<Similarity>("-0.1", "invalid similarity");
    assert_rejected::<Similarity>("1e40", "invalid similarity");
    assert_rejected::<Share>("2.0", "invalid share");
    assert_rejected::<Share>("-0.5", "invalid share");
}

#[test]
fn non_empty_refuses_an_empty_array() {
    assert_rejected::<NonEmpty<AgentId>>("[]", "invalid non-empty list: EmptyList");
    assert_round_trips(&NonEmpty::new(1u8));
}

#[test]
fn checked_text_refuses_what_its_constructor_refuses() {
    assert_rejected::<NonBlank>(r#""   ""#, "invalid non-blank text: Blank");
    assert_rejected::<DisplayText<8>>(r#""  ""#, "invalid display text: Blank");
    assert_rejected::<DisplayText<8>>(
        r#""nine char""#,
        "invalid display text: TooLong { max: 8, got: 9 }",
    );
    assert_rejected::<DisplayText<8>>(r#""a\u001bb""#, "invalid display text: ControlCharacter");
    // Decoding normalizes as the constructor does.
    let trimmed: Result<NonBlank, _> = serde_json::from_str(r#""  wiki  ""#);
    assert_eq!(trimmed.ok(), NonBlank::new("wiki").ok());
}

#[test]
fn capped_lists_refuse_what_their_constructor_refuses() {
    assert_rejected::<Capped<u32, 3>>(
        r#"{"shown": [1, 2, 3, 4], "total": 9}"#,
        "invalid capped list: TooMany { max: 3, got: 4 }",
    );
    assert_rejected::<Capped<u32, 3>>(
        r#"{"shown": [1, 2], "total": 1}"#,
        "invalid capped list: TotalBelowShown { total: 1, shown: 2 }",
    );
    assert_rejected::<Capped<u32, 3>>(
        r#"{"shown": [1], "total": 1, "hidden": 0}"#,
        "unknown field `hidden`",
    );
}

#[test]
fn fieldless_enums_are_strings() {
    assert_rejected::<Change>(r#""reverted""#, "unknown variant `reverted`");
    assert_rejected::<Change>(r#"{"type": "applied"}"#, "unknown variant `type`");
}

#[test]
fn watermarked_refuses_unknown_fields() {
    assert_rejected::<Watermarked<u64>>(
        r#"{"watermark": "2026-10-04T12:00:00.000000Z", "value": 1, "stale": false}"#,
        "unknown field `stale`",
    );
}
