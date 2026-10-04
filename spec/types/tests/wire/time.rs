//! Timestamps on the wire: RFC 3339 UTC at fixed microsecond precision.

use super::harness::{assert_golden, assert_rejected};
use crate::support::Timestamp;
use crate::wire::time::{
    InvalidTimestamp, MAX, MAX_TEXT, TEXT_LEN, TimestampField, TooLateForText, civil_from_days,
    days_from_civil,
};

/// (micros since the epoch, text), computed independently with Python's
/// `datetime`.
const REFERENCE: [(u64, &str); 8] = [
    (0, "1970-01-01T00:00:00.000000Z"),
    (946_598_400_000_001, "1999-12-31T00:00:00.000001Z"),
    (951_868_799_999_999, "2000-02-29T23:59:59.999999Z"),
    (1_709_164_800_000_000, "2024-02-29T00:00:00.000000Z"),
    (1_791_117_296_789_012, "2026-10-04T12:34:56.789012Z"),
    (1_791_118_800_250_000, "2026-10-04T13:00:00.250000Z"),
    (4_107_564_428_090_100, "2100-03-01T06:07:08.090100Z"),
    (253_402_300_799_999_999, "9999-12-31T23:59:59.999999Z"),
];

#[test]
fn timestamps_match_reference_texts_both_ways() {
    for (micros, text) in REFERENCE {
        let at = Timestamp::from_micros(micros);
        assert_eq!(at.rfc3339(), Ok(text.to_owned()));
        assert_eq!(Timestamp::parse_rfc3339(text), Ok(at));
        assert_eq!(text.len(), TEXT_LEN);
        assert_eq!(serde_json::to_string(&at).ok(), Some(format!("\"{text}\"")));
    }
}

#[test]
fn timestamp_golden() {
    assert_golden(
        "support",
        "timestamp",
        &Timestamp::from_micros(1_791_117_296_789_012),
    );
}

/// The civil-date arithmetic agrees with counting days one at a time, for
/// every day the text form covers (1970-01-01 through 9999-12-31).
#[test]
fn civil_dates_agree_with_day_by_day_counting() {
    fn is_leap(year: u64) -> bool {
        year.is_multiple_of(4) && (!year.is_multiple_of(100) || year.is_multiple_of(400))
    }
    let (mut year, mut month, mut day) = (1970u64, 1u64, 1u64);
    let mut days = 0u64;
    while year < 10_000 {
        assert_eq!(civil_from_days(days), (year, month, day), "day {days}");
        assert_eq!(
            days_from_civil(year, month, day),
            days,
            "{year}-{month}-{day}"
        );
        let length = match month {
            2 if is_leap(year) => 29,
            2 => 28,
            4 | 6 | 9 | 11 => 30,
            _ => 31,
        };
        day += 1;
        if day > length {
            day = 1;
            month += 1;
            if month > 12 {
                month = 1;
                year += 1;
            }
        }
        days += 1;
    }
}

/// Round trips across the whole range, at uneven steps so every field
/// takes many values.
#[test]
fn timestamps_round_trip_across_the_range() {
    let step = 7_919_000_123_457u64;
    let mut micros = 0u64;
    while micros <= MAX.as_micros() {
        let at = Timestamp::from_micros(micros);
        let text = at
            .rfc3339()
            .unwrap_or_else(|error| panic!("{micros}: {error:?}"));
        assert_eq!(Timestamp::parse_rfc3339(&text), Ok(at), "{text}");
        micros += step;
    }
}

#[test]
fn the_last_timestamp_with_a_text_is_the_end_of_9999() {
    assert_eq!(MAX.rfc3339(), Ok(MAX_TEXT.to_owned()));
    let later = Timestamp::from_micros(MAX.as_micros() + 1);
    assert_eq!(later.rfc3339(), Err(TooLateForText(later)));
    assert!(serde_json::to_string(&later).is_err());
    assert!(serde_json::to_string(&Timestamp::from_micros(u64::MAX)).is_err());
}

#[test]
fn parsing_refuses_every_other_shape() {
    let format = [
        "2026-10-04T12:34:56.789012z",
        "2026-10-04t12:34:56.789012Z",
        "2026-10-04 12:34:56.789012Z",
        "2026-10-04T12:34:56Z",
        "2026-10-04T12:34:56.789Z",
        "2026-10-04T12:34:56.789012345Z",
        "2026-10-04T12:34:56.789012+00:00",
        "2026-10-04T12:34:56.789012",
        "+2026-10-04T12:34:56.78901Z",
        "2026-1a-04T12:34:56.789012Z",
        " 2026-10-04T12:34:56.78901Z",
        "",
    ];
    for text in format {
        assert_eq!(
            Timestamp::parse_rfc3339(text),
            Err(InvalidTimestamp::Format),
            "{text:?}"
        );
    }
    let out_of_range = [
        ("2026-13-04T12:34:56.789012Z", TimestampField::Month),
        ("2026-00-04T12:34:56.789012Z", TimestampField::Month),
        ("2026-09-31T12:34:56.789012Z", TimestampField::Day),
        ("2026-02-29T12:34:56.789012Z", TimestampField::Day),
        ("2100-02-29T12:34:56.789012Z", TimestampField::Day),
        ("2026-10-00T12:34:56.789012Z", TimestampField::Day),
        ("2026-10-04T24:00:00.000000Z", TimestampField::Hour),
        ("2026-10-04T12:60:56.789012Z", TimestampField::Minute),
        ("2016-12-31T23:59:60.000000Z", TimestampField::Second),
    ];
    for (text, field) in out_of_range {
        assert_eq!(
            Timestamp::parse_rfc3339(text),
            Err(InvalidTimestamp::OutOfRange(field)),
            "{text}"
        );
    }
    assert_eq!(
        Timestamp::parse_rfc3339("1969-12-31T23:59:59.999999Z"),
        Err(InvalidTimestamp::BeforeEpoch)
    );
    assert_eq!(
        Timestamp::parse_rfc3339("2000-02-29T00:00:00.000000Z").map(Timestamp::as_micros),
        Ok(951_782_400_000_000)
    );
}

#[test]
fn timestamps_reject_anything_but_the_text() {
    assert_rejected::<Timestamp>(r#""2026-10-04T12:34:56Z""#, "invalid timestamp: Format");
    assert_rejected::<Timestamp>(
        r#""2026-02-30T00:00:00.000000Z""#,
        "invalid timestamp: OutOfRange(Day)",
    );
    assert_rejected::<Timestamp>("1791117296789012", "invalid type");
}
