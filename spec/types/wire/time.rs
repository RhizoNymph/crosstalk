//! Timestamps on the wire: RFC 3339 in UTC at fixed microsecond precision,
//! `YYYY-MM-DDTHH:MM:SS.ffffffZ`, always 27 characters.
//!
//! A [`Timestamp`] is microseconds since the Unix epoch, so the text form is
//! exact in both directions: every timestamp from `1970-01-01T00:00:00.000000Z`
//! through [`MAX_TEXT`] has exactly one text, and decoding accepts only that
//! text. Lower-case `t` or `z`, an offset other than `Z`, a precision other
//! than six digits, a leap second, a date that does not exist and a time
//! before the epoch are all refused. Later timestamps would need a
//! five-digit year, which RFC 3339 does not allow, so encoding one fails
//! ([`TooLateForText`]); no gateway clock reaches it.
//!
//! The civil-date arithmetic is written out (Howard Hinnant's
//! `days_from_civil` and `civil_from_days`, about thirty lines) rather than
//! taken from a date crate: the format is one fixed shape, parsing must
//! refuse everything else, and a general RFC 3339 parser would have to be
//! wrapped in the same shape checks anyway. `tests::wire::time` checks the
//! arithmetic against day-by-day counting over the whole range.

use serde::ser::Error as _;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use super::decode_text;
use crate::support::Timestamp;

const MICROS_PER_SECOND: u64 = 1_000_000;
const SECONDS_PER_DAY: u64 = 86_400;
const MICROS_PER_DAY: u64 = MICROS_PER_SECOND * SECONDS_PER_DAY;

/// Seconds from the epoch to 10000-01-01T00:00:00Z.
const SECONDS_TO_YEAR_10000: u64 = 253_402_300_800;

/// The last timestamp with a text form: 9999-12-31T23:59:59.999999Z.
pub const MAX: Timestamp = Timestamp::from_micros(SECONDS_TO_YEAR_10000 * MICROS_PER_SECOND - 1);

/// The text of [`MAX`].
pub const MAX_TEXT: &str = "9999-12-31T23:59:59.999999Z";

/// The length of every timestamp's text.
pub const TEXT_LEN: usize = 27;

/// A timestamp after [`MAX`]: its year has five digits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TooLateForText(pub Timestamp);

/// Why text is not a timestamp.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InvalidTimestamp {
    /// Not `YYYY-MM-DDTHH:MM:SS.ffffffZ`: the wrong length, a separator out
    /// of place, a non-digit where a digit belongs, a lower-case `t` or `z`.
    Format,
    /// A field outside its range: month 13, the 31st of a 30-day month,
    /// February 29th of a common year, hour 24, minute 60, second 60.
    OutOfRange(TimestampField),
    /// A valid time before 1970-01-01T00:00:00Z.
    BeforeEpoch,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimestampField {
    Month,
    Day,
    Hour,
    Minute,
    Second,
}

impl Timestamp {
    /// The RFC 3339 text: `YYYY-MM-DDTHH:MM:SS.ffffffZ`.
    pub fn rfc3339(self) -> Result<String, TooLateForText> {
        if self > MAX {
            return Err(TooLateForText(self));
        }
        let micros = self.as_micros();
        let days = micros / MICROS_PER_DAY;
        let of_day = micros % MICROS_PER_DAY;
        let (year, month, day) = civil_from_days(days);
        let second_of_day = of_day / MICROS_PER_SECOND;
        let fraction = of_day % MICROS_PER_SECOND;
        let (hour, minute, second) = (
            second_of_day / 3600,
            second_of_day / 60 % 60,
            second_of_day % 60,
        );
        Ok(format!(
            "{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}.{fraction:06}Z"
        ))
    }

    /// The timestamp `text` names, accepting exactly the text
    /// [`Timestamp::rfc3339`] writes.
    pub fn parse_rfc3339(text: &str) -> Result<Self, InvalidTimestamp> {
        let bytes = text.as_bytes();
        if bytes.len() != TEXT_LEN {
            return Err(InvalidTimestamp::Format);
        }
        let separators = [
            (4, b'-'),
            (7, b'-'),
            (10, b'T'),
            (13, b':'),
            (16, b':'),
            (19, b'.'),
            (26, b'Z'),
        ];
        if separators.iter().any(|&(at, byte)| bytes[at] != byte) {
            return Err(InvalidTimestamp::Format);
        }
        let number = |from: usize, to: usize| -> Result<u64, InvalidTimestamp> {
            bytes[from..to].iter().try_fold(0u64, |value, &byte| {
                if byte.is_ascii_digit() {
                    Ok(value * 10 + u64::from(byte - b'0'))
                } else {
                    Err(InvalidTimestamp::Format)
                }
            })
        };
        let year = number(0, 4)?;
        let month = number(5, 7)?;
        let day = number(8, 10)?;
        let hour = number(11, 13)?;
        let minute = number(14, 16)?;
        let second = number(17, 19)?;
        let fraction = number(20, 26)?;

        let out_of_range = |field| Err(InvalidTimestamp::OutOfRange(field));
        if !(1..=12).contains(&month) {
            return out_of_range(TimestampField::Month);
        }
        if day == 0 || day > days_in_month(year, month) {
            return out_of_range(TimestampField::Day);
        }
        if hour > 23 {
            return out_of_range(TimestampField::Hour);
        }
        if minute > 59 {
            return out_of_range(TimestampField::Minute);
        }
        if second > 59 {
            return out_of_range(TimestampField::Second);
        }
        if year < 1970 {
            return Err(InvalidTimestamp::BeforeEpoch);
        }
        let days = days_from_civil(year, month, day);
        let seconds = days * SECONDS_PER_DAY + hour * 3600 + minute * 60 + second;
        Ok(Self::from_micros(seconds * MICROS_PER_SECOND + fraction))
    }
}

impl Serialize for Timestamp {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let text = self
            .rfc3339()
            .map_err(|error| S::Error::custom(super::Rejected::new("timestamp", error)))?;
        serializer.serialize_str(&text)
    }
}

impl<'de> Deserialize<'de> for Timestamp {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        decode_text(deserializer, "timestamp", |text| Self::parse_rfc3339(&text))
    }
}

fn is_leap(year: u64) -> bool {
    year.is_multiple_of(4) && (!year.is_multiple_of(100) || year.is_multiple_of(400))
}

fn days_in_month(year: u64, month: u64) -> u64 {
    match month {
        2 if is_leap(year) => 29,
        2 => 28,
        4 | 6 | 9 | 11 => 30,
        _ => 31,
    }
}

/// Days since 1970-01-01 of a date on or after it (Hinnant's
/// `days_from_civil`, with the year starting in March so the leap day is
/// last).
pub(crate) fn days_from_civil(year: u64, month: u64, day: u64) -> u64 {
    let year = if month <= 2 { year - 1 } else { year };
    let era = year / 400;
    let year_of_era = year - era * 400;
    let month_from_march = (month + 9) % 12;
    let day_of_year = (153 * month_from_march + 2) / 5 + day - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    // 719_468 days from 0000-03-01 to 1970-01-01.
    era * 146_097 + day_of_era - 719_468
}

/// The date `days` after 1970-01-01, as (year, month, day) (Hinnant's
/// `civil_from_days`).
pub(crate) fn civil_from_days(days: u64) -> (u64, u64, u64) {
    let shifted = days + 719_468;
    let era = shifted / 146_097;
    let day_of_era = shifted % 146_097;
    let year_of_era =
        (day_of_era - day_of_era / 1460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_from_march = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * month_from_march + 2) / 5 + 1;
    let month = if month_from_march < 10 {
        month_from_march + 3
    } else {
        month_from_march - 9
    };
    let year = year_of_era + era * 400 + u64::from(month <= 2);
    (year, month, day)
}
