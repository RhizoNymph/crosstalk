//! Times in the AI Village tables, village days and windows.
//!
//! Every table stamps rows with UTC text like `2026-07-10 17:00:16.333633`
//! (microsecond precision, no zone). [`parse_timestamp`] reads that (and the
//! same with a `T` separator or a trailing `Z`) into a spec `Timestamp`.
//!
//! A **village day** runs from 10:00 UTC to 10:00 UTC the next day. The
//! village works 9am–5pm Pacific (16:00–00:00 UTC in summer), so a day's
//! whole session falls inside one village day, and no agent is active
//! across a boundary. The converter builds one world per village day.

use std::fmt;

use crosstalk_spec::support::Timestamp;

const MICROS_PER_SECOND: u64 = 1_000_000;
const SECONDS_PER_DAY: u64 = 86_400;
/// Village days start at 10:00 UTC.
pub const DAY_OFFSET_SECONDS: u64 = 10 * 3_600;

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum TimeError {
    #[error("{0:?} is not a timestamp like 2026-07-10 17:00:16.333633")]
    Timestamp(String),
    #[error("{0:?} is not a day like 2026-07-13")]
    Day(String),
    #[error("{0:?} is before 1970")]
    BeforeEpoch(String),
    #[error("window from {from} to {to} is empty")]
    EmptyWindow { from: Day, to: Day },
    #[error("{0} hours is not a slice of a village day (1 to 24)")]
    Hours(u32),
}

/// A calendar date.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Day {
    pub year: i64,
    pub month: u32,
    pub day: u32,
}

impl fmt::Display for Day {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:04}-{:02}-{:02}", self.year, self.month, self.day)
    }
}

impl Day {
    /// Parses `YYYY-MM-DD`.
    pub fn parse(text: &str) -> Result<Self, TimeError> {
        let error = || TimeError::Day(text.to_owned());
        let mut parts = text.trim().splitn(3, '-');
        let year = parts.next().and_then(|p| p.parse::<i64>().ok());
        let month = parts.next().and_then(|p| p.parse::<u32>().ok());
        let day = parts.next().and_then(|p| p.parse::<u32>().ok());
        match (year, month, day) {
            (Some(year), Some(month @ 1..=12), Some(day @ 1..=31)) => Ok(Self { year, month, day }),
            _ => Err(error()),
        }
    }

    /// Days since 1970-01-01 (Howard Hinnant's `days_from_civil`).
    pub fn days_since_epoch(self) -> i64 {
        let year = if self.month <= 2 {
            self.year - 1
        } else {
            self.year
        };
        let era = year.div_euclid(400);
        let year_of_era = year - era * 400;
        let month = i64::from(self.month);
        let day_of_year = (153 * (if month > 2 { month - 3 } else { month + 9 }) + 2) / 5
            + i64::from(self.day)
            - 1;
        let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
        era * 146_097 + day_of_era - 719_468
    }

    /// The date `days` days after 1970-01-01 (`civil_from_days`).
    pub fn from_days_since_epoch(days: i64) -> Self {
        let z = days + 719_468;
        let era = z.div_euclid(146_097);
        let day_of_era = z - era * 146_097;
        let year_of_era =
            (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
        let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
        let mp = (5 * day_of_year + 2) / 153;
        let day = day_of_year - (153 * mp + 2) / 5 + 1;
        let month = if mp < 10 { mp + 3 } else { mp - 9 };
        let year = year_of_era + era * 400 + i64::from(month <= 2);
        // `day` is in 1..=31 and `month` in 1..=12 by construction.
        Self {
            year,
            month: u32::try_from(month).unwrap_or(1),
            day: u32::try_from(day).unwrap_or(1),
        }
    }

    /// The next date.
    pub fn next(self) -> Self {
        Self::from_days_since_epoch(self.days_since_epoch() + 1)
    }

    /// When village day `self` starts: 10:00 UTC on that date.
    pub fn village_start(self) -> Result<Timestamp, TimeError> {
        let days = u64::try_from(self.days_since_epoch())
            .map_err(|_| TimeError::BeforeEpoch(self.to_string()))?;
        Ok(Timestamp::from_micros(
            (days * SECONDS_PER_DAY + DAY_OFFSET_SECONDS) * MICROS_PER_SECOND,
        ))
    }
}

/// The village day `at` falls in.
pub fn village_day(at: Timestamp) -> Day {
    let seconds = at.as_micros() / MICROS_PER_SECOND;
    let shifted = seconds.saturating_sub(DAY_OFFSET_SECONDS);
    // Days since 1970 fit an i64 for any u64 microsecond count.
    Day::from_days_since_epoch(i64::try_from(shifted / SECONDS_PER_DAY).unwrap_or(i64::MAX))
}

/// Parses a table timestamp: `YYYY-MM-DD HH:MM:SS[.ffffff]`, also with a
/// `T` separator and a trailing `Z`.
pub fn parse_timestamp(text: &str) -> Result<Timestamp, TimeError> {
    let error = || TimeError::Timestamp(text.to_owned());
    let trimmed = text.trim().trim_end_matches('Z');
    let (date, time) = trimmed
        .split_once(' ')
        .or_else(|| trimmed.split_once('T'))
        .ok_or_else(error)?;
    let day = Day::parse(date).map_err(|_| error())?;
    let (clock, fraction) = match time.split_once('.') {
        Some((clock, fraction)) => (clock, fraction),
        None => (time, ""),
    };
    let mut fields = clock.splitn(3, ':');
    let hour = fields.next().and_then(|p| p.parse::<u64>().ok());
    let minute = fields.next().and_then(|p| p.parse::<u64>().ok());
    let second = fields.next().and_then(|p| p.parse::<u64>().ok());
    let (Some(hour @ 0..=23), Some(minute @ 0..=59), Some(second @ 0..=60)) =
        (hour, minute, second)
    else {
        return Err(error());
    };
    if fraction.len() > 9 || !fraction.bytes().all(|b| b.is_ascii_digit()) {
        return Err(error());
    }
    let mut micros = 0u64;
    for (position, digit) in fraction.bytes().take(6).enumerate() {
        micros += u64::from(digit - b'0') * 10u64.pow(5 - u32::try_from(position).unwrap_or(5));
    }
    let days = u64::try_from(day.days_since_epoch())
        .map_err(|_| TimeError::BeforeEpoch(text.to_owned()))?;
    let seconds = days * SECONDS_PER_DAY + hour * 3_600 + minute * 60 + second;
    Ok(Timestamp::from_micros(seconds * MICROS_PER_SECOND + micros))
}

/// Formats a timestamp the way the tables write it, to the second.
pub fn format_seconds(at: Timestamp) -> String {
    let seconds = at.as_micros() / MICROS_PER_SECOND;
    let day = Day::from_days_since_epoch(i64::try_from(seconds / SECONDS_PER_DAY).unwrap_or(0));
    let in_day = seconds % SECONDS_PER_DAY;
    format!(
        "{day} {:02}:{:02}:{:02}",
        in_day / 3_600,
        (in_day / 60) % 60,
        in_day % 60
    )
}

/// A half-open span of time `[from, to)`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Window {
    pub from: Timestamp,
    pub to: Timestamp,
}

impl Window {
    /// Village days `first` to `last`, both included.
    pub fn days(first: Day, last: Day) -> Result<Self, TimeError> {
        if last < first {
            return Err(TimeError::EmptyWindow {
                from: first,
                to: last,
            });
        }
        Ok(Self {
            from: first.village_start()?,
            to: last.next().village_start()?,
        })
    }

    /// The first `hours` hours (1 to 24) of village day `day`, from its
    /// 10:00 UTC start.
    pub fn first_hours(day: Day, hours: u32) -> Result<Self, TimeError> {
        if hours == 0 || hours > 24 {
            return Err(TimeError::Hours(hours));
        }
        let from = day.village_start()?;
        Ok(Self {
            from,
            to: Timestamp::from_micros(
                from.as_micros() + u64::from(hours) * 3_600 * MICROS_PER_SECOND,
            ),
        })
    }

    /// Everything.
    pub fn all() -> Self {
        Self {
            from: Timestamp::from_micros(0),
            to: Timestamp::from_micros(u64::MAX),
        }
    }

    pub fn contains(&self, at: Timestamp) -> bool {
        self.from <= at && at < self.to
    }

    /// Whether the window overlaps `[from, to]`.
    pub fn overlaps(&self, from: Timestamp, to: Timestamp) -> bool {
        from < self.to && self.from <= to
    }
}
