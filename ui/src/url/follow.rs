//! The `follow=<span>` key: a window that ends at the backend's present.
//!
//! A followed page's URL carries `follow` in place of `from`/`to`. Every
//! render resolves it to `[head − span, head)`, where `head` is the
//! present aligned up to a bucket boundary, so the window slides as the
//! present moves while the URL stays the same. Only the four presets are
//! spans: anything else is not representable.

use crosstalk_spec::aggregates::series::BucketWidth;
use crosstalk_spec::support::{EmptyWindow, TimeWindow, Timestamp};

use super::scope::{align_down, align_up};

const HOUR: u64 = 3_600_000_000;

/// How much a followed window covers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FollowSpan {
    Hour,
    SixHours,
    Day,
    Week,
}

/// `follow` text that is not one of the presets.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("follow: expected 1h, 6h, 1d or 7d")]
pub struct InvalidFollowSpan;

impl FollowSpan {
    pub const ALL: [Self; 4] = [Self::Hour, Self::SixHours, Self::Day, Self::Week];

    /// What `/` and `/topology` follow when the URL names no window.
    pub const DEFAULT: Self = Self::Day;

    /// Reads the URL text of a preset.
    pub fn parse(text: &str) -> Result<Self, InvalidFollowSpan> {
        Self::ALL
            .into_iter()
            .find(|span| span.as_str() == text)
            .ok_or(InvalidFollowSpan)
    }

    /// The URL text: `1h`, `6h`, `1d` or `7d`.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Hour => "1h",
            Self::SixHours => "6h",
            Self::Day => "1d",
            Self::Week => "7d",
        }
    }

    /// The span for people: `1 h`, `6 h`, `1 d` or `7 d`.
    pub const fn label(self) -> &'static str {
        match self {
            Self::Hour => "1 h",
            Self::SixHours => "6 h",
            Self::Day => "1 d",
            Self::Week => "7 d",
        }
    }

    pub const fn micros(self) -> u64 {
        match self {
            Self::Hour => HOUR,
            Self::SixHours => 6 * HOUR,
            Self::Day => 24 * HOUR,
            Self::Week => 7 * 24 * HOUR,
        }
    }

    /// The followed window at `now`: `[align_up(now) − span, align_up(now))`,
    /// its start aligned down when the bucket width does not divide the span.
    /// Empty only when `now` is the epoch.
    pub fn window(self, now: Timestamp, bucket: BucketWidth) -> Result<TimeWindow, EmptyWindow> {
        let end = align_up(now, bucket);
        let start = align_down(
            Timestamp::from_micros(end.as_micros().saturating_sub(self.micros())),
            bucket,
        );
        TimeWindow::new(start, end)
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU64;

    use super::*;

    const MINUTE: u64 = 60_000_000;

    fn five_minutes() -> BucketWidth {
        BucketWidth::from_micros(NonZeroU64::new(5 * MINUTE).expect("five minutes"))
    }

    fn ts(text: &str) -> Timestamp {
        let at: jiff::Timestamp = text.parse().expect("time");
        Timestamp::from_micros(u64::try_from(at.as_microsecond()).expect("after the epoch"))
    }

    #[test]
    fn presets_parse_and_print_their_own_text() {
        for span in FollowSpan::ALL {
            assert_eq!(FollowSpan::parse(span.as_str()), Ok(span));
        }
        assert_eq!(FollowSpan::parse("1d"), Ok(FollowSpan::Day));
        assert_eq!(FollowSpan::DEFAULT, FollowSpan::Day);
    }

    #[test]
    fn anything_but_a_preset_is_refused() {
        for text in ["", "24h", "1D", "2d", "15m", "1 d", "d", "-1d", "1d ", "7"] {
            assert_eq!(FollowSpan::parse(text), Err(InvalidFollowSpan), "{text:?}");
        }
    }

    #[test]
    fn the_window_ends_on_the_bucket_after_now() {
        let now = ts("2026-10-03T12:03:10Z");
        let window = FollowSpan::Day.window(now, five_minutes()).expect("window");
        assert_eq!(window.end(), ts("2026-10-03T12:05:00Z"));
        assert_eq!(window.start(), ts("2026-10-02T12:05:00Z"));
        let on_boundary = FollowSpan::Hour
            .window(ts("2026-10-03T12:05:00Z"), five_minutes())
            .expect("window");
        assert_eq!(on_boundary.start(), ts("2026-10-03T11:05:00Z"));
        assert_eq!(on_boundary.end(), ts("2026-10-03T12:05:00Z"));
    }

    #[test]
    fn a_later_now_slides_the_window_by_whole_buckets() {
        let first = FollowSpan::SixHours
            .window(ts("2026-10-03T12:03:00Z"), five_minutes())
            .expect("window");
        let same = FollowSpan::SixHours
            .window(ts("2026-10-03T12:04:59Z"), five_minutes())
            .expect("window");
        let next = FollowSpan::SixHours
            .window(ts("2026-10-03T12:05:01Z"), five_minutes())
            .expect("window");
        assert_eq!(first, same);
        assert_eq!(
            next.start().as_micros() - first.start().as_micros(),
            5 * MINUTE
        );
        assert_eq!(next.end().as_micros() - first.end().as_micros(), 5 * MINUTE);
    }

    #[test]
    fn a_bucket_wider_than_the_span_covers_one_bucket() {
        let day = BucketWidth::from_micros(NonZeroU64::new(24 * HOUR).expect("a day"));
        let window = FollowSpan::Hour
            .window(ts("2026-10-03T12:00:00Z"), day)
            .expect("window");
        assert_eq!(window.start(), ts("2026-10-03T00:00:00Z"));
        assert_eq!(window.end(), ts("2026-10-04T00:00:00Z"));
    }

    #[test]
    fn only_the_epoch_leaves_no_window() {
        assert!(
            FollowSpan::Day
                .window(Timestamp::from_micros(0), five_minutes())
                .is_err()
        );
    }
}
