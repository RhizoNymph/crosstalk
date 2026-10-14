//! A [`TimeWindow`] as bounds on a time column.
//!
//! Every layer stores a time as Unix microseconds in a `bigint` column, so a
//! stored time lies in `0..=i64::MAX`: a later time does not encode, and
//! storing one is an error. A [`TimeWindow`] spans all of `u64`, and a read
//! takes any window, so [`ColumnWindow::of`] maps the half-open window
//! `[start, end)` onto the column without refusing it:
//!
//! | Window | Column bounds |
//! | --- | --- |
//! | `start` and `end` at most `i64::MAX` | both, as given |
//! | `end` past `i64::MAX` | `start` and no upper bound: every stored time from `start` on, one at exactly `i64::MAX` included |
//! | `start` past `i64::MAX` | [`ColumnWindow::Beyond`]: no storable time; the read is empty (zero counts), not an error |
//!
//! Clamping `end` to `i64::MAX` instead would drop a row stored at exactly
//! `i64::MAX`. A read binds [`ColumnBounds::start`] and
//! [`ColumnBounds::end`] (`NULL` when unbounded) and tests
//! `t >= $start AND ($end::bigint IS NULL OR t < $end)`.

use crosstalk_spec::support::TimeWindow;

/// A [`TimeWindow`] over a `bigint` time column ([`ColumnWindow::of`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColumnWindow {
    /// The window starts past `i64::MAX`: no stored time lies in it.
    Beyond,
    /// The stored times the window holds.
    Within(ColumnBounds),
}

/// The stored times `t` with `start <= t`, and `t < end` when there is an
/// end. Built only by [`ColumnWindow::of`], so `start < end`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ColumnBounds {
    start: i64,
    end: Option<i64>,
}

impl ColumnWindow {
    /// `window` on the column: its bounds, the end dropped when it lies
    /// past `i64::MAX`, or [`ColumnWindow::Beyond`] when the start does.
    pub fn of(window: TimeWindow) -> Self {
        match i64::try_from(window.start().as_micros()) {
            Err(_) => Self::Beyond,
            Ok(start) => Self::Within(ColumnBounds {
                start,
                end: i64::try_from(window.end().as_micros()).ok(),
            }),
        }
    }

    /// The bounds, or `None` when no stored time lies in the window.
    pub fn bounds(self) -> Option<ColumnBounds> {
        match self {
            Self::Beyond => None,
            Self::Within(bounds) => Some(bounds),
        }
    }

    /// Whether the stored time `stored` lies in the window, as the read's
    /// SQL tests it.
    pub fn contains(self, stored: i64) -> bool {
        self.bounds().is_some_and(|bounds| bounds.contains(stored))
    }
}

impl ColumnBounds {
    /// The first stored time in the window.
    pub fn start(self) -> i64 {
        self.start
    }

    /// The first stored time after the window, or `None` when every stored
    /// time from [`ColumnBounds::start`] on lies in it.
    pub fn end(self) -> Option<i64> {
        self.end
    }

    /// Whether the stored time `stored` lies in the bounds.
    pub fn contains(self, stored: i64) -> bool {
        self.start <= stored && self.end.is_none_or(|end| stored < end)
    }
}

#[cfg(test)]
mod tests {
    use crosstalk_spec::support::Timestamp;

    use super::*;

    const LAST: u64 = i64::MAX as u64;

    fn window(start: u64, end: u64) -> TimeWindow {
        TimeWindow::new(Timestamp::from_micros(start), Timestamp::from_micros(end))
            .unwrap_or_else(|error| panic!("{start}..{end}: {error:?}"))
    }

    fn within(start: i64, end: Option<i64>) -> ColumnWindow {
        ColumnWindow::Within(ColumnBounds { start, end })
    }

    #[test]
    fn a_window_inside_the_column_keeps_both_bounds() {
        assert_eq!(ColumnWindow::of(window(10, 20)), within(10, Some(20)));
        assert_eq!(ColumnWindow::of(window(0, 1)), within(0, Some(1)));
        assert_eq!(ColumnWindow::of(window(0, LAST)), within(0, Some(i64::MAX)));
        assert_eq!(
            ColumnWindow::of(window(LAST - 1, LAST)),
            within(i64::MAX - 1, Some(i64::MAX))
        );
    }

    #[test]
    fn an_end_past_the_column_is_no_upper_bound() {
        assert_eq!(ColumnWindow::of(window(0, u64::MAX)), within(0, None));
        assert_eq!(ColumnWindow::of(window(5, LAST + 1)), within(5, None));
        assert_eq!(
            ColumnWindow::of(window(LAST, u64::MAX)),
            within(i64::MAX, None)
        );
    }

    #[test]
    fn a_start_past_the_column_is_beyond_it() {
        assert_eq!(
            ColumnWindow::of(window(LAST + 1, u64::MAX)),
            ColumnWindow::Beyond
        );
        assert_eq!(
            ColumnWindow::of(window(u64::MAX - 1, u64::MAX)),
            ColumnWindow::Beyond
        );
        assert_eq!(ColumnWindow::Beyond.bounds(), None);
        assert!(!ColumnWindow::Beyond.contains(i64::MAX));
        assert!(!ColumnWindow::Beyond.contains(0));
    }

    #[test]
    fn an_unbounded_end_keeps_a_time_stored_at_the_column_maximum() {
        let unbounded = ColumnWindow::of(window(0, u64::MAX));
        assert!(unbounded.contains(i64::MAX));
        assert!(unbounded.contains(0));
        assert!(ColumnWindow::of(window(LAST, u64::MAX)).contains(i64::MAX));
        // Clamping the end to `i64::MAX` would have dropped it.
        assert!(!ColumnWindow::of(window(0, LAST)).contains(i64::MAX));
    }

    /// The column bounds keep exactly the stored times the window contains.
    #[test]
    fn the_bounds_keep_what_the_window_contains() {
        let edges = [
            0,
            1,
            2,
            1_000,
            LAST - 2,
            LAST - 1,
            LAST,
            LAST + 1,
            LAST + 2,
            u64::MAX - 1,
            u64::MAX,
        ];
        let stored = [
            0,
            1,
            2,
            999,
            1_000,
            1_001,
            i64::MAX - 2,
            i64::MAX - 1,
            i64::MAX,
        ];
        for start in edges {
            for end in edges.into_iter().filter(|end| *end > start) {
                let window = window(start, end);
                let column = ColumnWindow::of(window);
                for at in stored {
                    let time = Timestamp::from_micros(at.unsigned_abs());
                    assert_eq!(
                        column.contains(at),
                        window.contains(time),
                        "{start}..{end} at {at}"
                    );
                }
            }
        }
    }
}
