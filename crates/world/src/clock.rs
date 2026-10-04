//! The world's time: an anchor every generated instant is an offset from,
//! and the clock a host reads the present from.
//!
//! The world covers [`DAYS`] days of traffic ending at its anchor. Nothing
//! in the world reads a clock while it is generated or seeded: every time
//! is the anchor minus an offset, and every store write is given its time.
//! [`WorldClock`] is the spec `Clock` a host hands the stores' callers
//! afterwards: fixed at the anchor for tests, or moving on from it in real
//! time for a demo server, so actions taken while it runs are stamped after
//! the data.

use std::num::NonZeroU64;
use std::time::Instant;

use crosstalk_spec::aggregates::series::BucketWidth;
use crosstalk_spec::support::{Clock, EmptyWindow, TimeWindow, Timestamp};

use crate::error::WorldError;

pub const SECOND: u64 = 1_000_000;
pub const MINUTE: u64 = 60 * SECOND;
pub const HOUR: u64 = 60 * MINUTE;
pub const DAY: u64 = 24 * HOUR;

/// How many days of traffic the world holds before its anchor.
pub const DAYS: u64 = 7;

/// How long before the first traffic the deployment's config was first
/// applied (`Anchor::config_at`).
pub const CONFIG_LEAD: u64 = 30 * DAY;

/// The width of every aggregate bucket the world is laid out on: the
/// anchor, the start of traffic and every window the world names start and
/// end on its multiples.
pub const BUCKET: BucketWidth = BucketWidth::from_micros(match NonZeroU64::new(5 * MINUTE) {
    Some(width) => width,
    None => NonZeroU64::MIN,
});

/// The UI fixture's anchor, 2026-10-03T00:00:00Z: the default present of
/// the operator UI's synthetic backend.
pub const UI_ANCHOR: Timestamp = Timestamp::from_micros(1_790_985_600_000_000);

/// The earliest instant the world reaches back to from its anchor: config
/// was applied [`CONFIG_LEAD`] before the first traffic.
const REACH: u64 = DAYS * DAY + CONFIG_LEAD;

/// The world's present, on a bucket boundary. Every time the world names is
/// this minus an offset.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Anchor(Timestamp);

impl Anchor {
    /// The anchor for `at`: `at` rounded down to a bucket boundary.
    /// `TooEarly` when the world, which reaches [`CONFIG_LEAD`] plus
    /// [`DAYS`] days back, would start before the epoch.
    pub fn new(at: Timestamp) -> Result<Self, WorldError> {
        let width = BUCKET.as_micros().get();
        let micros = at.as_micros() - at.as_micros() % width;
        if micros < REACH {
            return Err(WorldError::TooEarly { at });
        }
        Ok(Self(Timestamp::from_micros(micros)))
    }

    /// The end of the generated data.
    pub fn now(self) -> Timestamp {
        self.0
    }

    /// The first instant of generated traffic.
    pub fn start(self) -> Timestamp {
        self.ago(DAYS * DAY)
    }

    /// When the deployment's configuration was first applied.
    pub fn config_at(self) -> Timestamp {
        minus(self.start(), CONFIG_LEAD)
    }

    /// The anchor minus `micros`.
    pub fn ago(self, micros: u64) -> Timestamp {
        minus(self.0, micros)
    }

    /// The start of traffic plus `micros`.
    pub fn after_start(self, micros: u64) -> Timestamp {
        plus(self.start(), micros)
    }

    /// A bucket-aligned window holding every generated access and
    /// confirmation, `[start, now + bucket)`: what a read "over all time"
    /// counts in.
    pub fn all_time(self) -> Result<TimeWindow, EmptyWindow> {
        TimeWindow::new(self.start(), plus(self.0, BUCKET.as_micros().get()))
    }
}

/// `at` plus `micros`, saturating.
pub const fn plus(at: Timestamp, micros: u64) -> Timestamp {
    Timestamp::from_micros(at.as_micros().saturating_add(micros))
}

/// `at` minus `micros`, saturating at the epoch.
pub const fn minus(at: Timestamp, micros: u64) -> Timestamp {
    Timestamp::from_micros(at.as_micros().saturating_sub(micros))
}

/// The present a host reads after seeding.
#[derive(Debug, Clone, Copy)]
pub enum WorldClock {
    /// Always the anchor: for tests and anything that must be reproducible.
    Fixed(Anchor),
    /// The anchor plus the real time since `started`: for serving, so what
    /// an operator does is stamped after the generated data.
    Live { anchor: Anchor, started: Instant },
}

impl WorldClock {
    /// A clock moving on from `anchor` from now on.
    pub fn live(anchor: Anchor) -> Self {
        Self::Live {
            anchor,
            started: Instant::now(),
        }
    }
}

impl Clock for WorldClock {
    fn now(&self) -> Timestamp {
        match self {
            Self::Fixed(anchor) => anchor.now(),
            Self::Live { anchor, started } => {
                let elapsed = u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX);
                plus(anchor.now(), elapsed)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_anchor_rounds_down_to_a_bucket() -> Result<(), WorldError> {
        let anchor = Anchor::new(plus(UI_ANCHOR, 4 * MINUTE + 7))?;
        assert_eq!(anchor.now(), UI_ANCHOR);
        assert_eq!(Anchor::new(UI_ANCHOR)?.now(), UI_ANCHOR);
        Ok(())
    }

    #[test]
    fn every_named_instant_sits_on_a_bucket_boundary() -> Result<(), WorldError> {
        let anchor = Anchor::new(UI_ANCHOR)?;
        for at in [anchor.now(), anchor.start(), anchor.config_at()] {
            assert!(BUCKET.is_boundary(at), "{at:?}");
        }
        assert_eq!(
            anchor.now().as_micros() - anchor.start().as_micros(),
            7 * DAY
        );
        Ok(())
    }

    #[test]
    fn an_anchor_too_close_to_the_epoch_is_refused() {
        assert_eq!(
            Anchor::new(Timestamp::from_micros(DAY)),
            Err(WorldError::TooEarly {
                at: Timestamp::from_micros(DAY)
            })
        );
    }

    #[test]
    fn the_ui_anchor_is_2026_10_03() {
        assert_eq!(UI_ANCHOR.as_micros(), 1_790_985_600_000_000);
    }

    #[test]
    fn a_fixed_clock_stays_and_a_live_one_moves_on() -> Result<(), WorldError> {
        let anchor = Anchor::new(UI_ANCHOR)?;
        assert_eq!(WorldClock::Fixed(anchor).now(), UI_ANCHOR);
        let started = Instant::now()
            .checked_sub(std::time::Duration::from_secs(60))
            .ok_or(WorldError::TooEarly { at: UI_ANCHOR })?;
        let live = WorldClock::Live { anchor, started };
        assert!(live.now() >= plus(UI_ANCHOR, MINUTE));
        Ok(())
    }
}
