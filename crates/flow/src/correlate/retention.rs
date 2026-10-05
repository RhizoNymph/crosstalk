//! How long a write can still be confirmed by content
//! (`flow.correlator.content-confirms-past-window`).
//!
//! The correlation window bounds access-only pairing: a write and a read
//! with no content between them are a co-access only within it. A read
//! whose tool result holds a span the writer's write carried is evidence
//! on its own, whatever the lag: a dead-drop wiki is read hours or days
//! after it was written. That pairing is bounded by L4's span index
//! instead: a span still indexed can still be matched, so the correlator
//! remembers a write's spans for [`ContentRetention`], by default L4's
//! index retention ([`DEFAULT_CONTENT_RETENTION`]).

use std::time::Duration;

use crosstalk_spec::derived::flow::timing::CorrelationTiming;

/// L4's default span index retention: 30 days.
pub const DEFAULT_CONTENT_RETENTION: Duration = Duration::from_secs(30 * 24 * 60 * 60);

/// The longest write-to-read lag a content-confirmed channel transmission
/// may have: never shorter than the correlation window, so content never
/// bounds pairing tighter than access alone.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ContentRetention(Duration);

/// Why a content retention was refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum InvalidRetention {
    #[error("content retention {retention:?} is shorter than the correlation window {window:?}")]
    ShorterThanWindow {
        retention: Duration,
        window: Duration,
    },
}

impl ContentRetention {
    /// `retention`, checked against `timing`'s correlation window.
    pub fn new(retention: Duration, timing: CorrelationTiming) -> Result<Self, InvalidRetention> {
        let window = timing.correlation_window();
        if retention < window {
            return Err(InvalidRetention::ShorterThanWindow { retention, window });
        }
        Ok(Self(retention))
    }

    /// The default retention for `timing`: L4's index retention, or the
    /// correlation window when that is longer.
    pub fn default_for(timing: CorrelationTiming) -> Self {
        Self(DEFAULT_CONTENT_RETENTION.max(timing.correlation_window()))
    }

    pub fn get(self) -> Duration {
        self.0
    }
}
