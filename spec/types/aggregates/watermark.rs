//! When an edge bucket is final.
//!
//! Buckets are keyed by `Confirmed::at`, the time the reader received the
//! content, not by when the gateway confirmed the transmission. A late
//! content match (a suspected transmission upgraded to confirmed) or a slow
//! consumer therefore adds to a bucket that has already closed. The
//! [`Watermark`] is the time before which that can no longer happen.
//!
//! **Definition.** L7 computes the watermark from a [`PipelineFrontier`]:
//!
//! ```text
//! settled = align_down( min( ticked_through − settle_after, oldest_pending ) )
//! ```
//!
//! - `settle_after` is `evidence_window + suspected_ttl`
//!   ([`CorrelationTiming::settle_after`]). After its tick at `τ` the
//!   correlator holds open no transmission whose time is before
//!   `τ − settle_after`, so any transmission it holds can still be
//!   confirmed only at or after that time.
//! - `oldest_pending` covers what the correlator and later stages have not
//!   processed yet: the earliest event time of an exchange still in flight
//!   at the proxy or of a delivery not yet acked on the way to the edge
//!   store (see `FrontierSource`).
//! - `align_down` rounds down to a bucket boundary, so a bucket is final
//!   exactly when its end is at or before the watermark
//!   ([`Watermark::finalizes`]).
//!
//! In a caught-up pipeline (nothing pending, every shard ticked through
//! now) the watermark is `now − settle_after`, rounded down to a bucket.
//! While a consumer lags or a delivery sits dead-lettered, the watermark
//! waits for it rather than declaring its buckets final.
//!
//! **Advancing.** L7 recomputes the watermark at least once per bucket width
//! and exposes `max(exposed, settled)`. The watermark *advances* when that
//! value strictly increases; each advance is published once as
//! `WatermarkAdvanced`. Values are bucket boundaries, so the watermark moves
//! in whole buckets: in steady state once per bucket width, and after a
//! stall in one jump.
//!
//! **What it promises.** Once a watermark `W` has been exposed, no bucket of
//! a topic-model version that has been activated whose window ends at or
//! before `W` changes again. Query results over such buckets can still
//! change through what is resolved at query time, which the response or the
//! feed reports separately: agent merges and unmerges, operator verdicts, and
//! the activation of another topic-model version.
//!
//! [`CorrelationTiming::settle_after`]: crate::derived::flow::timing::CorrelationTiming::settle_after

use crate::aggregates::series::BucketWidth;
use crate::derived::flow::timing::{CorrelationTiming, sub};
use crate::support::{TimeWindow, Timestamp};

pub use crate::support::Watermark;

/// What L7 knows about the progress of everything upstream of it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PipelineFrontier {
    /// The earliest of the flow correlator shards' last processed ticks.
    pub ticked_through: Timestamp,
    /// The earliest event time of anything not yet processed on the way to
    /// the edge store; `None` when nothing is pending.
    pub oldest_pending: Option<Timestamp>,
}

impl Watermark {
    /// The watermark a frontier settles, by the definition in the module
    /// docs: always a bucket boundary.
    pub fn settled(
        frontier: PipelineFrontier,
        timing: CorrelationTiming,
        width: BucketWidth,
    ) -> Self {
        let closed = sub(frontier.ticked_through, timing.settle_after());
        let earliest = frontier
            .oldest_pending
            .map_or(closed, |pending| pending.min(closed));
        let micros = earliest.as_micros();
        let aligned = micros - micros % width.as_micros();
        Self(Timestamp::from_micros(aligned))
    }

    pub fn at(self) -> Timestamp {
        self.0
    }

    /// Whether every instant of `window` is before the watermark, so the
    /// buckets it covers are final.
    pub fn finalizes(self, window: TimeWindow) -> bool {
        window.end() <= self.0
    }

    /// The exposed watermark after recomputing it as `settled`: `Some` with
    /// the new value when it advances, `None` when it does not. The exposed
    /// watermark never moves back.
    pub fn advance(self, settled: Watermark) -> Option<Watermark> {
        (settled > self).then_some(settled)
    }
}

/// An aggregate response and the watermark in effect when its read began.
/// Every part of `value` before `watermark` is final in the sense of the
/// module docs.
#[derive(Debug, Clone, PartialEq)]
pub struct Watermarked<T> {
    pub watermark: Watermark,
    pub value: T,
}
