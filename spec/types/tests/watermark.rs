use std::num::NonZeroU64;
use std::time::Duration;

use crate::aggregates::series::BucketWidth;
use crate::aggregates::watermark::{PipelineFrontier, Watermark, Watermarked};
use crate::derived::flow::timing::{CorrelationTiming, InvalidTiming};
use crate::events::Subject;
use crate::events::insight::InsightEvent;
use crate::support::{TimeWindow, Timestamp};
use crate::tests::fixtures::at;

fn micros(n: u64) -> Duration {
    Duration::from_micros(n)
}

/// Correlation window 1000, evidence window 100, suspected TTL 200 (µs).
fn timing() -> CorrelationTiming {
    CorrelationTiming::new(micros(1000), micros(100), micros(200)).expect("non-zero")
}

/// 50-microsecond buckets.
fn width() -> BucketWidth {
    BucketWidth::from_micros(NonZeroU64::new(50).expect("non-zero"))
}

fn frontier(ticked_through: u64, oldest_pending: Option<u64>) -> PipelineFrontier {
    PipelineFrontier {
        ticked_through: at(ticked_through),
        oldest_pending: oldest_pending.map(at),
    }
}

fn settled(ticked_through: u64, oldest_pending: Option<u64>) -> Watermark {
    Watermark::settled(frontier(ticked_through, oldest_pending), timing(), width())
}

fn window(start: u64, end: u64) -> TimeWindow {
    TimeWindow::new(at(start), at(end)).expect("start < end")
}

// ── CorrelationTiming ───────────────────────────────────────────────────────

#[test]
fn timing_rejects_zero_durations() {
    assert_eq!(
        CorrelationTiming::new(Duration::ZERO, micros(1), micros(1)),
        Err(InvalidTiming::ZeroCorrelationWindow)
    );
    assert_eq!(
        CorrelationTiming::new(micros(1), Duration::ZERO, micros(1)),
        Err(InvalidTiming::ZeroEvidenceWindow)
    );
    assert_eq!(
        CorrelationTiming::new(micros(1), micros(1), Duration::ZERO),
        Err(InvalidTiming::ZeroSuspectedTtl)
    );
}

#[test]
fn timing_places_the_lifecycle() {
    let timing = timing();
    assert_eq!(timing.correlation_window(), micros(1000));
    assert_eq!(timing.window_closes_at(at(1_000)), at(1_100));
    assert_eq!(timing.expires_at(at(1_100)), at(1_300));
    assert_eq!(timing.settle_after(), micros(300));
    assert_eq!(
        timing.expires_at(timing.window_closes_at(at(1_000))),
        at(1_000 + timing.settle_after().as_micros() as u64)
    );
}

#[test]
fn timing_saturates_at_the_largest_timestamp() {
    let timing = CorrelationTiming::new(micros(1), Duration::MAX, Duration::MAX).expect("non-zero");
    assert_eq!(timing.settle_after(), Duration::MAX);
    assert_eq!(
        timing.window_closes_at(at(5)),
        Timestamp::from_micros(u64::MAX)
    );
}

// ── Watermark::settled ──────────────────────────────────────────────────────

#[test]
fn caught_up_watermark_is_settle_after_behind_the_tick() {
    // 1000 − 300 = 700, a bucket boundary.
    assert_eq!(settled(1_000, None), Watermark(at(700)));
    // 1020 − 300 = 720, rounded down to 700.
    assert_eq!(settled(1_020, None), Watermark(at(700)));
    assert_eq!(settled(1_050, None), Watermark(at(750)));
}

#[test]
fn pending_input_holds_the_watermark_back() {
    assert_eq!(settled(1_000, Some(650)), Watermark(at(650)));
    assert_eq!(settled(1_000, Some(640)), Watermark(at(600)));
    // Pending input later than the correlator's bound does not matter.
    assert_eq!(settled(1_000, Some(900)), Watermark(at(700)));
}

#[test]
fn watermark_before_settle_after_is_the_epoch() {
    assert_eq!(settled(250, None), Watermark(at(0)));
    assert_eq!(settled(0, Some(0)), Watermark(at(0)));
}

#[test]
fn settled_watermark_is_always_a_bucket_boundary() {
    for tick in (0..2_000).step_by(7) {
        for pending in [None, Some(tick / 3), Some(tick + 11)] {
            let watermark = settled(tick, pending);
            assert!(width().is_boundary(watermark.at()));
            assert!(watermark.at() <= at(tick.saturating_sub(300)));
            if let Some(pending) = pending {
                assert!(watermark.at() <= at(pending));
            }
        }
    }
}

#[test]
fn settled_watermark_never_falls_as_the_frontier_moves_on() {
    let mut previous = settled(0, None);
    for tick in (0..3_000).step_by(13) {
        let next = settled(tick, Some(tick / 2 + 400));
        assert!(next >= previous);
        previous = next;
    }
}

// ── finalizes and advance ───────────────────────────────────────────────────

#[test]
fn watermark_finalizes_buckets_ending_at_or_before_it() {
    let watermark = Watermark(at(700));
    assert!(watermark.finalizes(window(650, 700)));
    assert!(watermark.finalizes(window(0, 50)));
    assert!(!watermark.finalizes(window(700, 750)));
    assert!(!watermark.finalizes(window(650, 750)));
}

#[test]
fn advance_only_moves_forward() {
    let exposed = Watermark(at(700));
    assert_eq!(
        exposed.advance(Watermark(at(750))),
        Some(Watermark(at(750)))
    );
    assert_eq!(exposed.advance(Watermark(at(700))), None);
    assert_eq!(exposed.advance(Watermark(at(650))), None);
}

#[test]
fn watermarked_carries_its_value() {
    let response = Watermarked {
        watermark: Watermark(at(700)),
        value: 3_u32,
    };
    assert_eq!(response.value, 3);
    assert_eq!(response.watermark.at(), at(700));
}

#[test]
fn watermark_advanced_has_its_own_subject() {
    let event = InsightEvent::WatermarkAdvanced(Watermark(at(700)));
    assert_eq!(event.subject(), Subject::WatermarkAdvanced);
}
