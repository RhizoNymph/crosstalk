//! Small, local fault helpers for the simulation tests. Candidates to move
//! to `crosstalk-sim` once it lands (see `docs/features/transport.md`).
//!
//! - [`Behaviour`]: a seeded consumer fault model (ack, ack after the
//!   timeout, nack, stall, crash).
//! - [`nack_until_dead`]: drive one envelope through its whole retry budget.
//! - [`depth`]: a group's depth after letting every ready task run.
//! - The dead-letter store outage is `MpscBus::start_with_failing_puts`,
//!   a test-only hook inside the bus task.

use std::time::Duration;

use crosstalk_spec::interfaces::l2_transport::{ConsumerGroup, DeadLetter, Delivery, Subscription};
use crosstalk_spec::paging::{DeadLetterList, Page};

use crate::rng::SplitMix64;
use crate::testing::{next_ok, settle};
use crate::{GroupDepth, MpscBus};

/// What a simulated consumer does with a delivery.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Behaviour {
    /// Handle briefly, then ack.
    Ack,
    /// Handle past the ack timeout, then ack (refused: the bus took it back).
    SlowAck,
    /// Handle briefly, then nack with a random delay.
    Nack,
    /// Never settle it; the ack timeout takes it back.
    Ignore,
    /// Drop the subscription while holding it, then resubscribe.
    Crash,
}

impl Behaviour {
    pub(super) fn pick(rng: &mut SplitMix64) -> Self {
        match rng.below(100) {
            0..55 => Self::Ack,
            55..65 => Self::SlowAck,
            65..80 => Self::Nack,
            80..90 => Self::Ignore,
            _ => Self::Crash,
        }
    }

    /// How long handling takes: under 20 ms, or past the ack timeout for a
    /// slow ack.
    pub(super) fn handling(self, rng: &mut SplitMix64, ack_timeout: Duration) -> Duration {
        let jitter = Duration::from_millis(rng.below(20) as u64);
        match self {
            Self::SlowAck => ack_timeout + Duration::from_millis(1) + jitter,
            _ => jitter,
        }
    }
}

/// Take `attempts` deliveries from `sub` and nack each with
/// `reason(attempt)`; returns them. The caller sizes `attempts` to the
/// group's budget.
pub(super) async fn nack_until_dead<S: Subscription>(
    sub: &mut S,
    attempts: u32,
    reason: impl Fn(u32) -> String,
) -> Vec<Delivery> {
    let mut taken = Vec::new();
    for _ in 0..attempts {
        let delivery = next_ok(sub).await;
        let attempt = delivery.attempt.get();
        sub.nack(delivery.id, Duration::ZERO, reason(attempt))
            .await
            .expect("nack");
        taken.push(delivery);
    }
    settle().await;
    taken
}

/// The items of a dead-letter page.
pub(super) fn letters_of(page: Page<DeadLetter, DeadLetterList>) -> Vec<DeadLetter> {
    page.into_parts().0
}

/// The group's depth once every ready task has run.
pub(super) async fn depth(bus: &MpscBus, group: &ConsumerGroup) -> GroupDepth {
    settle().await;
    bus.depth(group)
        .await
        .expect("bus is running")
        .expect("group exists")
}
