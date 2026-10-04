//! Deterministic simulation tests of the in-process bus.
//!
//! Every test runs under tokio's paused clock on one thread
//! (`#[tokio::test(start_paused = true)]`): time moves only when every task
//! waits, and the bus task's biased `select!` makes each run a function of
//! its inputs. Faults are injected locally ([`faults`]): consumer crashes,
//! stalls past the ack timeout, nacks, a dead-letter store outage, and a
//! seeded delivery order. The seeded [`scenario`] runs them all at random;
//! its checks name the seed that failed.
//!
//! The functions named by transport invariants' `dst` evidence live here,
//! at `crosstalk_transport::dst::<name>`.

mod dedup;
mod faults;
mod replay;
mod scenario;

use std::collections::HashSet;
use std::num::NonZeroUsize;
use std::time::Duration;

use crosstalk_spec::events::Subject;
use crosstalk_spec::interfaces::l2_transport::{
    BusError, DeadLetter, DeadLetterStore, EventBus, Subscription,
};
use tokio::time::Instant;

use self::faults::{depth, letters_of, nack_until_dead};
use self::scenario::{End, INITIAL_BACKOFF, MAX_ATTEMPTS, MAX_BACKOFF, Outcome, for_seeds};
use crate::testing::{
    changed, config, default_retry, group, next_ok, next_within, non_zero, page, retry, settle,
    watermark,
};
use crate::{BusConfig, DeliveryOrder, MpscBus};

const SECOND: Duration = Duration::from_secs(1);
const LONG: Duration = Duration::from_secs(600);

/// Seeds per scenario check.
const SEEDS: u64 = 48;

// ---------------------------------------------------------------------------
// Delivery: at least once, attempts, holders, redelivery.
// ---------------------------------------------------------------------------

/// `transport.delivery.at-least-once`: under random consumer faults, every
/// envelope reaches every group subscribed before it was published: it is
/// acked there, or dead-lettered for that group.
#[tokio::test(start_paused = true)]
async fn every_published_envelope_reaches_every_group() {
    for_seeds(SEEDS, |outcome| {
        let by_envelope = outcome.by_envelope();
        for (group, event) in outcome.owed() {
            let acked = by_envelope
                .get(&(group, event))
                .is_some_and(|records| records.iter().any(|r| matches!(r.end, End::Acked(_))));
            let dead = outcome.letter(group, event).is_some();
            if acked == dead {
                return Err(format!(
                    "{group}/{event}: acked {acked}, dead-lettered {dead}"
                ));
            }
        }
        Ok(())
    })
    .await;
}

/// `transport.ack.ends-redelivery`: after an ack returns `Ok`, the group
/// never sees that envelope again.
#[tokio::test(start_paused = true)]
async fn acked_envelope_never_redelivered() {
    for_seeds(SEEDS, |outcome| {
        for ((group, event), records) in outcome.by_envelope() {
            let Some(acked_at) = records.iter().find_map(|r| match r.end {
                End::Acked(at) => Some(at),
                _ => None,
            }) else {
                continue;
            };
            if let Some(later) = records.iter().find(|r| r.received > acked_at) {
                return Err(format!(
                    "{group}/{event} acked at {acked_at:?}, delivered again at {:?}",
                    later.received
                ));
            }
        }
        Ok(())
    })
    .await;
}

/// `transport.delivery.attempt-counts-deliveries`: per group, attempts run
/// 1, 2, 3.. in delivery order, whatever ended each one; a replay starts
/// again at 1.
#[tokio::test(start_paused = true)]
async fn attempt_counts_deliveries_per_group() {
    for_seeds(SEEDS, |outcome| {
        for ((group, event), records) in outcome.by_envelope() {
            let attempts: Vec<u32> = records.iter().map(|r| r.attempt).collect();
            let expected: Vec<u32> = (1..=records.len() as u32).collect();
            if attempts != expected {
                return Err(format!("{group}/{event}: attempts {attempts:?}"));
            }
        }
        Ok(())
    })
    .await;

    // A replay restarts the count, in the replayed group only.
    let bus = MpscBus::start(config()).expect("bus starts");
    let (flow, analysis) = (group("flow"), group("analysis"));
    let mut flow_sub = bus
        .subscribe(&[Subject::Changed], flow.clone(), default_retry())
        .await
        .expect("subscribe");
    let mut analysis_sub = bus
        .subscribe(&[Subject::Changed], analysis.clone(), default_retry())
        .await
        .expect("subscribe");
    bus.publish(changed(1)).await.expect("publish");
    let first = nack_until_dead(&mut flow_sub, 3, |_| "fails".into()).await;
    assert_eq!(
        first.iter().map(|d| d.attempt.get()).collect::<Vec<_>>(),
        vec![1, 2, 3]
    );
    let other = next_ok(&mut analysis_sub).await;
    assert_eq!(other.attempt.get(), 1, "attempts are counted per group");
    bus.dead_letters()
        .replay(&flow, changed(1).id)
        .await
        .expect("replays");
    let again = nack_until_dead(&mut flow_sub, 2, |_| "fails".into()).await;
    assert_eq!(
        again.iter().map(|d| d.attempt.get()).collect::<Vec<_>>(),
        vec![1, 2]
    );
}

/// `transport.delivery.single-holder`: one envelope's deliveries in one
/// group never overlap: each starts after the previous holder acked,
/// nacked, crashed or timed out.
#[tokio::test(start_paused = true)]
async fn one_holder_per_envelope_per_group() {
    for_seeds(SEEDS, |outcome| {
        for ((group, event), records) in outcome.by_envelope() {
            for pair in records.windows(2) {
                let end = outcome.hold_end(pair[0]);
                if pair[1].received < end {
                    return Err(format!(
                        "{group}/{event}: delivery {} at {:?} while {} was held until {end:?}",
                        pair[1].delivery.0, pair[1].received, pair[0].delivery.0
                    ));
                }
            }
        }
        Ok(())
    })
    .await;
}

/// `transport.delivery.redelivered-until-acked`: a delivery that times out,
/// is nacked, or whose holder crashes comes back until the budget is spent,
/// and then not again.
#[tokio::test(start_paused = true)]
async fn unacked_delivery_is_redelivered_until_budget() {
    for_seeds(SEEDS, |outcome| {
        for ((group, event), records) in outcome.by_envelope() {
            let acked = records.iter().any(|r| matches!(r.end, End::Acked(_)));
            if !acked && records.len() != MAX_ATTEMPTS as usize {
                return Err(format!(
                    "{group}/{event}: never acked, {} deliveries",
                    records.len()
                ));
            }
        }
        Ok(())
    })
    .await;

    // One envelope, one failure of each kind.
    let config = BusConfig {
        ack_timeout: non_zero(SECOND),
        ..config()
    };
    let bus = MpscBus::start(config).expect("bus starts");
    let flow = group("flow");
    let mut sub = bus
        .subscribe(&[Subject::Changed], flow.clone(), default_retry())
        .await
        .expect("subscribe");
    bus.publish(changed(1)).await.expect("publish");
    let first = next_ok(&mut sub).await;
    assert_eq!(first.attempt.get(), 1);
    // Times out.
    let second = next_ok(&mut sub).await;
    assert_eq!(second.attempt.get(), 2);
    sub.nack(second.id, Duration::ZERO, "nack".into())
        .await
        .expect("nack");
    let third = next_ok(&mut sub).await;
    assert_eq!(third.attempt.get(), 3);
    // The holder crashes on the last attempt.
    drop(sub);
    let mut sub = bus
        .subscribe(&[Subject::Changed], flow.clone(), default_retry())
        .await
        .expect("subscribe");
    assert!(next_within(&mut sub, LONG).await.is_none(), "budget spent");
    let letters = letters_of(
        bus.dead_letters()
            .list(None, &page(10))
            .await
            .expect("lists"),
    );
    assert_eq!(letters.len(), 1);
    assert_eq!(letters[0].attempts.get(), 3);
}

// ---------------------------------------------------------------------------
// Ack, nack and backoff timing.
// ---------------------------------------------------------------------------

/// `transport.ack.unknown-delivery`, the expired case: after the ack timeout
/// the old holder's ack and nack are unknown, and the new holder keeps the
/// delivery.
#[tokio::test(start_paused = true)]
async fn ack_after_ack_timeout_is_unknown() {
    let bus = MpscBus::start(config()).expect("bus starts");
    let flow = group("flow");
    let mut a = bus
        .subscribe(&[Subject::Changed], flow.clone(), default_retry())
        .await
        .expect("subscribe");
    let mut b = bus
        .subscribe(&[Subject::Changed], flow.clone(), default_retry())
        .await
        .expect("subscribe");
    bus.publish(changed(1)).await.expect("publish");
    let stale = next_ok(&mut a).await;
    tokio::time::sleep(SECOND + Duration::from_millis(1)).await;
    let unknown = Err(BusError::UnknownDelivery(stale.id));
    assert_eq!(a.ack(stale.id).await, unknown);
    assert_eq!(a.nack(stale.id, SECOND, "late".into()).await, unknown);

    let fresh = next_ok(&mut b).await;
    assert_eq!((fresh.attempt.get(), &fresh.envelope), (2, &changed(1)));
    assert_eq!(a.ack(stale.id).await, unknown);
    let held = depth(&bus, &flow).await;
    assert_eq!(held.held, 1, "the stale ack changed nothing");
    assert_eq!(b.ack(fresh.id).await, Ok(()));
}

/// `transport.nack.retry-after`: a nack delays the redelivery by its
/// `retry_after`, clamped to the policy's backoff range, so never less than
/// `min(retry_after, max_backoff)`.
#[tokio::test(start_paused = true)]
async fn nack_delays_redelivery() {
    let policy = retry(10, INITIAL_BACKOFF, MAX_BACKOFF);
    let bus = MpscBus::start(config()).expect("bus starts");
    let mut sub = bus
        .subscribe(&[Subject::Changed], group("flow"), policy)
        .await
        .expect("subscribe");
    let delays = [0, 5, 10, 33, 80, 500, 3_600_000].map(Duration::from_millis);
    for (n, retry_after) in delays.into_iter().enumerate() {
        bus.publish(changed(n as u128 + 1)).await.expect("publish");
        let first = next_ok(&mut sub).await;
        let nacked = Instant::now();
        sub.nack(first.id, retry_after, "later".into())
            .await
            .expect("nack");
        let again = next_ok(&mut sub).await;
        let gap = nacked.elapsed();
        assert_eq!(again.envelope, first.envelope);
        assert!(
            gap >= retry_after.min(MAX_BACKOFF),
            "{retry_after:?}: {gap:?}"
        );
        assert_eq!(gap, retry_after.clamp(INITIAL_BACKOFF, MAX_BACKOFF));
        sub.ack(again.id).await.expect("ack");
    }
}

/// `transport.retry.backoff-floor`: no redelivery comes sooner than the
/// initial backoff after a nack (even with no delay asked), a timeout or a
/// crash.
#[tokio::test(start_paused = true)]
async fn redelivery_waits_at_least_initial_backoff() {
    for_seeds(SEEDS, |outcome: &Outcome| {
        for ((group, event), records) in outcome.by_envelope() {
            for pair in records.windows(2) {
                let earliest = outcome.hold_end(pair[0]) + INITIAL_BACKOFF;
                if pair[1].received < earliest {
                    return Err(format!(
                        "{group}/{event}: redelivered at {:?}, before {earliest:?}",
                        pair[1].received
                    ));
                }
            }
        }
        Ok(())
    })
    .await;

    let config = BusConfig {
        ack_timeout: non_zero(SECOND),
        ..config()
    };
    let policy = retry(10, INITIAL_BACKOFF, MAX_BACKOFF);
    let bus = MpscBus::start(config).expect("bus starts");
    let flow = group("flow");
    let mut sub = bus
        .subscribe(&[Subject::Changed], flow.clone(), policy)
        .await
        .expect("subscribe");
    bus.publish(changed(1)).await.expect("publish");

    // A nack asking for no delay.
    let first = next_ok(&mut sub).await;
    let at = Instant::now();
    sub.nack(first.id, Duration::ZERO, "now".into())
        .await
        .expect("nack");
    let _second = next_ok(&mut sub).await;
    assert!(at.elapsed() >= INITIAL_BACKOFF);

    // A timeout: the hold ends at the deadline.
    let expired = Instant::now() + SECOND;
    let third = next_ok(&mut sub).await;
    assert!(Instant::now() >= expired + INITIAL_BACKOFF);

    // A crash.
    drop(sub);
    let crashed = Instant::now();
    let mut sub = bus
        .subscribe(&[Subject::Changed], flow, policy)
        .await
        .expect("subscribe");
    let fourth = next_ok(&mut sub).await;
    assert!(crashed.elapsed() >= INITIAL_BACKOFF);
    assert_eq!(
        (third.attempt.get(), fourth.attempt.get()),
        (3, 4),
        "each failure was one attempt"
    );
}

/// `transport.retry.backoff-ceiling`: with a consumer waiting, a failed
/// delivery comes back no later than the maximum backoff after the
/// failure, however the backoff grows and whatever delay a nack asks for.
#[tokio::test(start_paused = true)]
async fn redelivery_available_within_max_backoff() {
    let max = Duration::from_millis(40);
    let policy = retry(8, INITIAL_BACKOFF, max);
    let config = BusConfig {
        ack_timeout: non_zero(SECOND),
        ..config()
    };
    let bus = MpscBus::start(config).expect("bus starts");
    let flow = group("flow");
    let mut sub = bus
        .subscribe(&[Subject::Changed], flow.clone(), policy)
        .await
        .expect("subscribe");
    bus.publish(changed(1)).await.expect("publish");

    // Timeouts: the backoff doubles from 10 ms and stops at 40 ms.
    let mut gaps = Vec::new();
    let mut delivered = next_ok(&mut sub).await;
    let mut received = Instant::now();
    for _ in 0..5 {
        let next = next_ok(&mut sub).await;
        gaps.push(Instant::now() - (received + SECOND));
        assert_eq!(next.attempt.get(), delivered.attempt.get() + 1);
        delivered = next;
        received = Instant::now();
    }
    assert_eq!(gaps, [10, 20, 40, 40, 40].map(Duration::from_millis));
    assert!(gaps.iter().all(|gap| *gap <= max));

    // A nack asking for an hour.
    let at = Instant::now();
    sub.nack(delivered.id, Duration::from_secs(3600), "much later".into())
        .await
        .expect("nack");
    let after_nack = next_ok(&mut sub).await;
    assert!(at.elapsed() <= max);

    // A crash.
    drop(sub);
    let crashed = Instant::now();
    let mut sub = bus
        .subscribe(&[Subject::Changed], flow, policy)
        .await
        .expect("subscribe");
    let _last = next_ok(&mut sub).await;
    assert!(crashed.elapsed() <= max);
    assert_eq!(after_nack.attempt.get(), 7);
}

// ---------------------------------------------------------------------------
// Backpressure.
// ---------------------------------------------------------------------------

/// `transport.backpressure.bounded-queue` and `publish-waits`: a group never
/// holds more than its capacity; a publish into a full group waits (it
/// neither drops nor errors) and completes once there is room.
#[tokio::test(start_paused = true)]
async fn mpsc_queue_never_exceeds_capacity() {
    let capacity = 3;
    let config = BusConfig {
        group_capacity: NonZeroUsize::new(capacity).expect("non-zero"),
        ..config()
    };
    for seed in scenario::seeds(16) {
        let config = BusConfig {
            order: DeliveryOrder::Shuffled { seed },
            ..config.clone()
        };
        let bus = MpscBus::start(config).expect("bus starts");
        let flow = group("flow");
        let mut sub = bus
            .subscribe(
                &[Subject::Changed],
                flow.clone(),
                retry(10, INITIAL_BACKOFF, MAX_BACKOFF),
            )
            .await
            .expect("subscribe");
        let (done, mut published) = tokio::sync::mpsc::unbounded_channel();
        let publisher = bus.clone();
        let task = tokio::spawn(async move {
            for n in 1..=12 {
                let result = publisher.publish(changed(n)).await;
                let _ = done.send((n, result));
            }
        });

        let held = depth(&bus, &flow).await;
        assert_eq!(held.tracked(), capacity, "seed {seed}");
        assert_eq!(held.waiting, 1, "seed {seed}: the fourth publish waits");
        let mut finished = Vec::new();
        while let Ok(item) = published.try_recv() {
            finished.push(item);
        }
        assert_eq!(finished.len(), capacity, "seed {seed}");

        let mut rng = crate::rng::SplitMix64::new(seed);
        let mut acked = HashSet::new();
        while acked.len() < 12 {
            let delivery = next_ok(&mut sub).await;
            if rng.below(3) == 0 {
                sub.nack(delivery.id, Duration::ZERO, "again".into())
                    .await
                    .expect("nack");
            } else {
                sub.ack(delivery.id).await.expect("ack");
                acked.insert(delivery.envelope.id);
            }
            let now = depth(&bus, &flow).await;
            assert!(now.tracked() <= capacity, "seed {seed}: {now:?}");
        }
        task.await.expect("publisher ends");
        while let Ok(item) = published.try_recv() {
            finished.push(item);
        }
        assert_eq!(finished.len(), 12, "seed {seed}");
        assert!(
            finished.iter().all(|(_, result)| result.is_ok()),
            "seed {seed}"
        );
    }
}

// ---------------------------------------------------------------------------
// Dead letters.
// ---------------------------------------------------------------------------

/// `transport.deadletter.last-error-is-nack-reason`: the final nack's reason
/// is the letter's `last_error`; a final timeout or crash records that.
#[tokio::test(start_paused = true)]
async fn dead_letter_last_error_is_final_nack_reason() {
    let bus = MpscBus::start(config()).expect("bus starts");
    let letters = bus.dead_letters();
    let flow = group("flow");
    let mut sub = bus
        .subscribe(&[Subject::Changed], flow.clone(), default_retry())
        .await
        .expect("subscribe");

    bus.publish(changed(1)).await.expect("publish");
    nack_until_dead(&mut sub, 3, |attempt| format!("reason {attempt}")).await;

    bus.publish(changed(2)).await.expect("publish");
    nack_until_dead(&mut sub, 2, |attempt| format!("reason {attempt}")).await;
    let _times_out = next_ok(&mut sub).await;
    tokio::time::sleep(2 * SECOND).await;

    bus.publish(changed(3)).await.expect("publish");
    nack_until_dead(&mut sub, 2, |attempt| format!("reason {attempt}")).await;
    let _crashes = next_ok(&mut sub).await;
    drop(sub);
    settle().await;

    let stored = letters_of(letters.list(Some(&flow), &page(10)).await.expect("lists"));
    let errors: Vec<(u128, String)> = stored
        .iter()
        .map(|l| (l.envelope.id.as_ulid(), l.last_error.clone()))
        .collect();
    assert_eq!(errors.len(), 3);
    assert_eq!(errors[2], (1, "reason 3".to_owned()));
    assert!(errors[1].1.contains("ack timeout"), "{errors:?}");
    assert!(errors[0].1.contains("dropped"), "{errors:?}");
}

/// `transport.deadletter.record-contents`: the letter names the exhausted
/// group, carries the envelope as published, and records the policy's
/// `max_attempts`, for every budget.
#[tokio::test(start_paused = true)]
async fn dead_letter_records_group_envelope_and_attempts() {
    for_seeds(SEEDS, |outcome| {
        for letter in &outcome.letters {
            let event = letter.envelope.id.as_ulid();
            if outcome.published(event) != Some(&letter.envelope) {
                return Err(format!("letter for {event} carries another envelope"));
            }
            if letter.attempts.get() != MAX_ATTEMPTS {
                return Err(format!("letter for {event}: {} attempts", letter.attempts));
            }
            let owed = outcome
                .owed()
                .iter()
                .any(|(group, owed)| *group == letter.group.0 && *owed == event);
            if !owed {
                return Err(format!("letter for {event} names group {:?}", letter.group));
            }
        }
        Ok(())
    })
    .await;

    let bus = MpscBus::start(config()).expect("bus starts");
    for budget in 1..=4u32 {
        let name = group(&format!("budget-{budget}"));
        let mut sub = bus
            .subscribe(
                &[Subject::WatermarkAdvanced],
                name.clone(),
                retry(budget, INITIAL_BACKOFF, MAX_BACKOFF),
            )
            .await
            .expect("subscribe");
        let envelope = watermark(u128::from(budget));
        bus.publish(envelope.clone()).await.expect("publish");
        nack_until_dead(&mut sub, budget, |_| "no".into()).await;
        let stored = letters_of(
            bus.dead_letters()
                .list(Some(&name), &page(10))
                .await
                .expect("lists"),
        );
        assert_eq!(stored.len(), 1);
        assert_eq!(stored[0].group, name);
        assert_eq!(stored[0].envelope, envelope);
        assert_eq!(stored[0].attempts.get(), budget);
    }
}

/// `transport.deadletter.not-redelivered`: once dead-lettered for a group,
/// the envelope does not come back to it; other groups keep their own
/// delivery.
#[tokio::test(start_paused = true)]
async fn dead_lettered_envelope_not_redelivered() {
    for_seeds(SEEDS, |outcome| {
        for letter in &outcome.letters {
            let key = (letter.group.0.as_str(), letter.envelope.id.as_ulid());
            let deliveries = outcome
                .records
                .iter()
                .filter(|r| (r.group, r.event) == key)
                .count();
            if deliveries != MAX_ATTEMPTS as usize {
                return Err(format!(
                    "{key:?}: {deliveries} deliveries for a dead letter"
                ));
            }
        }
        Ok(())
    })
    .await;

    let bus = MpscBus::start(config()).expect("bus starts");
    let (flow, analysis) = (group("flow"), group("analysis"));
    let mut flow_sub = bus
        .subscribe(&[Subject::Changed], flow.clone(), default_retry())
        .await
        .expect("subscribe");
    let mut analysis_sub = bus
        .subscribe(&[Subject::Changed], analysis.clone(), default_retry())
        .await
        .expect("subscribe");
    bus.publish(changed(1)).await.expect("publish");
    nack_until_dead(&mut flow_sub, 3, |_| "poison".into()).await;
    assert!(next_within(&mut flow_sub, LONG).await.is_none());
    let other = next_ok(&mut analysis_sub).await;
    assert_eq!(other.envelope, changed(1));
    analysis_sub.ack(other.id).await.expect("ack");
    assert!(next_within(&mut analysis_sub, LONG).await.is_none());
}

/// `transport.deadletter.replay-consumes`: a replay that returns `Ok`
/// removes the letter; if the envelope fails again, it is a new letter.
#[tokio::test(start_paused = true)]
async fn replay_removes_dead_letter() {
    let bus = MpscBus::start(config()).expect("bus starts");
    let store = bus.dead_letters();
    let flow = group("flow");
    let mut sub = bus
        .subscribe(&[Subject::Changed], flow.clone(), default_retry())
        .await
        .expect("subscribe");
    bus.publish(changed(1)).await.expect("publish");
    nack_until_dead(&mut sub, 3, |_| "first life".into()).await;
    assert_eq!(
        letters_of(store.list(None, &page(10)).await.expect("lists")).len(),
        1
    );

    store.replay(&flow, changed(1).id).await.expect("replays");
    assert!(letters_of(store.list(None, &page(10)).await.expect("lists")).is_empty());
    assert_eq!(
        store.replay(&flow, changed(1).id).await,
        Err(BusError::UnknownDeadLetter {
            group: flow.clone(),
            id: changed(1).id
        })
    );

    nack_until_dead(&mut sub, 3, |_| "second life".into()).await;
    let stored = letters_of(store.list(None, &page(10)).await.expect("lists"));
    assert_eq!(stored.len(), 1);
    assert_eq!(stored[0].last_error, "second life");
}

/// `transport.deadletter.replay-redelivers`: a replay delivers the envelope
/// to its group and to no other.
#[tokio::test(start_paused = true)]
async fn replay_redelivers_to_one_group() {
    let bus = MpscBus::start(config()).expect("bus starts");
    let store = bus.dead_letters();
    let (flow, analysis) = (group("flow"), group("analysis"));
    let mut flow_sub = bus
        .subscribe(&[Subject::Changed], flow.clone(), default_retry())
        .await
        .expect("subscribe");
    let mut analysis_sub = bus
        .subscribe(&[Subject::Changed], analysis.clone(), default_retry())
        .await
        .expect("subscribe");
    bus.publish(changed(1)).await.expect("publish");
    nack_until_dead(&mut flow_sub, 3, |_| "flow fails".into()).await;
    nack_until_dead(&mut analysis_sub, 3, |_| "analysis fails".into()).await;
    assert_eq!(
        letters_of(store.list(None, &page(10)).await.expect("lists")).len(),
        2
    );

    store.replay(&flow, changed(1).id).await.expect("replays");
    let replayed = next_ok(&mut flow_sub).await;
    assert_eq!((replayed.attempt.get(), replayed.envelope), (1, changed(1)));
    flow_sub.ack(replayed.id).await.expect("ack");
    assert!(next_within(&mut analysis_sub, LONG).await.is_none());
    let left = letters_of(store.list(None, &page(10)).await.expect("lists"));
    assert_eq!(left.len(), 1);
    assert_eq!(left[0].group, analysis);
}

/// `transport.deadletter.replay-unknown`, concurrently: of two replays of
/// one letter, exactly one succeeds and one delivery follows, whether the
/// group has room or both replays wait for it.
#[tokio::test(start_paused = true)]
async fn concurrent_replays_of_one_letter_succeed_once() {
    let letter = |group| DeadLetter {
        group,
        envelope: changed(1),
        attempts: std::num::NonZeroU32::MIN,
        last_error: "x".into(),
    };
    for room in [true, false] {
        let config = BusConfig {
            group_capacity: NonZeroUsize::new(1).expect("non-zero"),
            ..config()
        };
        let bus = MpscBus::start(config).expect("bus starts");
        let store = bus.dead_letters();
        let flow = group("flow");
        let mut sub = bus
            .subscribe(&[Subject::Changed], flow.clone(), default_retry())
            .await
            .expect("subscribe");
        store.put(letter(flow.clone())).await.expect("put");
        let blocker = if room {
            None
        } else {
            bus.publish(changed(2)).await.expect("publish");
            Some(next_ok(&mut sub).await)
        };

        let replays: Vec<_> = (0..2)
            .map(|_| {
                let store = store.clone();
                let flow = flow.clone();
                tokio::spawn(async move { store.replay(&flow, changed(1).id).await })
            })
            .collect();
        if let Some(blocker) = blocker {
            assert_eq!(depth(&bus, &flow).await.waiting, 2, "both replays wait");
            sub.ack(blocker.id).await.expect("ack");
        }
        let mut results = Vec::new();
        for replay in replays {
            results.push(replay.await.expect("replay task"));
        }
        let ok = results.iter().filter(|r| r.is_ok()).count();
        assert_eq!(ok, 1, "room {room}: {results:?}");
        assert!(results.contains(&Err(BusError::UnknownDeadLetter {
            group: flow.clone(),
            id: changed(1).id
        })));
        let replayed = next_ok(&mut sub).await;
        assert_eq!(replayed.envelope, changed(1));
        sub.ack(replayed.id).await.expect("ack");
        assert!(next_within(&mut sub, LONG).await.is_none(), "room {room}");
    }
}

/// `transport.deadletter.stored-before-release`: while the dead-letter
/// store refuses the put, the exhausted delivery stays tracked (holding its
/// place against capacity, never redelivered) and the bus retries; it is
/// released only once the letter is stored.
#[tokio::test(start_paused = true)]
async fn exhausted_delivery_dead_lettered_before_release() {
    let retry_every = Duration::from_millis(50);
    let config = BusConfig {
        group_capacity: NonZeroUsize::new(1).expect("non-zero"),
        dead_letter_retry: non_zero(retry_every),
        ..config()
    };
    let bus = MpscBus::start_with_failing_puts(config, 2).expect("bus starts");
    let store = bus.dead_letters();
    let flow = group("flow");
    let mut sub = bus
        .subscribe(&[Subject::Changed], flow.clone(), default_retry())
        .await
        .expect("subscribe");
    bus.publish(changed(1)).await.expect("publish");
    nack_until_dead(&mut sub, 3, |_| "poison".into()).await;

    // The first put failed: still tracked, nothing stored.
    let now = depth(&bus, &flow).await;
    assert_eq!((now.exhausted, now.tracked()), (1, 1));
    assert!(letters_of(store.list(None, &page(10)).await.expect("lists")).is_empty());
    let publisher = bus.clone();
    let waiting = tokio::spawn(async move { publisher.publish(changed(2)).await });
    assert_eq!(depth(&bus, &flow).await.waiting, 1, "no room yet");

    // The first retry fails too.
    tokio::time::sleep(retry_every + Duration::from_millis(10)).await;
    let now = depth(&bus, &flow).await;
    assert_eq!((now.exhausted, now.waiting), (1, 1));
    assert!(letters_of(store.list(None, &page(10)).await.expect("lists")).is_empty());

    // The second retry stores it; then the entry is released.
    tokio::time::sleep(retry_every).await;
    let stored = letters_of(store.list(None, &page(10)).await.expect("lists"));
    assert_eq!(stored.len(), 1);
    assert_eq!(waiting.await.expect("publisher"), Ok(()));
    let now = depth(&bus, &flow).await;
    assert_eq!((now.exhausted, now.waiting, now.tracked()), (0, 0, 1));
    // The next delivery is the new envelope, not the dead one.
    assert_eq!(next_ok(&mut sub).await.envelope, changed(2));
}

// ---------------------------------------------------------------------------
// Order.
// ---------------------------------------------------------------------------

/// `transport.ordering.unconstrained`: with a seeded shuffled order, the bus
/// delivers every pair of pending envelopes, of one subject or of two, in
/// both orders across seeds.
#[tokio::test(start_paused = true)]
async fn sim_bus_reaches_every_pairwise_order() {
    let envelopes = [changed(1), watermark(2), changed(3), watermark(4)];
    let mut seen = HashSet::new();
    for seed in 0..64 {
        let config = BusConfig {
            order: DeliveryOrder::Shuffled { seed },
            ..config()
        };
        let bus = MpscBus::start(config).expect("bus starts");
        let mut sub = bus
            .subscribe(
                &[Subject::Changed, Subject::WatermarkAdvanced],
                group("flow"),
                default_retry(),
            )
            .await
            .expect("subscribe");
        for envelope in &envelopes {
            bus.publish(envelope.clone()).await.expect("publish");
        }
        let mut order = Vec::new();
        for _ in 0..envelopes.len() {
            let delivery = next_ok(&mut sub).await;
            order.push(delivery.envelope.id.as_ulid());
            sub.ack(delivery.id).await.expect("ack");
        }
        for (i, first) in order.iter().enumerate() {
            for second in &order[i + 1..] {
                seen.insert((*first, *second));
            }
        }
    }
    for a in 1..=4u128 {
        for b in 1..=4u128 {
            if a != b {
                assert!(seen.contains(&(a, b)), "{a} never came before {b}");
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Dedup (bodies in `dedup`).
// ---------------------------------------------------------------------------

/// `transport.dedup.at-most-once`: after the wrapper acked an envelope that
/// consumer logic handled, consumer logic of the group never sees its id
/// again: not from a republish, a lost ack, or another consumer.
#[tokio::test(start_paused = true)]
async fn dedup_hands_each_id_once_per_group() {
    dedup::dedup_hands_each_id_once_per_group().await;
}

/// `transport.dedup.duplicate-acked`: a withheld duplicate is acked, so it
/// neither comes back nor ends as a false dead letter.
#[tokio::test(start_paused = true)]
async fn dedup_acks_suppressed_duplicates() {
    dedup::dedup_acks_suppressed_duplicates().await;
}

/// `transport.dedup.suppress-only-handled`: the wrapper withholds an id only
/// after consumer logic of the same group handled it: a failed handling, a
/// duplicate of an unhandled id, and another group's handling withhold
/// nothing.
#[tokio::test(start_paused = true)]
async fn dedup_never_suppresses_unhandled_id() {
    dedup::dedup_never_suppresses_unhandled_id().await;
}
