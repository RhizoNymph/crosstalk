//! Ending a held delivery without an ack: nack, ack timeout, or the drop of
//! the subscription holding it. And the reaper task, which owns every ack
//! deadline of the process.
//!
//! Deadlines live in process memory (tokio `Instant`s), as on `MpscBus`.
//! A subscription tells the reaper about each hold before the hold
//! commits (a provisional deadline, for a `next` dropped mid-commit), again
//! at the handout (the real deadline, handout plus the ack timeout), and
//! about each ack; at a deadline the reaper fails the hold
//! with [`Failure::Timeout`]. Every failure is conditional on the row still
//! being held at that attempt, so a late or repeated one is a no-op.
//! Failing on the last attempt stores the dead letter and deletes the
//! delivery in one transaction (`transport.deadletter.stored-before-release`).

use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use crosstalk_spec::interfaces::l2_transport::RetryPolicy;
use tokio::sync::mpsc;
use tokio::time::Instant;

use super::Shared;
use super::row::{duration_micros, failure, unavailable};

/// One hold: the group's delivery row at the attempt its holder was given.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) struct HoldKey {
    pub(crate) group: String,
    pub(crate) seq: i64,
    pub(crate) attempt: i32,
}

/// How a held delivery ended without an ack.
#[derive(Debug, Clone)]
pub(crate) enum Failure {
    Nack {
        retry_after: Duration,
        reason: String,
    },
    Timeout,
    Dropped,
}

/// What subscriptions tell the reaper.
#[derive(Debug)]
pub(crate) enum ReaperMsg {
    /// A hold is about to commit; take it back at `deadline`.
    Hold {
        key: HoldKey,
        deadline: Instant,
        retry: RetryPolicy,
    },
    /// The hold was acked or nacked; its deadline no longer matters.
    Settled(HoldKey),
    /// The subscription holding these was dropped (a consumer crash).
    Dropped(Vec<(HoldKey, RetryPolicy)>),
}

/// Exponential backoff after a timeout or a dropped holder, as `MpscBus`
/// computes it: the initial backoff doubled per earlier delivery, capped.
pub(crate) fn backoff(retry: &RetryPolicy, deliveries: u32) -> Duration {
    let doublings = deliveries.saturating_sub(1).min(31);
    retry
        .initial_backoff()
        .checked_mul(1u32 << doublings)
        .map_or(retry.max_backoff(), |delay| delay.min(retry.max_backoff()))
}

pub(crate) const RESTART_REASON: &str = "process restarted while holding the delivery";
const DROPPED_REASON: &str = "subscription dropped while holding the delivery";

/// Fail `key`'s hold. `Ok(false)` when the row is no longer held at that
/// attempt (acked, already failed, or never committed).
pub(crate) async fn fail(
    shared: &Shared,
    key: &HoldKey,
    retry: &RetryPolicy,
    failure: Failure,
) -> Result<bool, sqlx::Error> {
    let mut tx = shared.pool.begin().await?;
    let row: Option<(String, i64, String)> = sqlx::query_as(
        "SELECT e.id, e.at, e.envelope FROM transport.deliveries d \
         JOIN transport.events e ON e.seq = d.seq \
         WHERE d.group_name = $1 AND d.seq = $2 AND d.state = 'held' AND d.attempt = $3 \
         FOR UPDATE OF d",
    )
    .bind(&key.group)
    .bind(key.seq)
    .bind(key.attempt)
    .fetch_optional(&mut *tx)
    .await?;
    let Some((event_id, at, envelope)) = row else {
        tx.rollback().await?;
        return Ok(false);
    };
    let deliveries = u32::try_from(key.attempt).unwrap_or(0);
    let ack_timeout = shared.config.ack_timeout.get();
    if deliveries >= retry.max_attempts().get() {
        let last_error = match &failure {
            Failure::Nack { reason, .. } => reason.clone(),
            Failure::Timeout => format!("ack timeout after {} ms", ack_timeout.as_millis()),
            Failure::Dropped => DROPPED_REASON.to_owned(),
        };
        sqlx::query(
            "INSERT INTO transport.dead_letters \
             (group_name, event_id, seq, at, envelope, attempts, last_error) \
             VALUES ($1, $2, $3, $4, $5, $6, $7) \
             ON CONFLICT (group_name, event_id) DO UPDATE SET seq = excluded.seq, \
             at = excluded.at, envelope = excluded.envelope, attempts = excluded.attempts, \
             last_error = excluded.last_error",
        )
        .bind(&key.group)
        .bind(&event_id)
        .bind(key.seq)
        .bind(at)
        .bind(&envelope)
        .bind(key.attempt.max(1))
        .bind(&last_error)
        .execute(&mut *tx)
        .await?;
        sqlx::query("DELETE FROM transport.deliveries WHERE group_name = $1 AND seq = $2")
            .bind(&key.group)
            .bind(key.seq)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        tracing::info!(group = %key.group, event = %event_id, attempts = key.attempt, "dead-lettered");
        return Ok(true);
    }
    let (delay, reason, cause) = match failure {
        Failure::Nack {
            retry_after,
            reason,
        } => (
            retry_after.clamp(retry.initial_backoff(), retry.max_backoff()),
            reason,
            "nack",
        ),
        Failure::Timeout => (
            backoff(retry, deliveries),
            format!("ack timeout after {} ms", ack_timeout.as_millis()),
            "ack timeout",
        ),
        Failure::Dropped => (
            backoff(retry, deliveries),
            DROPPED_REASON.to_owned(),
            "holder dropped",
        ),
    };
    let available_at = shared.now_micros().saturating_add(duration_micros(delay));
    sqlx::query(
        "UPDATE transport.deliveries SET state = 'delayed', available_at = $3, last_error = $4 \
         WHERE group_name = $1 AND seq = $2",
    )
    .bind(&key.group)
    .bind(key.seq)
    .bind(available_at)
    .bind(&reason)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    tracing::debug!(
        group = %key.group,
        seq = key.seq,
        attempt = key.attempt,
        cause,
        delay_ms = u64::try_from(delay.as_millis()).unwrap_or(u64::MAX),
        "redelivery scheduled"
    );
    Ok(true)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Cause {
    Timeout,
    Dropped,
}

type Entry = Reverse<(Instant, HoldKey, Cause, RetryKey)>;

/// A [`RetryPolicy`] in a heap entry: ordered by nothing that matters,
/// only carried.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RetryKey(RetryPolicy);

impl PartialOrd for RetryKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for RetryKey {
    fn cmp(&self, _: &Self) -> std::cmp::Ordering {
        std::cmp::Ordering::Equal
    }
}

async fn sleep_until(deadline: Option<Instant>) {
    match deadline {
        Some(deadline) => tokio::time::sleep_until(deadline).await,
        None => std::future::pending().await,
    }
}

/// The reaper task: runs until the bus is shut down or dropped.
pub(crate) async fn run(shared: Arc<Shared>, mut messages: mpsc::UnboundedReceiver<ReaperMsg>) {
    let mut timers: BinaryHeap<Entry> = BinaryHeap::new();
    let mut settled: HashSet<HoldKey> = HashSet::new();
    loop {
        let deadline = timers.peek().map(|Reverse((at, ..))| *at);
        tokio::select! {
            biased;
            message = messages.recv() => match message {
                None => break,
                Some(ReaperMsg::Hold { key, deadline, retry }) => {
                    timers.push(Reverse((deadline, key, Cause::Timeout, RetryKey(retry))));
                }
                Some(ReaperMsg::Settled(key)) => {
                    settled.insert(key);
                }
                Some(ReaperMsg::Dropped(holds)) => {
                    let now = Instant::now();
                    for (key, retry) in holds {
                        timers.push(Reverse((now, key, Cause::Dropped, RetryKey(retry))));
                    }
                }
            },
            () = sleep_until(deadline) => {
                fire_due(&shared, &mut timers, &mut settled).await;
            }
        }
    }
}

async fn fire_due(shared: &Shared, timers: &mut BinaryHeap<Entry>, settled: &mut HashSet<HoldKey>) {
    let now = Instant::now();
    let mut retry_later = Vec::new();
    while let Some(Reverse((at, ..))) = timers.peek() {
        if *at > now {
            break;
        }
        let Some(Reverse((_, key, cause, RetryKey(retry)))) = timers.pop() else {
            break;
        };
        if settled.remove(&key) {
            continue;
        }
        let how = match cause {
            Cause::Timeout => Failure::Timeout,
            Cause::Dropped => Failure::Dropped,
        };
        match fail(shared, &key, &retry, how).await {
            Ok(true) => {
                tracing::debug!(group = %key.group, seq = key.seq, attempt = key.attempt, cause = ?cause, "hold taken back");
                shared.wake_all();
            }
            Ok(false) => {}
            Err(error) => {
                tracing::warn!(
                    group = %key.group,
                    seq = key.seq,
                    failure = %failure(&error),
                    unavailable = unavailable(&error),
                    "could not take a hold back; retrying"
                );
                retry_later.push(Reverse((
                    now + shared.config.poll.get(),
                    key,
                    cause,
                    RetryKey(retry),
                )));
            }
        }
    }
    timers.extend(retry_later);
}
