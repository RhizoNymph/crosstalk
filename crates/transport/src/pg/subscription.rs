//! Joining a group, and [`PgSubscription`]: taking, acking and nacking
//! deliveries.
//!
//! `next` runs one transaction per try:
//!
//! 1. lock the group's row (so a group admits in one place at a time);
//! 2. admit new log entries of the group's subjects, in `seq` order, while
//!    the group tracks fewer than `group_capacity`, and advance
//!    `admitted_through` (to the last admitted entry, or to the log head
//!    the same statement saw when every candidate fit);
//! 3. turn due `delayed` rows `ready` (due by the bus clock);
//! 4. take the lowest-`seq` `ready` row `FOR UPDATE SKIP LOCKED`, set it
//!    `held` and count the attempt.
//!
//! When nothing is ready it waits for a wake-up (a local publish, ack or
//! nack, or a `NOTIFY` from another connection), the next delayed row's due
//! time, or the poll interval, whichever comes first. Database failures in
//! `next` are logged and retried at the poll interval: a consumer's loop
//! only ever sees deliveries, decode errors, or `None` at shutdown.

use std::collections::HashMap;
use std::num::NonZeroU32;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use crosstalk_spec::events::Subject;
use crosstalk_spec::interfaces::l2_transport::{
    BusError, ConsumerGroup, Delivery, DeliveryId, RetryPolicy, Subscription,
};
use tokio::sync::watch;
use tokio::time::Instant;

use super::Shared;
use super::reaper::{Failure, HoldKey, ReaperMsg, fail};
use super::row::{bus_error, decode_envelope, duration_micros, failure, subject_set};

/// Join (or create) `group` with `subjects` and `retry`. A new group starts
/// at the log head: it gets every envelope published after this returns.
pub(crate) async fn subscribe(
    shared: &Arc<Shared>,
    subjects: &[Subject],
    group: ConsumerGroup,
    retry: RetryPolicy,
) -> Result<PgSubscription, BusError> {
    let names = subject_set(subjects);
    let max_attempts =
        i32::try_from(retry.max_attempts().get()).map_err(|_| BusError::PublishRejected {
            reason: "max_attempts exceeds what the bus stores".to_owned(),
        })?;
    let initial = duration_micros(retry.initial_backoff()).max(1);
    let max = duration_micros(retry.max_backoff()).max(initial);
    sqlx::query(
        "INSERT INTO transport.groups \
         (name, subjects, max_attempts, initial_backoff_micros, max_backoff_micros, admitted_through) \
         VALUES ($1, $2, $3, $4, $5, (SELECT coalesce(max(seq), 0) FROM transport.events)) \
         ON CONFLICT (name) DO NOTHING",
    )
    .bind(&group.0)
    .bind(&names)
    .bind(max_attempts)
    .bind(initial)
    .bind(max)
    .execute(&shared.pool)
    .await
    .map_err(|error| bus_error("subscribe", &error))?;
    let (stored_subjects, stored_attempts, stored_initial, stored_max): (
        Vec<String>,
        i32,
        i64,
        i64,
    ) = sqlx::query_as(
        "SELECT subjects, max_attempts, initial_backoff_micros, max_backoff_micros \
             FROM transport.groups WHERE name = $1",
    )
    .bind(&group.0)
    .fetch_one(&shared.pool)
    .await
    .map_err(|error| bus_error("subscribe", &error))?;
    if stored_subjects != names {
        return Err(BusError::GroupSubjectMismatch { group });
    }
    if (stored_attempts, stored_initial, stored_max) != (max_attempts, initial, max) {
        return Err(BusError::GroupRetryMismatch { group });
    }
    tracing::info!(group = %group.0, subjects = names.len(), "subscribed");
    Ok(PgSubscription {
        shared: Arc::clone(shared),
        group,
        retry,
        wake: shared.wake.subscribe(),
        closed: shared.closed.subscribe(),
        held: HashMap::new(),
        abandoned: false,
    })
}

/// A held delivery as its subscription knows it.
#[derive(Debug, Clone, Copy)]
struct Hold {
    seq: i64,
    attempt: i32,
    deadline: Instant,
}

/// One try of `next`'s transaction.
enum Took {
    Row {
        seq: i64,
        attempt: i32,
        subject: String,
        envelope: String,
    },
    /// Nothing ready; the earliest delayed row's due time, if any.
    Empty { next_due: Option<i64> },
}

const ADMIT: &str = "\
WITH head AS (SELECT coalesce(max(seq), 0) AS seq FROM transport.events), \
candidates AS ( \
    SELECT seq, at FROM transport.events \
    WHERE seq > $2 AND routed AND subject = ANY($3) \
    ORDER BY seq LIMIT $4 + 1), \
admitted AS ( \
    INSERT INTO transport.deliveries (group_name, seq, at, state) \
    SELECT $1, seq, at, 'ready' FROM candidates ORDER BY seq LIMIT $4 \
    RETURNING seq) \
SELECT (SELECT seq FROM head), (SELECT count(*) FROM candidates), (SELECT max(seq) FROM admitted)";

const TAKE: &str = "\
UPDATE transport.deliveries AS d SET state = 'held', attempt = d.attempt + 1 \
FROM (SELECT seq FROM transport.deliveries \
      WHERE group_name = $1 AND state = 'ready' \
      ORDER BY seq LIMIT 1 FOR UPDATE SKIP LOCKED) AS pick, \
     transport.events AS e \
WHERE d.group_name = $1 AND d.seq = pick.seq AND e.seq = d.seq \
RETURNING d.seq, d.attempt, e.subject, e.envelope";

/// A subscription to a [`PgBus`](super::PgBus) consumer group.
///
/// Dropping it is a consumer crash: the deliveries it holds are taken back
/// at once and redelivered after their backoff, or dead-lettered on their
/// last attempt. A process that stops while holding deliveries leaves them
/// `held` in the database; [`PgBus::recover_held`](super::PgBus::recover_held)
/// returns them at the next start (`transport.restart.held-redelivered`).
///
/// `next` is cancel-safe up to one attempt: a call dropped while its
/// transaction commits may leave the row held, which the ack timeout takes
/// back (the reaper knows the hold before it commits).
#[derive(Debug)]
pub struct PgSubscription {
    shared: Arc<Shared>,
    group: ConsumerGroup,
    retry: RetryPolicy,
    wake: watch::Receiver<u64>,
    closed: watch::Receiver<bool>,
    held: HashMap<DeliveryId, Hold>,
    /// Test hook: drop without telling the reaper, as a process death does.
    abandoned: bool,
}

impl PgSubscription {
    pub fn group(&self) -> &ConsumerGroup {
        &self.group
    }

    /// Drop the subscription the way a killed process does: its holds stay
    /// `held` in the database until a restart's `recover_held`.
    #[cfg(test)]
    pub(crate) fn abandon(mut self) {
        self.abandoned = true;
    }

    async fn try_take(&self, hold_deadline: Instant) -> Result<Took, sqlx::Error> {
        let shared = &self.shared;
        let group = self.group.0.as_str();
        let capacity = i64::try_from(shared.config.group_capacity.get()).unwrap_or(i64::MAX);
        let now = shared.now_micros();
        let mut tx = shared.pool.begin().await?;
        let (through, subjects): (i64, Vec<String>) = sqlx::query_as(
            "SELECT admitted_through, subjects FROM transport.groups WHERE name = $1 FOR UPDATE",
        )
        .bind(group)
        .fetch_one(&mut *tx)
        .await?;
        let tracked: i64 =
            sqlx::query_scalar("SELECT count(*) FROM transport.deliveries WHERE group_name = $1")
                .bind(group)
                .fetch_one(&mut *tx)
                .await?;
        let room = capacity.saturating_sub(tracked).max(0);
        let (head, candidates, last_admitted): (i64, i64, Option<i64>) = sqlx::query_as(ADMIT)
            .bind(group)
            .bind(through)
            .bind(&subjects)
            .bind(room)
            .fetch_one(&mut *tx)
            .await?;
        // Every candidate fit: the group has seen the log up to the head
        // this statement saw (publishes commit in seq order). Otherwise it
        // stops after the last entry it admitted.
        let advanced = if candidates > room {
            last_admitted.unwrap_or(through)
        } else {
            head.max(through)
        };
        if advanced != through {
            sqlx::query("UPDATE transport.groups SET admitted_through = $2 WHERE name = $1")
                .bind(group)
                .bind(advanced)
                .execute(&mut *tx)
                .await?;
        }
        sqlx::query(
            "UPDATE transport.deliveries SET state = 'ready', available_at = NULL \
             WHERE group_name = $1 AND state = 'delayed' AND available_at <= $2",
        )
        .bind(group)
        .bind(now)
        .execute(&mut *tx)
        .await?;
        let taken: Option<(i64, i32, String, String)> = sqlx::query_as(TAKE)
            .bind(group)
            .fetch_optional(&mut *tx)
            .await?;
        let took = match taken {
            Some((seq, attempt, subject, envelope)) => {
                // Known to the reaper before it commits, so a call dropped
                // mid-commit is still taken back at the deadline.
                let key = HoldKey {
                    group: group.to_owned(),
                    seq,
                    attempt,
                };
                let _ = shared.reaper.send(ReaperMsg::Hold {
                    key,
                    deadline: hold_deadline,
                    retry: self.retry,
                });
                Took::Row {
                    seq,
                    attempt,
                    subject,
                    envelope,
                }
            }
            None => {
                let next_due: Option<i64> = sqlx::query_scalar(
                    "SELECT min(available_at) FROM transport.deliveries \
                     WHERE group_name = $1 AND state = 'delayed'",
                )
                .bind(group)
                .fetch_one(&mut *tx)
                .await?;
                Took::Empty { next_due }
            }
        };
        tx.commit().await?;
        Ok(took)
    }

    /// End an undecodable delivery: no redelivery, no dead letter
    /// (`transport.codec.undecodable-not-redelivered`).
    async fn terminate(&self, seq: i64, attempt: i32, error: &BusError) {
        let deleted = sqlx::query(
            "DELETE FROM transport.deliveries \
             WHERE group_name = $1 AND seq = $2 AND state = 'held' AND attempt = $3",
        )
        .bind(&self.group.0)
        .bind(seq)
        .bind(attempt)
        .execute(&self.shared.pool)
        .await;
        let key = HoldKey {
            group: self.group.0.clone(),
            seq,
            attempt,
        };
        let _ = self.shared.reaper.send(ReaperMsg::Settled(key));
        match deleted {
            Ok(_) => {
                tracing::warn!(group = %self.group.0, seq, error = ?error, "undecodable message terminated")
            }
            // The hold times out and is retried; it fails to decode again.
            Err(e) => {
                tracing::warn!(group = %self.group.0, seq, failure = %failure(&e), "terminating an undecodable message failed")
            }
        }
    }

    /// The hold `id` names if this subscription holds it and its deadline
    /// has not passed; removed from the map either way.
    fn take_hold(&mut self, id: DeliveryId) -> Result<Hold, BusError> {
        match self.held.remove(&id) {
            Some(hold) if hold.deadline > Instant::now() => Ok(hold),
            _ => Err(BusError::UnknownDelivery(id)),
        }
    }

    fn key(&self, hold: &Hold) -> HoldKey {
        HoldKey {
            group: self.group.0.clone(),
            seq: hold.seq,
            attempt: hold.attempt,
        }
    }

    /// How long to wait before trying again when nothing is ready.
    fn idle_wait(&self, next_due: Option<i64>) -> Duration {
        let poll = self.shared.config.poll.get();
        match next_due {
            Some(due) => {
                let until = due.saturating_sub(self.shared.now_micros()).max(0);
                Duration::from_micros(u64::try_from(until).unwrap_or(0)).min(poll)
            }
            None => poll,
        }
    }
}

impl Subscription for PgSubscription {
    async fn next(&mut self) -> Option<Result<Delivery, BusError>> {
        let ack_timeout = self.shared.config.ack_timeout.get();
        loop {
            if *self.closed.borrow_and_update() {
                return None;
            }
            let now = Instant::now();
            self.held.retain(|_, hold| hold.deadline > now);
            // Any wake-up from here on interrupts the wait below.
            self.wake.borrow_and_update();
            let deadline = now + ack_timeout;
            let wait = match self.try_take(deadline).await {
                Ok(Took::Row {
                    seq,
                    attempt,
                    subject,
                    envelope,
                }) => match decode_envelope(&subject, &envelope) {
                    Ok(envelope) => {
                        let id =
                            DeliveryId(self.shared.next_delivery.fetch_add(1, Ordering::Relaxed));
                        self.held.insert(
                            id,
                            Hold {
                                seq,
                                attempt,
                                deadline,
                            },
                        );
                        tracing::debug!(group = %self.group.0, seq, attempt, delivery = id.0, "delivered");
                        let attempt = u32::try_from(attempt)
                            .ok()
                            .and_then(NonZeroU32::new)
                            .unwrap_or(NonZeroU32::MIN);
                        return Some(Ok(Delivery {
                            id,
                            attempt,
                            envelope,
                        }));
                    }
                    Err(error) => {
                        self.terminate(seq, attempt, &error).await;
                        return Some(Err(error));
                    }
                },
                Ok(Took::Empty { next_due }) => self.idle_wait(next_due),
                Err(error) => {
                    tracing::warn!(group = %self.group.0, failure = %failure(&error), "next failed; retrying");
                    self.shared.config.poll.get()
                }
            };
            tokio::select! {
                changed = self.closed.changed() => if changed.is_err() { return None },
                _ = self.wake.changed() => {}
                () = tokio::time::sleep(wait) => {}
            }
        }
    }

    async fn ack(&mut self, id: DeliveryId) -> Result<(), BusError> {
        let hold = self.take_hold(id)?;
        let key = self.key(&hold);
        let deleted = sqlx::query(
            "DELETE FROM transport.deliveries \
             WHERE group_name = $1 AND seq = $2 AND state = 'held' AND attempt = $3",
        )
        .bind(&key.group)
        .bind(key.seq)
        .bind(key.attempt)
        .execute(&self.shared.pool)
        .await;
        match deleted {
            Ok(done) => {
                let _ = self.shared.reaper.send(ReaperMsg::Settled(key));
                if done.rows_affected() == 0 {
                    return Err(BusError::UnknownDelivery(id));
                }
                tracing::debug!(group = %self.group.0, seq = hold.seq, delivery = id.0, "delivery acked");
                self.shared.wake_all();
                Ok(())
            }
            Err(error) => {
                // Still held: the ack timeout takes it back and it is
                // redelivered (at least once).
                self.held.insert(id, hold);
                Err(bus_error("ack", &error))
            }
        }
    }

    async fn nack(
        &mut self,
        id: DeliveryId,
        retry_after: Duration,
        reason: String,
    ) -> Result<(), BusError> {
        let hold = self.take_hold(id)?;
        let key = self.key(&hold);
        let failed = fail(
            &self.shared,
            &key,
            &self.retry,
            Failure::Nack {
                retry_after,
                reason,
            },
        )
        .await;
        match failed {
            Ok(true) => {
                let _ = self.shared.reaper.send(ReaperMsg::Settled(key));
                self.shared.wake_all();
                Ok(())
            }
            Ok(false) => {
                let _ = self.shared.reaper.send(ReaperMsg::Settled(key));
                Err(BusError::UnknownDelivery(id))
            }
            Err(error) => {
                self.held.insert(id, hold);
                Err(bus_error("nack", &error))
            }
        }
    }
}

impl Drop for PgSubscription {
    fn drop(&mut self) {
        if self.abandoned {
            return;
        }
        let now = Instant::now();
        let holds: Vec<(HoldKey, RetryPolicy)> = self
            .held
            .values()
            .filter(|hold| hold.deadline > now)
            .map(|hold| (self.key(hold), self.retry))
            .collect();
        if !holds.is_empty() {
            tracing::debug!(group = %self.group.0, released = holds.len(), "subscription dropped");
            // Fails only when the bus has stopped; a restart recovers them.
            let _ = self.shared.reaper.send(ReaperMsg::Dropped(holds));
        }
    }
}
