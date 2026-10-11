//! What each take in flight is doing, for a stall's diagnosis, and the
//! bound that keeps a stalled take from holding its transaction.
//!
//! ```text
//! take_task ─ register(group) ─▶ Tracked(id, try_take) spawned
//!   try_take: phase("acquire") ─ phase("lock group") ─ ... ─ phase("commit")
//!   Tracked::poll: last poll time, every poll
//!   Tracked dropped (finished or aborted) ─▶ unregistered
//! PgSubscription::next: a take running past `take_limit` outside "acquire" is
//!   aborted (its transaction rolled back) and retried; counted `stalled`
//! PgBus::takes(): every take in flight: group, phase, how long it runs,
//!   how long in its phase, how long since it was last polled
//! ```
//!
//! "Since last polled" tells a take its runtime stopped polling (a
//! scheduling stall) from one polled but waiting on the database (I/O or a
//! server-side wait), which `pg_stat_activity` then names.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::task::{Context, Poll};
use std::time::Duration;

use tokio::time::Instant;

use super::Shared;

/// One take in flight.
#[derive(Debug, Clone)]
struct Progress {
    group: String,
    phase: &'static str,
    began: Instant,
    phase_at: Instant,
    last_poll: Instant,
}

/// The reaper's liveness: when it last went round its loop, and how many
/// deadlines it was waiting on then.
#[derive(Debug, Default)]
pub(crate) struct ReaperBeat {
    at: Mutex<Option<Instant>>,
    timers: AtomicU64,
}

/// The reaper's state as [`PgBus::reaper`](super::PgBus::reaper) reports it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReaperReport {
    /// How long since the reaper last went round its loop (`None` before
    /// its first round).
    pub since_active: Option<Duration>,
    /// The deadlines it was waiting on then.
    pub timers: u64,
}

impl ReaperBeat {
    pub(crate) fn beat(&self, timers: usize) {
        *self.at.lock().unwrap_or_else(PoisonError::into_inner) = Some(Instant::now());
        self.timers
            .store(u64::try_from(timers).unwrap_or(u64::MAX), Ordering::Relaxed);
    }

    pub(crate) fn report(&self) -> ReaperReport {
        let at = *self.at.lock().unwrap_or_else(PoisonError::into_inner);
        ReaperReport {
            since_active: at.map(|at| Instant::now().saturating_duration_since(at)),
            timers: self.timers.load(Ordering::Relaxed),
        }
    }
}

/// Every take in flight, by id, and the stalled ones counted.
#[derive(Debug, Default)]
pub(crate) struct Takes {
    next: AtomicU64,
    stalled: AtomicU64,
    running: Mutex<HashMap<u64, Progress>>,
}

/// A take in flight as [`PgBus::takes`](super::PgBus::takes) reports it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TakeReport {
    pub group: String,
    /// The statement the take was at (`acquire` while waiting for a
    /// pooled connection).
    pub phase: &'static str,
    pub running: Duration,
    pub in_phase: Duration,
    /// How long since its task was last polled.
    pub since_poll: Duration,
}

impl Takes {
    fn running(&self) -> std::sync::MutexGuard<'_, HashMap<u64, Progress>> {
        // Only plain map updates happen under the lock: a poisoned map is
        // still consistent.
        self.running.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Register a take of `group`; its id.
    pub(crate) fn register(&self, group: &str) -> u64 {
        let id = self.next.fetch_add(1, Ordering::Relaxed);
        let now = Instant::now();
        self.running().insert(
            id,
            Progress {
                group: group.to_owned(),
                phase: "acquire",
                began: now,
                phase_at: now,
                last_poll: now,
            },
        );
        id
    }

    pub(crate) fn phase(&self, id: u64, phase: &'static str) {
        if let Some(progress) = self.running().get_mut(&id) {
            progress.phase = phase;
            progress.phase_at = Instant::now();
        }
    }

    pub(crate) fn polled(&self, id: u64) {
        if let Some(progress) = self.running().get_mut(&id) {
            progress.last_poll = Instant::now();
        }
    }

    pub(crate) fn finished(&self, id: u64) {
        self.running().remove(&id);
    }

    /// The phase take `id` is in, if it still runs.
    pub(crate) fn phase_of(&self, id: u64) -> Option<&'static str> {
        self.running().get(&id).map(|progress| progress.phase)
    }

    pub(crate) fn report(&self, id: u64) -> Option<TakeReport> {
        let now = Instant::now();
        self.running()
            .get(&id)
            .map(|progress| report(progress, now))
    }

    pub(crate) fn count_stalled(&self) {
        self.stalled.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn stalled(&self) -> u64 {
        self.stalled.load(Ordering::Relaxed)
    }

    pub(crate) fn reports(&self) -> Vec<TakeReport> {
        let now = Instant::now();
        let mut reports: Vec<TakeReport> = self
            .running()
            .values()
            .map(|progress| report(progress, now))
            .collect();
        reports.sort_by_key(|report| std::cmp::Reverse(report.running));
        reports
    }
}

fn report(progress: &Progress, now: Instant) -> TakeReport {
    TakeReport {
        group: progress.group.clone(),
        phase: progress.phase,
        running: now.saturating_duration_since(progress.began),
        in_phase: now.saturating_duration_since(progress.phase_at),
        since_poll: now.saturating_duration_since(progress.last_poll),
    }
}

/// A take's future, recording each poll and unregistering the take when
/// it finishes or is dropped (aborted).
pub(crate) struct Tracked<T> {
    inner: Pin<Box<dyn Future<Output = T> + Send>>,
    shared: Arc<Shared>,
    id: u64,
}

impl<T> Tracked<T> {
    pub(crate) fn new(
        shared: Arc<Shared>,
        id: u64,
        inner: impl Future<Output = T> + Send + 'static,
    ) -> Self {
        Self {
            inner: Box::pin(inner),
            shared,
            id,
        }
    }
}

impl<T> Future for Tracked<T> {
    type Output = T;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<T> {
        self.shared.takes.polled(self.id);
        self.inner.as_mut().poll(cx)
    }
}

impl<T> Drop for Tracked<T> {
    fn drop(&mut self) {
        self.shared.takes.finished(self.id);
    }
}

/// How long a take may run, past waiting for its connection, before it
/// is abandoned: twice the publish timeout, at least ten seconds. Its
/// transaction's `lock_timeout` is the same, so a server-side lock wait
/// ends with an error first.
pub(crate) fn take_limit(publish_timeout: Duration) -> Duration {
    (2 * publish_timeout).max(Duration::from_secs(10))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_take_is_reported_until_it_finishes() {
        let takes = Takes::default();
        let id = takes.register("relay");
        assert_eq!(takes.phase_of(id), Some("acquire"));
        takes.phase(id, "take");
        takes.polled(id);
        let reports = takes.reports();
        assert_eq!(reports.len(), 1);
        assert_eq!(
            (reports[0].group.as_str(), reports[0].phase),
            ("relay", "take")
        );
        takes.finished(id);
        assert!(takes.reports().is_empty());
        assert_eq!(takes.phase_of(id), None);
        takes.count_stalled();
        assert_eq!(takes.stalled(), 1);
    }

    #[test]
    fn the_take_limit_is_twice_the_publish_timeout_and_at_least_ten_seconds() {
        assert_eq!(take_limit(Duration::from_secs(2)), Duration::from_secs(10));
        assert_eq!(take_limit(Duration::from_secs(8)), Duration::from_secs(16));
    }
}
