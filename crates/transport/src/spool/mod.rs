//! The publish spool: [`SpoolingBus`], an [`EventBus`] decorator that
//! spools to local disk what its inner bus cannot take, and sends it, in
//! order and under its own ids, when the inner bus answers again (decision
//! Q5). `serve` puts it in front of [`PgBus`](crate::PgBus) whenever a
//! database is configured, so capture never drops an exchange while the
//! database is down; only a full spool refuses.
//!
//! - Only [`BusError::Disconnected`] from the inner bus is spooled; every
//!   other error passes through.
//! - [`SpoolingBus::publish`] returns `Ok` once the envelope is in the
//!   inner bus or `fdatasync`ed in the spool
//!   (`transport.spool.ok-means-durable`).
//! - States ([`SpoolState`]) change only under the publish mutex. Once
//!   anything is spooled, every later publish appends behind it until the
//!   spool is empty (`transport.spool.no-overtaking`).
//! - The drainer task probes the inner bus every `probe` interval while
//!   spooling, then sends `drain_batch` records at a time through
//!   [`DrainTarget::publish_batch`] and moves the cursor after each batch
//!   commits. A crash between the two resends the batch, which an inner bus
//!   idempotent on envelope ids absorbs
//!   (`transport.spool.drained-once-under-its-id`). Reaching the tail, it
//!   takes the publish mutex, finds the spool empty and switches to
//!   `Direct`.
//! - A full spool refuses with [`BusError::SpoolFull`] and never waits
//!   (`transport.spool.bounded`); a disk error is `Disconnected`.
//! - [`SpoolingBus::oldest_at`] is the earliest `at` still spooled, which
//!   the frontier counts as pending (`topology.frontier.covers-spool`).
//!
//! The on-disk format and recovery are in the private `log` (files, fsync
//! points, torn tails, corruption), `record` and `cursor` modules; see
//! `docs/features/publish_spool.md`.

mod config;
mod cursor;
mod drain;
mod error;
mod log;
mod record;
mod state;

#[cfg(test)]
pub(crate) mod tests;

use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use crosstalk_spec::events::{Envelope, Subject};
use crosstalk_spec::interfaces::l2_transport::{BusError, ConsumerGroup, EventBus, RetryPolicy};
use crosstalk_spec::support::Timestamp;
use tokio::sync::{Notify, watch};
use tokio::task::{AbortHandle, JoinHandle};

pub use config::{InvalidSpoolConfig, SpoolConfig};
pub use error::{SpoolError, SpoolOp};
pub use log::Discarded;
pub use state::{SpoolState, SpoolStats};

use self::log::{AppendError, SpoolLog, segment_name};
use crate::codec;

/// What a [`SpoolingBus`] needs from the bus it wraps beyond
/// [`EventBus`]: a cheap reachability check, and a batch publish that
/// lands all of its envelopes or none, in order. The inner bus should be
/// idempotent on envelope ids (as `PgBus` is): a drain repeated after a
/// crash resends what it may already hold.
pub trait DrainTarget: EventBus {
    /// `Ok` when a publish would reach the bus now.
    fn probe(&self) -> impl Future<Output = Result<(), BusError>> + Send;

    /// Publish `envelopes` in order; `Ok` means every one is in the bus.
    fn publish_batch(
        &self,
        envelopes: Vec<Envelope>,
    ) -> impl Future<Output = Result<(), BusError>> + Send;
}

/// A seeded crash point inside the drainer, for the simulation tests.
#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CrashPoint {
    /// The inner bus committed a batch; the cursor has not moved.
    AfterBatchCommit,
    /// The spool is empty; the state has not switched to `Direct`.
    BeforeDirectSwitch,
}

pub(crate) struct Shared<B> {
    pub(crate) inner: B,
    pub(crate) config: SpoolConfig,
    /// The publish mutex: publishes and state changes take turns.
    pub(crate) turn: tokio::sync::Mutex<()>,
    /// The files and index; locked only on the blocking pool, or briefly
    /// for a snapshot.
    pub(crate) log: Arc<Mutex<SpoolLog>>,
    pub(crate) state: watch::Sender<SpoolState>,
    /// Wakes the drainer after an append.
    pub(crate) wake: Notify,
    #[cfg(test)]
    pub(crate) crash: Mutex<Option<CrashPoint>>,
}

impl<B> Shared<B> {
    /// Change the state; callers hold the publish mutex.
    pub(crate) fn set_state(&self, next: SpoolState) {
        let previous = self.state.send_replace(next.clone());
        if previous != next {
            match &next {
                SpoolState::Corrupt { segment, offset } => {
                    tracing::error!(from = previous.label(), segment = %segment, offset, "spool corrupt");
                }
                SpoolState::Spooling => {
                    tracing::warn!(from = previous.label(), "inner bus unreachable; spooling");
                }
                other => tracing::info!(
                    from = previous.label(),
                    to = other.label(),
                    "spool state changed"
                ),
            }
        }
    }

    pub(crate) fn lock_log(&self) -> MutexGuard<'_, SpoolLog> {
        // A panic inside a spool operation leaves the index as it was at
        // the panic; the files are the truth, and the next open rebuilds
        // the index from them.
        self.log.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Whether the armed crash point is `point` (disarming it).
    #[cfg(test)]
    pub(crate) fn crash_at(&self, point: CrashPoint) -> bool {
        let mut armed = self.crash.lock().unwrap_or_else(PoisonError::into_inner);
        if *armed == Some(point) {
            *armed = None;
            true
        } else {
            false
        }
    }
}

/// Run `work` on the log, on the blocking pool. The work finishes even if
/// the caller is cancelled, so the files and the index never diverge.
pub(crate) async fn blocking<T, F>(log: &Arc<Mutex<SpoolLog>>, work: F) -> Result<T, SpoolError>
where
    T: Send + 'static,
    F: FnOnce(&mut SpoolLog) -> T + Send + 'static,
{
    let log = Arc::clone(log);
    tokio::task::spawn_blocking(move || {
        let mut guard = log.lock().unwrap_or_else(PoisonError::into_inner);
        work(&mut guard)
    })
    .await
    .map_err(|_| SpoolError::Interrupted)
}

#[derive(Debug)]
struct Drainer {
    abort: AbortHandle,
    join: tokio::sync::Mutex<Option<JoinHandle<()>>>,
}

impl Drop for Drainer {
    fn drop(&mut self) {
        self.abort.abort();
    }
}

/// The spooling decorator: see the [module docs](self).
///
/// Cloning gives another handle on the same spool. The drainer stops when
/// [`SpoolingBus::close`] is called or every handle is dropped; the spool's
/// `LOCK` is released once the last handle and the drainer are gone.
pub struct SpoolingBus<B> {
    shared: Arc<Shared<B>>,
    drainer: Arc<Drainer>,
}

impl<B> Clone for SpoolingBus<B> {
    fn clone(&self) -> Self {
        Self {
            shared: Arc::clone(&self.shared),
            drainer: Arc::clone(&self.drainer),
        }
    }
}

impl<B> std::fmt::Debug for SpoolingBus<B> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SpoolingBus")
            .field("config", &self.shared.config)
            .field("state", &*self.shared.state.borrow())
            .finish_non_exhaustive()
    }
}

impl<B> SpoolingBus<B>
where
    B: DrainTarget + Send + Sync + 'static,
{
    /// Open the spool in `config.dir()` (taking its `LOCK`, truncating a
    /// torn tail) in front of `inner`, and start the drainer. A non-empty
    /// spool starts `Spooling` (the drainer probes the inner bus at once);
    /// one with a corruption starts `Corrupt`.
    pub async fn open(inner: B, config: SpoolConfig) -> Result<Self, SpoolError> {
        let opening = config.clone();
        let log = tokio::task::spawn_blocking(move || SpoolLog::open(&opening))
            .await
            .map_err(|_| SpoolError::Interrupted)??;
        let state = match log.barrier() {
            Some(barrier) => SpoolState::Corrupt {
                segment: segment_name(barrier.segment),
                offset: barrier.offset,
            },
            None if log.records() > 0 => SpoolState::Spooling,
            None => SpoolState::Direct,
        };
        tracing::info!(
            dir = %config.dir().display(),
            state = state.label(),
            records = log.records(),
            "publish spool opened"
        );
        let shared = Arc::new(Shared {
            inner,
            config,
            turn: tokio::sync::Mutex::new(()),
            log: Arc::new(Mutex::new(log)),
            state: watch::Sender::new(state),
            wake: Notify::new(),
            #[cfg(test)]
            crash: Mutex::new(None),
        });
        let join = tokio::spawn(drain::run(Arc::clone(&shared)));
        let drainer = Arc::new(Drainer {
            abort: join.abort_handle(),
            join: tokio::sync::Mutex::new(Some(join)),
        });
        Ok(Self { shared, drainer })
    }

    /// The wrapped bus.
    pub fn inner(&self) -> &B {
        &self.shared.inner
    }

    /// The current state.
    pub fn state(&self) -> SpoolState {
        self.shared.state.borrow().clone()
    }

    /// The state as it changes, for readiness reporting.
    pub fn watch_state(&self) -> watch::Receiver<SpoolState> {
        self.shared.state.subscribe()
    }

    /// The earliest `Envelope::at` of the records not yet in the inner bus,
    /// `None` when there are none (`topology.frontier.covers-spool`). May
    /// wait for an append in progress.
    pub fn oldest_at(&self) -> Option<Timestamp> {
        self.shared.lock_log().oldest_at()
    }

    /// A snapshot for `/healthz` and `/metrics`. May wait for an append in
    /// progress.
    pub fn stats(&self) -> SpoolStats {
        let state = self.state();
        let log = self.shared.lock_log();
        let counters = log.counters();
        SpoolStats {
            state,
            records: log.records() as u64,
            bytes: log.bytes(),
            oldest_at: log.oldest_at(),
            max_bytes: self.shared.config.max_bytes(),
            appended: counters.appended,
            drained: counters.drained,
            rejected_full: counters.rejected_full,
            rejected_io: counters.rejected_io,
            truncated_bytes: counters.truncated_bytes,
        }
    }

    /// Stop the drainer and wait for it. The spool's files stay as they
    /// are; the next open resumes from them.
    pub async fn close(&self) {
        self.drainer.abort.abort();
        if let Some(join) = self.drainer.join.lock().await.take() {
            // An aborted task's join reports the cancellation; nothing to do.
            let _ = join.await;
        }
        tracing::info!(dir = %self.shared.config.dir().display(), "publish spool closed");
    }

    /// Arm a crash point in the drainer: when reached, the drainer stops
    /// there, as a killed process would.
    #[cfg(test)]
    pub(crate) fn arm_crash(&self, point: CrashPoint) {
        *self
            .shared
            .crash
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = Some(point);
    }

    /// Kill this spool: stop the drainer, let every blocking operation in
    /// flight finish (a dead process's last `write` either happened or did
    /// not), and drop everything without any shutdown step. Must be the
    /// last handle; the `LOCK` is free once this returns.
    #[cfg(test)]
    pub(crate) async fn crash(self) {
        self.close().await;
        while Arc::strong_count(&self.shared.log) > 1 {
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }
        let Self { shared, drainer } = self;
        drop(drainer);
        assert_eq!(Arc::strong_count(&shared), 1, "crash needs the last handle");
        drop(shared);
    }

    /// Append `envelope` to the spool's tail, durably.
    async fn append(&self, envelope: &Envelope) -> Result<(), BusError> {
        let message = codec::encode(envelope)?;
        let payload = message.bytes.to_vec();
        let at = envelope.at;
        let event = envelope.id.ulid_text();
        let outcome = blocking(&self.shared.log, move |log| log.append(&payload, at)).await;
        self.shared.wake.notify_one();
        match outcome {
            Ok(Ok(())) => {
                tracing::debug!(event = %event, "spooled");
                Ok(())
            }
            Ok(Err(AppendError::Full { bytes })) => {
                tracing::warn!(event = %event, bytes, max_bytes = self.shared.config.max_bytes(), "spool full; publish refused");
                Err(BusError::SpoolFull { bytes })
            }
            Ok(Err(AppendError::TooLarge)) => Err(BusError::Encode {
                reason: "the envelope exceeds the spool's 16 MiB record limit".to_owned(),
            }),
            Ok(Err(AppendError::Io(error))) | Err(error) => {
                tracing::error!(event = %event, error = %error, "spool append failed");
                Err(BusError::Disconnected)
            }
        }
    }
}

/// Remove the corruption that stopped a spool's draining: the corrupt
/// record and everything after it in its segment (`crosstalk spool
/// --discard-corrupt`). Takes the spool's `LOCK`, so no process may have
/// it open. `None` when the spool has no corruption.
pub async fn discard_corrupt(config: &SpoolConfig) -> Result<Option<Discarded>, SpoolError> {
    let config = config.clone();
    tokio::task::spawn_blocking(move || SpoolLog::open(&config)?.discard_corrupt())
        .await
        .map_err(|_| SpoolError::Interrupted)?
}

impl<B> EventBus for SpoolingBus<B>
where
    B: DrainTarget + Send + Sync + 'static,
{
    type Subscription = B::Subscription;

    /// To the inner bus while the spool is empty; otherwise, or when the
    /// inner bus is unreachable, to the spool's tail. `Ok` means the
    /// envelope is in the inner bus or durably spooled.
    async fn publish(&self, envelope: Envelope) -> Result<(), BusError> {
        let _turn = self.shared.turn.lock().await;
        let direct = *self.shared.state.borrow() == SpoolState::Direct;
        if direct {
            match self.shared.inner.publish(envelope.clone()).await {
                Err(BusError::Disconnected) => self.shared.set_state(SpoolState::Spooling),
                other => return other,
            }
        }
        self.append(&envelope).await
    }

    async fn subscribe(
        &self,
        subjects: &[Subject],
        group: ConsumerGroup,
        retry: RetryPolicy,
    ) -> Result<B::Subscription, BusError> {
        self.shared.inner.subscribe(subjects, group, retry).await
    }
}
