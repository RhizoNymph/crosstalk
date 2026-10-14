//! The fence wait over a scripted lock session on tokio's paused clock:
//! it never gives up, reports its blockers at a bounded rate, and a lock
//! session that fails or stops answering while it waits is a lock loss.
//! The epoch's arithmetic and wire form.

use std::time::Duration;

use crosstalk_store::sqlx;
use tokio::time::Instant;

use super::wait::{Blocker, FenceSession, FenceWaitError, wait_for_fence};
use super::{Epoch, EpochError, FENCE_POLL, FENCE_REPORT};
use crate::store::lock::{DETECTION, LockLoss, PING_TIMEOUT, PIPELINE_FENCE, PIPELINE_LOCK};

/// How the scripted session answers one exclusive try.
#[derive(Debug, Clone, Copy)]
enum Fault {
    /// The statement fails: the session was reset or ended.
    Error,
    /// The statement never answers: a silent partition.
    Hang,
}

/// A lock session whose fence frees after `refusals` tries.
struct Scripted {
    refusals: u64,
    fault: Option<(u64, Fault)>,
    release: bool,
    tries: u64,
    blocker_reads: u64,
    released: bool,
}

impl Scripted {
    fn free_after(refusals: u64) -> Self {
        Self {
            refusals,
            fault: None,
            release: true,
            tries: 0,
            blocker_reads: 0,
            released: false,
        }
    }
}

impl FenceSession for Scripted {
    async fn try_exclusive(&mut self) -> Result<bool, sqlx::Error> {
        self.tries += 1;
        match self.fault {
            Some((at, Fault::Error)) if at == self.tries => Err(sqlx::Error::Io(
                std::io::Error::from(std::io::ErrorKind::ConnectionReset),
            )),
            Some((at, Fault::Hang)) if at == self.tries => std::future::pending().await,
            _ => Ok(self.tries > self.refusals),
        }
    }

    async fn release_exclusive(&mut self) -> Result<bool, sqlx::Error> {
        self.released = true;
        Ok(self.release)
    }

    async fn blockers(&mut self) -> Result<Vec<Blocker>, sqlx::Error> {
        self.blocker_reads += 1;
        Ok(vec![Blocker {
            pid: 7,
            application_name: "crosstalk-pipeline-1-1-e1".to_owned(),
        }])
    }
}

/// Safety over liveness: an hour of refusals (an old owner stopped with
/// its connections open) never ends the wait; once the fence frees, the
/// wait takes it and releases it. Its blockers were read at most once per
/// [`FENCE_REPORT`].
#[tokio::test(start_paused = true)]
async fn the_wait_never_gives_up_and_completes_once_the_fence_is_free() {
    let hour = Duration::from_secs(3600);
    let refusals = u64::try_from(hour.as_millis() / FENCE_POLL.as_millis()).expect("fits");
    let mut session = Scripted::free_after(refusals);
    let started = Instant::now();
    let fenced = wait_for_fence(&mut session, Epoch::new(2))
        .await
        .expect("the fence frees");
    assert!(started.elapsed() >= hour, "{:?}", started.elapsed());
    assert_eq!(session.tries, refusals + 1);
    assert_eq!(fenced.polls, refusals + 1);
    assert!(fenced.waited >= hour, "{fenced:?}");
    assert!(session.released, "the exclusive fence is released");
    let most = hour.as_secs() / FENCE_REPORT.as_secs() + 1;
    assert!(
        (1..=most).contains(&session.blocker_reads),
        "{} blocker reads in an hour, at most {most}",
        session.blocker_reads
    );
    assert_eq!(fenced.reports, session.blocker_reads);
}

/// No earlier connection left: the first try takes the fence, no blocker
/// is read, nothing is waited for.
#[tokio::test(start_paused = true)]
async fn a_free_fence_completes_at_once() {
    let mut session = Scripted::free_after(0);
    let started = Instant::now();
    let fenced = wait_for_fence(&mut session, Epoch::new(1))
        .await
        .expect("free");
    assert_eq!(started.elapsed(), Duration::ZERO);
    assert_eq!((fenced.polls, fenced.reports), (1, 0));
    assert_eq!(session.blocker_reads, 0);
    assert!(session.released);
}

/// A try that fails mid-wait: the lock's session is gone, and with it the
/// lock: a `PingFailed` loss, never a take.
#[tokio::test(start_paused = true)]
async fn a_failed_try_while_waiting_is_a_lock_loss() {
    let mut session = Scripted {
        fault: Some((3, Fault::Error)),
        ..Scripted::free_after(10)
    };
    let failed = wait_for_fence(&mut session, Epoch::new(3)).await;
    assert!(
        matches!(
            failed,
            Err(FenceWaitError::Lost(LockLoss::PingFailed { .. }))
        ),
        "{failed:?}"
    );
    assert!(!session.released);
}

/// A try that never answers (a silent partition) is a `PingTimedOut`
/// loss within a poll and the ping's deadline, inside the lock's
/// detection bound.
#[tokio::test(start_paused = true)]
async fn a_try_without_an_answer_is_a_lock_loss_within_the_ping_deadline() {
    let mut session = Scripted {
        fault: Some((2, Fault::Hang)),
        ..Scripted::free_after(10)
    };
    let started = Instant::now();
    let failed = wait_for_fence(&mut session, Epoch::new(3)).await;
    assert!(
        matches!(
            failed,
            Err(FenceWaitError::Lost(LockLoss::PingTimedOut { after })) if after == PING_TIMEOUT
        ),
        "{failed:?}"
    );
    assert!(started.elapsed() <= FENCE_POLL + PING_TIMEOUT);
}

/// Releasing a fence the session does not hold is refused, typed.
#[tokio::test(start_paused = true)]
async fn a_release_that_finds_no_fence_is_refused() {
    let mut session = Scripted {
        release: false,
        ..Scripted::free_after(0)
    };
    let refused = wait_for_fence(&mut session, Epoch::new(1)).await;
    assert!(
        matches!(refused, Err(FenceWaitError::NotHeld)),
        "{refused:?}"
    );
}

/// A lost session is noticed while fencing at least as fast as while
/// holding: one poll and a ping's deadline is within the detection bound.
#[test]
fn the_wait_notices_a_lost_session_within_the_detection_bound() {
    assert!(FENCE_POLL + PING_TIMEOUT <= DETECTION);
    assert!(FENCE_POLL < FENCE_REPORT);
}

/// The fence's advisory key is its own: distinct from the lock's and
/// positive, so `pg_locks` names it as `classid = key >> 32`, `objid =
/// key & 0xFFFF_FFFF`.
#[test]
fn the_fence_key_is_distinct_and_positive() {
    assert_ne!(PIPELINE_FENCE, PIPELINE_LOCK);
    assert!(PIPELINE_FENCE > 0 && PIPELINE_LOCK > 0);
}

/// Epochs count up from 0 (the migration's seed, before any take), are
/// never negative, and travel as plain numbers.
#[test]
fn epochs_count_up_from_zero_and_never_go_negative() {
    assert_eq!(Epoch::default(), Epoch::new(0));
    assert_eq!(Epoch::new(0).next(), Epoch::new(1));
    assert!(Epoch::new(1) < Epoch::new(2));
    assert_eq!(Epoch::new(41).next().get(), 42);
    assert_eq!(Epoch::new(3).to_string(), "3");
    assert_eq!(Epoch::try_from(5_i64), Ok(Epoch::new(5)));
    assert_eq!(
        Epoch::try_from(-1_i64),
        Err(EpochError::Negative { stored: -1 })
    );
    assert_eq!(serde_json::to_string(&Epoch::new(9)).expect("encodes"), "9");
    assert_eq!(
        serde_json::from_str::<Epoch>("9").expect("decodes"),
        Epoch::new(9)
    );
    assert!(serde_json::from_str::<Epoch>("-9").is_err());
}
