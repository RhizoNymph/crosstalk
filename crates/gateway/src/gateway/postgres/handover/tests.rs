//! The handover's order and bounds, over a scripted incarnation on a
//! paused clock: no database.

use std::sync::{Arc, Mutex, PoisonError};

use super::*;
use crate::config::ShutdownConfig;
use crate::live::recovery::{PipelineStatus, StatusReader};

/// A step of the scripted incarnation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Call {
    Quiesce,
    ReleaseHolds,
    ClosePool,
    ReleaseLock,
}

/// Each call, with the status the handover reported when it was made.
type Calls = Arc<Mutex<Vec<(Call, PipelinePhase, LockState)>>>;

/// An incarnation that records its calls and stalls (never answers) in
/// the steps it is told to.
struct Scripted {
    calls: Calls,
    status: StatusReader,
    stalls: Vec<Call>,
    released: u64,
}

impl Scripted {
    fn new(status: &StatusReporter, stalls: &[Call]) -> (Self, Calls) {
        let calls = Calls::default();
        (
            Self {
                calls: Arc::clone(&calls),
                status: status.reader(),
                stalls: stalls.to_vec(),
                released: 2,
            },
            calls,
        )
    }

    async fn call(&self, call: Call) {
        let now = self.status.snapshot();
        self.calls
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push((call, now.phase, now.lock));
        if self.stalls.contains(&call) {
            std::future::pending::<()>().await;
        }
    }
}

impl Outgoing for Scripted {
    async fn quiesce(&mut self, _deadline: Instant) -> QuiesceReport {
        // A stalled quiesce ignores its deadline: the backstop is tested.
        self.call(Call::Quiesce).await;
        QuiesceReport {
            stages_finished: 7,
            stages_aborted: 0,
            log_finished: true,
            takes_joined: true,
            surface_stopped: true,
        }
    }

    async fn release_holds(&mut self) -> Result<u64, BusError> {
        self.call(Call::ReleaseHolds).await;
        Ok(self.released)
    }

    async fn close_pool(&mut self) {
        self.call(Call::ClosePool).await;
    }

    async fn release_lock(self, _closed: PoolsClosed) -> Result<(), ReleaseError> {
        self.call(Call::ReleaseLock).await;
        Ok(())
    }
}

fn running_status() -> StatusReporter {
    let mut status = PipelineStatus::waiting();
    status.lock = LockState::Held;
    status.phase = PipelinePhase::Running;
    StatusReporter::new(status)
}

fn calls_of(calls: &Calls) -> Vec<(Call, PipelinePhase, LockState)> {
    calls.lock().unwrap_or_else(PoisonError::into_inner).clone()
}

const QUIESCE: Duration = Duration::from_secs(3);

/// The steps run once each, in order, each while the status names it, the
/// lock held until the last: quiesce, release the holds, close the pool,
/// release the lock. Then `handed_over` and `released`.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn the_steps_run_in_order_each_reported_while_it_runs() {
    let status = running_status();
    let (outgoing, calls) = Scripted::new(&status, &[]);
    let report = hand_over(outgoing, status.clone(), Instant::now() + QUIESCE).await;
    let held = LockState::Held;
    let at = |step| PipelinePhase::HandingOver(step);
    assert_eq!(
        calls_of(&calls),
        vec![
            (Call::Quiesce, at(HandoverStep::Quiescing), held),
            (Call::ReleaseHolds, at(HandoverStep::ReleasingHolds), held),
            (Call::ClosePool, at(HandoverStep::ClosingPool), held),
            (Call::ReleaseLock, at(HandoverStep::ReleasingLock), held),
        ]
    );
    let after = status.snapshot();
    assert_eq!(after.phase, at(HandoverStep::Done));
    assert_eq!(after.lock, LockState::Released);
    assert_eq!(after.pipeline_text(), "handed_over");
    assert!(report.clean(), "{report:?}");
    assert_eq!(report.released(), Some(2));
    assert_eq!(
        report.took,
        Duration::ZERO,
        "nothing waited on the paused clock"
    );
}

/// A quiesce that ignores its deadline is cut at its backstop: the
/// handover still releases the holds, closes the pool and releases the
/// lock, and ends by the quiesce deadline plus the tail.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn a_stalled_quiesce_still_hands_over_within_the_bound() {
    let status = running_status();
    let (outgoing, calls) = Scripted::new(&status, &[Call::Quiesce]);
    let began = Instant::now();
    let report = hand_over(outgoing, status.clone(), began + QUIESCE).await;
    assert_eq!(
        began.elapsed(),
        QUIESCE + QUIESCE_BACKSTOP,
        "cut at the backstop"
    );
    assert!(began.elapsed() <= QUIESCE + TAIL);
    assert_eq!(
        report.quiesce,
        Bounded::TimedOut {
            after: QUIESCE + QUIESCE_BACKSTOP
        }
    );
    let made: Vec<Call> = calls_of(&calls)
        .into_iter()
        .map(|(call, ..)| call)
        .collect();
    assert_eq!(
        made,
        [
            Call::Quiesce,
            Call::ReleaseHolds,
            Call::ClosePool,
            Call::ReleaseLock
        ],
        "every later step still runs"
    );
    assert_eq!(report.lock, Bounded::Done(Ok(())));
    assert_eq!(status.snapshot().lock, LockState::Released);
    assert!(!report.clean());
}

/// Each later step stalled on its own is cut at its bound, the steps
/// after it still run, and the handover ends within the tail.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn each_stalled_step_is_cut_at_its_bound() {
    for (stalled, bound) in [
        (Call::ReleaseHolds, RELEASE_HOLDS_BOUND),
        (Call::ClosePool, CLOSE_BOUND),
        (Call::ReleaseLock, UNLOCK_BOUND),
    ] {
        let status = running_status();
        let (outgoing, calls) = Scripted::new(&status, &[stalled]);
        let began = Instant::now();
        let report = hand_over(outgoing, status.clone(), began + QUIESCE).await;
        assert_eq!(began.elapsed(), bound, "{stalled:?}: cut at its bound");
        let made: Vec<Call> = calls_of(&calls)
            .into_iter()
            .map(|(call, ..)| call)
            .collect();
        assert_eq!(made.len(), 4, "{stalled:?}: every step ran: {made:?}");
        let timed_out = Bounded::<()>::TimedOut { after: bound };
        match stalled {
            Call::ReleaseHolds => {
                assert_eq!(report.holds, Bounded::TimedOut { after: bound });
                assert_eq!(report.released(), None);
            }
            Call::ClosePool => assert_eq!(report.pool_closed, timed_out),
            Call::ReleaseLock => assert_eq!(report.lock, Bounded::TimedOut { after: bound }),
            Call::Quiesce => unreachable!("not stalled here"),
        }
        let after = status.snapshot();
        assert_eq!(after.lock, LockState::Released, "{stalled:?}");
        assert_eq!(after.handover(), Some(HandoverStep::Done), "{stalled:?}");
    }
}

/// Every step stalled: the handover still ends, by the quiesce deadline
/// plus exactly the tail.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn every_step_stalled_ends_by_the_deadline_and_the_tail() {
    let status = running_status();
    let (outgoing, _calls) = Scripted::new(
        &status,
        &[
            Call::Quiesce,
            Call::ReleaseHolds,
            Call::ClosePool,
            Call::ReleaseLock,
        ],
    );
    let began = Instant::now();
    let report = hand_over(outgoing, status.clone(), began + QUIESCE).await;
    assert_eq!(began.elapsed(), QUIESCE + TAIL);
    assert_eq!(report.took, QUIESCE + TAIL);
    assert_eq!(
        status.snapshot().phase.clone(),
        PipelinePhase::HandingOver(HandoverStep::Done)
    );
}

/// The tail fits inside the default flush timeout with seconds to spare
/// for the quiesce, so the whole handover ends within the flush timeout
/// `serve`'s shutdown already allows for.
#[test]
fn the_tail_fits_inside_the_default_flush_timeout() {
    let flush = ShutdownConfig::default().flush_timeout();
    assert_eq!(
        TAIL,
        QUIESCE_BACKSTOP + RELEASE_HOLDS_BOUND + CLOSE_BOUND + UNLOCK_BOUND
    );
    assert!(
        flush.saturating_sub(TAIL) >= Duration::from_secs(4),
        "tail {TAIL:?}, flush {flush:?}"
    );
}
