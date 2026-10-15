//! The gate's watch over a scripted group check on tokio's paused clock:
//! it opens the gate at once when every group already exists, keeps it
//! closed (polling every [`GATE_POLL`]) while one is missing or the check
//! fails, never queries while this process holds the lock, and ends
//! without a query once the pipeline's subscribe opened the gate.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use crosstalk_store::DbFailure;

use super::{
    GATE_POLL, GateWatch, GroupCheck, GroupCheckError, GroupsFound, WatchEnd, missing_from, watch,
};
use crate::live::pg::groups::{ExchangeLogGroup, PipelineGroups};
use crate::live::recovery::{LockState, PipelineStatus, StatusReporter};
use crate::spool::Gate;

/// How the scripted check answers one look.
#[derive(Debug, Clone)]
enum Answer {
    Missing(Vec<String>),
    All,
    Fails,
    TimesOut,
}

/// A group check answering from a script; past its end, `Missing`.
#[derive(Clone, Default)]
struct Scripted {
    answers: Arc<Mutex<VecDeque<Answer>>>,
    looks: Arc<AtomicU64>,
    asked: Arc<Mutex<Vec<Vec<String>>>>,
}

impl Scripted {
    fn answering(answers: impl IntoIterator<Item = Answer>) -> Self {
        Self {
            answers: Arc::new(Mutex::new(answers.into_iter().collect())),
            ..Self::default()
        }
    }

    fn looks(&self) -> u64 {
        self.looks.load(Ordering::SeqCst)
    }
}

impl GroupCheck for Scripted {
    async fn found(&self, names: &[String]) -> Result<GroupsFound, GroupCheckError> {
        self.looks.fetch_add(1, Ordering::SeqCst);
        self.asked
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(names.to_vec());
        let next = self
            .answers
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .pop_front();
        match next {
            Some(Answer::All) => Ok(missing_from(Vec::new())),
            Some(Answer::Missing(missing)) => Ok(missing_from(missing)),
            Some(Answer::Fails) => Err(GroupCheckError::Failed(DbFailure::ConnectionLost)),
            Some(Answer::TimesOut) => Err(GroupCheckError::TimedOut { bound_ms: 20_000 }),
            None => Ok(missing_from(vec!["live-l7-topology".to_owned()])),
        }
    }
}

fn groups() -> PipelineGroups {
    PipelineGroups::new(ExchangeLogGroup::Subscribed)
}

fn status(lock: LockState) -> StatusReporter {
    let status = StatusReporter::new(PipelineStatus::waiting());
    status.update(|status| status.lock = lock);
    status
}

/// Let the watch's task run what is due now.
async fn settle() {
    for _ in 0..16 {
        tokio::task::yield_now().await;
    }
}

/// A process that restarts while another holds the lock, on a database
/// whose groups exist (the node0 fault run): the first look, at start,
/// finds them and opens the gate. Nothing waits for the lock.
#[tokio::test(start_paused = true)]
async fn the_gate_opens_at_start_when_every_group_already_exists() {
    let gate = Gate::closed();
    let check = Scripted::answering([Answer::All]);
    let reporter = status(LockState::HeldElsewhere);
    let task = tokio::spawn(watch(
        gate.clone(),
        groups(),
        check.clone(),
        reporter.reader(),
    ));
    settle().await;
    assert!(gate.is_open(), "the gate opened at the first look");
    assert_eq!(check.looks(), 1);
    assert_eq!(task.await.expect("the watch ends"), WatchEnd::Found);
    let asked = check
        .asked
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .clone();
    assert_eq!(
        asked,
        vec![groups().names()],
        "the watch asks for exactly the pipeline's groups"
    );
}

/// A fresh database: while any group is missing the gate stays closed
/// (captures spool) and the watch looks again every [`GATE_POLL`]; the
/// look that finds them all opens it.
#[tokio::test(start_paused = true)]
async fn the_gate_stays_closed_while_a_group_is_missing_and_opens_once_all_exist() {
    let gate = Gate::closed();
    let missing = || Answer::Missing(vec!["live-l5-flow".to_owned()]);
    let check = Scripted::answering([missing(), missing(), missing(), missing(), Answer::All]);
    let reporter = status(LockState::NotTaken);
    let task = tokio::spawn(watch(
        gate.clone(),
        groups(),
        check.clone(),
        reporter.reader(),
    ));
    settle().await;
    assert_eq!(check.looks(), 1, "a look at start");
    for look in 2..=4 {
        tokio::time::advance(GATE_POLL).await;
        settle().await;
        assert_eq!(check.looks(), look, "one look per poll");
        assert!(!gate.is_open(), "closed while a group is missing");
    }
    tokio::time::advance(GATE_POLL - Duration::from_millis(1)).await;
    settle().await;
    assert_eq!(check.looks(), 4, "no look before the poll is due");
    tokio::time::advance(Duration::from_millis(1)).await;
    settle().await;
    assert!(gate.is_open(), "opened by the look that found every group");
    assert_eq!(task.await.expect("the watch ends"), WatchEnd::Found);
    assert_eq!(check.looks(), 5);
}

/// A check that fails or does not answer within its bound never opens
/// the gate; the watch keeps looking and opens it once a look succeeds.
#[tokio::test(start_paused = true)]
async fn a_failed_or_timed_out_look_never_opens_the_gate() {
    let gate = Gate::closed();
    let check = Scripted::answering([Answer::Fails, Answer::TimesOut, Answer::Fails, Answer::All]);
    let reporter = status(LockState::Lost);
    let task = tokio::spawn(watch(
        gate.clone(),
        groups(),
        check.clone(),
        reporter.reader(),
    ));
    for look in 1..=3 {
        settle().await;
        assert_eq!(check.looks(), look);
        assert!(!gate.is_open(), "a failed look keeps the gate closed");
        tokio::time::advance(GATE_POLL).await;
    }
    settle().await;
    assert!(gate.is_open());
    assert_eq!(task.await.expect("the watch ends"), WatchEnd::Found);
}

/// While this process holds the lock its own pipeline subscribes the
/// groups and opens the gate: the watch does not query meanwhile, and
/// looks again once the lock is held elsewhere.
#[tokio::test(start_paused = true)]
async fn the_watch_does_not_look_while_this_process_holds_the_lock() {
    let gate = Gate::closed();
    let check = Scripted::answering([Answer::All]);
    let reporter = status(LockState::Held);
    let task = tokio::spawn(watch(
        gate.clone(),
        groups(),
        check.clone(),
        reporter.reader(),
    ));
    for _ in 0..10 {
        settle().await;
        tokio::time::advance(GATE_POLL).await;
    }
    settle().await;
    assert_eq!(check.looks(), 0, "no look while the lock is held");
    assert!(!gate.is_open());

    reporter.update(|status| status.lock = LockState::HeldElsewhere);
    tokio::time::advance(GATE_POLL).await;
    settle().await;
    assert_eq!(check.looks(), 1);
    assert!(gate.is_open());
    assert_eq!(task.await.expect("the watch ends"), WatchEnd::Found);
}

/// The pipeline's subscribe opened the gate first (a fresh database, this
/// process took the lock): the watch ends at its next tick without a look.
#[tokio::test(start_paused = true)]
async fn the_watch_ends_without_a_look_once_the_pipeline_opened_the_gate() {
    let gate = Gate::closed();
    let check = Scripted::answering([]);
    let reporter = status(LockState::Held);
    let task = tokio::spawn(watch(
        gate.clone(),
        groups(),
        check.clone(),
        reporter.reader(),
    ));
    settle().await;
    gate.open();
    tokio::time::advance(GATE_POLL).await;
    settle().await;
    assert_eq!(
        task.await.expect("the watch ends"),
        WatchEnd::OpenedBySubscribe
    );
    assert_eq!(check.looks(), 0);
}

/// Dropping the handle (the pipeline task ending, or aborted at a
/// shutdown's deadline) stops the watch.
#[tokio::test(start_paused = true)]
async fn dropping_the_handle_stops_the_watch() {
    let gate = Gate::closed();
    let check = Scripted::answering([]);
    let reporter = status(LockState::HeldElsewhere);
    let handle = GateWatch::spawn(watch(
        gate.clone(),
        groups(),
        check.clone(),
        reporter.reader(),
    ));
    settle().await;
    assert_eq!(check.looks(), 1);
    drop(handle);
    for _ in 0..5 {
        tokio::time::advance(GATE_POLL).await;
        settle().await;
    }
    assert_eq!(check.looks(), 1, "no look after the handle was dropped");
    assert!(!gate.is_open());
}

/// The answer's type: no missing group is "every group exists"; the
/// missing ones are kept, in order.
#[test]
fn an_empty_missing_list_is_every_group_found() {
    assert_eq!(missing_from(Vec::new()), GroupsFound::All);
    match missing_from(vec![
        "exchange-log".to_owned(),
        "live-l3-reconstruct".to_owned(),
    ]) {
        GroupsFound::Missing(missing) => assert_eq!(
            missing.names(),
            ["exchange-log".to_owned(), "live-l3-reconstruct".to_owned()]
        ),
        GroupsFound::All => panic!("two groups are missing"),
    }
}

/// The poll is about a second: a standby's captures reach the bus within
/// about a second of the last group existing.
#[test]
fn the_poll_is_about_a_second() {
    assert_eq!(GATE_POLL, Duration::from_secs(1));
}
