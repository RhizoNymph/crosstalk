//! Which of the role's long-running tasks are still running, for
//! `/readyz`.
//!
//! [`Tasks::spawn`] starts a task with a guard that marks it stopped when
//! the task's future ends or is dropped (on completion, panic or abort), so
//! the flag needs no cooperation from the task. The flags are the only
//! state shared between the tasks and the ops listener: one `AtomicBool`
//! per task, written once.

use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use tokio::task::JoinHandle;

/// The running flags of a set of named tasks.
#[derive(Debug, Clone, Default)]
pub struct Tasks {
    flags: Vec<(&'static str, Flag)>,
}

/// How one entry knows whether it runs.
#[derive(Clone)]
enum Flag {
    /// A task spawned here, marked stopped by its guard.
    Spawned(Arc<AtomicBool>),
    /// Something tracked elsewhere (a group of tasks), asked each time.
    Probe(Arc<dyn Fn() -> bool + Send + Sync>),
}

impl std::fmt::Debug for Flag {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Spawned(flag) => write!(f, "Spawned({})", flag.load(Ordering::Acquire)),
            Self::Probe(_) => f.write_str("Probe"),
        }
    }
}

impl Flag {
    fn running(&self) -> bool {
        match self {
            Self::Spawned(flag) => flag.load(Ordering::Acquire),
            Self::Probe(probe) => probe(),
        }
    }
}

/// Marks its task stopped when dropped.
struct Running(Arc<AtomicBool>);

impl Drop for Running {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}

impl Tasks {
    pub fn new() -> Self {
        Self::default()
    }

    /// Spawn `task` as `name`, tracked from now on.
    pub fn spawn<F>(&mut self, name: &'static str, task: F) -> JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        let flag = Arc::new(AtomicBool::new(true));
        let guard = Running(Arc::clone(&flag));
        self.flags.push((name, Flag::Spawned(flag)));
        tokio::spawn(async move {
            let _guard = guard;
            task.await
        })
    }

    /// Track `name` as running while `probe` says so: for a group of tasks
    /// reported as one (`live`: every layer stage).
    pub fn probe(&mut self, name: &'static str, probe: impl Fn() -> bool + Send + Sync + 'static) {
        self.flags.push((name, Flag::Probe(Arc::new(probe))));
    }

    /// Each task's name and whether it is still running, in spawn order.
    pub fn states(&self) -> Vec<(&'static str, bool)> {
        self.flags
            .iter()
            .map(|(name, flag)| (*name, flag.running()))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_task_is_running_until_it_ends_or_is_aborted() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("a runtime");
        runtime.block_on(async {
            let mut tasks = Tasks::new();
            let (finish, finished) = tokio::sync::oneshot::channel::<()>();
            let ends = tasks.spawn("ends", async move {
                let _ = finished.await;
            });
            let aborted = tasks.spawn("aborted", std::future::pending::<()>());
            assert_eq!(tasks.states(), vec![("ends", true), ("aborted", true)]);
            let _ = finish.send(());
            let _ = ends.await;
            aborted.abort();
            let _ = aborted.await;
            assert_eq!(tasks.states(), vec![("ends", false), ("aborted", false)]);
        });
    }
}
