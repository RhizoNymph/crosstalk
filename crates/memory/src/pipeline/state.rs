//! The lock around a store's plain data.
//!
//! The pipeline stores keep their state behind a `std::sync::RwLock`, not a
//! `tokio` one, for two reasons:
//!
//! - `AgentDirectory::canonical` and `ChannelDirectory::canonical` are
//!   synchronous, and are called from inside async tasks, where a `tokio`
//!   lock can only be taken with `try_read` (which fails under contention)
//!   or `blocking_read` (which panics inside a runtime).
//! - No store method awaits while it holds the lock: every operation is a
//!   pure function over the state, run inside one critical section. That
//!   critical section is the store's transaction, and holding a `std` lock
//!   there is what `tokio` recommends for short, non-awaiting sections.
//!
//! The futures the trait methods return hold no guard, so they are `Send`
//! whenever the store is `Sync`.

use std::sync::{Arc, PoisonError, RwLock, RwLockReadGuard, RwLockWriteGuard};

/// Shared, lock-protected state. Cloning shares the state: every clone is a
/// handle on one store.
#[derive(Debug, Default)]
pub struct State<T> {
    inner: Arc<RwLock<T>>,
}

impl<T> Clone for State<T> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<T> State<T> {
    pub fn new(value: T) -> Self {
        Self {
            inner: Arc::new(RwLock::new(value)),
        }
    }

    /// A consistent snapshot for a read.
    ///
    /// A poisoned lock is recovered rather than reported: every write runs
    /// its checks before it changes anything, so a panic inside a critical
    /// section (a bug) leaves the data as the last completed write did.
    pub fn read(&self) -> RwLockReadGuard<'_, T> {
        self.inner.read().unwrap_or_else(PoisonError::into_inner)
    }

    /// Exclusive access for one transaction. Poisoning is recovered as in
    /// [`State::read`].
    pub fn write(&self) -> RwLockWriteGuard<'_, T> {
        self.inner.write().unwrap_or_else(PoisonError::into_inner)
    }
}
