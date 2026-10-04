//! The locks around a store's plain data.
//!
//! The L3 to L5 stores keep their state in a [`State`], behind a
//! `std::sync::RwLock`; the L6 to L8 stores behind a `std::sync::Mutex`
//! taken with [`lock`]. Neither is a `tokio` lock, for two reasons:
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

use std::sync::{Arc, Mutex, MutexGuard, PoisonError, RwLock, RwLockReadGuard, RwLockWriteGuard};

/// Locks `mutex`, recovering the state from a poisoned lock. Every store
/// mutates its state only through methods that leave it consistent before
/// they can panic, so a poisoned lock still guards a valid state.
pub fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

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
