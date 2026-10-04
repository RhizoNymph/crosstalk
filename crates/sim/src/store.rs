//! [`FaultyStore`]: the store fault pattern. Wraps any store and injects
//! the faults of a [`StoreFaults`] plan around each call.
//!
//! [`FaultyStore::call`] is the generic helper: it takes the store's own
//! (lazy) call future and a function that turns an [`InjectedFault`] into
//! the store's error type, and around the call it may add latency, fail
//! before the call runs, fail after it committed, or crash the node after
//! it committed. A transaction either commits or does not, so these are
//! the observable outcomes of a crash mid-transaction: the caller either
//! knows it failed, or does not know that it succeeded.
//!
//! The kit implements the spec's L2 stores ([`BlobStore`],
//! [`DeadLetterStore`]) for `FaultyStore`. For another store trait, a test
//! crate writes a local wrapper type and delegates each method through
//! [`FaultyStore::call`] (the orphan rule keeps it from implementing a spec
//! trait for `FaultyStore` itself), or the impl is added here.

use std::fmt;
use std::sync::{Arc, Mutex, PoisonError};

use crosstalk_spec::ids::{EventId, MessageHash};
use crosstalk_spec::interfaces::l2_transport::{
    BlobError, BlobStore, BusError, ConsumerGroup, DeadLetter, DeadLetterStore,
};
use crosstalk_spec::paging::{DeadLetterList, Page, PageRequest};

use crate::node::NodeHandle;
use crate::plan::StoreFaults;
use crate::rng::SimRng;
use crate::trace::{FaultKind, FaultSite};

/// Which failure was injected into a store call.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum StoreFaultKind {
    /// The call did not run.
    FailBefore,
    /// The call ran and succeeded; its result was replaced by this failure.
    FailAfter,
}

/// An injected store failure, for the wrapper to turn into the store's own
/// error type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct InjectedFault {
    pub kind: StoreFaultKind,
    pub op: &'static str,
}

impl fmt::Display for InjectedFault {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.kind {
            StoreFaultKind::FailBefore => write!(f, "sim: {} failed before running", self.op),
            StoreFaultKind::FailAfter => write!(f, "sim: {} failed after committing", self.op),
        }
    }
}

/// What one call draws, all at once so a call's faults do not depend on
/// how far it got.
struct Draw {
    latency: Option<std::time::Duration>,
    fail_before: bool,
    crash_after: bool,
    fail_after: bool,
}

#[derive(Debug)]
struct Injector {
    faults: StoreFaults,
    rng: Mutex<SimRng>,
    node: NodeHandle,
}

impl Injector {
    fn draw(&self) -> Draw {
        let mut rng = self.rng.lock().unwrap_or_else(PoisonError::into_inner);
        let latency = self.faults.latency.and_then(|timed| {
            rng.chance(timed.chance)
                .then(|| rng.duration_in(timed.within))
        });
        let fail_before = rng.chance(self.faults.fail_before);
        let crash_after = !fail_before && rng.chance(self.faults.crash_after);
        let fail_after = !fail_before && !crash_after && rng.chance(self.faults.fail_after);
        Draw {
            latency,
            fail_before,
            crash_after,
            fail_after,
        }
    }
}

/// A store whose calls go through fault injection. Clones share the
/// inner store (when `S` is a shared handle), the plan and the random
/// stream.
#[derive(Debug)]
pub struct FaultyStore<S> {
    inner: S,
    injector: Arc<Injector>,
}

impl<S: Clone> Clone for FaultyStore<S> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            injector: Arc::clone(&self.injector),
        }
    }
}

impl<S> FaultyStore<S> {
    pub fn new(inner: S, faults: StoreFaults, rng: SimRng, node: NodeHandle) -> Self {
        Self {
            inner,
            injector: Arc::new(Injector {
                faults,
                rng: Mutex::new(rng),
                node,
            }),
        }
    }

    /// The wrapped store, for calls that should see no faults.
    pub fn inner(&self) -> &S {
        &self.inner
    }

    /// Runs `call` (a store call's future, not yet polled) under the plan.
    /// `op` names the operation in the trace; `injected` builds the
    /// store's error for an injected failure.
    ///
    /// The store's work must happen when the future is polled, as it does
    /// for an `async fn`: a failure before the call drops the future
    /// without polling it.
    pub async fn call<T, E, Fut>(
        &self,
        op: &'static str,
        injected: impl FnOnce(InjectedFault) -> E,
        call: Fut,
    ) -> Result<T, E>
    where
        Fut: Future<Output = Result<T, E>>,
    {
        let draw = self.injector.draw();
        let node = &self.injector.node;
        let site = FaultSite::Store { op };
        if let Some(latency) = draw.latency {
            node.fault(FaultKind::StoreLatency, site.clone());
            tokio::time::sleep(latency).await;
        }
        if draw.fail_before {
            node.fault(FaultKind::StoreFailBefore, site);
            return Err(injected(InjectedFault {
                kind: StoreFaultKind::FailBefore,
                op,
            }));
        }
        let result = call.await;
        if result.is_ok() {
            if draw.crash_after {
                match node.crash(FaultKind::StoreCrashAfter, site).await {}
            }
            if draw.fail_after {
                node.fault(FaultKind::StoreFailAfter, site);
                return Err(injected(InjectedFault {
                    kind: StoreFaultKind::FailAfter,
                    op,
                }));
            }
        }
        result
    }
}

fn blob_unavailable(fault: InjectedFault) -> BlobError {
    BlobError::Unavailable {
        reason: fault.to_string(),
    }
}

/// Injected failures read as [`BlobError::Unavailable`].
impl<S: BlobStore + Sync> BlobStore for FaultyStore<S> {
    fn put(&self, bytes: &[u8]) -> impl Future<Output = Result<MessageHash, BlobError>> + Send {
        self.call("blob.put", blob_unavailable, self.inner.put(bytes))
    }

    fn get(
        &self,
        hash: MessageHash,
    ) -> impl Future<Output = Result<Option<Vec<u8>>, BlobError>> + Send {
        self.call("blob.get", blob_unavailable, self.inner.get(hash))
    }
}

fn bus_disconnected(fault: InjectedFault) -> BusError {
    tracing::debug!(op = fault.op, kind = ?fault.kind, "sim dead-letter store fault");
    BusError::Disconnected
}

/// Injected failures read as [`BusError::Disconnected`].
impl<S: DeadLetterStore + Sync> DeadLetterStore for FaultyStore<S> {
    fn put(&self, letter: DeadLetter) -> impl Future<Output = Result<(), BusError>> + Send {
        self.call("dead_letter.put", bus_disconnected, self.inner.put(letter))
    }

    fn replay(
        &self,
        group: &ConsumerGroup,
        id: EventId,
    ) -> impl Future<Output = Result<(), BusError>> + Send {
        self.call(
            "dead_letter.replay",
            bus_disconnected,
            self.inner.replay(group, id),
        )
    }

    fn list(
        &self,
        group: Option<&ConsumerGroup>,
        page: &PageRequest<DeadLetterList>,
    ) -> impl Future<Output = Result<Page<DeadLetter, DeadLetterList>, BusError>> + Send {
        self.call(
            "dead_letter.list",
            bus_disconnected,
            self.inner.list(group, page),
        )
    }
}
