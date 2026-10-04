//! Simulated nodes: the unit a crash takes down and a supervisor restarts.
//!
//! A crash fault (crash on publish, crash before ack, crash after a store
//! commit) is injected by the wrapper the node's code calls through: the
//! faulted call records the crash, reports it on the node's crash channel,
//! and never returns, so nothing the node would have done next happens.
//! The node's [`Node::supervise`] loop receives the report, aborts the
//! node's root task (dropping its subscriptions and everything else it
//! owns) and starts a fresh incarnation, as a process restart would.
//!
//! Everything a node runs must be owned by its root task (await it, or
//! hold it in a `tokio::task::JoinSet`, which aborts its tasks on drop), or
//! it survives the crash.

use std::any::Any;
use std::convert::Infallible;
use std::future::Future;

use tokio::sync::mpsc;
use tokio::task::JoinError;

use crate::trace::{FaultEvent, FaultKind, FaultSite, NodeName, TraceEvent, Tracer};

/// What fault wrappers hold: the node's name, its crash channel and the
/// tracer. Cheap to clone.
#[derive(Debug, Clone)]
pub struct NodeHandle {
    name: NodeName,
    crashes: mpsc::UnboundedSender<FaultEvent>,
    tracer: Tracer,
}

impl NodeHandle {
    pub fn name(&self) -> &NodeName {
        &self.name
    }

    pub fn tracer(&self) -> &Tracer {
        &self.tracer
    }

    /// Records a fault that does not take the node down.
    pub(crate) fn fault(&self, kind: FaultKind, site: FaultSite) {
        self.tracer.fault(kind, &self.name, site);
    }

    /// Records the crash, reports it to the node's supervisor and never
    /// completes. Without a supervisor the calling task simply hangs; a
    /// scenario waiting on it then ends at the time limit.
    pub(crate) async fn crash(&self, kind: FaultKind, site: FaultSite) -> Infallible {
        self.tracer.fault(kind, &self.name, site.clone());
        let event = FaultEvent {
            kind,
            node: self.name.clone(),
            site,
        };
        if self.crashes.send(event).is_err() {
            tracing::debug!(node = %self.name, "sim crash with no supervisor listening");
        }
        std::future::pending::<Infallible>().await
    }
}

/// Which run of a supervised node this is: 0 for the first, one more per
/// restart.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Incarnation(pub u32);

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SuperviseError {
    #[error("node {node} crashed more than {restarts} times")]
    RestartBudgetExhausted { node: NodeName, restarts: u32 },
    #[error("node {node} panicked: {message}")]
    Panicked { node: NodeName, message: String },
    #[error("node {node}'s task was cancelled")]
    Cancelled { node: NodeName },
}

/// A simulated node. Built by [`SimCtx::node`](crate::SimCtx::node).
#[derive(Debug)]
pub struct Node {
    handle: NodeHandle,
    crashes: mpsc::UnboundedReceiver<FaultEvent>,
}

impl Node {
    pub(crate) fn new(name: &str, tracer: Tracer) -> Self {
        let (tx, rx) = mpsc::unbounded_channel();
        Self {
            handle: NodeHandle {
                name: NodeName::new(name),
                crashes: tx,
                tracer,
            },
            crashes: rx,
        }
    }

    pub fn name(&self) -> &NodeName {
        &self.handle.name
    }

    /// The handle to give this node's fault wrappers.
    pub fn handle(&self) -> NodeHandle {
        self.handle.clone()
    }

    /// The next crash reported on this node, for a hand-written supervisor.
    pub async fn next_crash(&mut self) -> FaultEvent {
        match self.crashes.recv().await {
            Some(event) => event,
            // The node holds a sender in its own handle, so the channel
            // never closes while `self` lives.
            None => std::future::pending().await,
        }
    }

    /// Runs `factory(incarnation)` as the node's root task, restarting it
    /// with the next incarnation after each crash, up to `restarts` times.
    /// Returns the output of the first incarnation that finishes.
    ///
    /// Crashes reported before an incarnation starts (by a task the last
    /// incarnation left behind) count against the new one too, so a node
    /// must own every task it runs.
    pub async fn supervise<F, Fut>(
        &mut self,
        restarts: u32,
        mut factory: F,
    ) -> Result<Fut::Output, SuperviseError>
    where
        F: FnMut(Incarnation) -> Fut,
        Fut: Future + Send + 'static,
        Fut::Output: Send + 'static,
    {
        let mut incarnation = Incarnation(0);
        loop {
            let mut task = tokio::spawn(factory(incarnation));
            tokio::select! {
                biased;
                joined = &mut task => {
                    return joined.map_err(|error| self.join_error(error));
                }
                crash = self.next_crash() => {
                    task.abort();
                    // The task is pending in the crashed call or at another
                    // await; abort ends it at that point. Its result is the
                    // cancellation, unless it finished first.
                    if let Ok(output) = task.await {
                        tracing::debug!(node = %self.name(), kind = ?crash.kind, "sim crash raced completion");
                        return Ok(output);
                    }
                    if incarnation.0 >= restarts {
                        return Err(SuperviseError::RestartBudgetExhausted {
                            node: self.name().clone(),
                            restarts,
                        });
                    }
                    incarnation = Incarnation(incarnation.0 + 1);
                    self.handle.tracer.record(TraceEvent::Restart {
                        node: self.name().clone(),
                        incarnation: incarnation.0,
                    });
                }
            }
        }
    }

    fn join_error(&self, error: JoinError) -> SuperviseError {
        if error.is_panic() {
            SuperviseError::Panicked {
                node: self.name().clone(),
                message: panic_message(error.into_panic().as_ref()),
            }
        } else {
            SuperviseError::Cancelled {
                node: self.name().clone(),
            }
        }
    }
}

/// The text of a panic payload: the `&str` or `String` `panic!` carries.
pub(crate) fn panic_message(payload: &(dyn Any + Send)) -> String {
    if let Some(text) = payload.downcast_ref::<&str>() {
        (*text).to_owned()
    } else if let Some(text) = payload.downcast_ref::<String>() {
        text.clone()
    } else {
        "a panic with a non-text payload".to_owned()
    }
}
