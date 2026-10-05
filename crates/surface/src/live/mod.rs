//! The live feed ([`LiveFeed`]): an id-only log of every `Changed` the
//! stores publish, streamed to each subscriber with resume, resync,
//! heartbeats and an end reason.
//!
//! - `log`: the feed log of one epoch, pruned by retention.
//! - `writer`: the task that owns the log and every stream's sending end
//!   ([`FeedWriter`]), and the bus consumer that feeds it.
//! - `stream`: one subscriber's [`FeedStream`].
//!
//! The surface's `subscribe` checks View, then asks the writer for a
//! stream planned by `FeedWindow::resume`.

mod log;
mod stream;
mod writer;

use crosstalk_spec::events::changed::Changed;
use crosstalk_spec::ids::OperatorId;
use crosstalk_spec::interfaces::l8_surface::audit::ConfigChange;
use crosstalk_spec::interfaces::l8_surface::live::{LiveConfig, LiveCursor, LiveFeed, Resume};
use crosstalk_spec::interfaces::l8_surface::{Caller, Permission, QueryError};
use tokio::sync::{mpsc, oneshot, watch};

use crate::service::{Surface, require};
use crate::stores::SurfaceStores;

pub use stream::FeedStream;
pub use writer::FeedWriter;

use writer::Command;

/// The feed writer has stopped.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("the live feed writer has stopped")]
pub struct FeedClosed;

/// A handle on a running feed writer. Clones share the writer.
#[derive(Debug, Clone)]
pub struct FeedHandle {
    commands: mpsc::Sender<Command>,
    head: watch::Receiver<LiveCursor>,
    config: LiveConfig,
}

impl FeedHandle {
    /// Append `changed` as the next entry, returning its cursor once it is
    /// in the log and has been offered to every open stream.
    pub async fn append(&self, changed: Changed) -> Result<LiveCursor, FeedClosed> {
        let (done, appended) = oneshot::channel();
        self.commands
            .send(Command::Append { changed, done })
            .await
            .map_err(|_| FeedClosed)?;
        appended.await.map_err(|_| FeedClosed)
    }

    /// A stream for `caller`, starting as `FeedWindow::resume` plans for
    /// `resume`. Checks no permission: [`LiveFeed::subscribe`] does.
    pub async fn open(&self, caller: &Caller, resume: Resume) -> Result<FeedStream, FeedClosed> {
        let (reply, stream) = oneshot::channel();
        self.commands
            .send(Command::Subscribe {
                caller: caller.clone(),
                resume,
                reply,
            })
            .await
            .map_err(|_| FeedClosed)?;
        stream.await.map_err(|_| FeedClosed)
    }

    /// End every open stream of `operator` with `SessionEnded`: its session
    /// expired or was revoked. Returns how many streams it ended.
    pub async fn end_sessions(&self, operator: OperatorId) -> Result<usize, FeedClosed> {
        self.end(Some(operator)).await
    }

    /// End every open stream with `SessionEnded`.
    pub async fn end_all_sessions(&self) -> Result<usize, FeedClosed> {
        self.end(None).await
    }

    /// End the streams a config load invalidated: every stream when it
    /// switched the access mode, otherwise the streams of each operator it
    /// changed or removed. Call it with the changes `OperatorStore::load`
    /// returned, before serving anything appended after the load.
    pub async fn config_loaded(&self, changes: &[ConfigChange]) -> Result<usize, FeedClosed> {
        if changes
            .iter()
            .any(|change| matches!(change, ConfigChange::SetAccessMode(_)))
        {
            return self.end_all_sessions().await;
        }
        let mut ended = 0;
        for change in changes {
            match change {
                ConfigChange::SetOperator { operator, .. }
                | ConfigChange::RemoveOperator { operator } => {
                    ended += self.end_sessions(*operator).await?;
                }
                ConfigChange::DeclareChannel { .. }
                | ConfigChange::SetPolicy { .. }
                | ConfigChange::RegisterAgent { .. }
                | ConfigChange::ProvisionRule { .. }
                | ConfigChange::SetAccessMode(_)
                | ConfigChange::SetSink { .. }
                | ConfigChange::RemoveSink { .. }
                | ConfigChange::SetTopicRetention(_)
                | ConfigChange::SetFrameRetention { .. } => {}
            }
        }
        Ok(ended)
    }

    /// Stop the writer: every open stream ends with `ShuttingDown`.
    pub async fn shutdown(&self) {
        // Already stopped is stopped.
        let _ = self.commands.send(Command::Shutdown).await;
    }

    /// The cursor of the newest entry.
    pub fn head(&self) -> LiveCursor {
        *self.head.borrow()
    }

    /// The feed's limits.
    pub fn config(&self) -> LiveConfig {
        self.config
    }

    async fn end(&self, operator: Option<OperatorId>) -> Result<usize, FeedClosed> {
        let (done, ended) = oneshot::channel();
        self.commands
            .send(Command::EndSessions { operator, done })
            .await
            .map_err(|_| FeedClosed)?;
        ended.await.map_err(|_| FeedClosed)
    }
}

impl<S: SurfaceStores> LiveFeed for Surface<S> {
    type Stream = FeedStream;

    async fn subscribe(&self, caller: &Caller, resume: Resume) -> Result<FeedStream, QueryError> {
        require(caller, Permission::View)?;
        self.feed
            .open(caller, resume)
            .await
            .map_err(|error| QueryError::Store {
                reason: error.to_string(),
            })
    }
}
