//! The fixture's live feed, as `l8_surface::live` defines it:
//!
//! ```text
//! act / fit_projection commits ─▶ Changed* ─▶ Feed::publish ─append─▶ FeedLog (epoch E, seq 1, 2, …;
//!                                                   │                  kept for LiveConfig::retention)
//!                                                   └─send─▶ broadcast (LiveConfig::buffer entries)
//! subscribe(caller, resume): View ─▶ FeedWindow::resume ─▶ Resync | replay | live ─▶ FeedStream
//! ```
//!
//! Every committed change publishes the `Changed` notifications its store
//! would (`actions::changes`, and `Projection` for every job a fit
//! records), converted with `UiEvent::from`, while the state's write lock
//! is still held, so the log's order is the commit order and no event is
//! published before its change is visible. A subscriber reads the log and
//! joins the fan-out under the log's lock, so nothing appended in between
//! is missed. The fixture's watermark never advances and its sessions
//! never end, so it publishes no `Watermark` and no stream ends with
//! `SessionEnded`. The epoch is taken from the clock when the backend is
//! built: a restarted fixture (a new world) is a new log, and a client
//! holding an old cursor resyncs.

mod log;
mod stream;

use std::collections::VecDeque;
use std::num::NonZeroU32;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime};

use crosstalk_spec::events::changed::Changed;
use crosstalk_spec::interfaces::l8_surface::live::{
    FeedEpoch, LiveConfig, LiveCursor, LiveItem, Resume, ResumePlan, UiEvent,
};
use crosstalk_spec::interfaces::l8_surface::{Caller, Permission, QueryError};
use tokio::sync::{RwLock, broadcast};
use tokio::time::Instant;

use super::queries::require;
use crate::backend::Result;

use log::{Entry, FeedLog};
pub use stream::FeedStream;

/// Items a stream may hold undelivered before it ends with `Lagged`.
pub const BUFFER: NonZeroU32 = match NonZeroU32::new(256) {
    Some(buffer) => buffer,
    None => NonZeroU32::MIN,
};
pub const HEARTBEAT: Duration = Duration::from_secs(15);
pub const RETENTION: Duration = Duration::from_secs(15 * 60);

/// The fixture's feed limits.
pub fn config() -> std::result::Result<LiveConfig, QueryError> {
    LiveConfig::new(BUFFER, HEARTBEAT, RETENTION).map_err(|e| QueryError::Store {
        reason: format!("fixture live config: {e:?}"),
    })
}

/// A fresh epoch: milliseconds since the Unix epoch, made distinct per feed
/// within one process.
fn epoch() -> FeedEpoch {
    static BUILT: AtomicU64 = AtomicU64::new(0);
    let millis = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map_or(0, |since| {
            u64::try_from(since.as_millis()).unwrap_or(u64::MAX)
        });
    let built = BUILT.fetch_add(1, Ordering::Relaxed);
    FeedEpoch(millis.saturating_mul(1024).saturating_add(built % 1024))
}

#[derive(Debug)]
pub struct Feed {
    epoch: FeedEpoch,
    config: LiveConfig,
    log: RwLock<FeedLog>,
    fanout: broadcast::Sender<Entry>,
}

impl Feed {
    pub fn new(config: LiveConfig) -> Self {
        let capacity = usize::try_from(config.buffer().get()).unwrap_or(usize::MAX / 4);
        let (fanout, _) = broadcast::channel(capacity);
        Self {
            epoch: epoch(),
            config,
            log: RwLock::new(FeedLog::new(config.retention())),
            fanout,
        }
    }

    #[cfg(test)]
    pub fn epoch(&self) -> FeedEpoch {
        self.epoch
    }

    /// Appends each notification to the log, in order, and sends it to
    /// every stream. Never waits for a stream.
    pub async fn publish(&self, changes: impl IntoIterator<Item = Changed>) {
        let mut log = self.log.write().await;
        let now = Instant::now();
        for changed in changes {
            let entry = log.append(UiEvent::from(changed), now);
            // An error means no stream is subscribed: the entry stays in
            // the log for any that resumes.
            if self.fanout.send(entry).is_err() {
                tracing::debug!(
                    seq = entry.seq,
                    "live entry logged with no stream subscribed"
                );
            }
        }
    }

    /// `LiveFeed::subscribe`: View, then the stream `FeedWindow::resume`
    /// plans for `resume`.
    pub async fn subscribe(&self, caller: &Caller, resume: Resume) -> Result<FeedStream> {
        require(caller, Permission::View)?;
        let mut log = self.log.write().await;
        log.expire(Instant::now());
        let window = log.window(self.epoch).map_err(|e| QueryError::Store {
            reason: format!("fixture feed log: {e:?}"),
        })?;
        let head = window.head();
        let cursor = |seq| LiveCursor {
            epoch: self.epoch,
            seq,
        };
        let pending: VecDeque<LiveItem> = match window.resume(resume) {
            ResumePlan::Live => VecDeque::new(),
            ResumePlan::Resync(reason) => VecDeque::from([LiveItem::Resync {
                cursor: head,
                reason,
            }]),
            ResumePlan::Replay { after } => log
                .after(after)
                .filter(|entry| entry.event.visible_to(caller))
                .map(|entry| LiveItem::Event {
                    cursor: cursor(entry.seq),
                    event: entry.event,
                })
                .collect(),
        };
        // Joined under the log's lock: every entry after the head reaches
        // the receiver.
        let receiver = self.fanout.subscribe();
        Ok(FeedStream::new(
            caller.clone(),
            self.epoch,
            pending,
            receiver,
            head.seq,
            self.config.heartbeat(),
        ))
    }
}
