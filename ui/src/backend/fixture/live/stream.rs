//! One subscription to the fixture's feed: what `FeedWindow::resume`
//! planned (a `Resync`, or the replayed entries), then live entries from
//! the fan-out, filtered by `UiEvent::visible_to`, with a heartbeat every
//! `LiveConfig::heartbeat`.
//!
//! The fan-out is a `tokio::sync::broadcast` channel holding
//! `LiveConfig::buffer` entries: the feed never waits for a stream, and a
//! stream that falls further behind than that reads `Lagged` and ends
//! with `LiveEnd::Lagged`. Entries the replay already sent are skipped by
//! sequence number.

use std::collections::VecDeque;
use std::time::Duration;

use crosstalk_spec::interfaces::l8_surface::Caller;
use crosstalk_spec::interfaces::l8_surface::live::{
    FeedEpoch, LiveCursor, LiveEnd, LiveItem, LiveStream,
};
use tokio::sync::broadcast::Receiver;
use tokio::sync::broadcast::error::RecvError;
use tokio::time::Instant;

use super::log::Entry;

/// `delay` from now (now, should that overflow the clock).
fn after(delay: Duration) -> Instant {
    let now = Instant::now();
    now.checked_add(delay).unwrap_or(now)
}

/// The stream `LiveFeed::subscribe` returns.
#[derive(Debug)]
pub struct FeedStream {
    caller: Caller,
    epoch: FeedEpoch,
    /// Planned items not yet returned: a resync, or replayed events.
    pending: VecDeque<LiveItem>,
    receiver: Receiver<Entry>,
    /// The newest entry the stream has passed, delivered or passed over.
    passed: u64,
    heartbeat: Duration,
    next_beat: Instant,
    ended: Option<LiveEnd>,
}

impl FeedStream {
    pub(super) fn new(
        caller: Caller,
        epoch: FeedEpoch,
        pending: VecDeque<LiveItem>,
        receiver: Receiver<Entry>,
        passed: u64,
        heartbeat: Duration,
    ) -> Self {
        Self {
            caller,
            epoch,
            pending,
            receiver,
            passed,
            heartbeat,
            next_beat: after(heartbeat),
            ended: None,
        }
    }

    fn cursor(&self, seq: u64) -> LiveCursor {
        LiveCursor {
            epoch: self.epoch,
            seq,
        }
    }

    fn end(&mut self, end: LiveEnd) -> Result<LiveItem, LiveEnd> {
        self.ended = Some(end);
        Err(end)
    }
}

impl LiveStream for FeedStream {
    async fn next(&mut self) -> Result<LiveItem, LiveEnd> {
        if let Some(end) = self.ended {
            return Err(end);
        }
        if let Some(item) = self.pending.pop_front() {
            return Ok(item);
        }
        loop {
            tokio::select! {
                received = self.receiver.recv() => match received {
                    Ok(entry) if entry.seq <= self.passed => {}
                    Ok(entry) => {
                        self.passed = entry.seq;
                        if entry.event.visible_to(&self.caller) {
                            return Ok(LiveItem::Event {
                                cursor: self.cursor(entry.seq),
                                event: entry.event,
                            });
                        }
                    }
                    Err(RecvError::Lagged(_)) => return self.end(LiveEnd::Lagged),
                    Err(RecvError::Closed) => return self.end(LiveEnd::ShuttingDown),
                },
                () = tokio::time::sleep_until(self.next_beat) => {
                    self.next_beat = after(self.heartbeat);
                    return Ok(LiveItem::Heartbeat {
                        cursor: self.cursor(self.passed),
                    });
                }
            }
        }
    }
}
