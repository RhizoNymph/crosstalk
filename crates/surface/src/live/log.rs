//! The feed log: the events of one incarnation, numbered from 1, kept for
//! the configured retention. Plain data; the feed writer owns it.

use std::collections::VecDeque;
use std::time::Duration;

use crosstalk_spec::interfaces::l8_surface::live::{
    FeedEpoch, FeedWindow, FloorAboveHead, LiveCursor, UiEvent,
};
use tokio::time::Instant;

/// One appended event and when it was appended (monotonic time, so
/// retention never depends on two wall-clock readings).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Entry {
    pub seq: u64,
    pub event: UiEvent,
    pub appended: Instant,
}

/// Entries `floor + 1 ..= head` of one epoch.
#[derive(Debug)]
pub(crate) struct FeedLog {
    epoch: FeedEpoch,
    /// The last entry retention dropped; 0 if none.
    floor: u64,
    /// The last entry appended; 0 if none.
    head: u64,
    entries: VecDeque<Entry>,
}

impl FeedLog {
    pub fn new(epoch: FeedEpoch) -> Self {
        Self {
            epoch,
            floor: 0,
            head: 0,
            entries: VecDeque::new(),
        }
    }

    /// Append `event` as the next entry and return its cursor.
    pub fn append(&mut self, event: UiEvent, appended: Instant) -> LiveCursor {
        self.head += 1;
        self.entries.push_back(Entry {
            seq: self.head,
            event,
            appended,
        });
        self.cursor(self.head)
    }

    /// Drop every entry appended more than `retention` before `now`.
    pub fn prune(&mut self, now: Instant, retention: Duration) {
        while let Some(oldest) = self.entries.front() {
            if now.saturating_duration_since(oldest.appended) <= retention {
                break;
            }
            self.floor = oldest.seq;
            self.entries.pop_front();
        }
    }

    /// The span a subscription can replay from. `prune` drops only
    /// appended entries, so the floor is never above the head and this
    /// never fails; the error is passed on rather than assumed away.
    pub fn window(&self) -> Result<FeedWindow, FloorAboveHead> {
        FeedWindow::new(self.epoch, self.floor, self.head)
    }

    /// The retained entries after `seq`, oldest first.
    pub fn after(&self, seq: u64) -> impl Iterator<Item = &Entry> {
        self.entries.iter().filter(move |entry| entry.seq > seq)
    }

    pub fn cursor(&self, seq: u64) -> LiveCursor {
        LiveCursor {
            epoch: self.epoch,
            seq,
        }
    }

    pub fn head(&self) -> LiveCursor {
        self.cursor(self.head)
    }
}
