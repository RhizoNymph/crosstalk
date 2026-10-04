//! The feed log: every event the feed has published, numbered from 1 in
//! one epoch, kept for `LiveConfig::retention` so a reconnecting stream
//! can replay what it missed.

use std::collections::VecDeque;
use std::time::Duration;

use crosstalk_spec::interfaces::l8_surface::live::{
    FeedEpoch, FeedWindow, FloorAboveHead, UiEvent,
};
use tokio::time::Instant;

/// One numbered event of the log.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Entry {
    pub seq: u64,
    pub event: UiEvent,
}

#[derive(Debug)]
pub struct FeedLog {
    retention: Duration,
    /// The last entry retention dropped (0 if none).
    floor: u64,
    /// The last entry appended (0 if none).
    head: u64,
    /// Entries `floor + 1 ..= head`, oldest first, with when they were
    /// appended.
    entries: VecDeque<(Instant, Entry)>,
}

impl FeedLog {
    pub fn new(retention: Duration) -> Self {
        Self {
            retention,
            floor: 0,
            head: 0,
            entries: VecDeque::new(),
        }
    }

    /// Drops the entries older than the retention.
    pub fn expire(&mut self, now: Instant) {
        while let Some((at, entry)) = self.entries.front() {
            if now.saturating_duration_since(*at) <= self.retention {
                break;
            }
            self.floor = entry.seq;
            self.entries.pop_front();
        }
    }

    /// Appends `event` as the next entry.
    pub fn append(&mut self, event: UiEvent, now: Instant) -> Entry {
        self.expire(now);
        self.head = self.head.saturating_add(1);
        let entry = Entry {
            seq: self.head,
            event,
        };
        self.entries.push_back((now, entry));
        entry
    }

    /// The span a stream can replay from.
    pub fn window(&self, epoch: FeedEpoch) -> Result<FeedWindow, FloorAboveHead> {
        FeedWindow::new(epoch, self.floor, self.head)
    }

    /// The retained entries after `seq`, oldest first.
    pub fn after(&self, seq: u64) -> impl Iterator<Item = Entry> + '_ {
        self.entries
            .iter()
            .map(|(_, entry)| *entry)
            .filter(move |entry| entry.seq > seq)
    }
}

#[cfg(test)]
mod tests {
    use crosstalk_spec::ids::AlertId;

    use super::*;

    fn alert(n: u128) -> UiEvent {
        UiEvent::AlertChanged {
            id: AlertId::from_ulid(n),
        }
    }

    #[test]
    fn entries_are_numbered_and_expire_after_the_retention() {
        let start = Instant::now();
        let mut log = FeedLog::new(Duration::from_secs(10));
        assert_eq!(log.append(alert(1), start).seq, 1);
        assert_eq!(log.append(alert(2), start + Duration::from_secs(5)).seq, 2);
        let epoch = FeedEpoch(7);
        assert_eq!(log.window(epoch), FeedWindow::new(epoch, 0, 2));
        log.append(alert(3), start + Duration::from_secs(12));
        assert_eq!(log.window(epoch), FeedWindow::new(epoch, 1, 3));
        assert_eq!(log.after(0).map(|e| e.seq).collect::<Vec<_>>(), [2, 3]);
        assert_eq!(log.after(2).map(|e| e.seq).collect::<Vec<_>>(), [3]);
        log.expire(start + Duration::from_secs(30));
        assert_eq!(log.window(epoch), FeedWindow::new(epoch, 3, 3));
        assert_eq!(log.after(0).count(), 0);
    }
}
