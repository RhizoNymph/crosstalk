//! One open live stream: its replay or resync items, then live entries from
//! its bounded buffer, a heartbeat whenever it has sent nothing for the
//! heartbeat interval, and the reason it ended.

use std::collections::VecDeque;
use std::time::Duration;

use crosstalk_spec::interfaces::l8_surface::live::{LiveCursor, LiveEnd, LiveItem, LiveStream};
use tokio::sync::{mpsc, oneshot, watch};
use tokio::time::Instant;

/// A subscription's stream (`LiveFeed::Stream`).
///
/// Every item's cursor is of the feed's one epoch, and its seq never goes
/// down: replayed entries come before live ones and both in feed order,
/// and a heartbeat carries the newer of the head (read before the buffer
/// was found empty) and the last entry delivered.
#[derive(Debug)]
pub struct FeedStream {
    initial: VecDeque<LiveItem>,
    items: mpsc::Receiver<LiveItem>,
    end: oneshot::Receiver<LiveEnd>,
    head: watch::Receiver<LiveCursor>,
    /// The newest cursor this stream has delivered or passed over.
    passed: LiveCursor,
    heartbeat: Duration,
    /// When the last item was sent (or the stream opened).
    last_sent: Instant,
    ended: Option<LiveEnd>,
}

impl FeedStream {
    pub(super) fn new(
        initial: VecDeque<LiveItem>,
        start: LiveCursor,
        items: mpsc::Receiver<LiveItem>,
        end: oneshot::Receiver<LiveEnd>,
        head: watch::Receiver<LiveCursor>,
        heartbeat: Duration,
    ) -> Self {
        Self {
            initial,
            items,
            end,
            head,
            passed: start,
            heartbeat,
            last_sent: Instant::now(),
            ended: None,
        }
    }

    fn sent(&mut self, item: LiveItem) -> Result<LiveItem, LiveEnd> {
        let cursor = item.cursor();
        if cursor.seq > self.passed.seq {
            self.passed = cursor;
        }
        self.last_sent = Instant::now();
        Ok(item)
    }

    fn end(&mut self, reason: LiveEnd) -> Result<LiveItem, LiveEnd> {
        self.ended = Some(reason);
        self.initial.clear();
        self.items.close();
        Err(reason)
    }

    /// The reason the writer gave, once it closed this stream's buffer; a
    /// writer that is gone without one has shut down.
    async fn closed(&mut self) -> Result<LiveItem, LiveEnd> {
        let reason = (&mut self.end).await.unwrap_or(LiveEnd::ShuttingDown);
        self.end(reason)
    }

    async fn next_item(&mut self) -> Result<LiveItem, LiveEnd> {
        if let Some(reason) = self.ended {
            return Err(reason);
        }
        // An ending (lag, session end, shutdown) wins over anything still
        // buffered: the client resumes from the last cursor it received.
        match self.end.try_recv() {
            Ok(reason) => return self.end(reason),
            Err(oneshot::error::TryRecvError::Closed) => return self.end(LiveEnd::ShuttingDown),
            Err(oneshot::error::TryRecvError::Empty) => {}
        }
        if let Some(item) = self.initial.pop_front() {
            return self.sent(item);
        }
        let deadline = self.last_sent + self.heartbeat;
        tokio::select! {
            biased;
            reason = &mut self.end => {
                let reason = reason.unwrap_or(LiveEnd::ShuttingDown);
                self.end(reason)
            }
            item = self.items.recv() => match item {
                Some(item) => self.sent(item),
                None => self.closed().await,
            },
            () = tokio::time::sleep_until(deadline) => {
                // The head first, then the buffer: the writer sends an entry
                // before it publishes its seq as the head, so an empty buffer
                // here means every entry up to the head reached this stream
                // or was passed over.
                let head = *self.head.borrow_and_update();
                match self.items.try_recv() {
                    Ok(item) => self.sent(item),
                    Err(mpsc::error::TryRecvError::Disconnected) => self.closed().await,
                    Err(mpsc::error::TryRecvError::Empty) => {
                        let cursor = if head.seq > self.passed.seq { head } else { self.passed };
                        self.sent(LiveItem::Heartbeat { cursor })
                    }
                }
            }
        }
    }
}

impl LiveStream for FeedStream {
    async fn next(&mut self) -> Result<LiveItem, LiveEnd> {
        self.next_item().await
    }
}
