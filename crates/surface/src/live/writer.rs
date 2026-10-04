//! The feed writer: one task that owns the feed log and every open stream's
//! sending end, fed by commands over a channel.
//!
//! ```text
//! bus (group `live`) ─▶ consumer task ─Append─▶ ┐                      ┌─▶ stream 1 (bounded)
//! FeedHandle::append ────────────────Append─▶   ├─ writer task: log ──┼─▶ stream 2 (bounded)
//! FeedHandle::subscribe / end_sessions ──────▶  ┘   head (watch)      └─▶ …
//! ```
//!
//! The writer never waits on a stream: it offers each item with `try_send`,
//! and a stream whose buffer is full is ended with `Lagged`. It sends an
//! appended entry to every stream before it publishes the new head, so a
//! stream that reads the head and then finds its buffer empty has been sent
//! every entry up to that head (the heartbeat's guarantee).

use std::collections::VecDeque;

use crosstalk_spec::events::BusEvent;
use crosstalk_spec::events::changed::Changed;
use crosstalk_spec::ids::OperatorId;
use crosstalk_spec::interfaces::l2_transport::Subscription;
use crosstalk_spec::interfaces::l8_surface::Caller;
use crosstalk_spec::interfaces::l8_surface::live::{
    FeedEpoch, LiveConfig, LiveCursor, LiveEnd, LiveItem, Resume, ResumePlan, ResyncReason, UiEvent,
};
use tokio::sync::{mpsc, oneshot, watch};
use tokio::time::Instant;

use super::log::FeedLog;
use super::stream::FeedStream;
use super::{FeedClosed, FeedHandle};

/// How many commands may wait for the writer before a sender waits.
const COMMANDS: usize = 256;

pub(super) enum Command {
    Append {
        changed: Changed,
        done: oneshot::Sender<LiveCursor>,
    },
    Subscribe {
        caller: Caller,
        resume: Resume,
        reply: oneshot::Sender<FeedStream>,
    },
    EndSessions {
        operator: Option<OperatorId>,
        done: oneshot::Sender<usize>,
    },
    Shutdown,
}

/// One open stream as the writer sees it.
struct Subscriber {
    caller: Caller,
    items: mpsc::Sender<LiveItem>,
    end: oneshot::Sender<LiveEnd>,
}

/// Starts the feed writer.
#[derive(Debug, Clone, Copy)]
pub struct FeedWriter;

impl FeedWriter {
    /// Spawn the writer for one incarnation of the log, `epoch`, on the
    /// current tokio runtime. Events reach it through
    /// [`FeedHandle::append`] or a consumer started with
    /// [`FeedWriter::consume`].
    pub fn spawn(config: LiveConfig, epoch: FeedEpoch) -> FeedHandle {
        let (commands, receiver) = mpsc::channel(COMMANDS);
        let (head_tx, head_rx) = watch::channel(LiveCursor { epoch, seq: 0 });
        let writer = Writer {
            config,
            log: FeedLog::new(epoch),
            subscribers: Vec::new(),
            head: head_tx,
        };
        tokio::spawn(writer.run(receiver));
        FeedHandle {
            commands,
            head: head_rx,
            config,
        }
    }

    /// Spawn a consumer that appends every `Changed` delivered on
    /// `subscription` (the `live` consumer group) to `feed`'s log and acks
    /// each delivery only after its append returned. Other events are acked
    /// unread. A failed append nacks the delivery, so the bus redelivers it.
    /// The task ends when the bus shuts down or the writer stops.
    pub fn consume<S>(feed: FeedHandle, mut subscription: S) -> tokio::task::JoinHandle<()>
    where
        S: Subscription + Send + 'static,
    {
        tokio::spawn(async move {
            while let Some(next) = subscription.next().await {
                let delivery = match next {
                    Ok(delivery) => delivery,
                    Err(error) => {
                        tracing::warn!(error = ?error, "live feed delivery failed");
                        continue;
                    }
                };
                let appended = match delivery.envelope.event {
                    BusEvent::Changed(changed) => feed.append(changed).await.map(Some),
                    BusEvent::Ingest(_) | BusEvent::Detect(_) | BusEvent::Insight(_) => Ok(None),
                };
                let settled = match appended {
                    Ok(_) => subscription.ack(delivery.id).await,
                    Err(FeedClosed) => {
                        let retry = feed.config.heartbeat();
                        let reason = "live feed writer stopped".to_owned();
                        let nacked = subscription.nack(delivery.id, retry, reason).await;
                        if let Err(error) = nacked {
                            tracing::warn!(error = ?error, "live feed nack failed");
                        }
                        return;
                    }
                };
                if let Err(error) = settled {
                    tracing::warn!(error = ?error, "live feed ack failed");
                }
            }
            tracing::info!("live feed consumer stopped: the bus shut down");
        })
    }
}

struct Writer {
    config: LiveConfig,
    log: FeedLog,
    subscribers: Vec<Subscriber>,
    head: watch::Sender<LiveCursor>,
}

impl Writer {
    async fn run(mut self, mut commands: mpsc::Receiver<Command>) {
        while let Some(command) = commands.recv().await {
            match command {
                Command::Append { changed, done } => {
                    let cursor = self.append(UiEvent::from(changed));
                    // The appender may have gone away; the entry stays.
                    let _ = done.send(cursor);
                }
                Command::Subscribe {
                    caller,
                    resume,
                    reply,
                } => {
                    let stream = self.subscribe(caller, resume);
                    let _ = reply.send(stream);
                }
                Command::EndSessions { operator, done } => {
                    let ended = self.end_sessions(operator);
                    let _ = done.send(ended);
                }
                Command::Shutdown => break,
            }
        }
        for subscriber in self.subscribers.drain(..) {
            let _ = subscriber.end.send(LiveEnd::ShuttingDown);
        }
        tracing::info!(head = self.log.head().seq, "live feed writer stopped");
    }

    fn append(&mut self, event: UiEvent) -> LiveCursor {
        let now = Instant::now();
        self.log.prune(now, self.config.retention());
        let cursor = self.log.append(event, now);
        let item = LiveItem::Event { cursor, event };
        let mut kept = Vec::with_capacity(self.subscribers.len());
        for subscriber in self.subscribers.drain(..) {
            if !event.visible_to(&subscriber.caller) {
                // Passed over: its cursor reaches the stream through the
                // head, which its next heartbeat carries.
                kept.push(subscriber);
                continue;
            }
            match subscriber.items.try_send(item) {
                Ok(()) => kept.push(subscriber),
                Err(mpsc::error::TrySendError::Full(_)) => {
                    tracing::info!(
                        operator = ?subscriber.caller.operator(),
                        seq = cursor.seq,
                        "live stream lagged"
                    );
                    // The reason goes out before the item sender is
                    // dropped, so the stream finds it when its buffer
                    // closes.
                    let _ = subscriber.end.send(LiveEnd::Lagged);
                }
                Err(mpsc::error::TrySendError::Closed(_)) => {}
            }
        }
        self.subscribers = kept;
        self.head.send_replace(cursor);
        cursor
    }

    fn subscribe(&mut self, caller: Caller, resume: Resume) -> FeedStream {
        self.log.prune(Instant::now(), self.config.retention());
        let head = self.log.head();
        let initial: VecDeque<LiveItem> = match self.log.window() {
            // Never happens (the log keeps its floor at or below its
            // head); a client told to resync loses nothing.
            Err(error) => {
                tracing::error!(error = ?error, "live feed window refused");
                VecDeque::from([LiveItem::Resync {
                    cursor: head,
                    reason: ResyncReason::Expired,
                }])
            }
            Ok(window) => match window.resume(resume) {
                ResumePlan::Live => VecDeque::new(),
                ResumePlan::Resync(reason) => VecDeque::from([LiveItem::Resync {
                    cursor: window.head(),
                    reason,
                }]),
                ResumePlan::Replay { after } => self
                    .log
                    .after(after)
                    .filter(|entry| entry.event.visible_to(&caller))
                    .map(|entry| LiveItem::Event {
                        cursor: self.log.cursor(entry.seq),
                        event: entry.event,
                    })
                    .collect(),
            },
        };
        let buffer = usize::try_from(self.config.buffer().get()).unwrap_or(usize::MAX);
        let (items_tx, items_rx) = mpsc::channel(buffer);
        let (end_tx, end_rx) = oneshot::channel();
        self.subscribers.push(Subscriber {
            caller,
            items: items_tx,
            end: end_tx,
        });
        FeedStream::new(
            initial,
            head,
            items_rx,
            end_rx,
            self.head.subscribe(),
            self.config.heartbeat(),
        )
    }

    /// End every stream of `operator` (every stream when `None`) with
    /// `SessionEnded`. Returns how many it ended.
    fn end_sessions(&mut self, operator: Option<OperatorId>) -> usize {
        let (ending, keeping): (Vec<_>, Vec<_>) = self
            .subscribers
            .drain(..)
            .partition(|subscriber| operator.is_none_or(|op| subscriber.caller.operator() == op));
        self.subscribers = keeping;
        let ended = ending.len();
        for subscriber in ending {
            let _ = subscriber.end.send(LiveEnd::SessionEnded);
        }
        ended
    }
}
