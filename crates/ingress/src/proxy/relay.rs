//! The response tee: relays the upstream body to the client frame by frame
//! and, on the side, feeds the framer and keeps the bytes for capture.
//!
//! [`CaptureBody`] is the body hyper writes to the client. Each time hyper
//! asks for a frame it polls the upstream body once and returns what it got
//! at once, after a synchronous look (the framer's `push` and a reference
//! to the `Bytes`): it never waits for a later chunk or for capture
//! (`ingress.passthrough.relay-unbuffered`). hyper asks only as fast as the
//! client reads, so a slow client slows the upstream read, not memory: the
//! relay itself buffers nothing.
//!
//! **Capture buffer.** Capture keeps every response byte until the stream
//! ends, up to the response capture bound. Past it the kept bytes are let
//! go, the exchange is marked overflowed, and at hand-off it is counted
//! uncaptured (`response_too_large`) rather than captured with a cut body,
//! because a `RawExchange`'s bytes must be exactly what was received
//! (`ingress.raw-exchange.response-body-as-received`). The client keeps
//! getting every byte at full speed.
//!
//! **Ending.** Exactly one `ResponseRecord` goes to the capture task per
//! exchange: from `Pending` if no response head arrived, else from
//! [`CaptureBody`] when the stream ends (`None`), fails, times out, or is
//! dropped by hyper because the client left. A stage still open at that
//! point fails with the cause.

use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use bytes::Bytes;
use crosstalk_spec::interfaces::l0_ingress::{FrameError, FrameEvent, ResponseFramer};
use crosstalk_spec::observed::exchange::{ExchangeFailure, Transport};
use hyper::body::{Body, Frame, Incoming, SizeHint};
use tokio::sync::oneshot;
use tokio::time::{Instant, Sleep};

use crate::exchange::{CapturedBody, InFlight, ResponseRecord};

/// Why the relay ended the client's response early.
#[derive(Debug, thiserror::Error)]
pub enum RelayError {
    /// The upstream body failed (a reset or a cut connection).
    #[error("the upstream response body failed: {0}")]
    Upstream(#[from] hyper::Error),
    /// Nothing arrived from the upstream for the idle timeout.
    #[error("no upstream bytes for {0:?}")]
    IdleTimeout(Duration),
}

/// The bytes capture keeps, bounded.
#[derive(Debug)]
struct Kept {
    chunks: Vec<Bytes>,
    size: u64,
    limit: u64,
    overflowed: bool,
}

impl Kept {
    fn push(&mut self, data: &Bytes) {
        if self.overflowed {
            return;
        }
        self.size = self.size.saturating_add(data.len() as u64);
        if self.size > self.limit {
            self.overflowed = true;
            self.chunks = Vec::new();
        } else if !data.is_empty() {
            self.chunks.push(data.clone());
        }
    }

    fn take(&mut self) -> CapturedBody {
        if self.overflowed {
            CapturedBody::Overflowed
        } else {
            CapturedBody::Kept(std::mem::take(&mut self.chunks))
        }
    }
}

/// An exchange waiting for its response head. Dropped without an answer
/// (the client left), it records `ClientDisconnected`.
#[derive(Debug)]
pub(crate) struct Pending {
    exchange: Option<InFlight>,
    record: Option<oneshot::Sender<ResponseRecord>>,
}

impl Pending {
    pub fn new(exchange: InFlight, record: oneshot::Sender<ResponseRecord>) -> Self {
        Self {
            exchange: Some(exchange),
            record: Some(record),
        }
    }

    /// No response head will come: end the exchange with `failure`.
    pub fn fail(mut self, failure: ExchangeFailure) {
        self.end(failure);
    }

    fn end(&mut self, failure: ExchangeFailure) {
        if let (Some(exchange), Some(record)) = (self.exchange.take(), self.record.take()) {
            let ended = Instant::now();
            let _ = record.send(exchange.record(
                failure,
                None,
                Transport::Http,
                CapturedBody::Kept(Vec::new()),
                ended,
            ));
        }
    }

    /// The head arrived: relay `body` through a capture tee.
    pub fn respond<F>(
        mut self,
        body: Incoming,
        framer: F,
        status: u16,
        transport: Transport,
        limit: u64,
        idle: Option<Duration>,
    ) -> CaptureBody<F> {
        let mut exchange = self.exchange.take();
        let record = self.record.take();
        if let Some(exchange) = exchange.as_mut()
            && !(200..300).contains(&status)
        {
            // A non-2xx status before any content is the failure's cause.
            exchange.fail(ExchangeFailure::Upstream { status });
        }
        CaptureBody {
            inner: body,
            framer,
            exchange,
            record,
            status,
            transport,
            kept: Kept {
                chunks: Vec::new(),
                size: 0,
                limit,
                overflowed: false,
            },
            idle: idle.map(|timeout| Idle {
                timeout,
                sleep: Box::pin(tokio::time::sleep(timeout)),
            }),
        }
    }
}

impl Drop for Pending {
    fn drop(&mut self) {
        self.end(ExchangeFailure::ClientDisconnected);
    }
}

#[derive(Debug)]
struct Idle {
    timeout: Duration,
    sleep: Pin<Box<Sleep>>,
}

/// The client's response body for a captured exchange.
#[derive(Debug)]
pub struct CaptureBody<F> {
    inner: Incoming,
    framer: F,
    /// `None` once the record has been sent.
    exchange: Option<InFlight>,
    record: Option<oneshot::Sender<ResponseRecord>>,
    status: u16,
    transport: Transport,
    kept: Kept,
    idle: Option<Idle>,
}

impl<F: ResponseFramer> CaptureBody<F> {
    /// Tee one chunk: keep it, frame it, move the stage.
    fn observe(&mut self, data: &Bytes) {
        let now = Instant::now();
        self.kept.push(data);
        let pushed = self.framer.push(data);
        let held = match &pushed {
            // A chunk that completed events may hold an error back.
            Ok(events) if !events.is_empty() => self.framer.push(&[]).err(),
            _ => None,
        };
        let Some(exchange) = self.exchange.as_mut() else {
            return;
        };
        match pushed {
            Ok(events) => {
                for event in events {
                    match event {
                        FrameEvent::FirstContent => {
                            exchange.first_content(now);
                        }
                        FrameEvent::Finished => {
                            exchange.finished();
                        }
                    }
                }
            }
            Err(error) => {
                exchange.fail(failure_of(error));
            }
        }
        if let Some(error) = held {
            exchange.fail(failure_of(error));
        }
    }

    /// The stream is over: fail an open stage with `open` and hand the
    /// record off.
    fn end(&mut self, open: ExchangeFailure) {
        if let (Some(exchange), Some(record)) = (self.exchange.take(), self.record.take()) {
            let body = self.kept.take();
            let _ = record.send(exchange.record(
                open,
                Some(self.status),
                self.transport,
                body,
                Instant::now(),
            ));
        }
    }
}

fn failure_of(error: FrameError) -> ExchangeFailure {
    match error {
        FrameError::MalformedFrame { offset } => ExchangeFailure::MalformedStream { offset },
        FrameError::UpstreamErrorEvent { .. } => ExchangeFailure::UpstreamErrorEvent,
    }
}

impl<F> Drop for CaptureBody<F> {
    fn drop(&mut self) {
        // Dropped before its end: hyper let go of it because the client did.
        if let (Some(exchange), Some(record)) = (self.exchange.take(), self.record.take()) {
            let body = self.kept.take();
            let _ = record.send(exchange.record(
                ExchangeFailure::ClientDisconnected,
                Some(self.status),
                self.transport,
                body,
                Instant::now(),
            ));
        }
    }
}

impl<F: ResponseFramer + Unpin> Body for CaptureBody<F> {
    type Data = Bytes;
    type Error = RelayError;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, RelayError>>> {
        let this = self.get_mut();
        if this.record.is_none() {
            return Poll::Ready(None);
        }
        match Pin::new(&mut this.inner).poll_frame(cx) {
            Poll::Ready(Some(Ok(frame))) => {
                if let Some(data) = frame.data_ref() {
                    this.observe(data);
                    if let Some(idle) = this.idle.as_mut() {
                        idle.sleep.as_mut().reset(Instant::now() + idle.timeout);
                    }
                }
                Poll::Ready(Some(Ok(frame)))
            }
            Poll::Ready(Some(Err(error))) => {
                this.end(ExchangeFailure::StreamTruncated);
                Poll::Ready(Some(Err(RelayError::Upstream(error))))
            }
            Poll::Ready(None) => {
                this.end(ExchangeFailure::StreamTruncated);
                Poll::Ready(None)
            }
            Poll::Pending => {
                let Some(idle) = this.idle.as_mut() else {
                    return Poll::Pending;
                };
                if idle.sleep.as_mut().poll(cx).is_pending() {
                    return Poll::Pending;
                }
                let timeout = idle.timeout;
                this.end(ExchangeFailure::Timeout);
                Poll::Ready(Some(Err(RelayError::IdleTimeout(timeout))))
            }
        }
    }

    /// Never ends early: hyper must poll once more and see the upstream's
    /// end, or the exchange would read as abandoned by the client.
    fn is_end_stream(&self) -> bool {
        self.record.is_none()
    }

    fn size_hint(&self) -> SizeHint {
        self.inner.size_hint()
    }
}
