//! The request-body tee: forwards every frame of the client's body upstream
//! as hyper asks for it, and keeps a copy for the decoder.
//!
//! The copy is the frames' `Bytes`, shared, not duplicated. It is bounded by
//! the tee limit (`ingress.decode.tee-bounded`): past it the copy is dropped
//! and the decoder told so, while forwarding carries on. When the body ends
//! the copy goes to the exchange's capture task over a one-shot channel; the
//! forward path never waits for anyone to take it.
//!
//! If hyper lets go of the body before its end (the upstream could not be
//! reached, or answered before reading it all), the unread rest goes to the
//! capture task too, which reads it from the client itself, under the same
//! bound: a failed exchange still carries its request.

use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::Bytes;
use http_body_util::BodyExt;
use hyper::body::{Body, Frame, SizeHint};
use tokio::sync::oneshot;

/// What the tee holds when the body is done with.
#[derive(Debug)]
pub(crate) enum TeeOutcome<B> {
    /// The whole body, as the chunks it arrived in.
    Complete(Vec<Bytes>),
    /// The body was larger than the limit.
    Overflow { limit: u64 },
    /// The body failed before its end (the client aborted the upload).
    Incomplete,
    /// hyper stopped reading: what was kept, and the rest of the body.
    Unread {
        kept: Vec<Bytes>,
        size: u64,
        rest: B,
    },
}

#[derive(Debug)]
pub(crate) struct TeeBody<B> {
    /// `None` only once dropped.
    inner: Option<B>,
    chunks: Vec<Bytes>,
    size: u64,
    limit: u64,
    overflowed: bool,
    done: Option<oneshot::Sender<TeeOutcome<B>>>,
}

impl<B: Body> TeeBody<B> {
    pub fn new(inner: B, limit: u64, done: oneshot::Sender<TeeOutcome<B>>) -> Self {
        let ended = inner.is_end_stream();
        let mut tee = Self {
            inner: Some(inner),
            chunks: Vec::new(),
            size: 0,
            limit,
            overflowed: false,
            done: Some(done),
        };
        // An empty body is never polled: it is complete already.
        if ended {
            tee.complete();
        }
        tee
    }
}

impl<B> TeeBody<B> {
    fn keep(&mut self, data: &Bytes) {
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

    fn send(&mut self, outcome: TeeOutcome<B>) {
        if let Some(done) = self.done.take() {
            // The capture task may be gone; the body is forwarded either way.
            let _ = done.send(outcome);
        }
    }

    fn complete(&mut self) {
        let outcome = if self.overflowed {
            TeeOutcome::Overflow { limit: self.limit }
        } else {
            TeeOutcome::Complete(std::mem::take(&mut self.chunks))
        };
        self.send(outcome);
    }
}

impl<B> Drop for TeeBody<B> {
    fn drop(&mut self) {
        if self.done.is_none() {
            return;
        }
        let outcome = match self.inner.take() {
            Some(_) if self.overflowed => TeeOutcome::Overflow { limit: self.limit },
            Some(rest) => TeeOutcome::Unread {
                kept: std::mem::take(&mut self.chunks),
                size: self.size,
                rest,
            },
            None => TeeOutcome::Incomplete,
        };
        self.send(outcome);
    }
}

impl<B> Body for TeeBody<B>
where
    B: Body<Data = Bytes> + Unpin,
{
    type Data = Bytes;
    type Error = B::Error;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, B::Error>>> {
        let this = self.get_mut();
        let Some(inner) = this.inner.as_mut() else {
            return Poll::Ready(None);
        };
        let polled = Pin::new(&mut *inner).poll_frame(cx);
        let ended = inner.is_end_stream();
        match &polled {
            Poll::Ready(Some(Ok(frame))) => {
                if let Some(data) = frame.data_ref() {
                    this.keep(data);
                }
                if ended {
                    this.complete();
                }
            }
            Poll::Ready(None) => this.complete(),
            Poll::Ready(Some(Err(_))) => this.send(TeeOutcome::Incomplete),
            Poll::Pending => {}
        }
        polled
    }

    fn is_end_stream(&self) -> bool {
        self.inner.as_ref().is_none_or(Body::is_end_stream)
    }

    fn size_hint(&self) -> SizeHint {
        self.inner
            .as_ref()
            .map_or_else(|| SizeHint::with_exact(0), Body::size_hint)
    }
}

/// Read the rest of a body hyper let go of, keeping at most `limit` bytes
/// in all.
pub(crate) async fn read_rest<B>(
    mut kept: Vec<Bytes>,
    mut size: u64,
    mut rest: B,
    limit: u64,
) -> TeeOutcome<B>
where
    B: Body<Data = Bytes> + Unpin,
{
    loop {
        match rest.frame().await {
            None => return TeeOutcome::Complete(kept),
            Some(Err(_)) => return TeeOutcome::Incomplete,
            Some(Ok(frame)) => {
                if let Ok(data) = frame.into_data() {
                    size = size.saturating_add(data.len() as u64);
                    if size > limit {
                        return TeeOutcome::Overflow { limit };
                    }
                    if !data.is_empty() {
                        kept.push(data);
                    }
                }
            }
        }
    }
}
