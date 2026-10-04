//! The response body the fake upstream serves, and the task that feeds a
//! paced or faulty one.

use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::Bytes;
use hyper::body::{Body, Frame, SizeHint};
use tokio::sync::mpsc;

use crate::upstream::script::{Fault, Framing, Pacing};

/// The body was cut on purpose ([`Fault::Disconnect`]). Returned to hyper
/// as a body error, which makes it drop the connection mid-body.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("the fake upstream cut the response body")]
pub struct BodyCut;

type Chunk = Result<Bytes, BodyCut>;

/// A response body: all at once, or fed chunk by chunk from a task.
#[derive(Debug)]
pub enum ReplyBody {
    /// The whole body, sent with `content-length`; `None` once sent.
    Whole(Option<Bytes>),
    /// Chunks from a feeder task. `length` is the declared total when the
    /// reply is framed whole, so hyper sends `content-length`.
    Fed {
        chunks: mpsc::Receiver<Chunk>,
        length: Option<u64>,
    },
}

impl Body for ReplyBody {
    type Data = Bytes;
    type Error = BodyCut;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, BodyCut>>> {
        match self.get_mut() {
            Self::Whole(bytes) => Poll::Ready(bytes.take().map(|bytes| Ok(Frame::data(bytes)))),
            Self::Fed { chunks, .. } => chunks
                .poll_recv(cx)
                .map(|chunk| chunk.map(|chunk| chunk.map(Frame::data))),
        }
    }

    fn is_end_stream(&self) -> bool {
        matches!(self, Self::Whole(None))
    }

    fn size_hint(&self) -> SizeHint {
        match self {
            Self::Whole(Some(bytes)) => SizeHint::with_exact(bytes.len() as u64),
            Self::Whole(None) => SizeHint::with_exact(0),
            Self::Fed {
                length: Some(length),
                ..
            } => SizeHint::with_exact(*length),
            Self::Fed { length: None, .. } => SizeHint::default(),
        }
    }
}

/// The body for `chunks`: whole when nothing needs pacing or a fault, else
/// fed from a spawned task. The task ends when every chunk is sent, when the
/// client goes away, or (for a stall) when the body is dropped.
pub fn reply_body(
    chunks: Vec<Bytes>,
    framing: Framing,
    pacing: Pacing,
    fault: Option<Fault>,
) -> ReplyBody {
    let length: u64 = chunks.iter().map(|chunk| chunk.len() as u64).sum();
    if framing == Framing::Whole && pacing == Pacing::IMMEDIATE && fault.is_none() {
        return ReplyBody::Whole(Some(chunks.concat().into()));
    }
    // Capacity 1: the feeder waits for hyper to take each chunk, so a slow
    // reader holds the feeder back as it would a real server.
    let (sender, receiver) = mpsc::channel(1);
    tokio::spawn(feed(sender, chunks, pacing, fault));
    ReplyBody::Fed {
        chunks: receiver,
        length: (framing == Framing::Whole).then_some(length),
    }
}

/// Send `chunks` with `pacing`, applying `fault` at its chunk.
async fn feed(
    sender: mpsc::Sender<Chunk>,
    chunks: Vec<Bytes>,
    pacing: Pacing,
    fault: Option<Fault>,
) {
    let cut_at = match fault {
        Some(Fault::Stall { after_chunks } | Fault::Disconnect { after_chunks }) => {
            Some(after_chunks)
        }
        Some(Fault::NoResponse) | None => None,
    };
    for (index, chunk) in chunks.into_iter().enumerate() {
        if cut_at == Some(index) {
            break;
        }
        let delay = if index == 0 {
            pacing.first_chunk
        } else {
            pacing.between_chunks
        };
        if !delay.is_zero() {
            tokio::time::sleep(delay).await;
        }
        if sender.send(Ok(chunk)).await.is_err() {
            tracing::debug!(chunk = index, "fake upstream client went away");
            return;
        }
    }
    match fault {
        Some(Fault::Stall { after_chunks }) => {
            tracing::debug!(after_chunks, "fake upstream stalling");
            sender.closed().await;
        }
        Some(Fault::Disconnect { after_chunks }) => {
            tracing::debug!(after_chunks, "fake upstream cutting the body");
            // The receiver may already be gone; either way the body ends.
            let _ = sender.send(Err(BodyCut)).await;
        }
        Some(Fault::NoResponse) | None => {}
    }
}
