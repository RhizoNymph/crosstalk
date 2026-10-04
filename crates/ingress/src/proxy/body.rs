//! The two body types the proxy hands hyper: the request body it sends
//! upstream, and the response body it sends the client.

use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::Bytes;
use crosstalk_spec::interfaces::l0_ingress::ResponseFramer;
use http_body_util::Full;
use hyper::body::{Body, Frame, Incoming, SizeHint};

use super::relay::{CaptureBody, RelayError};
use super::tee::TeeBody;

/// The body forwarded upstream.
#[derive(Debug)]
pub(crate) enum UpstreamBody {
    /// Not captured: the client's body as it streams in.
    Plain(Incoming),
    /// A generation request: the same, with a copy kept for the decoder.
    Tee(TeeBody<Incoming>),
}

impl Body for UpstreamBody {
    type Data = Bytes;
    type Error = hyper::Error;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, hyper::Error>>> {
        match self.get_mut() {
            Self::Plain(body) => Pin::new(body).poll_frame(cx),
            Self::Tee(body) => Pin::new(body).poll_frame(cx),
        }
    }

    fn is_end_stream(&self) -> bool {
        match self {
            Self::Plain(body) => body.is_end_stream(),
            Self::Tee(body) => body.is_end_stream(),
        }
    }

    fn size_hint(&self) -> SizeHint {
        match self {
            Self::Plain(body) => body.size_hint(),
            Self::Tee(body) => body.size_hint(),
        }
    }
}

/// The body relayed to the client.
#[derive(Debug)]
pub enum ProxyBody<F> {
    /// An answer the proxy made itself (421, 502, 504).
    Local(Full<Bytes>),
    /// Not captured: the upstream body as it streams in.
    Plain(Incoming),
    /// A generation exchange: relayed through the capture tee.
    Capture(CaptureBody<F>),
}

impl<F: ResponseFramer + Unpin> Body for ProxyBody<F> {
    type Data = Bytes;
    type Error = RelayError;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, RelayError>>> {
        match self.get_mut() {
            Self::Local(body) => Pin::new(body)
                .poll_frame(cx)
                .map(|frame| frame.map(|frame| frame.map_err(|never| match never {}))),
            Self::Plain(body) => Pin::new(body)
                .poll_frame(cx)
                .map(|frame| frame.map(|frame| frame.map_err(RelayError::Upstream))),
            Self::Capture(body) => Pin::new(body).poll_frame(cx),
        }
    }

    fn is_end_stream(&self) -> bool {
        match self {
            Self::Local(body) => body.is_end_stream(),
            Self::Plain(body) => body.is_end_stream(),
            Self::Capture(body) => body.is_end_stream(),
        }
    }

    fn size_hint(&self) -> SizeHint {
        match self {
            Self::Local(body) => body.size_hint(),
            Self::Plain(body) => body.size_hint(),
            Self::Capture(body) => body.size_hint(),
        }
    }
}
