//! Request decoding for capture, off the hot path.
//!
//! The proxy forwards a generation request as soon as it is routed and tees
//! its body ([`crate::proxy`]). When the tee holds the whole body, the
//! exchange's capture task hands it to a [`RequestDecoder`], concurrently
//! with the response relay; nothing the forward path does waits for it
//! (`ingress.forwarding.never-waits-on-decode`).
//!
//! [`AdapterDecoder`] is the production decoder: it undoes the
//! `content-encoding` within the decoded-size bound, then calls the
//! adapter's `decode_request` on the plain bytes. The trait exists so a
//! simulation can stall or slow decoding and show the forward path does not
//! notice.

use std::future::Future;
use std::num::NonZeroU64;
use std::sync::Arc;

use crosstalk_spec::interfaces::l0_ingress::{
    BodyDecodeError, ContentEncoding, DecodedRequest, ProviderAdapter, RequestHead,
};
use crosstalk_spec::observed::client::ClientContext;

use crate::encoding::{self, EncodingError};

/// One request body to decode.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecodeJob {
    /// The head as forwarded upstream, without its credential headers.
    pub head: RequestHead,
    /// The body exactly as forwarded (still compressed if it was).
    pub body: Vec<u8>,
    pub client: ClientContext,
}

/// Why a generation request produced no `DecodedRequest`. Counted as an
/// uncaptured decode error; the request was forwarded regardless.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum CaptureDecodeError {
    #[error("the adapter refused the body: {0:?}")]
    Body(BodyDecodeError),
    #[error(transparent)]
    Encoding(EncodingError),
    /// The body was larger than the tee bound
    /// (`ingress.decode.tee-bounded`).
    #[error("the request body is larger than the {limit}-byte tee bound")]
    TeeOverflow { limit: u64 },
    /// The body never arrived in full: the client aborted the upload, or
    /// the upstream answered before reading all of it.
    #[error("the request body did not arrive in full")]
    BodyIncomplete,
}

/// Decodes a teed request body for capture.
pub trait RequestDecoder: Send + Sync + 'static {
    fn decode(
        &self,
        job: DecodeJob,
    ) -> impl Future<Output = Result<DecodedRequest, CaptureDecodeError>> + Send;
}

/// Decompress within the bound, then let the adapter read the body.
#[derive(Debug)]
pub struct AdapterDecoder<A> {
    adapter: Arc<A>,
    decoded_limit: NonZeroU64,
}

impl<A> AdapterDecoder<A> {
    pub fn new(adapter: Arc<A>, decoded_limit: NonZeroU64) -> Self {
        Self {
            adapter,
            decoded_limit,
        }
    }

    pub fn adapter(&self) -> &Arc<A> {
        &self.adapter
    }
}

impl<A: ProviderAdapter + Send + Sync + 'static> AdapterDecoder<A> {
    /// The decode itself, synchronous: plain CPU work on a body in memory.
    pub fn decode_now(&self, job: DecodeJob) -> Result<DecodedRequest, CaptureDecodeError> {
        let encoding =
            encoding::content_encoding(&job.head).map_err(CaptureDecodeError::Encoding)?;
        let body = match encoding {
            ContentEncoding::Identity => {
                if job.body.len() as u64 > self.decoded_limit.get() {
                    return Err(CaptureDecodeError::Encoding(EncodingError::TooLarge {
                        encoding,
                        limit: self.decoded_limit.get(),
                    }));
                }
                job.body
            }
            ContentEncoding::Gzip | ContentEncoding::Zstd => {
                encoding::decode(&job.body, encoding, self.decoded_limit.get())
                    .map_err(CaptureDecodeError::Encoding)?
            }
        };
        // The adapter sees the plain body, so the head it gets no longer
        // claims an encoding.
        let plain = RequestHead {
            headers: job
                .head
                .headers
                .into_iter()
                .filter(|(name, _)| !name.eq_ignore_ascii_case("content-encoding"))
                .collect(),
            ..job.head
        };
        let harness = self
            .adapter
            .decode_request(&plain, &body, &job.client)
            .map_err(CaptureDecodeError::Body)?;
        Ok(DecodedRequest {
            harness,
            body,
            encoding,
        })
    }
}

impl<A: ProviderAdapter + Send + Sync + 'static> RequestDecoder for AdapterDecoder<A> {
    async fn decode(&self, job: DecodeJob) -> Result<DecodedRequest, CaptureDecodeError> {
        // Let the relay that woke this task run first.
        tokio::task::yield_now().await;
        self.decode_now(job)
    }
}
