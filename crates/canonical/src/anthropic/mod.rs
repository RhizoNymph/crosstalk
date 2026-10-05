//! The Anthropic Messages normalizer.
//!
//! One normalizer for every dialect of the protocol: Anthropic's API and
//! subscriptions (`Reference`), Copilot's proxy, and vLLM's and SGLang's
//! Anthropic-compatible endpoints. Their differences (explicit nulls, id
//! formats, finish reasons) are absorbed by the block mapping, so nothing
//! here branches on the dialect, and a request over HTTP always carries the
//! full transcript; the exchange's continuation is copied from the decoded
//! request as it is.
//!
//! - [`request`]: the request body to messages.
//! - [`response`]: the response to the outcome, through [`stream`] for an
//!   event stream.
//! - `blocks`: the content block mapping both share.
//! - [`shape`]: a refused request body's top-level shape (keys, roles,
//!   content kinds; never a value), for the gateway's debug log.
//! - [`usage`](mod@usage): the token usage mapping.

mod blocks;
pub mod request;
pub mod response;
pub mod shape;
pub mod stream;
pub mod usage;

use crosstalk_spec::interfaces::l0_ingress::RawExchange;
use crosstalk_spec::interfaces::l1_canonical::{NormalizeError, NormalizedExchange, Normalizer};
use crosstalk_spec::observed::client::Dialect;
use crosstalk_spec::observed::exchange::WireProtocol;

use crate::assemble::{self, MediaSink};

pub use request::RequestError;
pub use shape::RequestShape;

/// The normalizer for [`WireProtocol::AnthropicMessages`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct AnthropicMessages;

impl Normalizer for AnthropicMessages {
    fn protocol(&self) -> WireProtocol {
        WireProtocol::AnthropicMessages
    }

    fn handles(&self, dialect: Dialect) -> bool {
        match dialect {
            Dialect::Reference | Dialect::Vllm | Dialect::Sglang | Dialect::Copilot => true,
        }
    }

    fn normalize(&self, raw: &RawExchange) -> Result<NormalizedExchange, NormalizeError> {
        normalize(raw)
    }
}

/// Normalizes one Anthropic Messages exchange: the exchange, its messages,
/// the media bytes they name and the warnings. `Err` only when the request
/// body is not an Anthropic Messages request (or the exchange is not of
/// this protocol); the response never fails normalization.
pub fn normalize(raw: &RawExchange) -> Result<NormalizedExchange, NormalizeError> {
    if raw.meta.protocol != WireProtocol::AnthropicMessages
        || raw.request.harness.protocol != WireProtocol::AnthropicMessages
    {
        return Err(NormalizeError::RequestBody {
            reason: "not an Anthropic Messages exchange".to_owned(),
        });
    }
    let mut sink = MediaSink::default();
    let request = request::normalize(&raw.request.body, &mut sink).map_err(|error| {
        NormalizeError::RequestBody {
            reason: error.to_string(),
        }
    })?;
    let response = response::read(raw, &mut sink);
    Ok(assemble::assemble(raw, request, response, sink))
}
