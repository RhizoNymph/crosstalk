//! L0 ingress for crosstalk: the reverse proxy, upstream routing, credential
//! hashing, provider adapters, response framing and the response tee.
//!
//! Implements [`crosstalk_spec::interfaces::l0_ingress`] for the Anthropic
//! Messages protocol over HTTP and SSE (roadmap P2.4). Point a harness's base
//! URL at a route (`ANTHROPIC_BASE_URL=http://gateway:8080/anthropic`) and
//! every request is forwarded upstream unchanged, every response relayed
//! back unchanged, and every generation exchange handed to the capture
//! channel as a `RawExchange`. Forward-proxy mode and WebSocket taps are P8.
//!
//! | Spec trait | Implementation |
//! | --- | --- |
//! | `UpstreamRouter` | [`routing::Routes`] |
//! | `ClientIdentifier` | [`identify::HeaderIdentifier`] |
//! | `ProviderAdapter` | [`adapter::AnthropicAdapter`] |
//! | `ResponseFramer` | [`framer::AnthropicFramer`] |
//! | the response tee | [`proxy::relay::CaptureBody`] |
//!
//! A layer crate: it depends on the spec, never on another layer crate.
//! Concurrency is tokio only; the capture hand-off is a bounded channel.

pub mod adapter;
pub mod capture;
pub mod config;
pub mod credential;
pub mod decode;
pub mod encoding;
pub mod exchange;
pub mod framer;
pub mod identify;
pub mod proxy;
pub mod routing;

use std::sync::Arc;

use crosstalk_spec::ids::{SeededRandom, UlidGenerator};
use crosstalk_spec::support::Clock;

use adapter::AnthropicAdapter;
use capture::CaptureSender;
use config::IngressConfig;
use credential::{SecretError, load_secrets};
use decode::AdapterDecoder;
use identify::HeaderIdentifier;
use proxy::{Proxy, ProxyParts, connector};
use routing::{ConfigError, Routes};

/// The production proxy: Anthropic Messages over TLS or plain HTTP.
pub type AnthropicProxy =
    Proxy<AnthropicAdapter, AdapterDecoder<AnthropicAdapter>, connector::Https>;

/// Why a proxy could not be built from its configuration.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum BuildError {
    #[error(transparent)]
    Routes(#[from] ConfigError),
    #[error(transparent)]
    Secrets(#[from] SecretError),
}

/// Build the Anthropic proxy from `config`, reading the secrets' environment
/// variables through `lookup` and handing exchanges to `capture`.
pub fn anthropic_proxy(
    config: &IngressConfig,
    lookup: impl Fn(&str) -> Option<String>,
    capture: CaptureSender,
    clock: Arc<dyn Clock>,
) -> Result<AnthropicProxy, BuildError> {
    let routes = Routes::new(&config.routes)?;
    let keys = load_secrets(&config.secrets, lookup)?;
    let limits = config.limits;
    let adapter = Arc::new(AnthropicAdapter::new(
        limits.decoded_bytes,
        limits.sse_event_bytes,
    ));
    Ok(Proxy::new(ProxyParts {
        routes,
        identifier: HeaderIdentifier::new(keys),
        decoder: AdapterDecoder::new(Arc::clone(&adapter), limits.decoded_bytes),
        adapter,
        connector: connector::https(),
        capture,
        ids: UlidGenerator::new(Arc::clone(&clock), SeededRandom::from_entropy()),
        clock,
        limits,
        observer: None,
    }))
}

#[cfg(test)]
mod benches;
#[cfg(test)]
mod tests;
