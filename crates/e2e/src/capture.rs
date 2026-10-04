//! Captured traffic: what the proxy would hand the pipeline for a scripted
//! exchange, made with L0's and L1's own code rather than imitated.
//!
//! ```text
//! WireExchange ─ Routes::resolve ─▶ upstream path, IngressMode, Upstream
//!              ─ HeaderIdentifier::context ─▶ ClientContext (credential hashed)
//!              ─ AdapterDecoder::decode_now (head without credentials) ─▶ DecodedRequest
//!              ─▶ RawExchange ─ AnthropicMessages::normalize (L1) ─▶ NormalizedExchange
//! ```
//!
//! The same steps the proxy takes in `crosstalk_ingress::proxy` for a
//! generation request, minus the network: the route table, the identifier
//! and the adapter are the production ones, keyed with a fixed deployment
//! secret so credential digests are stable across runs.

use std::sync::Arc;

use crosstalk_canonical::AnthropicMessages;
use crosstalk_ingress::adapter::AnthropicAdapter;
use crosstalk_ingress::config::{LimitsConfig, RouteConfig, UpstreamConfig};
use crosstalk_ingress::decode::{AdapterDecoder, CaptureDecodeError, DecodeJob};
use crosstalk_ingress::identify::{self, HeaderIdentifier};
use crosstalk_ingress::routing::{ConfigError, Routes};
use crosstalk_spec::ids::{DeploymentSecret, KeyedHasher, SecretVersion};
use crosstalk_spec::interfaces::l0_ingress::{
    ProviderAdapter, RawExchange, RawResponse, RequestHead,
};
use crosstalk_spec::interfaces::l1_canonical::{NormalizeError, NormalizedExchange, Normalizer};
use crosstalk_spec::observed::client::{EndpointKind, RouteName, UpstreamId, UpstreamKind, Vendor};
use crosstalk_spec::observed::exchange::{ExchangeMeta, Transport};

use crate::scenario::WireExchange;

/// The route a harness's `ANTHROPIC_BASE_URL=http://gateway:8080/anthropic`
/// reaches.
pub const ROUTE: &str = "anthropic";

/// The deployment secret the scenario's credentials are keyed with: fixed,
/// so a credential's digest is the same on every run.
const SECRET: [u8; 32] = *b"crosstalk-e2e-deployment-secret!";

/// Why a scripted exchange could not be captured. Each case means the
/// scenario is not traffic the proxy would capture.
#[derive(Debug, thiserror::Error)]
pub enum CaptureError {
    #[error("the route table refused its config: {0}")]
    Routes(#[from] ConfigError),
    #[error("exchange {label}: no route matches {path}")]
    Unrouted { label: &'static str, path: String },
    #[error("exchange {label}: the adapter classifies it as {kind:?}, not a generation")]
    NotGeneration {
        label: &'static str,
        kind: Option<EndpointKind>,
    },
    #[error("exchange {label}: the request did not decode: {source}")]
    Decode {
        label: &'static str,
        #[source]
        source: CaptureDecodeError,
    },
    #[error("exchange {label}: L1 refused it: {refused:?}")]
    Normalize {
        label: &'static str,
        refused: NormalizeError,
    },
}

/// L0 and L1 as the proxy runs them, without the network.
pub struct Capture {
    routes: Routes,
    identifier: HeaderIdentifier,
    decoder: AdapterDecoder<AnthropicAdapter>,
}

impl Capture {
    /// One route, `/anthropic` to the Anthropic API.
    pub fn new() -> Result<Self, CaptureError> {
        let routes = Routes::new(&[RouteConfig {
            name: RouteName(ROUTE.to_owned()),
            prefix: format!("/{ROUTE}"),
            upstream: UpstreamConfig {
                id: UpstreamId("anthropic-api".to_owned()),
                kind: UpstreamKind::VendorApi(Vendor::Anthropic),
                base_url: "https://api.anthropic.com".to_owned(),
            },
        }])?;
        let limits = LimitsConfig::default();
        let adapter = Arc::new(AnthropicAdapter::new(
            limits.decoded_bytes,
            limits.sse_event_bytes,
        ));
        let keys = KeyedHasher::new(DeploymentSecret::new(SecretVersion(0), SECRET));
        Ok(Self {
            routes,
            identifier: HeaderIdentifier::new(keys),
            decoder: AdapterDecoder::new(adapter, limits.decoded_bytes),
        })
    }

    /// The raw exchange the proxy would hand to the capture channel.
    pub fn raw(&self, exchange: &WireExchange) -> Result<RawExchange, CaptureError> {
        let label = exchange.label;
        let request = &exchange.request;
        let resolved = self
            .routes
            .resolve(&request.path, request.query.as_deref())
            .ok_or_else(|| CaptureError::Unrouted {
                label,
                path: request.path.clone(),
            })?;
        let head = RequestHead {
            method: request.method.clone(),
            path: resolved.upstream_path.clone(),
            query: request.query.clone(),
            headers: request.headers.clone(),
        };
        match self.decoder.adapter().classify(&head) {
            Some(EndpointKind::Generation) => {}
            kind => return Err(CaptureError::NotGeneration { label, kind }),
        }
        let client =
            self.identifier
                .context(&head, resolved.mode, resolved.upstream, exchange.started_at);
        let decoded = self
            .decoder
            .decode_now(DecodeJob {
                head: identify::without_credentials(&head),
                body: request.body.clone(),
                client: client.clone(),
            })
            .map_err(|source| CaptureError::Decode { label, source })?;
        let transport = match exchange.response.headers.iter().any(|(name, value)| {
            name.eq_ignore_ascii_case("content-type") && value.starts_with("text/event-stream")
        }) {
            true => Transport::Sse,
            false => Transport::Http,
        };
        Ok(RawExchange {
            meta: ExchangeMeta {
                id: exchange.id,
                protocol: decoded.harness.protocol,
                transport,
                model: decoded.harness.model.clone(),
                client,
                started_at: exchange.started_at,
            },
            request: decoded,
            response: RawResponse::Complete {
                status: exchange.response.status,
                body: exchange.response.body.clone(),
            },
            first_chunk_at: Some(exchange.first_chunk_at),
            ended_at: exchange.ended_at,
        })
    }

    /// The normalized exchange `Pipeline::ingest` takes.
    pub fn normalized(&self, exchange: &WireExchange) -> Result<NormalizedExchange, CaptureError> {
        let raw = self.raw(exchange)?;
        AnthropicMessages
            .normalize(&raw)
            .map_err(|refused| CaptureError::Normalize {
                label: exchange.label,
                refused,
            })
    }
}
