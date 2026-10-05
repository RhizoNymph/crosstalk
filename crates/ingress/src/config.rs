//! The ingress configuration: routes, the deployment secrets' environment
//! variables, capture limits and the capture channel's capacity.
//!
//! Structured JSON in the spec's wire conventions (snake_case keys, unknown
//! fields refused). Secrets are never in the document: [`SecretRef`] names the
//! environment variable that holds one. [`IngressConfig::from_json`] parses;
//! [`crate::routing::Routes::new`] and [`crate::credential::load_secrets`]
//! check what the types cannot.

use std::net::SocketAddr;
use std::num::{NonZeroU64, NonZeroUsize};
use std::time::Duration;

use crosstalk_spec::ids::SecretVersion;
use crosstalk_spec::observed::client::{RouteName, UpstreamId, UpstreamKind};
use crosstalk_spec::support::Timestamp;
use serde::{Deserialize, Serialize};

/// Everything the L0 proxy is configured with.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct IngressConfig {
    /// Where the reverse proxy listens.
    pub listen: SocketAddr,
    /// Reverse-proxy routes, matched by path prefix (longest wins).
    pub routes: Vec<RouteConfig>,
    pub secrets: SecretsConfig,
    #[serde(default)]
    pub limits: LimitsConfig,
    #[serde(default)]
    pub capture: CaptureConfig,
}

impl IngressConfig {
    /// Parse a JSON document, refusing unknown fields.
    pub fn from_json(text: &str) -> Result<Self, serde_json::Error> {
        serde_json::from_str(text)
    }
}

/// One reverse-proxy route: requests under `prefix` go to `upstream`.
///
/// The prefix is the path of the harness's base URL
/// (`ANTHROPIC_BASE_URL=http://gateway:8080/anthropic` has prefix
/// `/anthropic`). It is removed from the request path, and the upstream base
/// URL's path is put in its place.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct RouteConfig {
    pub name: RouteName,
    /// `/` or `/segment[/segment…]`, without a trailing slash, query or
    /// fragment.
    pub prefix: String,
    pub upstream: UpstreamConfig,
}

/// Where a route forwards to.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct UpstreamConfig {
    pub id: UpstreamId,
    pub kind: UpstreamKind,
    /// `http://` or `https://`, a host, an optional port and an optional
    /// path (`https://api.anthropic.com`,
    /// `https://chatgpt.com/backend-api/codex`). No query.
    pub base_url: String,
}

/// The deployment secrets that key credential and account digests.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct SecretsConfig {
    /// The version every new digest is keyed with.
    pub current: SecretRef,
    /// During a rotation, the version before it, older than `current`: the
    /// proxy also computes digests under it
    /// (`ClientContext::previous_digests`) for exchanges that start before
    /// its `overlap_ends`.
    #[serde(default)]
    pub previous: Option<PreviousSecretRef>,
}

/// A secret by reference: its version and the environment variable that
/// holds its 32 bytes as 64 hex digits (surrounding whitespace, such as a
/// trailing newline, is ignored).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct SecretRef {
    pub version: SecretVersion,
    pub env: String,
}

/// The previous secret of a rotation, by reference, and when its overlap
/// ends.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct PreviousSecretRef {
    pub version: SecretVersion,
    pub env: String,
    /// The end of the overlap, exclusive: an exchange that starts before it
    /// is also hashed under this version, one that starts at or after it is
    /// not. RFC 3339 in UTC at microsecond precision, as every wire
    /// timestamp (`2026-11-01T00:00:00.000000Z`).
    pub overlap_ends: Timestamp,
}

/// Bounds on what capture may hold or wait for. None of them ever slows or
/// stops forwarding; exceeding one costs only the exchange's capture.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct LimitsConfig {
    /// The most request-body bytes the decode tee keeps for one exchange
    /// (`ingress.decode.tee-bounded`).
    #[serde(default = "defaults::request_tee_bytes")]
    pub request_tee_bytes: NonZeroU64,
    /// The most bytes a compressed request body may decode to
    /// (`ingress.encoding.decoded-size-bounded`).
    #[serde(default = "defaults::decoded_bytes")]
    pub decoded_bytes: NonZeroU64,
    /// The most response-body bytes capture keeps for one exchange. Past it,
    /// capture of that exchange is abandoned and counted; the client still
    /// gets every byte.
    #[serde(default = "defaults::response_capture_bytes")]
    pub response_capture_bytes: NonZeroU64,
    /// The largest single server-sent event the framer buffers; a longer
    /// one is a malformed frame.
    #[serde(default = "defaults::sse_event_bytes")]
    pub sse_event_bytes: NonZeroUsize,
    /// How long a captured exchange may wait for the response head, or for
    /// the next body chunk, before the proxy gives up on it with
    /// `ExchangeFailure::Timeout` and ends the client's response. `None`
    /// (the default) never times out: the client's own timeout applies, as
    /// it would without the proxy.
    #[serde(default)]
    pub upstream_idle_timeout_ms: Option<NonZeroU64>,
}

impl LimitsConfig {
    pub fn upstream_idle_timeout(&self) -> Option<Duration> {
        self.upstream_idle_timeout_ms
            .map(|millis| Duration::from_millis(millis.get()))
    }
}

impl Default for LimitsConfig {
    fn default() -> Self {
        Self {
            request_tee_bytes: defaults::request_tee_bytes(),
            decoded_bytes: defaults::decoded_bytes(),
            response_capture_bytes: defaults::response_capture_bytes(),
            sse_event_bytes: defaults::sse_event_bytes(),
            upstream_idle_timeout_ms: None,
        }
    }
}

/// The capture hand-off.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct CaptureConfig {
    /// How many finished exchanges may wait for the capture task.
    #[serde(default = "defaults::channel_capacity")]
    pub channel_capacity: NonZeroUsize,
}

impl Default for CaptureConfig {
    fn default() -> Self {
        Self {
            channel_capacity: defaults::channel_capacity(),
        }
    }
}

mod defaults {
    use std::num::{NonZeroU64, NonZeroUsize};

    const MIB: u64 = 1024 * 1024;

    pub fn request_tee_bytes() -> NonZeroU64 {
        NonZeroU64::MIN.saturating_add(32 * MIB - 1)
    }

    pub fn decoded_bytes() -> NonZeroU64 {
        NonZeroU64::MIN.saturating_add(64 * MIB - 1)
    }

    pub fn response_capture_bytes() -> NonZeroU64 {
        NonZeroU64::MIN.saturating_add(32 * MIB - 1)
    }

    pub fn sse_event_bytes() -> NonZeroUsize {
        NonZeroUsize::MIN.saturating_add(8 * 1024 * 1024 - 1)
    }

    pub fn channel_capacity() -> NonZeroUsize {
        NonZeroUsize::MIN.saturating_add(1023)
    }
}
