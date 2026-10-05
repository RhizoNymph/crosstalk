//! Where the client connects, the credential it presents and how long it
//! waits. Every type here is checked when built, so a client never holds a
//! base URL it cannot join a path to, a token the surface would refuse as
//! malformed, or a timeout of zero.

use std::fmt;
use std::num::NonZeroU32;
use std::time::Duration;

use crosstalk_spec::interfaces::l8_surface::http::auth::{Credential, CredentialHeaders, Field};
use hyper::Uri;
use hyper::header::HeaderValue;

/// The API's root: `http://host[:port][/prefix]`. Templates are relative to
/// it, so a deployment that mounts the API under a prefix on a shared
/// origin includes the prefix here (`http_api.md`, non-scope).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BaseUrl {
    /// `http://host:port`, without a trailing `/`.
    origin: String,
    /// `""`, or the prefix starting with `/` and without a trailing `/`.
    prefix: String,
}

/// Why text is not a [`BaseUrl`].
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum InvalidBaseUrl {
    #[error("not a URI: {reason}")]
    NotAUri { reason: String },
    /// Only plain `http` is spoken; TLS is the deployment's (a sidecar or a
    /// local listener), outside the binding.
    #[error("the scheme must be http")]
    NotHttp,
    #[error("no host")]
    NoHost,
    #[error("a base URL has no query")]
    Query,
}

impl BaseUrl {
    pub fn parse(text: &str) -> Result<Self, InvalidBaseUrl> {
        let uri: Uri = text
            .parse()
            .map_err(
                |error: hyper::http::uri::InvalidUri| InvalidBaseUrl::NotAUri {
                    reason: error.to_string(),
                },
            )?;
        if uri.scheme_str() != Some("http") {
            return Err(InvalidBaseUrl::NotHttp);
        }
        let authority = uri.authority().ok_or(InvalidBaseUrl::NoHost)?;
        if authority.host().is_empty() {
            return Err(InvalidBaseUrl::NoHost);
        }
        if uri.query().is_some() {
            return Err(InvalidBaseUrl::Query);
        }
        Ok(Self {
            origin: format!("http://{authority}"),
            prefix: uri.path().trim_end_matches('/').to_owned(),
        })
    }

    /// The request target for `path` (a route's filled template, starting
    /// with `/`) and an already form-encoded `query` (empty for none).
    pub(crate) fn join(&self, path: &str, query: &str) -> String {
        let mut target = format!("{}{}{}", self.origin, self.prefix, path);
        if !query.is_empty() {
            target.push('?');
            target.push_str(query);
        }
        target
    }
}

impl fmt::Display for BaseUrl {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}{}", self.origin, self.prefix)
    }
}

/// A bearer token, sent as `Authorization: Bearer <token>`: the credential
/// of a client that is not a browser (`http_api.md`, Authentication).
///
/// Built only through [`BearerToken::new`], which accepts exactly the
/// tokens the binding's own credential reader takes as a bearer token
/// (`b64token` text), so the surface never refuses one as malformed. Its
/// `Debug` hides it (`surface.api.no-raw-credentials`), and its header
/// value is marked sensitive.
#[derive(Clone, PartialEq, Eq)]
pub struct BearerToken {
    header: HeaderValue,
}

/// A token the surface would read as a malformed credential.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("not a bearer token: b64token text expected")]
pub struct InvalidToken;

impl BearerToken {
    pub fn new(token: &str) -> Result<Self, InvalidToken> {
        let value = format!("Bearer {token}");
        let headers = CredentialHeaders {
            authorization: Field::One(&value),
            cookie: None,
        };
        match headers.credential() {
            Ok(Some(Credential::Bearer(secret))) if secret.expose() == token => {
                let mut header = HeaderValue::from_str(&value).map_err(|_| InvalidToken)?;
                header.set_sensitive(true);
                Ok(Self { header })
            }
            Ok(_) | Err(_) => Err(InvalidToken),
        }
    }

    /// The `Authorization` header's value.
    pub(crate) fn header(&self) -> &HeaderValue {
        &self.header
    }
}

impl fmt::Debug for BearerToken {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("BearerToken(<redacted>)")
    }
}

/// How the live feed reconnects after its connection is cut: up to
/// `attempts` tries in a row, waiting `first_delay` before the second and
/// doubling up to `max_delay`. A delivered item starts the count again.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReconnectPolicy {
    attempts: NonZeroU32,
    first_delay: Duration,
    max_delay: Duration,
}

impl ReconnectPolicy {
    pub fn new(
        attempts: NonZeroU32,
        first_delay: Duration,
        max_delay: Duration,
    ) -> Result<Self, InvalidConfig> {
        if max_delay < first_delay {
            return Err(InvalidConfig::DelayCapBelowFirst);
        }
        Ok(Self {
            attempts,
            first_delay,
            max_delay,
        })
    }

    pub fn attempts(self) -> NonZeroU32 {
        self.attempts
    }

    /// The wait before try `attempt` (from 1): none before the first, then
    /// `first_delay`, doubling, at most `max_delay`.
    pub fn delay(self, attempt: u32) -> Duration {
        if attempt <= 1 {
            return Duration::ZERO;
        }
        let doublings = (attempt - 2).min(31);
        self.first_delay
            .saturating_mul(1_u32 << doublings)
            .min(self.max_delay)
    }
}

impl Default for ReconnectPolicy {
    fn default() -> Self {
        Self {
            attempts: NonZeroU32::new(8).unwrap_or(NonZeroU32::MIN),
            first_delay: Duration::from_millis(250),
            max_delay: Duration::from_secs(10),
        }
    }
}

/// Why a [`ClientConfig`] or [`ReconnectPolicy`] was refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum InvalidConfig {
    #[error("a timeout of zero")]
    ZeroTimeout,
    #[error("a response limit of zero bytes")]
    ZeroLimit,
    #[error("the reconnect delay cap is below the first delay")]
    DelayCapBelowFirst,
}

/// The client's limits. Built with [`ClientConfig::default`] and the
/// checked setters, so no timeout or limit is zero.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClientConfig {
    request_timeout: Duration,
    idle_timeout: Duration,
    max_response_bytes: usize,
    reconnect: ReconnectPolicy,
    frame_cache: usize,
}

impl Default for ClientConfig {
    /// 30 s per call, 60 s of silence ends a stream (the live feed sends a
    /// heartbeat well within it), 64 MiB per body or line, the default
    /// reconnect policy, and four projection frames kept for revalidation.
    fn default() -> Self {
        Self {
            request_timeout: Duration::from_secs(30),
            idle_timeout: Duration::from_secs(60),
            max_response_bytes: 64 * 1024 * 1024,
            reconnect: ReconnectPolicy::default(),
            frame_cache: 4,
        }
    }
}

impl ClientConfig {
    /// How long a call that answers with one body (JSON or a frame) may
    /// take, from sending to the last byte; and how long a stream's
    /// response head may take.
    pub fn with_request_timeout(mut self, timeout: Duration) -> Result<Self, InvalidConfig> {
        if timeout.is_zero() {
            return Err(InvalidConfig::ZeroTimeout);
        }
        self.request_timeout = timeout;
        Ok(self)
    }

    /// How long a stream (the live feed, an export) may send nothing
    /// before the client treats it as cut.
    pub fn with_idle_timeout(mut self, timeout: Duration) -> Result<Self, InvalidConfig> {
        if timeout.is_zero() {
            return Err(InvalidConfig::ZeroTimeout);
        }
        self.idle_timeout = timeout;
        Ok(self)
    }

    /// The largest body read whole, and the longest SSE or JSONL line.
    pub fn with_max_response_bytes(mut self, bytes: usize) -> Result<Self, InvalidConfig> {
        if bytes == 0 {
            return Err(InvalidConfig::ZeroLimit);
        }
        self.max_response_bytes = bytes;
        Ok(self)
    }

    pub fn with_reconnect(mut self, reconnect: ReconnectPolicy) -> Self {
        self.reconnect = reconnect;
        self
    }

    /// How many ready projection frames to keep for `If-None-Match`
    /// revalidation; zero keeps none.
    pub fn with_frame_cache(mut self, frames: usize) -> Self {
        self.frame_cache = frames;
        self
    }

    pub fn request_timeout(&self) -> Duration {
        self.request_timeout
    }

    pub fn idle_timeout(&self) -> Duration {
        self.idle_timeout
    }

    pub fn max_response_bytes(&self) -> usize {
        self.max_response_bytes
    }

    pub fn reconnect(&self) -> ReconnectPolicy {
        self.reconnect
    }

    pub fn frame_cache(&self) -> usize {
        self.frame_cache
    }
}
