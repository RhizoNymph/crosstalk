//! The config's sections and the checked values they are made of.

use std::net::SocketAddr;
use std::num::{NonZeroU32, NonZeroU64};
use std::path::PathBuf;
use std::time::Duration;

use crosstalk_store::PoolSettings;
use serde::Deserialize;

/// The name of an environment variable that holds a secret:
/// `{"env": "CROSSTALK_API_TOKEN"}`. Never the secret itself. Non-empty,
/// without `=` or NUL.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct EnvRef {
    pub env: EnvVarName,
}

/// A checked environment variable name.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(try_from = "String")]
pub struct EnvVarName(String);

/// Why a variable name is refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum InvalidEnvVarName {
    #[error("an environment variable name is empty")]
    Empty,
    #[error("an environment variable name contains '=' or NUL")]
    Forbidden,
}

impl TryFrom<String> for EnvVarName {
    type Error = InvalidEnvVarName;

    fn try_from(name: String) -> Result<Self, Self::Error> {
        if name.is_empty() {
            return Err(InvalidEnvVarName::Empty);
        }
        if name.contains(['=', '\0']) {
            return Err(InvalidEnvVarName::Forbidden);
        }
        Ok(Self(name))
    }
}

impl EnvVarName {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// An `http://` or `https://` URL with a host.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(try_from = "String")]
pub struct HttpUrl(String);

/// Why a URL is refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum InvalidHttpUrl {
    #[error("not a URL")]
    Unparseable,
    #[error("the scheme is not http or https")]
    Scheme,
    #[error("the URL has no host")]
    NoHost,
}

impl TryFrom<String> for HttpUrl {
    type Error = InvalidHttpUrl;

    fn try_from(text: String) -> Result<Self, Self::Error> {
        let uri: hyper::Uri = text.parse().map_err(|_| InvalidHttpUrl::Unparseable)?;
        match uri.scheme_str() {
            Some("http" | "https") => {}
            _ => return Err(InvalidHttpUrl::Scheme),
        }
        if uri.host().is_none_or(str::is_empty) {
            return Err(InvalidHttpUrl::NoHost);
        }
        Ok(Self(text))
    }
}

impl HttpUrl {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// A non-empty string.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(try_from = "String")]
pub struct NonEmpty(String);

/// The string was empty.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("the value is empty")]
pub struct EmptyString;

impl TryFrom<String> for NonEmpty {
    type Error = EmptyString;

    fn try_from(text: String) -> Result<Self, Self::Error> {
        if text.trim().is_empty() {
            Err(EmptyString)
        } else {
            Ok(Self(text))
        }
    }
}

impl NonEmpty {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// The L8 HTTP binding (roadmap P7.1). Accepted and checked; not bound
/// until the API exists.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct ApiConfig {
    pub listen: SocketAddr,
    /// The operator bearer token's variable.
    pub token: EnvRef,
}

/// The ops listener: `GET /metrics`, `/healthz`, `/readyz`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct OpsConfig {
    pub listen: SocketAddr,
}

/// Postgres. The URL is a secret and comes from `DATABASE_URL`; only the
/// pool sizing is configured here.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct StoreSection {
    pub pool: PoolSettings,
}

/// The blob store.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct BlobsConfig {
    /// `FsBlobStore`'s root. Its parent is the gateway's data directory:
    /// everything else the gateway writes to disk goes under it.
    pub root: PathBuf,
}

/// The OpenAI-compatible embeddings endpoint (roadmap P6). Accepted and
/// checked; nothing uses it yet.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct EmbeddingsConfig {
    pub base_url: HttpUrl,
    pub model: NonEmpty,
    pub api_key: EnvRef,
}

/// How the capture stage retries blob puts.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct PipelineConfig {
    /// How many times one exchange's bodies are put before the exchange is
    /// given up (and counted `store_failed`). Puts are idempotent, so a
    /// retry rewrites nothing that was stored.
    #[serde(default = "defaults::blob_put_attempts")]
    pub blob_put_attempts: NonZeroU32,
    /// The wait between two attempts.
    #[serde(default = "defaults::blob_put_backoff_ms")]
    pub blob_put_backoff_ms: NonZeroU64,
}

impl PipelineConfig {
    pub fn blob_put_backoff(&self) -> Duration {
        Duration::from_millis(self.blob_put_backoff_ms.get())
    }
}

impl Default for PipelineConfig {
    fn default() -> Self {
        Self {
            blob_put_attempts: defaults::blob_put_attempts(),
            blob_put_backoff_ms: defaults::blob_put_backoff_ms(),
        }
    }
}

/// How long a graceful shutdown waits. The defaults (45 s, then 10 s)
/// fit inside compose's 60 s stop grace period.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct ShutdownConfig {
    /// How long in-flight client connections may take to finish after the
    /// listener closes. A connection still open then is closed, and its
    /// exchange is captured as failed (`client_disconnected`).
    #[serde(default = "defaults::drain_timeout_ms")]
    pub drain_timeout_ms: NonZeroU64,
    /// How long the capture stage and the exchange log together may take
    /// to handle what the drained connections left them.
    #[serde(default = "defaults::flush_timeout_ms")]
    pub flush_timeout_ms: NonZeroU64,
}

impl ShutdownConfig {
    pub fn drain_timeout(&self) -> Duration {
        Duration::from_millis(self.drain_timeout_ms.get())
    }

    pub fn flush_timeout(&self) -> Duration {
        Duration::from_millis(self.flush_timeout_ms.get())
    }
}

impl Default for ShutdownConfig {
    fn default() -> Self {
        Self {
            drain_timeout_ms: defaults::drain_timeout_ms(),
            flush_timeout_ms: defaults::flush_timeout_ms(),
        }
    }
}

mod defaults {
    use std::num::{NonZeroU32, NonZeroU64};

    pub fn blob_put_attempts() -> NonZeroU32 {
        NonZeroU32::MIN.saturating_add(2)
    }

    pub fn blob_put_backoff_ms() -> NonZeroU64 {
        NonZeroU64::MIN.saturating_add(99)
    }

    pub fn drain_timeout_ms() -> NonZeroU64 {
        NonZeroU64::MIN.saturating_add(44_999)
    }

    pub fn flush_timeout_ms() -> NonZeroU64 {
        NonZeroU64::MIN.saturating_add(9_999)
    }
}
