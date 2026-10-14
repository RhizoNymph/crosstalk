//! The config's sections and the checked values they are made of.

use std::net::SocketAddr;
use std::num::{NonZeroU32, NonZeroU64};
use std::path::PathBuf;
use std::time::Duration;

use crosstalk_spec::ids::OperatorId;
use crosstalk_spec::interfaces::l8_surface::operators::OperatorName;
use crosstalk_store::PoolSettings;
use crosstalk_transport::{InvalidSpoolConfig, NonZeroDuration, PgBusConfig, SpoolConfig};
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

/// The L8 HTTP binding (roadmap P7.1), served on `listen` over the live
/// process's surface by the `all` and `api` roles.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct ApiConfig {
    pub listen: SocketAddr,
    /// The operator bearer token's variable. A request carrying
    /// `Authorization: Bearer <token>` is made as [`ApiConfig::operator`].
    pub token: EnvRef,
    /// The one operator the token is mapped to. It holds every
    /// permission. Defaults to an operator named `admin`.
    #[serde(default)]
    pub operator: ApiOperator,
}

/// The operator the API's bearer token signs in as: `{"name": "admin"}`.
/// Its id is fixed ([`ApiOperator::ID`]), so renaming it keeps its audit
/// history.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct ApiOperator {
    pub name: OperatorName,
}

impl ApiOperator {
    /// The operator's id: the first ULID, in every deployment.
    pub const ID: OperatorId = OperatorId::from_ulid(1);
}

impl Default for ApiOperator {
    fn default() -> Self {
        Self {
            name: OperatorName::new("admin").unwrap_or_else(|_| unreachable_name()),
        }
    }
}

/// "admin" is a valid operator name (non-empty, short, no control
/// characters), so this is never reached; it exists so the default needs
/// no `expect`.
fn unreachable_name() -> OperatorName {
    unreachable!("\"admin\" is a valid operator name")
}

/// The ops listener: `GET /metrics`, `/healthz`, `/readyz`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct OpsConfig {
    pub listen: SocketAddr,
}

/// Postgres. The URL is a secret and comes from `DATABASE_URL`; only the
/// pool sizing and the durable bus are configured here. With this section
/// `serve` runs on the Postgres stores and `PgBus` (behind the publish
/// spool); without it, on the memory stores and `MpscBus` (decision Q7).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct StoreSection {
    /// The pool every store, the bus and the background connector share.
    /// The bus holds one connection for `LISTEN` and the pipeline lock one
    /// of its own (outside the pool), so `max_connections` must be at
    /// least [`StoreSection::MIN_CONNECTIONS`].
    pub pool: PoolSettings,
    /// `PgBus`: group capacity, ack timeout, poll, publish timeout and log
    /// retention, each defaulted (`*_micros`).
    #[serde(default)]
    pub bus: PgBusConfig,
}

impl StoreSection {
    /// The fewest pooled connections a pipeline process runs on: the bus's
    /// `LISTEN` connection, one for a consumer's transaction, one for a
    /// store write.
    pub const MIN_CONNECTIONS: u32 = 3;
}

/// The publish spool (`<data dir>/spool/` by default): where envelopes go
/// while Postgres is unreachable. Every key is defaulted:
/// `{"max_bytes": 1073741824, "segment_bytes": 67108864, "drain_batch": 256,
/// "probe_ms": 1000}`, plus an optional `dir` under the data directory.
/// Only used with a `store` section.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct SpoolSection {
    /// The spool's directory: relative to the data directory, or absolute
    /// inside it. Defaults to `spool`.
    #[serde(default)]
    pub dir: Option<PathBuf>,
    #[serde(default = "defaults::spool_max_bytes")]
    pub max_bytes: NonZeroU64,
    #[serde(default = "defaults::spool_segment_bytes")]
    pub segment_bytes: NonZeroU64,
    #[serde(default = "defaults::spool_drain_batch")]
    pub drain_batch: std::num::NonZeroUsize,
    #[serde(default = "defaults::spool_probe_ms")]
    pub probe_ms: NonZeroU64,
}

impl Default for SpoolSection {
    fn default() -> Self {
        Self {
            dir: None,
            max_bytes: defaults::spool_max_bytes(),
            segment_bytes: defaults::spool_segment_bytes(),
            drain_batch: defaults::spool_drain_batch(),
            probe_ms: defaults::spool_probe_ms(),
        }
    }
}

/// Why the spool section does not describe a spool.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum InvalidSpoolSection {
    #[error("spool.dir {} is not inside the data directory {}", dir.display(), data_dir.display())]
    OutsideDataDir { dir: PathBuf, data_dir: PathBuf },
    #[error("spool.dir {} climbs out with `..`", .0.display())]
    ParentComponent(PathBuf),
    #[error(transparent)]
    Bounds(#[from] InvalidSpoolConfig),
}

impl SpoolSection {
    /// The spool's directory under `data_dir`.
    pub fn dir_in(&self, data_dir: &std::path::Path) -> Result<PathBuf, InvalidSpoolSection> {
        let dir = match &self.dir {
            None => return Ok(data_dir.join("spool")),
            Some(dir) => dir,
        };
        if dir
            .components()
            .any(|part| matches!(part, std::path::Component::ParentDir))
        {
            return Err(InvalidSpoolSection::ParentComponent(dir.clone()));
        }
        let resolved = match dir.is_absolute() {
            true => dir.clone(),
            false => data_dir.join(dir),
        };
        if !resolved.starts_with(data_dir) || resolved == data_dir {
            return Err(InvalidSpoolSection::OutsideDataDir {
                dir: dir.clone(),
                data_dir: data_dir.to_owned(),
            });
        }
        Ok(resolved)
    }

    /// The transport's checked spool config: the directory under
    /// `data_dir`, `segment_bytes` no larger than `max_bytes`.
    pub fn spool_config(
        &self,
        data_dir: &std::path::Path,
    ) -> Result<SpoolConfig, InvalidSpoolSection> {
        let probe = NonZeroDuration::from_micros(
            NonZeroU64::new(self.probe_ms.get().saturating_mul(1000)).unwrap_or(NonZeroU64::MAX),
        );
        Ok(SpoolConfig::with_limits(
            self.dir_in(data_dir)?,
            self.max_bytes,
            self.segment_bytes,
            self.drain_batch,
            probe,
        )?)
    }
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

    pub fn spool_max_bytes() -> NonZeroU64 {
        NonZeroU64::MIN.saturating_add((1 << 30) - 1)
    }

    pub fn spool_segment_bytes() -> NonZeroU64 {
        NonZeroU64::MIN.saturating_add((64 << 20) - 1)
    }

    pub fn spool_drain_batch() -> std::num::NonZeroUsize {
        std::num::NonZeroUsize::MIN.saturating_add(255)
    }

    pub fn spool_probe_ms() -> NonZeroU64 {
        NonZeroU64::MIN.saturating_add(999)
    }
}
