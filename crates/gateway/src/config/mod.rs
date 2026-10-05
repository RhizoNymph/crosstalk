//! The gateway's configuration: one JSON document in the spec's wire
//! conventions (snake_case keys, unknown fields refused at every level),
//! with secrets only as `{"env": ...}` references. The shape is the
//! deployment contract (`docs/features/deploy.md`):
//!
//! | Key | Section |
//! | --- | --- |
//! | `ingress` | ingress's [`IngressConfig`], unchanged: the proxy's listen address, routes, secrets, limits and capture channel capacity |
//! | `api` | [`ApiConfig`], optional: the L8 HTTP binding's listen address, bearer token variable and the operator the token signs in as |
//! | `ops` | [`OpsConfig`]: `/metrics`, `/healthz`, `/readyz` |
//! | `store` | [`StoreSection`], optional: Postgres pool sizing (the URL is `DATABASE_URL`) |
//! | `blobs` | [`BlobsConfig`]: the blob store's root, whose parent is the data directory |
//! | `embeddings` | [`EmbeddingsConfig`], optional; checked, unused until P6 |
//! | `bus`, `pipeline`, `shutdown` | optional tuning: transport's [`BusConfig`], [`PipelineConfig`], [`ShutdownConfig`] |
//! | `flow` | optional: L5's [`FlowConfig`] (`correlation_window_ms`, `evidence_window_ms`, `suspected_ttl_ms`, `content_retention_ms`, `shards`, `tick_ms`), each key defaulted |
//! | `extract` | optional: L5's extractors, [`ExtractConfig`] (`mcp_servers`, `http_tools`, `fetch_tools`, `sites`), each key defaulted |
//!
//! A relative `blobs.root` is resolved against the directory of the config
//! file it was read from ([`GatewayConfig::load`]).

mod sections;

use std::path::{Path, PathBuf};

pub use crosstalk_flow::consumer::FlowConfig;
pub use crosstalk_flow::extract::ExtractConfig;
use crosstalk_ingress::config::IngressConfig;
use crosstalk_transport::BusConfig;
use serde::Deserialize;

pub use sections::{
    ApiConfig, ApiOperator, BlobsConfig, EmbeddingsConfig, EmptyString, EnvRef, EnvVarName,
    HttpUrl, InvalidEnvVarName, InvalidHttpUrl, NonEmpty, OpsConfig, PipelineConfig,
    ShutdownConfig, StoreSection,
};

/// Everything the gateway is configured with.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct GatewayConfig {
    pub ingress: IngressConfig,
    #[serde(default)]
    pub api: Option<ApiConfig>,
    pub ops: OpsConfig,
    #[serde(default)]
    pub store: Option<StoreSection>,
    pub blobs: BlobsConfig,
    #[serde(default)]
    pub embeddings: Option<EmbeddingsConfig>,
    #[serde(default)]
    pub bus: BusConfig,
    #[serde(default)]
    pub pipeline: PipelineConfig,
    #[serde(default)]
    pub shutdown: ShutdownConfig,
    #[serde(default)]
    pub flow: FlowConfig,
    #[serde(default)]
    pub extract: ExtractConfig,
}

/// Why a config could not be read.
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("reading the config file {path}: {source}")]
    Read {
        path: PathBuf,
        source: std::io::Error,
    },
    /// The document is not a gateway config: bad JSON, a missing or
    /// unknown field, or a value a checked type refuses.
    #[error("parsing the config: {0}")]
    Parse(#[from] serde_json::Error),
    /// `blobs.root` has no parent directory to hold the gateway's other
    /// files.
    #[error("blobs.root {0} has no parent directory")]
    NoDataDir(PathBuf),
}

impl GatewayConfig {
    /// Parse a JSON document, refusing unknown fields. `blobs.root` is
    /// kept as written; [`GatewayConfig::data_dir`] checks it has a parent.
    pub fn from_json(text: &str) -> Result<Self, ConfigError> {
        Ok(serde_json::from_str(text)?)
    }

    /// Read and parse the config file at `path`, resolving a relative
    /// `blobs.root` against the file's directory.
    pub fn load(path: &Path) -> Result<Self, ConfigError> {
        let text = std::fs::read_to_string(path).map_err(|source| ConfigError::Read {
            path: path.to_owned(),
            source,
        })?;
        let mut config = Self::from_json(&text)?;
        if config.blobs.root.is_relative() {
            let base = path.parent().unwrap_or_else(|| Path::new(""));
            config.blobs.root = base.join(&config.blobs.root);
        }
        config.data_dir()?;
        Ok(config)
    }

    /// The data directory: the parent of `blobs.root`. Everything the
    /// gateway writes to disk is under it.
    pub fn data_dir(&self) -> Result<&Path, ConfigError> {
        self.blobs
            .root
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .ok_or_else(|| ConfigError::NoDataDir(self.blobs.root.clone()))
    }

    /// The exchange log (the P3 stopgap):
    /// `<data dir>/exchanges/exchange-log.jsonl`.
    pub fn exchange_log_path(&self) -> Result<PathBuf, ConfigError> {
        Ok(exchange_log_path(self.data_dir()?))
    }
}

/// The exchange log under a data directory.
pub fn exchange_log_path(data_dir: &Path) -> PathBuf {
    data_dir.join("exchanges").join("exchange-log.jsonl")
}

#[cfg(test)]
mod tests;
