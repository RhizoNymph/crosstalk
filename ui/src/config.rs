//! Startup configuration, read from a JSON file.
//!
//! `CROSSTALK_UI_CONFIG` names the file (default `config.json` in the
//! working directory). There are no secrets in it: trusted mode has no
//! credentials, and the L8 client's credentials will come from the
//! environment.

use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use crosstalk_spec::ids::OperatorId;
use crosstalk_spec::interfaces::l8_surface::{Caller, Permission};

use crate::url::ulid::{InvalidUlid, UlidId};

pub const CONFIG_ENV: &str = "CROSSTALK_UI_CONFIG";
pub const DEFAULT_PATH: &str = "config.json";

#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct RawConfig {
    listen: SocketAddr,
    operator: RawOperator,
    backend: BackendConfig,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct RawOperator {
    id: String,
    name: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum BackendConfig {
    /// Deterministic synthetic data generated from `seed`.
    Fixture { seed: u64 },
}

/// The single operator of trusted mode. It holds every permission.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TrustedOperator {
    pub id: OperatorId,
    pub name: String,
}

impl TrustedOperator {
    pub fn caller(&self) -> Caller {
        Caller {
            operator: self.id,
            permissions: vec![
                Permission::View,
                Permission::Content,
                Permission::Govern,
                Permission::Triage,
                Permission::Operate,
            ],
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Config {
    pub listen: SocketAddr,
    pub operator: TrustedOperator,
    pub backend: BackendConfig,
}

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("reading {path}: {source}")]
    Read {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("parsing {path}: {source}")]
    Parse {
        path: PathBuf,
        source: serde_json::Error,
    },
    #[error("operator.id: {0}")]
    OperatorId(InvalidUlid),
    #[error("operator.name is empty")]
    OperatorName,
}

impl Config {
    pub fn from_env() -> Result<Self, ConfigError> {
        let path =
            std::env::var_os(CONFIG_ENV).map_or_else(|| PathBuf::from(DEFAULT_PATH), PathBuf::from);
        Self::load(&path)
    }

    pub fn load(path: &Path) -> Result<Self, ConfigError> {
        let text = std::fs::read_to_string(path).map_err(|source| ConfigError::Read {
            path: path.to_owned(),
            source,
        })?;
        Self::parse(&text).map_err(|err| match err {
            ParseError::Json(source) => ConfigError::Parse {
                path: path.to_owned(),
                source,
            },
            ParseError::Config(err) => err,
        })
    }

    fn parse(text: &str) -> Result<Self, ParseError> {
        let raw: RawConfig = serde_json::from_str(text).map_err(ParseError::Json)?;
        let id = OperatorId::parse_ulid(&raw.operator.id)
            .map_err(|e| ParseError::Config(ConfigError::OperatorId(e)))?;
        let name = raw.operator.name.trim();
        if name.is_empty() {
            return Err(ParseError::Config(ConfigError::OperatorName));
        }
        Ok(Self {
            listen: raw.listen,
            operator: TrustedOperator {
                id,
                name: name.to_owned(),
            },
            backend: raw.backend,
        })
    }
}

enum ParseError {
    Json(serde_json::Error),
    Config(ConfigError),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_the_shipped_config() {
        let text = include_str!("../config.json");
        let config = Config::parse(text).ok().expect("shipped config parses");
        assert_eq!(config.operator.name, "researcher");
        assert_eq!(config.backend, BackendConfig::Fixture { seed: 7 });
        assert_eq!(config.operator.caller().permissions.len(), 5);
    }

    #[test]
    fn rejects_unknown_keys_and_blank_names() {
        let unknown = r#"{"listen":"127.0.0.1:1","operator":{"id":"00000000000000000000000001","name":"a"},"backend":{"fixture":{"seed":1}},"extra":1}"#;
        assert!(matches!(Config::parse(unknown), Err(ParseError::Json(_))));
        let blank = r#"{"listen":"127.0.0.1:1","operator":{"id":"00000000000000000000000001","name":"  "},"backend":{"fixture":{"seed":1}}}"#;
        assert!(matches!(
            Config::parse(blank),
            Err(ParseError::Config(ConfigError::OperatorName))
        ));
    }
}
