//! Startup configuration, read from a JSON file.
//!
//! `CROSSTALK_UI_CONFIG` names the file (default `config.json` in the
//! working directory). There are no secrets in it: trusted mode has no
//! credentials, and the L8 client's credentials will come from the
//! environment.

use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use crosstalk_spec::ids::OperatorId;
use crosstalk_spec::interfaces::l8_surface::Caller;
use crosstalk_spec::interfaces::l8_surface::operators::{
    AccessConfig, InvalidAccessConfig, InvalidOperatorName, OperatorDirectory, OperatorName,
    RequestIdentity, TrustedOperator, Unauthenticated,
};

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
    /// Deterministic synthetic data generated from `seed`; with `replay`,
    /// the data's last stretch plays out in accelerated real time.
    Fixture {
        seed: u64,
        #[serde(default)]
        replay: Option<ReplayConfig>,
    },
}

/// What a replay plays: the last `window_minutes` of the data at `speed`
/// times real time.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReplayConfig {
    pub window_minutes: u64,
    pub speed: u32,
}

/// Who uses the UI: the spec's operator directory loaded from the trusted
/// operator config names, and the caller it gives every request.
///
/// Built only by [`Access::trusted`], which loads the directory and asks it
/// for the request caller once: in trusted mode the directory answers every
/// request with the same caller (the configured operator with every
/// permission), so it is kept rather than asked again per request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Access {
    /// The directory every caller comes from; kept with the caller it gave.
    #[allow(dead_code)]
    directory: OperatorDirectory,
    caller: Caller,
    name: OperatorName,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum AccessError {
    #[error("access config: {0:?}")]
    Config(InvalidAccessConfig),
    #[error("no caller for the trusted operator: {0:?}")]
    Unauthenticated(Unauthenticated),
}

impl Access {
    /// The directory of a trusted deployment of `operator`.
    pub fn trusted(operator: TrustedOperator) -> Result<Self, AccessError> {
        let name = operator.name.clone();
        let (directory, _changes) = OperatorDirectory::load(None, &AccessConfig::Trusted(operator))
            .map_err(AccessError::Config)?;
        let caller = directory
            .caller(RequestIdentity::Anonymous)
            .map_err(AccessError::Unauthenticated)?;
        Ok(Self {
            directory,
            caller,
            name,
        })
    }

    /// The caller of a request.
    pub fn caller(&self) -> Caller {
        self.caller.clone()
    }

    /// The signed-in operator's display name.
    pub fn name(&self) -> &str {
        self.name.as_str()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Config {
    pub listen: SocketAddr,
    pub access: Access,
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
    #[error("operator.name: {0:?}")]
    OperatorName(InvalidOperatorName),
    #[error(transparent)]
    Access(#[from] AccessError),
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
        let name = OperatorName::new(&raw.operator.name)
            .map_err(|e| ParseError::Config(ConfigError::OperatorName(e)))?;
        let access = Access::trusted(TrustedOperator { id, name })
            .map_err(|e| ParseError::Config(ConfigError::Access(e)))?;
        Ok(Self {
            listen: raw.listen,
            access,
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
        assert_eq!(config.access.name(), "researcher");
        assert_eq!(
            config.backend,
            BackendConfig::Fixture {
                seed: 7,
                replay: None
            }
        );
        assert_eq!(
            config.access.caller().permissions(),
            crosstalk_spec::interfaces::l8_surface::PermissionSet::ALL
        );
    }

    #[test]
    fn parses_the_demo_config_with_a_replay() {
        let text = include_str!("../config.demo.json");
        let config = Config::parse(text).ok().expect("demo config parses");
        assert_eq!(
            config.backend,
            BackendConfig::Fixture {
                seed: 7,
                replay: Some(ReplayConfig {
                    window_minutes: 240,
                    speed: 10
                })
            }
        );
    }

    #[test]
    fn rejects_unknown_keys_and_blank_names() {
        let unknown = r#"{"listen":"127.0.0.1:1","operator":{"id":"00000000000000000000000001","name":"a"},"backend":{"fixture":{"seed":1}},"extra":1}"#;
        assert!(matches!(Config::parse(unknown), Err(ParseError::Json(_))));
        let blank = r#"{"listen":"127.0.0.1:1","operator":{"id":"00000000000000000000000001","name":"  "},"backend":{"fixture":{"seed":1}}}"#;
        assert!(matches!(
            Config::parse(blank),
            Err(ParseError::Config(ConfigError::OperatorName(
                InvalidOperatorName::Blank
            )))
        ));
    }
}
