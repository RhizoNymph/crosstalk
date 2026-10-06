//! Startup configuration, read from a JSON file.
//!
//! `CROSSTALK_UI_CONFIG` names the file (default `config.json` in the
//! working directory). There are no secrets in it: the local backends run
//! in trusted mode, which has no credentials, and the http backend's
//! bearer token is only ever named by its environment variable
//! (`{"env": "CROSSTALK_API_TOKEN"}`), read and checked while parsing.
//!
//! ```text
//! {"listen", "operator"?, "backend": {"fixture" | "world" | "http": ..}}
//!   fixture, world ─▶ need "operator": the trusted operator they act as  ─▶ Access
//!   http           ─▶ refuse "operator": the server says who the token is ─▶ HttpConfig
//!                     url ─▶ BaseUrl, token.env ─▶ the variable ─▶ BearerToken
//! ```

mod http;

use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use crosstalk_spec::ids::OperatorId;
use crosstalk_spec::interfaces::l8_surface::Caller;
use crosstalk_spec::interfaces::l8_surface::operators::{
    AccessConfig, InvalidAccessConfig, InvalidOperatorName, Operator, OperatorConfig,
    OperatorDirectory, OperatorName, RequestIdentity, TrustedOperator, Unauthenticated,
};

use crate::url::ulid::{InvalidUlid, UlidId};

use http::RawHttp;
pub use http::{HttpConfig, TokenError};

pub const CONFIG_ENV: &str = "CROSSTALK_UI_CONFIG";
pub const DEFAULT_PATH: &str = "config.json";

#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct RawConfig {
    listen: SocketAddr,
    #[serde(default)]
    operator: Option<RawOperator>,
    backend: RawBackend,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct RawOperator {
    id: String,
    name: String,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
enum RawBackend {
    Fixture {
        seed: u64,
        #[serde(default)]
        replay: Option<ReplayConfig>,
    },
    World {
        seed: u64,
    },
    Http(RawHttp),
}

/// The configured backend, with what it needs to start.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BackendConfig {
    /// Deterministic synthetic data generated from `seed`; with `replay`,
    /// the data's last stretch plays out in accelerated real time. Every
    /// request acts as the trusted operator of `access`.
    Fixture {
        access: Access,
        seed: u64,
        replay: Option<ReplayConfig>,
    },
    /// The real L8 surface in this process (`crosstalk_api::InProcess` over
    /// the memory stores), seeded with the synthetic world of `seed`
    /// (`crosstalk-world`). Every request acts as the trusted operator of
    /// `access`.
    World { access: Access, seed: u64 },
    /// A gateway's L8 surface over HTTP (`crosstalk-client`). Who the UI
    /// acts as is the server's answer for the token, not config's.
    Http(HttpConfig),
}

/// What a replay plays: the last `window_minutes` of the data at `speed`
/// times real time.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReplayConfig {
    pub window_minutes: u64,
    pub speed: u32,
}

/// Who uses the UI: the operator's display name and the caller every
/// request carries, built from a spec operator directory, so its
/// permissions are the ones that directory gives.
///
/// Built by [`Access::trusted`] (the local backends: the configured
/// operator with every permission) or [`Access::of_operator`] (the http
/// backend: the operator `QueryApi::me` answers for the token, with the
/// permissions the token was authenticated with).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Access {
    caller: Caller,
    name: OperatorName,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum AccessError {
    #[error("access config: {0:?}")]
    Config(InvalidAccessConfig),
    #[error("no caller for the operator: {0:?}")]
    Unauthenticated(Unauthenticated),
}

impl Access {
    /// The one operator of a trusted deployment, with every permission.
    pub fn trusted(operator: TrustedOperator) -> Result<Self, AccessError> {
        let name = operator.name.clone();
        Self::from_directory(
            &AccessConfig::Trusted(operator),
            RequestIdentity::Anonymous,
            name,
        )
    }

    /// `operator` as a directory lists it, with exactly its permissions:
    /// what the server's directory gives the operator its token names.
    /// Fails for a former operator (no permissions), whom no directory
    /// gives a caller.
    pub fn of_operator(operator: &Operator) -> Result<Self, AccessError> {
        let config = AccessConfig::Authenticated(vec![OperatorConfig {
            id: operator.id,
            name: operator.name.clone(),
            permissions: operator.permissions,
        }]);
        Self::from_directory(
            &config,
            RequestIdentity::Verified(operator.id),
            operator.name.clone(),
        )
    }

    fn from_directory(
        config: &AccessConfig,
        identity: RequestIdentity,
        name: OperatorName,
    ) -> Result<Self, AccessError> {
        let (directory, _changes) =
            OperatorDirectory::load(None, config).map_err(AccessError::Config)?;
        let caller = directory
            .caller(identity)
            .map_err(AccessError::Unauthenticated)?;
        Ok(Self { caller, name })
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
    #[error("the {backend} backend needs \"operator\": the trusted operator it acts as")]
    MissingOperator { backend: &'static str },
    #[error("the http backend takes no \"operator\": the server says who the token is")]
    OperatorBesideHttp,
    #[error("backend.http.url: {0}")]
    Url(crosstalk_client::InvalidBaseUrl),
    #[error("backend.http.token: the environment variable {env} is not set or not UTF-8")]
    TokenUnset { env: String },
    #[error("backend.http.token (from {env}): {reason}")]
    Token { env: String, reason: TokenError },
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
        Self::parse(&text, |name| std::env::var(name).ok()).map_err(|err| match err {
            ParseError::Json(source) => ConfigError::Parse {
                path: path.to_owned(),
                source,
            },
            ParseError::Config(err) => err,
        })
    }

    /// `text` checked into a config, reading environment variables through
    /// `env`.
    fn parse(text: &str, env: impl Fn(&str) -> Option<String>) -> Result<Self, ParseError> {
        let raw: RawConfig = serde_json::from_str(text).map_err(ParseError::Json)?;
        let backend = match raw.backend {
            RawBackend::Fixture { seed, replay } => BackendConfig::Fixture {
                access: trusted(raw.operator, "fixture")?,
                seed,
                replay,
            },
            RawBackend::World { seed } => BackendConfig::World {
                access: trusted(raw.operator, "world")?,
                seed,
            },
            RawBackend::Http(http) => {
                if raw.operator.is_some() {
                    return Err(ParseError::Config(ConfigError::OperatorBesideHttp));
                }
                BackendConfig::Http(http.check(env).map_err(ParseError::Config)?)
            }
        };
        Ok(Self {
            listen: raw.listen,
            backend,
        })
    }
}

/// The trusted operator a local backend acts as.
fn trusted(raw: Option<RawOperator>, backend: &'static str) -> Result<Access, ParseError> {
    let raw = raw.ok_or(ParseError::Config(ConfigError::MissingOperator { backend }))?;
    let id = OperatorId::parse_ulid(&raw.id)
        .map_err(|e| ParseError::Config(ConfigError::OperatorId(e)))?;
    let name = OperatorName::new(&raw.name)
        .map_err(|e| ParseError::Config(ConfigError::OperatorName(e)))?;
    Access::trusted(TrustedOperator { id, name })
        .map_err(|e| ParseError::Config(ConfigError::Access(e)))
}

enum ParseError {
    Json(serde_json::Error),
    Config(ConfigError),
}

#[cfg(test)]
mod tests;
