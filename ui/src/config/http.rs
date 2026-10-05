//! `"backend": {"http": ..}`: where the gateway's L8 HTTP API is, the
//! bearer token, and (only when the server lists several operators) which
//! one the token is.
//!
//! ```json
//! {"url": "http://crosstalk:8081", "token": {"env": "CROSSTALK_API_TOKEN"}}
//! ```
//!
//! The token is only ever an environment reference; an inline token does
//! not parse. It is read and checked here, so a missing variable, a short
//! token or a URL the client cannot use fails at startup:
//!
//! - `url` ─▶ `crosstalk_client::BaseUrl` (plain `http`, a host, no query);
//! - `token.env` ─▶ the variable's text ─▶ the API server's own check
//!   (`crosstalk_api::http::BearerToken::new`: at least 16 `b64token`
//!   characters, what the gateway accepts) ─▶ `crosstalk_client::BearerToken`.

use crosstalk_api::http::{BearerToken as ServerToken, InvalidBearerToken};
use crosstalk_client::{BaseUrl, BearerToken};
use crosstalk_spec::ids::OperatorId;

use super::ConfigError;
use crate::url::ulid::UlidId;

#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct RawHttp {
    url: String,
    token: EnvRef,
    #[serde(default)]
    operator: Option<String>,
}

/// A secret named by its environment variable: `{"env": "NAME"}`.
#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct EnvRef {
    env: String,
}

/// The http backend's settings, checked.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HttpConfig {
    pub url: BaseUrl,
    /// The operator's credential. Its `Debug` is redacted.
    pub token: BearerToken,
    pub operator: OperatorPick,
}

/// Which of the operators the server lists the token signs in as.
///
/// The spec has no "who am I" read, so the UI finds itself in
/// `QueryApi::operators`: the operators with permissions, current in the
/// server's directory.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OperatorPick {
    /// The only current operator (the gateway's: one token, one
    /// operator); several are a startup error.
    TheOnlyOne,
    /// The current operator with this id.
    Id(OperatorId),
}

/// Why the token's text is not one the API accepts.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum TokenError {
    #[error("shorter than {min} characters")]
    TooShort { min: usize },
    #[error("not b64token text (letters, digits, -._~+/ then any =)")]
    NotB64Token,
    #[error("the client refuses it as a bearer token")]
    Client,
}

impl RawHttp {
    pub(super) fn check(
        self,
        env: impl Fn(&str) -> Option<String>,
    ) -> Result<HttpConfig, ConfigError> {
        let url = BaseUrl::parse(&self.url).map_err(ConfigError::Url)?;
        let name = self.token.env;
        let text = env(&name).ok_or_else(|| ConfigError::TokenUnset { env: name.clone() })?;
        let token = token(&text).map_err(|reason| ConfigError::Token { env: name, reason })?;
        let operator = match self.operator {
            None => OperatorPick::TheOnlyOne,
            Some(id) => {
                OperatorPick::Id(OperatorId::parse_ulid(&id).map_err(ConfigError::HttpOperator)?)
            }
        };
        Ok(HttpConfig {
            url,
            token,
            operator,
        })
    }
}

/// The token, if the API server would hold it and the client send it.
fn token(text: &str) -> Result<BearerToken, TokenError> {
    ServerToken::new(text).map_err(|error| match error {
        InvalidBearerToken::TooShort { min } => TokenError::TooShort { min },
        // `new` never reads the environment.
        InvalidBearerToken::NotB64Token | InvalidBearerToken::Unset { .. } => {
            TokenError::NotB64Token
        }
    })?;
    BearerToken::new(text).map_err(|_| TokenError::Client)
}
