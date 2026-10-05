//! `"backend": {"http": ..}`: where the gateway's L8 HTTP API is and the
//! bearer token. Who the token is, the server says (`QueryApi::me`).
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

use super::ConfigError;

#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct RawHttp {
    url: String,
    token: EnvRef,
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
        Ok(HttpConfig { url, token })
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
