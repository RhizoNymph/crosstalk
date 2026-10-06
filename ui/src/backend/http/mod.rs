//! A backend over a gateway's L8 surface over HTTP: `crosstalk_client`'s
//! [`HttpClient`], which implements `QueryApi`, `OperatorActions` and
//! `LiveFeed` over the binding (`GET`/`POST` routes, the live feed as SSE,
//! exports as JSONL).
//!
//! ```text
//! start(HttpConfig { url, token })
//!   HttpClient::new(url, ClientConfig::default()).with_token(token)
//!   identity::resolve: me() ─▶ the token's operator ─▶ Access   (error: no start)
//!   identity::spawn_refresh: every 30 s, me() again ─▶ watch ─▶ Identity
//! ```
//!
//! The server derives the caller from the token; the `&Caller` the trait
//! methods take is ignored. The UI's own caller (for its permission
//! gating and the "signed in as" label) is the server's operator for the
//! token, with the server's permissions ([`identity`]).
//!
//! The live feed (`HttpLiveStream`) reconnects with `Last-Event-ID` after
//! a cut, per the client's `ReconnectPolicy`, so the server replays what
//! the cut lost; it ends with `SessionEnded` on `401`/`403` and with
//! `ShuttingDown` once out of attempts. `/data/live` then sends its `end`
//! event and the browser's `EventSource` reconnects to the UI, which
//! subscribes again from the browser's last id.
//!
//! Failures render as the pages' error states and are logged ([`log`]).

pub mod failure;
pub mod identity;
pub mod log;

use crosstalk_client::{ClientConfig, HttpClient};
use tokio::sync::watch;
use tokio::task::JoinHandle;

use crate::config::HttpConfig;
use crate::identity::Identity;
use crate::url::ulid::UlidId;
use identity::{IdentityError, REFRESH};

/// The started http backend.
pub struct HttpStarted {
    pub client: HttpClient,
    pub identity: Identity,
    /// The identity refresher.
    pub refresh: JoinHandle<()>,
}

/// Connects to the surface `config` names and learns who the token is.
pub async fn start(config: &HttpConfig) -> Result<HttpStarted, IdentityError> {
    let client = HttpClient::new(config.url.clone(), ClientConfig::default())
        .with_token(config.token.clone());
    let access = identity::resolve(&client).await?;
    let operator = access.caller().operator();
    tracing::info!(
        url = %failure::public_url(&config.url),
        operator = %operator.to_ulid(),
        name = access.name(),
        permissions = ?access.caller().permissions(),
        "acting as the token's operator"
    );
    let (sender, receiver) = watch::channel(access);
    let refresh = identity::spawn_refresh(client.clone(), sender, REFRESH);
    Ok(HttpStarted {
        client,
        identity: Identity::watching(receiver),
        refresh,
    })
}

#[cfg(test)]
mod tests;
