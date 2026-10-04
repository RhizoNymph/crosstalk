//! Postgres for the gateway: `crosstalk migrate`, and the connection that
//! `/readyz` checks under `crosstalk serve`.
//!
//! No layer stores anything in Postgres yet (the capture slice persists to
//! the blob store and the exchange log), so `serve` never needs the
//! database to forward or capture. With a `store` section it resolves
//! `DATABASE_URL` at start (a missing or malformed URL is a config error),
//! then connects in the background, retrying, and `/readyz` reports the
//! database reachable only once a pooled connection answers.

use std::ffi::OsString;
use std::time::Duration;

use crosstalk_store::{DbFailure, Layer, Store, StoreConfig, StoreError, classify};
use tokio::sync::watch;

use crate::config::{GatewayConfig, StoreSection};

/// How long the background connector waits between attempts.
const RECONNECT: Duration = Duration::from_secs(5);

/// How long a readiness probe waits for a pooled connection.
const PROBE: Duration = Duration::from_secs(2);

/// Resolve the store's config from `section` and `DATABASE_URL`.
pub fn store_config(
    section: StoreSection,
    lookup: impl Fn(&str) -> Option<String>,
) -> Result<StoreConfig, crosstalk_store::ConfigError> {
    StoreConfig::from_lookup(section.pool, |name| lookup(name).map(OsString::from))
}

/// Where the background connection stands.
#[derive(Debug, Clone)]
enum State {
    Connecting { last: Option<DbFailure> },
    Connected(Store),
}

/// What `/readyz` learns about the database.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StoreCheck {
    NotConfigured,
    Reachable,
    /// Not reachable, with why (never the URL).
    Unreachable(String),
}

/// The readiness side of the background connection.
#[derive(Debug, Clone)]
pub struct StoreProbe {
    state: Option<watch::Receiver<State>>,
}

impl StoreProbe {
    /// A probe for a gateway without a `store` section.
    pub fn not_configured() -> Self {
        Self { state: None }
    }

    /// Start connecting to `config` in the background, retrying until it
    /// succeeds. The task ends once connected, or when the probe and every
    /// clone of it are dropped.
    pub fn connect(config: StoreConfig) -> (Self, impl Future<Output = Option<Store>> + Send) {
        let (state, watched) = watch::channel(State::Connecting { last: None });
        let task = async move {
            loop {
                match Store::connect(&config).await {
                    Ok(store) => {
                        let _ = state.send(State::Connected(store.clone()));
                        return Some(store);
                    }
                    Err(error) => {
                        let failure = error.failure().cloned();
                        tracing::warn!(error = %error, retry_in_secs = RECONNECT.as_secs(), "postgres not reachable; retrying");
                        if state.send(State::Connecting { last: failure }).is_err() {
                            return None;
                        }
                        tokio::time::sleep(RECONNECT).await;
                    }
                }
            }
        };
        (
            Self {
                state: Some(watched),
            },
            task,
        )
    }

    /// Check the database now.
    pub async fn check(&self) -> StoreCheck {
        let Some(state) = &self.state else {
            return StoreCheck::NotConfigured;
        };
        let current = state.borrow().clone();
        match current {
            State::Connecting { last: None } => StoreCheck::Unreachable("connecting".to_owned()),
            State::Connecting {
                last: Some(failure),
            } => StoreCheck::Unreachable(format!("connecting: {failure}")),
            State::Connected(store) => {
                match tokio::time::timeout(PROBE, store.pool().acquire()).await {
                    Ok(Ok(_connection)) => StoreCheck::Reachable,
                    Ok(Err(error)) => StoreCheck::Unreachable(classify(&error).to_string()),
                    Err(_) => StoreCheck::Unreachable("no connection within 2s".to_owned()),
                }
            }
        }
    }
}

/// Why `crosstalk migrate` failed.
#[derive(Debug, thiserror::Error)]
pub enum MigrateError {
    #[error("the config has no store section; migrate needs one (and DATABASE_URL)")]
    NoStoreSection,
    #[error(transparent)]
    Config(#[from] crosstalk_store::ConfigError),
    #[error(transparent)]
    Store(#[from] StoreError),
}

/// Run every layer's migrations against the configured database, after
/// ensuring the required extensions. Idempotent.
pub async fn migrate(
    config: &GatewayConfig,
    lookup: impl Fn(&str) -> Option<String>,
) -> Result<(), MigrateError> {
    let section = config.store.ok_or(MigrateError::NoStoreSection)?;
    let store = Store::connect(&store_config(section, lookup)?).await?;
    let result = migrate_all(&store).await;
    store.close().await;
    result
}

async fn migrate_all(store: &Store) -> Result<(), MigrateError> {
    for installed in store.ensure_extensions().await? {
        tracing::info!(extension = %installed.extension, version = %installed.version, "extension ready");
    }
    // No layer crate embeds migrations yet; each one's `migrations/`
    // directory is run here (`Store::migrate`) as it gains one.
    for layer in Layer::ALL {
        tracing::info!(layer = %layer, migrations = 0, "layer migrations at head");
    }
    Ok(())
}
