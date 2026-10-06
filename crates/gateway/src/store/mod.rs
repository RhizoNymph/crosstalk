//! Postgres for the gateway: `crosstalk migrate`, the connection `/readyz`
//! checks under `crosstalk serve`, the migration head check and the
//! pipeline lock.
//!
//! With a `store` section `serve` resolves `DATABASE_URL` at start (a
//! missing or malformed URL is a config error), then connects in the
//! background, retrying, and `/readyz` reports the database reachable only
//! once a pooled connection answers. Forwarding and capture never wait for
//! it: capture spools while the database is down (`crate::spool`).
//!
//! - [`migrate`] (`crosstalk migrate`) runs every layer's migrations,
//!   `transport` first ([`migrations`]); `--reset-correlator` then resets
//!   L5's checkpoint (decision Q2).
//! - [`migrations::check_heads`]: `serve` never migrates; it refuses to
//!   consume against a database whose applied migrations are behind the
//!   binary's.
//! - [`lock::PipelineLock`]: one pipeline process per database.

use std::ffi::OsString;
use std::time::Duration;

pub mod lock;
pub mod migrations;

use crosstalk_spec::support::Clock;
use crosstalk_store::sqlx::PgPool;
use crosstalk_store::sqlx::postgres::PgPoolOptions;
use crosstalk_store::{DbFailure, Store, StoreConfig, StoreError, classify};
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

/// A pool over `config` that opens connections on demand: building it
/// touches no database, so a process can start (and spool) while Postgres
/// is down.
pub fn lazy_pool(config: &StoreConfig) -> PgPool {
    let settings = config.pool();
    PgPoolOptions::new()
        .max_connections(settings.max_connections().get())
        .min_connections(settings.min_connections())
        .acquire_timeout(settings.acquire_timeout())
        .connect_lazy_with(config.url().connect_options().clone())
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
    #[error("the flow config: {0}")]
    Flow(#[from] crosstalk_flow::consumer::InvalidFlowConfig),
    #[error("resetting the correlator: {0}")]
    Reset(#[from] crosstalk_flow::consumer::DurabilityError),
}

/// What `crosstalk migrate` does besides migrating.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct MigrateOptions {
    /// Replace L5's stored checkpoint with empty shards (after an
    /// `IncompatibleSnapshot`): the pairings pending at the checkpoint are
    /// lost, knowingly (decision Q2).
    pub reset_correlator: bool,
}

/// Run every layer's migrations against the configured database, after
/// ensuring the required extensions, then what `options` asks. Idempotent.
pub async fn migrate(
    config: &GatewayConfig,
    lookup: impl Fn(&str) -> Option<String>,
    options: MigrateOptions,
    clock: &dyn Clock,
) -> Result<(), MigrateError> {
    let section = config.store.ok_or(MigrateError::NoStoreSection)?;
    let store = Store::connect(&store_config(section, lookup)?).await?;
    let result = async {
        migrations::migrate_all(&store).await?;
        if options.reset_correlator {
            migrations::reset_correlator(store.pool(), config.flow, clock.now()).await?;
        }
        Ok(())
    }
    .await;
    store.close().await;
    result
}
