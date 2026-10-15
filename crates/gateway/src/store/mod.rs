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
//! - [`fence`]: no write from an older pipeline incarnation once a newer
//!   one runs (the epoch and the shared fence every pipeline connection
//!   joins).
//! - [`timeouts`]: the `statement_timeout` and `lock_timeout` every
//!   pipeline connection runs with, so a stalled statement is cancelled
//!   and retried instead of hanging a stage or the reaper.

use std::ffi::OsString;
use std::time::Duration;

pub mod fence;
pub mod lock;
pub mod migrations;
pub mod timeouts;

use crosstalk_spec::support::Clock;
use crosstalk_store::sqlx::PgPool;
use crosstalk_store::sqlx::postgres::{PgConnection, PgPoolOptions};
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

/// How long the server keeps a session of ours idle inside a transaction
/// before it ends it: the backstop that releases the row and advisory
/// locks of a transaction whose client stopped driving it (a task stalled
/// or abandoned mid-transaction). Every transaction of ours runs its
/// statements back to back, so a legitimate one never idles this long.
pub const IDLE_IN_TRANSACTION_TIMEOUT: &str = "30s";

/// The `application_name` a pool's sessions carry when the caller names
/// none.
pub const DEFAULT_APPLICATION: &str = "crosstalk";

/// A pool over `config` that opens connections on demand: building it
/// touches no database, so a process can start (and spool) while Postgres
/// is down. Its sessions are named [`DEFAULT_APPLICATION`]; see
/// [`lazy_pool_named`].
pub fn lazy_pool(config: &StoreConfig) -> PgPool {
    lazy_pool_named(config, DEFAULT_APPLICATION)
}

/// [`lazy_pool`] whose sessions carry `application` as their
/// `application_name` (`pg_stat_activity` then names the process and role
/// that owns each session) and run [`session_setup`] on every new
/// connection.
pub fn lazy_pool_named(config: &StoreConfig, application: &str) -> PgPool {
    let settings = config.pool();
    let options = config
        .url()
        .connect_options()
        .clone()
        .application_name(application);
    PgPoolOptions::new()
        .max_connections(settings.max_connections().get())
        .min_connections(settings.min_connections())
        .acquire_timeout(settings.acquire_timeout())
        .after_connect(|connection, _meta| Box::pin(session_setup(connection)))
        .connect_lazy_with(options)
}

/// What every pooled session of ours runs as it connects: end any
/// transaction left idle for [`IDLE_IN_TRANSACTION_TIMEOUT`]
/// (`idle_in_transaction_session_timeout`, set per session, so no server
/// setting is needed). `PgPoolOptions::after_connect` replaces any earlier
/// hook, so a pool that adds to it (a pipeline incarnation's sets the
/// statement and lock bounds and joins the fence,
/// `gateway::postgres::incarnation_pool_bounded`) runs this first itself.
pub async fn session_setup(
    connection: &mut PgConnection,
) -> Result<(), crosstalk_store::sqlx::Error> {
    crosstalk_store::sqlx::query(IDLE_IN_TRANSACTION)
        .execute(connection)
        .await
        .map(|_| ())
}

/// The session setting [`lazy_pool_named`] applies to every connection.
const IDLE_IN_TRANSACTION: &str = "SET idle_in_transaction_session_timeout = '30s'";

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
