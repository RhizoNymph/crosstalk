//! The data source behind every page.
//!
//! Pages and data routes read through the spec's L8 traits,
//! `crosstalk_spec::interfaces::l8_surface::{QueryApi, OperatorActions,
//! LiveFeed}`, plus the two gaps in `crate::contract` (`present::Present`:
//! the bucket width and the present; `formats::ExportFormats`: the export
//! formats the backend writes) and the port-shaped channel reads of the
//! channel-semantics stand-in (`crate::pending`).
//!
//! [`AppBackend`] is the configured backend, one of:
//!
//! - [`fixture::FixtureBackend`]: deterministic synthetic data, optionally
//!   replaying its last hours (the default);
//! - [`world::WorldBackend`]: the real surface (`crosstalk_api::InProcess`
//!   over the memory stores), seeded with `crosstalk-world`;
//! - `live::LiveBackend` (cargo feature `live`): the gateway's whole
//!   composition, a stub until the gateway provides it.
//!
//! [`start`] builds it with the [`Service`] it runs beside the server (the
//! replay ticker, or the in-process surface's relay and feed), which the
//! binary keeps alive while serving and shuts down after. [`dispatch`]
//! implements every trait on `AppBackend` by forwarding; its futures have
//! concrete types, so their `Send`-ness, which Topcoat's multi-threaded
//! runtime needs, is inferred where each `#[page]`, shard and `#[route]` is
//! registered.
//!
//! Implementations enforce permissions themselves and return
//! `QueryError::Forbidden`; pages also check, to render the "content
//! hidden" state instead of an error.

pub mod alert_state;
pub mod dispatch;
pub mod fixture;
#[cfg(feature = "live")]
pub mod live;
pub mod world;

use crosstalk_api::InProcess;
use crosstalk_spec::interfaces::l8_surface::QueryError;
use tokio::task::JoinHandle;

use crate::config::BackendConfig;
use fixture::{FixtureBackend, GenError};
use world::{WorldBackend, WorldStartError};

/// What the backends' reads return: the spec's `QueryError` on failure.
pub type Result<T> = std::result::Result<T, QueryError>;

/// The backend the pages read from, as configured.
#[derive(Debug)]
pub enum AppBackend {
    Fixture(Box<FixtureBackend>),
    World(WorldBackend),
    #[cfg(feature = "live")]
    Live(live::LiveBackend),
}

/// What a backend runs beside the server, kept alive while it serves.
pub enum Service {
    /// Nothing runs in the background.
    Idle,
    /// The fixture's replay ticker, publishing what each tick reveals.
    Replay(JoinHandle<()>),
    /// The in-process surface's relay task and live feed.
    World(Box<InProcess>),
}

impl Service {
    /// Stops what runs in the background; open live streams end.
    pub async fn shutdown(self) {
        match self {
            Self::Idle => {}
            Self::Replay(ticker) => ticker.abort(),
            Self::World(in_process) => in_process.shutdown().await,
        }
    }
}

/// A started backend and what it runs beside the server.
pub struct Started {
    pub backend: AppBackend,
    pub service: Service,
}

/// Why the configured backend did not start.
#[derive(Debug, thiserror::Error)]
pub enum StartError {
    #[error("the fixture could not be generated: {0}")]
    Fixture(#[from] GenError),
    #[error(transparent)]
    World(#[from] WorldStartError),
    #[cfg(feature = "live")]
    #[error(transparent)]
    Live(#[from] live::LiveStartError),
    #[cfg(not(feature = "live"))]
    #[error("the live backend needs the `live` cargo feature, which this build lacks")]
    LiveNotBuilt,
}

/// Starts the backend `config` names. Needs a tokio runtime.
pub async fn start(config: &BackendConfig) -> std::result::Result<Started, StartError> {
    match *config {
        BackendConfig::Fixture { seed, replay } => {
            let backend = match replay {
                None => FixtureBackend::try_live(seed)?,
                Some(replay) => {
                    tracing::info!(
                        window_minutes = replay.window_minutes,
                        speed = replay.speed,
                        "replaying the fixture's last stretch"
                    );
                    FixtureBackend::try_replay(seed, replay)?
                }
            };
            let service = backend
                .spawn_replay_ticker()
                .map_or(Service::Idle, Service::Replay);
            Ok(Started {
                backend: AppBackend::Fixture(Box::new(backend)),
                service,
            })
        }
        BackendConfig::World { seed } => {
            let (backend, in_process) = WorldBackend::start(seed).await?;
            Ok(Started {
                backend: AppBackend::World(backend),
                service: Service::World(Box::new(in_process)),
            })
        }
        #[cfg(feature = "live")]
        BackendConfig::Live(live) => {
            let backend = live::LiveBackend::start(live)?;
            Ok(Started {
                backend: AppBackend::Live(backend),
                service: Service::Idle,
            })
        }
        #[cfg(not(feature = "live"))]
        BackendConfig::Live(_) => Err(StartError::LiveNotBuilt),
    }
}
