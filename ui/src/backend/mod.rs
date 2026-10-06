//! The data source behind every page.
//!
//! Pages and data routes read only through the spec's L8 traits,
//! `crosstalk_spec::interfaces::l8_surface::{QueryApi, OperatorActions,
//! LiveFeed}`. The clock, bucket width, export formats and rule defaults
//! are `QueryApi::present`, read once per request (`crate::app::present`).
//! The one question that is not the spec's, where a default view ends, is
//! [`AppBackend::view_end`]: the present's `now`, unless the fixture
//! replays up to a fixed end.
//!
//! [`AppBackend`] is the configured backend, one of:
//!
//! - [`fixture::FixtureBackend`]: deterministic synthetic data, optionally
//!   replaying its last hours (the default);
//! - [`world::WorldBackend`]: the real surface (`crosstalk_api::InProcess`
//!   over the memory stores), seeded with `crosstalk-world`, for
//!   development and demos;
//! - [`crosstalk_client::HttpClient`] ([`http`]): a gateway's surface over
//!   HTTP, the only way real data reaches the UI.
//!
//! [`start`] builds it with the [`Service`] it runs beside the server (the
//! replay ticker, the in-process surface's relay and feed, or the http
//! identity refresher) and the [`Identity`] requests act as, which the
//! binary keeps alive while serving and shuts down after. [`dispatch`]
//! implements every trait on `AppBackend` by forwarding; its futures have
//! concrete types, so their `Send`-ness, which Topcoat's multi-threaded
//! runtime needs, is inferred where each `#[page]`, shard and `#[route]` is
//! registered.
//!
//! Implementations enforce permissions themselves and return
//! `QueryError::Forbidden`; pages also check, to render the "content
//! hidden" state instead of an error.

pub mod dispatch;
pub mod fixture;
pub mod http;
pub mod world;

use crosstalk_api::world::WorldInProcess;
use crosstalk_client::HttpClient;
use crosstalk_spec::interfaces::l8_surface::QueryError;
use tokio::task::JoinHandle;

use crate::config::BackendConfig;
use crate::identity::Identity;
use fixture::{FixtureBackend, GenError};
use http::identity::IdentityError;
use world::{WorldBackend, WorldStartError};

/// What the backends' reads return: the spec's `QueryError` on failure.
pub type Result<T> = std::result::Result<T, QueryError>;

/// The backend the pages read from, as configured.
#[derive(Debug)]
pub enum AppBackend {
    Fixture(Box<FixtureBackend>),
    World(WorldBackend),
    Http(HttpClient),
}

/// What a backend runs beside the server, kept alive while it serves.
pub enum Service {
    /// Nothing runs in the background.
    Idle,
    /// The fixture's replay ticker, publishing what each tick reveals.
    Replay(JoinHandle<()>),
    /// The in-process surface's relay task and live feed.
    World(Box<WorldInProcess>),
    /// The http backend's identity refresher.
    Http(JoinHandle<()>),
}

impl Service {
    /// Stops what runs in the background; open live streams end.
    pub async fn shutdown(self) {
        match self {
            Self::Idle => {}
            Self::Replay(ticker) => ticker.abort(),
            Self::World(in_process) => in_process.shutdown().await,
            Self::Http(refresh) => refresh.abort(),
        }
    }
}

/// A started backend, what it runs beside the server, and who requests
/// act as.
pub struct Started {
    pub backend: AppBackend,
    pub service: Service,
    pub identity: Identity,
}

/// Why the configured backend did not start.
#[derive(Debug, thiserror::Error)]
pub enum StartError {
    #[error("the fixture could not be generated: {0}")]
    Fixture(#[from] GenError),
    #[error(transparent)]
    World(#[from] WorldStartError),
    #[error("the http backend could not learn who its token is")]
    Http(#[from] IdentityError),
}

/// Starts the backend `config` names. Needs a tokio runtime.
pub async fn start(config: &BackendConfig) -> std::result::Result<Started, StartError> {
    match config {
        BackendConfig::Fixture {
            access,
            seed,
            replay,
        } => {
            let backend = match *replay {
                None => FixtureBackend::try_live(*seed)?,
                Some(replay) => {
                    tracing::info!(
                        window_minutes = replay.window_minutes,
                        speed = replay.speed,
                        "replaying the fixture's last stretch"
                    );
                    FixtureBackend::try_replay(*seed, replay)?
                }
            };
            let service = backend
                .spawn_replay_ticker()
                .map_or(Service::Idle, Service::Replay);
            Ok(Started {
                backend: AppBackend::Fixture(Box::new(backend)),
                service,
                identity: Identity::fixed(access.clone()),
            })
        }
        BackendConfig::World { access, seed } => {
            let (backend, in_process) = WorldBackend::start(*seed).await?;
            Ok(Started {
                backend: AppBackend::World(backend),
                service: Service::World(Box::new(in_process)),
                identity: Identity::fixed(access.clone()),
            })
        }
        BackendConfig::Http(config) => {
            let started = http::start(config).await?;
            Ok(Started {
                backend: AppBackend::Http(started.client),
                service: Service::Http(started.refresh),
                identity: started.identity,
            })
        }
    }
}
