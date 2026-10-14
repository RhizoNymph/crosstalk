//! The `api` role in Postgres mode: the surface over the stores, no
//! pipeline and no lock.

use std::sync::Arc;
use std::time::Duration;

use crosstalk_api::http::{Auth, HttpApi, HttpConfig};
use crosstalk_api::pg::{PgSettings, PgStores};
use crosstalk_api::{CursorSecret, InProcess, InProcessOptions, PgOpen};
use crosstalk_spec::ids::{KeyedHasher, RandomSource};
use crosstalk_spec::support::Clock;
use crosstalk_store::SerializableRetry;
use crosstalk_store::sqlx::PgPool;
use crosstalk_transport::PgBus;
use crosstalk_transport::blob::FsBlobStore;
use tokio::sync::{mpsc, oneshot};

use super::{
    ApiBinding, Late, SERVE_IDS, StopHandle, pause, wait_for_database, wait_for_migrations,
};
use crate::config::GatewayConfig;
use crate::gateway::{StartError, access};
use crate::live::recovery::{PipelinePhase, StatusReporter};
use crate::live::{LiveBlobs, LiveClock, LiveConfig, LiveError};

/// How often the `api` role rebuilds its node facts from the stores.
const NODES_EVERY: Duration = Duration::from_secs(5);

/// What the `api` role's surface is built from.
pub struct ApiStart {
    pub config: GatewayConfig,
    pub clock: Arc<dyn Clock>,
    pub blobs: FsBlobStore,
    pub pool: PgPool,
    pub bus: PgBus,
    pub secret: Arc<KeyedHasher>,
    pub status: StatusReporter,
    pub late: Late,
    pub api: ApiBinding,
}

/// The `api` role: the surface over the stores, no pipeline.
pub fn spawn_api(start: ApiStart) -> StopHandle<()> {
    let (stop, stopped) = oneshot::channel();
    let task = tokio::spawn(run_api(start, stopped));
    StopHandle { stop, task }
}

async fn run_api(start: ApiStart, mut stop: oneshot::Receiver<()>) {
    let status = start.status.clone();
    let hosted = loop {
        if !wait_for_database(&start.pool, &status, &mut stop).await
            || !wait_for_migrations(&start.pool, &status, &mut stop).await
        {
            return;
        }
        match host_api(&start).await {
            Ok(hosted) => break hosted,
            Err(error) => {
                tracing::warn!(error = %error, "the API surface did not start; retrying");
                status.phase(PipelinePhase::Stopped(error.to_string()));
                if pause(&mut stop).await {
                    return;
                }
            }
        }
    };
    status.phase(PipelinePhase::Running);
    let mut ticker = tokio::time::interval(NODES_EVERY);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            _ = &mut stop => break,
            _ = ticker.tick() => {
                if let Err(error) = hosted.nodes.rebuild().await {
                    tracing::warn!(error = %error, "node facts not rebuilt; retried");
                }
            }
        }
    }
    hosted.shutdown().await;
}

type ApiSurface = InProcess<PgStores<PgBus, LiveBlobs>>;

async fn host_api(start: &ApiStart) -> Result<ApiSurface, StartError> {
    let mut live = LiveConfig::new(
        LiveClock::Read(Arc::clone(&start.clock)),
        start.config.flow,
        0,
    )?;
    live.surface.access = access(start.config.api.as_ref().map(|api| &api.operator));
    let flow =
        crosstalk_flow::consumer::Settings::try_from(start.config.flow).map_err(LiveError::Flow)?;
    let options: InProcessOptions = InProcessOptions {
        timing: flow.timing,
        // The surface's minted ids are persisted: OS entropy.
        seed: SERVE_IDS.random(0x5F00).next_u64(),
        ..live.surface
    };
    let (stores, _topology_relay) = PgStores::open(PgOpen {
        pool: start.pool.clone(),
        bus: start.bus.clone(),
        dead_letters: start.bus.dead_letters(),
        blobs: LiveBlobs::Fs(start.blobs.clone()),
        clock: Arc::clone(&start.clock),
        secret: Arc::clone(&start.secret),
        ids: SERVE_IDS,
        settings: PgSettings {
            bucket_width: options.bucket_width,
            timing: options.timing,
            retention: options.retention,
            lineage_floor: options.lineage_floor,
            embedding_model: options.embedding_model.clone(),
            default_remap_threshold: options.surface.default_remap_threshold,
            projection_lease: options.projection_lease,
            frame_retention: options.surface.frame_retention.as_duration(),
            sinks: options.sinks.clone(),
            configure_sinks: false,
            threading: live.threading,
            retry: SerializableRetry::default(),
        },
    })
    .await
    .map_err(|error| StartError::Live(LiveError::Stores(error)))?;
    // The API role publishes only what an operator action decides; the
    // pipeline process relays L7's outbox. Nothing feeds this surface's
    // relay: the node facts are rebuilt on a timer, and the live feed
    // carries only config loads (live events need the pipeline process).
    let (_events, relayed) = mpsc::unbounded_channel();
    let hosted = InProcess::host(
        stores,
        options,
        relayed,
        CursorSecret::Derived(&start.secret),
        "gateway access config",
    )
    .await
    .map_err(|error| StartError::Live(LiveError::Surface(error)))?;
    if let Err(error) = hosted.surface.recover_interrupted().await {
        tracing::warn!(error = ?error, "interrupted operator actions not recorded yet");
    }
    match hosted.stores.operators.directory().await {
        Ok(Some(directory)) => {
            let auth = Auth::fixed(directory, start.api.tokens.clone());
            let http = HttpConfig {
                frame_retention: hosted.surface.config().frame_retention,
                clock: Arc::clone(&start.api.clock),
            };
            start
                .late
                .router
                .set(HttpApi::new(Arc::clone(&hosted.surface), auth, http).router());
        }
        Ok(None) => tracing::error!("no operator directory stored; the API stays unavailable"),
        Err(error) => tracing::error!(error = ?error, "reading the operator directory failed"),
    }
    Ok(hosted)
}
