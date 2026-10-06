//! Single-node wiring: the proxy, the HTTP API and the ops listener around a
//! [`Live`] process (the capture stage, the bus, the blob store, every layer
//! consumer, the surface and the exchange log), as tokio tasks joined by
//! channels, for one [`Role`].
//!
//! ```text
//! client ─▶ proxy (L0, crosstalk-ingress) ─ RawExchange, bounded mpsc ─▶ Live: capture stage
//!                                                                         │ normalize (L1), ingest
//!                                                                         │ store ─▶ FsBlobStore (blobs.root)
//!                                                                         ▼ publish
//!            MpscBus (L2) ─▶ L3 ▶ L4 ▶ L5 ▶ L6 ▶ L7 stages ─▶ memory stores ─▶ Surface
//!                     └────▶ group exchange-log ─▶ <data dir>/exchanges/exchange-log.jsonl
//! operator ─ Bearer <api.token> ─▶ HTTP API (api.listen) ─▶ Surface
//! ```
//!
//! [`start`] opens what the role needs, builds the proxy (reading its
//! secrets through the caller's environment lookup), binds the listeners,
//! starts a [`Live`] process (memory stores, the given clock, periodic
//! ticks) with the role's stages (the capture stage for a proxy role, the
//! exchange log for a pipeline role), mounts the HTTP API on its surface
//! for an API role, and spawns the proxy, API and ops listeners.
//! [`Running::shutdown`] stops in dependency order: the proxy listener
//! (in-flight exchanges drain), the API listener, the live process (the
//! capture channel drains, every group drains, the bus stops, the log is
//! synced), then the ops listener.
//!
//! With a `store` section (every role but `analysis`) the process runs in
//! Postgres mode instead ([`postgres`]): the bus is `PgBus` behind the
//! publish spool, the stores are the Postgres bundle, and the pipeline
//! starts once the database answers, its migrations are at head and the
//! pipeline lock is held, after the recovery sequence. Forwarding and
//! capture start at once either way. Without one, memory mode is kept
//! (decision Q7).

pub mod late;
pub mod postgres;

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use crosstalk_api::http::{
    Auth, BearerToken, HttpApi, HttpConfig, InvalidBearerToken, StaticTokens,
};
use crosstalk_ingress::capture::CaptureSender;
use crosstalk_ingress::{BuildError, anthropic_proxy};
use crosstalk_spec::interfaces::l8_surface::operators::{
    AccessConfig, OperatorConfig, TrustedOperator,
};
use crosstalk_spec::interfaces::l8_surface::permissions::PermissionSet;
use crosstalk_spec::support::Clock;
use crosstalk_transport::MpscBus;
use crosstalk_transport::blob::{FsBlobStore, OpenError};
use tokio::net::TcpListener;
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;
use tokio::time::Instant;

use crate::capture::CaptureStage;
use crate::config::{ApiConfig, ApiOperator, ConfigError, GatewayConfig, StoreSection};
use crate::live::{
    BlobConfig, DefaultsError, Live, LiveBlobs, LiveClock, LiveConfig, LiveError, StagesRunning,
    Ticking,
};
use crate::log::consumer::LogStats;
use crate::log::{ExchangeLog, LogError};
use crate::ops::{HealthReport, Ops, PgOps, Phase, Readiness};
use crate::pipeline::{PipelineStats, Settings};
use crate::role::Role;
use crate::server::{self, DrainReport, ServeOptions};
use crate::store::{StoreProbe, store_config};
use crate::tasks::Tasks;

/// Why the gateway did not start. Nothing is left running.
#[derive(Debug, thiserror::Error)]
pub enum StartError {
    #[error(transparent)]
    Config(#[from] ConfigError),
    #[error("the store section: {0}")]
    Store(#[from] crosstalk_store::ConfigError),
    #[error("opening the blob store: {0}")]
    Blobs(#[from] OpenError),
    #[error("opening the exchange log: {0}")]
    Log(#[from] LogError),
    #[error("building the proxy: {0}")]
    Proxy(#[from] BuildError),
    #[error("binding the {listener} listener on {addr}: {source}")]
    Bind {
        listener: &'static str,
        addr: SocketAddr,
        source: std::io::Error,
    },
    #[error("reading the {listener} listener's address: {source}")]
    LocalAddr {
        listener: &'static str,
        source: std::io::Error,
    },
    #[error("the live process's defaults: {0}")]
    Defaults(#[from] DefaultsError),
    #[error("starting the live process: {0}")]
    Live(#[from] LiveError),
    #[error("the API token in {variable}: {error}")]
    ApiToken {
        variable: String,
        error: InvalidBearerToken,
    },
    #[error("the operator directory was not loaded")]
    NoDirectory,
    #[error("postgres mode needs a store section")]
    NoStore,
    #[error(
        "store.pool.max_connections is {configured}; a pipeline process needs at least {needed}"
    )]
    PoolTooSmall { configured: u32, needed: u32 },
    #[error("loading the deployment secret: {0}")]
    Secrets(#[from] crosstalk_ingress::credential::SecretError),
    #[error("starting the postgres bus: {0:?}")]
    Bus(crosstalk_transport::StartError),
    #[error("the spool section: {0}")]
    SpoolSection(#[from] crate::config::InvalidSpoolSection),
    #[error("opening the publish spool: {0}")]
    Spool(crosstalk_transport::SpoolError),
    #[error("building the capture pipeline: {0}")]
    Pipeline(#[from] crate::pipeline::BuildError),
}

/// How the shutdown went.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ShutdownReport {
    pub proxy: DrainReport,
    /// Whether the capture stage handled every queued exchange in time.
    pub capture_drained: bool,
    /// Whether the exchange log consumed every published envelope in time.
    pub log_drained: bool,
    /// Whether every layer stage's group was empty before the bus stopped.
    pub stages_drained: bool,
}

/// The proxy listener, when the role runs it.
#[derive(Debug)]
struct ProxyTasks {
    addr: SocketAddr,
    stop: watch::Sender<bool>,
    server: JoinHandle<DrainReport>,
}

/// The HTTP API listener, when the role serves it.
#[derive(Debug)]
struct ApiTasks {
    addr: SocketAddr,
    stop: watch::Sender<bool>,
    server: JoinHandle<()>,
}

/// A running gateway.
pub struct Running {
    role: Role,
    data_dir: PathBuf,
    ops_addr: SocketAddr,
    blobs: FsBlobStore,
    live: Option<Live>,
    /// The Postgres side, in Postgres mode.
    postgres: Option<postgres::PgRunning>,
    ops: Ops,
    phase: watch::Sender<Phase>,
    proxy: Option<ProxyTasks>,
    api: Option<ApiTasks>,
    stop_ops: watch::Sender<bool>,
    ops_server: JoinHandle<DrainReport>,
    store: Option<JoinHandle<Option<crosstalk_store::Store>>>,
    drain: Duration,
    flush: Duration,
}

impl std::fmt::Debug for Running {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Running")
            .field("role", &self.role)
            .field("ops_addr", &self.ops_addr)
            .field("proxy", &self.proxy)
            .field("api", &self.api)
            .finish_non_exhaustive()
    }
}

/// Start the gateway's `role` on the wall clock (or any clock only read).
/// `lookup` reads environment variables (the deployment secrets,
/// `DATABASE_URL`, the API token); `clock` stamps exchanges and envelopes
/// and drives the live process's ticks.
pub async fn start(
    config: &GatewayConfig,
    role: Role,
    lookup: impl Fn(&str) -> Option<String>,
    clock: Arc<dyn Clock>,
) -> Result<Running, StartError> {
    start_on(config, role, lookup, LiveClock::Read(clock)).await
}

/// [`start`] on a [`LiveClock`]: a manual clock lets a test drive the
/// live process with [`Live::settle`] (through [`Running::live`]).
pub async fn start_on(
    config: &GatewayConfig,
    role: Role,
    lookup: impl Fn(&str) -> Option<String>,
    clock: LiveClock,
) -> Result<Running, StartError> {
    if config.store.is_some() && role.runs_live() {
        return start_postgres(config, role, lookup, clock).await;
    }
    let data_dir = config.data_dir()?.to_owned();
    let store_config = config
        .store
        .map(|section| store_config(section, &lookup))
        .transpose()?;
    let blobs = FsBlobStore::open(&config.blobs.root).await?;
    let exchange_log = match role.runs_pipeline() {
        true => Some(ExchangeLog::open(&config.exchange_log_path()?).await?),
        false => None,
    };
    let reader = clock.reader();
    let proxy = match role.runs_proxy() {
        true => {
            let (sender, captured) = mpsc::channel(config.ingress.capture.channel_capacity.get());
            let proxy = anthropic_proxy(
                &config.ingress,
                &lookup,
                CaptureSender::new(sender),
                Arc::clone(&reader),
            )?;
            let listener = bind("proxy", config.ingress.listen).await?;
            Some((proxy, captured, listener))
        }
        false => None,
    };
    let api = match (role.runs_api(), &config.api) {
        (true, Some(api)) => {
            let token = api_token(api, &lookup)?;
            let listener = bind("api", api.listen).await?;
            Some((api, token, listener))
        }
        (true, None) => {
            tracing::warn!(role = %role, "no api section: the HTTP API is not served");
            None
        }
        (false, _) => None,
    };
    let ops_listener = bind("ops", config.ops.listen).await?;
    let ops_addr = local_addr("ops", &ops_listener)?;
    let (proxy, captured) = match proxy {
        Some((proxy, captured, listener)) => {
            let addr = local_addr("proxy", &listener)?;
            (Some((proxy, listener, addr)), Some(captured))
        }
        None => (None, None),
    };

    let live = match role.runs_live() {
        true => {
            let mut live_config = LiveConfig::new(clock.clone(), config.flow, seed(&reader))?;
            live_config.blobs = BlobConfig::Open(LiveBlobs::Fs(blobs.clone()));
            live_config.bus = config.bus.clone();
            live_config.pipeline = Settings::from_config(config);
            live_config.extract = config.extract.clone();
            live_config.ticking = Ticking::Periodic;
            live_config.capture = captured;
            live_config.exchange_log = exchange_log;
            live_config.surface.access = access(config.api.as_ref().map(|api| &api.operator));
            Some(Live::start(live_config).await?)
        }
        false => None,
    };

    let mut tasks = live
        .as_ref()
        .map_or_else(Tasks::new, |live| live.tasks().clone());
    if let Some(live) = &live {
        let stages = live.stages_running();
        tasks.probe("live", move || stages.all());
    }
    let (phase, phase_watch) = watch::channel(Phase::Ok);
    let drain = config.shutdown.drain_timeout();
    let mut capture_stats = None;
    let proxy = match proxy {
        Some((proxy, listener, addr)) => {
            capture_stats = Some(proxy.stats());
            let (stop, stopped) = watch::channel(false);
            let server = tasks.spawn(
                "proxy",
                server::serve(
                    listener,
                    move |request| {
                        let proxy = proxy.clone();
                        async move { proxy.handle(request).await }
                    },
                    stopped,
                    ServeOptions {
                        name: "proxy",
                        drain,
                        date_header: false,
                    },
                ),
            );
            Some(ProxyTasks { addr, stop, server })
        }
        None => None,
    };
    let api = match (api, &live) {
        (Some((_, token, listener)), Some(live)) => {
            let addr = local_addr("api", &listener)?;
            let directory = live
                .stores()
                .operators
                .directory()
                .ok_or(StartError::NoDirectory)?;
            let auth = Auth::fixed(directory, StaticTokens::new([(token, ApiOperator::ID)]));
            let http = HttpConfig {
                frame_retention: live.surface().config().frame_retention,
                clock: Arc::clone(&reader),
            };
            let router = HttpApi::new(Arc::clone(live.surface()), auth, http).router();
            let (stop, mut stopped) = watch::channel(false);
            let shutdown = async move {
                // Fails only when the sender is gone: stop then too.
                let _ = stopped.wait_for(|stop| *stop).await;
            };
            let server = tasks.spawn("api", async move {
                if let Err(error) = crosstalk_api::http::serve(listener, router, shutdown).await {
                    tracing::error!(error = %error, "the api listener failed");
                }
            });
            Some(ApiTasks { addr, stop, server })
        }
        _ => None,
    };
    let (store_probe, store) = match store_config {
        Some(store_config) => {
            let (probe, connect) = StoreProbe::connect(store_config);
            (probe, Some(tokio::spawn(connect)))
        }
        None => (StoreProbe::not_configured(), None),
    };
    let ops = Ops {
        role,
        phase: phase_watch,
        capture: capture_stats,
        pipeline: live.as_ref().map_or_else(
            || Arc::new(PipelineStats::new()),
            |live| Arc::clone(live.pipeline().stats()),
        ),
        log: live.as_ref().map_or_else(
            || Arc::new(LogStats::new()),
            |live| Arc::clone(live.log_stats()),
        ),
        live: live.as_ref().map(Live::reporter),
        tasks,
        store: store_probe,
        postgres: None,
    };
    let (stop_ops, ops_stopped) = watch::channel(false);
    let ops_handler = ops.clone();
    let ops_server = tokio::spawn(server::serve(
        ops_listener,
        move |request| ops_handler.clone().handle(request),
        ops_stopped,
        ServeOptions {
            name: "ops",
            drain: Duration::from_secs(1),
            date_header: true,
        },
    ));
    announce(
        config,
        role,
        proxy.as_ref().map(|tasks| tasks.addr),
        api.as_ref().map(|tasks| tasks.addr),
        ops_addr,
        &data_dir,
    );
    Ok(Running {
        role,
        data_dir,
        ops_addr,
        blobs,
        live,
        postgres: None,
        ops,
        phase,
        proxy,
        api,
        stop_ops,
        ops_server,
        store,
        drain,
        flush: config.shutdown.flush_timeout(),
    })
}

/// [`start_on`] with a `store` section: see [`postgres`].
async fn start_postgres(
    config: &GatewayConfig,
    role: Role,
    lookup: impl Fn(&str) -> Option<String>,
    clock: LiveClock,
) -> Result<Running, StartError> {
    let data_dir = config.data_dir()?.to_owned();
    let section = config.store.ok_or(StartError::NoStore)?;
    let store_config = store_config(section, &lookup)?;
    let configured = store_config.pool().max_connections().get();
    if configured < StoreSection::MIN_CONNECTIONS {
        return Err(StartError::PoolTooSmall {
            configured,
            needed: StoreSection::MIN_CONNECTIONS,
        });
    }
    let blobs = FsBlobStore::open(&config.blobs.root).await?;
    let secret = Arc::new(crosstalk_ingress::credential::load_secrets(
        &config.ingress.secrets,
        &lookup,
    )?);
    let reader = clock.reader();
    let proxy = match role.runs_proxy() {
        true => {
            let (sender, captured) = mpsc::channel(config.ingress.capture.channel_capacity.get());
            let proxy = anthropic_proxy(
                &config.ingress,
                &lookup,
                CaptureSender::new(sender),
                Arc::clone(&reader),
            )?;
            let listener = bind("proxy", config.ingress.listen).await?;
            Some((proxy, captured, listener))
        }
        false => None,
    };
    let api = match (role.runs_api(), &config.api) {
        (true, Some(api)) => {
            let token = api_token(api, &lookup)?;
            let listener = bind("api", api.listen).await?;
            Some((token, listener))
        }
        (true, None) => {
            tracing::warn!(role = %role, "no api section: the HTTP API is not served");
            None
        }
        (false, _) => None,
    };
    let ops_listener = bind("ops", config.ops.listen).await?;
    let ops_addr = local_addr("ops", &ops_listener)?;

    let pool = crate::store::lazy_pool(&store_config);
    let side = postgres::capture_side(config, role, pool, &blobs, &reader).await?;
    let status = postgres::initial_status();
    let late = postgres::Late::default();
    let mut tasks = Tasks::new();
    let (proxy, captured) = match proxy {
        Some((proxy, captured, listener)) => {
            let addr = local_addr("proxy", &listener)?;
            (Some((proxy, listener, addr)), Some(captured))
        }
        None => (None, None),
    };
    let capture = match (captured, &side.pipeline) {
        (Some(captured), Some(pipeline)) => Some(tasks.spawn(
            "capture",
            CaptureStage::new(pipeline.ingester()).run(captured),
        )),
        _ => None,
    };
    if role != Role::Api {
        let log_tasks = Arc::clone(&late.log_tasks);
        if role.runs_pipeline() {
            tasks.probe("exchange_log", move || {
                log_tasks
                    .get()
                    .is_none_or(|tasks| tasks.states().iter().all(|(_, running)| *running))
            });
        }
        let stages = Arc::clone(&late.stages);
        tasks.probe("live", move || stages.get().is_none_or(StagesRunning::all));
    }
    let (phase, phase_watch) = watch::channel(Phase::Ok);
    let drain = config.shutdown.drain_timeout();
    let mut capture_stats = None;
    let proxy = match proxy {
        Some((proxy, listener, addr)) => {
            capture_stats = Some(proxy.stats());
            let (stop, stopped) = watch::channel(false);
            let server = tasks.spawn(
                "proxy",
                server::serve(
                    listener,
                    move |request| {
                        let proxy = proxy.clone();
                        async move { proxy.handle(request).await }
                    },
                    stopped,
                    ServeOptions {
                        name: "proxy",
                        drain,
                        date_header: false,
                    },
                ),
            );
            Some(ProxyTasks { addr, stop, server })
        }
        None => None,
    };
    let binding = api.as_ref().map(|(token, _)| postgres::ApiBinding {
        tokens: StaticTokens::new([(token.clone(), ApiOperator::ID)]),
        clock: Arc::clone(&reader),
    });
    let api = match api {
        Some((_, listener)) => {
            let addr = local_addr("api", &listener)?;
            let router = late.router.router();
            let (stop, mut stopped) = watch::channel(false);
            let shutdown = async move {
                // Fails only when the sender is gone: stop then too.
                let _ = stopped.wait_for(|stop| *stop).await;
            };
            let server = tasks.spawn("api", async move {
                if let Err(error) = crosstalk_api::http::serve(listener, router, shutdown).await {
                    tracing::error!(error = %error, "the api listener failed");
                }
            });
            Some(ApiTasks { addr, stop, server })
        }
        None => None,
    };
    let (store_probe, store) = {
        let (probe, connect) = StoreProbe::connect(store_config.clone());
        (probe, Some(tokio::spawn(connect)))
    };
    let pipeline = match (&side.spool, &side.pipeline) {
        (Some(spool), Some(pipeline)) => Some(postgres::spawn_pipeline(postgres::PipelineStart {
            config: config.clone(),
            role,
            clock: clock.clone(),
            blobs: blobs.clone(),
            pool: side.pool.clone(),
            url: store_config.url().clone(),
            bus: side.bus.clone(),
            spool: spool.clone(),
            gate: side.gate.clone(),
            pipeline: Arc::clone(pipeline),
            secret: Arc::clone(&secret),
            status: status.clone(),
            late: late.clone(),
            api: binding.clone(),
        })),
        _ => None,
    };
    let api_surface = match (role, binding) {
        (Role::Api, Some(binding)) => Some(postgres::spawn_api(postgres::ApiStart {
            config: config.clone(),
            clock: Arc::clone(&reader),
            blobs: blobs.clone(),
            pool: side.pool.clone(),
            bus: side.bus.clone(),
            secret: Arc::clone(&secret),
            status: status.clone(),
            late: late.clone(),
            api: binding,
        })),
        _ => None,
    };
    let ops = Ops {
        role,
        phase: phase_watch,
        capture: capture_stats,
        pipeline: side.pipeline.as_ref().map_or_else(
            || Arc::new(PipelineStats::new()),
            |pipeline| Arc::clone(pipeline.stats()),
        ),
        log: Arc::new(LogStats::new()),
        live: None,
        tasks,
        store: store_probe,
        postgres: Some(PgOps {
            status: status.reader(),
            bus: side.bus.clone(),
            spool: side.spool.clone(),
            live: Arc::clone(&late.reporter),
            clock: Arc::clone(&reader),
        }),
    };
    let (stop_ops, ops_stopped) = watch::channel(false);
    let ops_handler = ops.clone();
    let ops_server = tokio::spawn(server::serve(
        ops_listener,
        move |request| ops_handler.clone().handle(request),
        ops_stopped,
        ServeOptions {
            name: "ops",
            drain: Duration::from_secs(1),
            date_header: true,
        },
    ));
    announce(
        config,
        role,
        proxy.as_ref().map(|tasks| tasks.addr),
        api.as_ref().map(|tasks| tasks.addr),
        ops_addr,
        &data_dir,
    );
    Ok(Running {
        role,
        data_dir,
        ops_addr,
        blobs,
        live: None,
        postgres: Some(postgres::PgRunning {
            pool: side.pool,
            bus: side.bus,
            spool: side.spool,
            capture,
            capture_pipeline: side.pipeline,
            pipeline,
            api: api_surface,
            status,
            late,
        }),
        ops,
        phase,
        proxy,
        api,
        stop_ops,
        ops_server,
        store,
        drain,
        flush: config.shutdown.flush_timeout(),
    })
}

/// The API's bearer token, from the variable `api.token` names.
fn api_token(
    api: &ApiConfig,
    lookup: &impl Fn(&str) -> Option<String>,
) -> Result<BearerToken, StartError> {
    let variable = api.token.env.as_str();
    let value = lookup(variable).unwrap_or_default();
    BearerToken::new(&value).map_err(|error| StartError::ApiToken {
        variable: variable.to_owned(),
        error,
    })
}

/// Who may use the surface: with an API, the one operator its token signs
/// in as, holding every permission; without one, trusted mode (in-process
/// readers only).
pub(crate) fn access(operator: Option<&ApiOperator>) -> AccessConfig {
    match operator {
        Some(operator) => AccessConfig::Authenticated(vec![OperatorConfig {
            id: ApiOperator::ID,
            name: operator.name.clone(),
            permissions: PermissionSet::ALL,
        }]),
        None => AccessConfig::Trusted(TrustedOperator {
            id: ApiOperator::ID,
            name: ApiOperator::default().name,
        }),
    }
}

/// A seed for the live process's id generators: distinct per start.
fn seed(clock: &Arc<dyn Clock>) -> u64 {
    clock.now().as_micros() ^ (u64::from(std::process::id()) << 32)
}

fn announce(
    config: &GatewayConfig,
    role: Role,
    proxy: Option<SocketAddr>,
    api: Option<SocketAddr>,
    ops: SocketAddr,
    data_dir: &Path,
) {
    let shown =
        |addr: Option<SocketAddr>| addr.map_or_else(|| "none".to_owned(), |addr| addr.to_string());
    tracing::info!(
        role = %role,
        proxy = %shown(proxy),
        api = %shown(api),
        ops = %ops,
        data_dir = %data_dir.display(),
        routes = config.ingress.routes.len(),
        store = config.store.is_some(),
        live = role.runs_live(),
        "gateway started"
    );
    for missing in role.not_built() {
        tracing::info!(role = %role, not_built = *missing, "part of the role does not exist yet; nothing started for it");
    }
    if role != Role::All {
        tracing::warn!(
            role = %role,
            "the bus and the stores are in-process until the cross-node bus (P9): processes of different roles do not reach each other; use --role all to capture, detect and serve end to end"
        );
    }
}

async fn bind(listener: &'static str, addr: SocketAddr) -> Result<TcpListener, StartError> {
    TcpListener::bind(addr)
        .await
        .map_err(|source| StartError::Bind {
            listener,
            addr,
            source,
        })
}

fn local_addr(listener: &'static str, bound: &TcpListener) -> Result<SocketAddr, StartError> {
    bound
        .local_addr()
        .map_err(|source| StartError::LocalAddr { listener, source })
}

impl Running {
    pub fn role(&self) -> Role {
        self.role
    }

    /// Where the proxy listens (the harness's base URL host and port), when
    /// the role runs it.
    pub fn proxy_addr(&self) -> Option<SocketAddr> {
        self.proxy.as_ref().map(|proxy| proxy.addr)
    }

    /// Where the HTTP API listens, when the role serves it.
    pub fn api_addr(&self) -> Option<SocketAddr> {
        self.api.as_ref().map(|api| api.addr)
    }

    /// Where `/metrics`, `/healthz` and `/readyz` are served.
    pub fn ops_addr(&self) -> SocketAddr {
        self.ops_addr
    }

    /// The parent of `blobs.root`.
    pub fn data_dir(&self) -> &Path {
        &self.data_dir
    }

    /// The in-process bus, for additional consumers, when the role runs a
    /// live process.
    pub fn bus(&self) -> Option<&MpscBus> {
        self.live.as_ref().map(|live| &live.stores().bus)
    }

    /// The blob store the capture stage writes to.
    pub fn blobs(&self) -> &FsBlobStore {
        &self.blobs
    }

    /// The live process, when the role runs one: its pipeline, stores,
    /// surface and `settle`.
    pub fn live(&self) -> Option<&Live> {
        self.live.as_ref()
    }

    /// What `GET /healthz` would answer now, without the bus's backlog
    /// (which needs the database: [`Running::health_full`]).
    pub fn health(&self) -> HealthReport {
        self.ops.health()
    }

    /// What `GET /healthz` answers now.
    pub async fn health_full(&self) -> HealthReport {
        self.ops.health_full().await
    }

    /// The Postgres side, in Postgres mode: the pool, the bus, the spool,
    /// the pipeline's status.
    pub fn postgres(&self) -> Option<&postgres::PgRunning> {
        self.postgres.as_ref()
    }

    /// What `GET /readyz` would answer now.
    pub async fn readiness(&self) -> Readiness {
        self.ops.readiness().await
    }

    /// Stop gracefully: refuse new connections, let in-flight exchanges
    /// finish (up to the drain timeout), stop the API, let the live process
    /// handle everything it was given (up to the flush timeout), stop the
    /// bus, close the log, stop the ops listener.
    pub async fn shutdown(self) -> ShutdownReport {
        tracing::info!(role = %self.role, "gateway shutting down");
        // Fails only when no reader of the phase is left.
        let _ = self.phase.send(Phase::Draining);
        let mut report = ShutdownReport {
            capture_drained: true,
            log_drained: true,
            stages_drained: true,
            ..ShutdownReport::default()
        };
        if let Some(proxy) = self.proxy {
            let _ = proxy.stop.send(true);
            report.proxy = proxy.server.await.unwrap_or_else(|error| {
                tracing::error!(error = %error, "the proxy task failed");
                DrainReport::default()
            });
        }
        if let Some(api) = self.api {
            let _ = api.stop.send(true);
            if let Err(error) = tokio::time::timeout(self.drain, api.server).await {
                tracing::warn!(error = %error, "the api listener did not stop in time");
            }
        }
        // The proxy and its connections are gone, so the capture channel
        // closes once the last per-exchange capture task has handed off.
        // From here on, one deadline for the live process.
        if let Some(live) = self.live {
            let drained = live.shutdown(Instant::now() + self.flush).await;
            report.capture_drained = drained.capture;
            report.log_drained = drained.log;
            report.stages_drained = drained.stages;
        }
        if let Some(postgres) = self.postgres {
            let drained = postgres.shutdown(Instant::now() + self.flush).await;
            report.capture_drained = drained.capture;
            report.log_drained = drained.log;
            report.stages_drained = drained.stages;
        }
        if let Some(store) = self.store {
            store.abort();
            if let Ok(Some(store)) = store.await {
                store.close().await;
            }
        }
        let _ = self.stop_ops.send(true);
        if let Err(error) = self.ops_server.await {
            tracing::error!(error = %error, "the ops task failed");
        }
        tracing::info!(
            role = %self.role,
            drain_ms = u64::try_from(self.drain.as_millis()).unwrap_or(u64::MAX),
            connections_cut = report.proxy.cut,
            capture_drained = report.capture_drained,
            log_drained = report.log_drained,
            stages_drained = report.stages_drained,
            "gateway stopped"
        );
        report
    }
}
