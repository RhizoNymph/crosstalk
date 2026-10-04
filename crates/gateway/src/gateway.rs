//! Single-node wiring: the proxy, the capture stage, the bus, the blob
//! store, the exchange log and the ops listener, as tokio tasks joined by
//! channels, for one [`Role`].
//!
//! ```text
//! client ─▶ proxy (L0, crosstalk-ingress) ─ RawExchange, bounded mpsc ─▶ capture stage
//!                                                                         │ normalize (L1)
//!                                                                         │ store ─▶ FsBlobStore (blobs.root)
//!                                                                         ▼ publish
//!                                    MpscBus (L2) ── ExchangeCaptured ──▶ group exchange-log
//!                                                                         ▼
//!                                      <data dir>/exchanges/exchange-log.jsonl
//! ```
//!
//! [`start`] opens what the role needs, builds the proxy (reading its
//! secrets through the caller's environment lookup), binds the listeners,
//! subscribes the exchange log before anything can be published, and
//! spawns the tasks. [`Running::shutdown`] stops in dependency order: the
//! proxy listener (in-flight exchanges drain), the capture stage (the
//! channel drains), the exchange log's group (the bus drains), the bus,
//! the log (synced), then the ops listener.

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use crosstalk_ingress::capture::CaptureSender;
use crosstalk_ingress::{BuildError, anthropic_proxy};
use crosstalk_spec::events::Subject;
use crosstalk_spec::ids::{SeededRandom, UlidGenerator};
use crosstalk_spec::interfaces::l2_transport::{BusError, EventBus};
use crosstalk_spec::support::Clock;
use crosstalk_transport::blob::{FsBlobStore, OpenError};
use crosstalk_transport::{MpscBus, StartError as BusStartError};
use tokio::net::TcpListener;
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;
use tokio::time::Instant;

use crate::capture::{CaptureStage, PipelineStats, PutRetry};
use crate::config::{ConfigError, GatewayConfig};
use crate::log::consumer::{self, LogStats};
use crate::log::{ExchangeLog, LogError};
use crate::ops::{HealthReport, Ops, Phase, Readiness};
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
    #[error("starting the bus: {0}")]
    Bus(#[from] BusStartError),
    #[error("subscribing the exchange log: {0:?}")]
    Subscribe(BusError),
}

/// How the shutdown went.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ShutdownReport {
    pub proxy: DrainReport,
    /// Whether the capture stage handled every queued exchange in time.
    pub capture_drained: bool,
    /// Whether the exchange log consumed every published envelope in time.
    pub log_drained: bool,
}

/// The proxy and the capture stage, when the role runs them.
#[derive(Debug)]
struct ProxyTasks {
    addr: SocketAddr,
    stop: watch::Sender<bool>,
    server: JoinHandle<DrainReport>,
    capture: JoinHandle<()>,
}

/// A running gateway.
#[derive(Debug)]
pub struct Running {
    role: Role,
    data_dir: PathBuf,
    ops_addr: SocketAddr,
    bus: MpscBus,
    blobs: FsBlobStore,
    ops: Ops,
    phase: watch::Sender<Phase>,
    proxy: Option<ProxyTasks>,
    log: Option<JoinHandle<()>>,
    stop_ops: watch::Sender<bool>,
    ops_server: JoinHandle<DrainReport>,
    store: Option<JoinHandle<Option<crosstalk_store::Store>>>,
    drain: Duration,
    flush: Duration,
}

/// Start the gateway's `role`. `lookup` reads environment variables (the
/// deployment secrets, `DATABASE_URL`); `clock` stamps exchanges and
/// envelopes.
pub async fn start(
    config: &GatewayConfig,
    role: Role,
    lookup: impl Fn(&str) -> Option<String>,
    clock: Arc<dyn Clock>,
) -> Result<Running, StartError> {
    let data_dir = config.data_dir()?.to_owned();
    let store_config = config
        .store
        .map(|section| store_config(section, &lookup))
        .transpose()?;
    let blobs = FsBlobStore::open(&config.blobs.root).await?;
    let log = match role.runs_pipeline() {
        true => Some(ExchangeLog::open(&config.exchange_log_path()?).await?),
        false => None,
    };
    let proxy = match role.runs_proxy() {
        true => {
            let (sender, captured) = mpsc::channel(config.ingress.capture.channel_capacity.get());
            let proxy = anthropic_proxy(
                &config.ingress,
                &lookup,
                CaptureSender::new(sender),
                Arc::clone(&clock),
            )?;
            let listener = bind("proxy", config.ingress.listen).await?;
            Some((proxy, captured, listener))
        }
        false => None,
    };
    let ops_listener = bind("ops", config.ops.listen).await?;
    let ops_addr = local_addr("ops", &ops_listener)?;

    let bus = MpscBus::start(config.bus.clone())?;
    let subscription = match log {
        Some(_) => Some(
            bus.subscribe(
                &[Subject::ExchangeCaptured],
                consumer::group(),
                config.bus.retry,
            )
            .await
            .map_err(StartError::Subscribe)?,
        ),
        None => None,
    };

    let mut tasks = Tasks::new();
    let pipeline = Arc::new(PipelineStats::new());
    let log_stats = Arc::new(LogStats::new());
    let (phase, phase_watch) = watch::channel(Phase::Ok);
    let drain = config.shutdown.drain_timeout();

    let log = match (log, subscription) {
        (Some(log), Some(subscription)) => Some(tasks.spawn(
            "exchange_log",
            consumer::run(subscription, log, Arc::clone(&log_stats)),
        )),
        _ => None,
    };
    let mut capture_stats = None;
    let proxy = match proxy {
        Some((proxy, captured, listener)) => {
            let addr = local_addr("proxy", &listener)?;
            capture_stats = Some(proxy.stats());
            let stage = CaptureStage::new(
                blobs.clone(),
                bus.clone(),
                Arc::clone(&clock),
                UlidGenerator::new(clock, SeededRandom::from_entropy()),
                Arc::clone(&pipeline),
                PutRetry::from(config.pipeline),
            );
            let capture = tasks.spawn("capture", stage.run(captured));
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
            Some(ProxyTasks {
                addr,
                stop,
                server,
                capture,
            })
        }
        None => None,
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
        pipeline,
        log: log_stats,
        tasks,
        store: store_probe,
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
        ops_addr,
        &data_dir,
    );
    Ok(Running {
        role,
        data_dir,
        ops_addr,
        bus,
        blobs,
        ops,
        phase,
        proxy,
        log,
        stop_ops,
        ops_server,
        store,
        drain,
        flush: config.shutdown.flush_timeout(),
    })
}

fn announce(
    config: &GatewayConfig,
    role: Role,
    proxy: Option<SocketAddr>,
    ops: SocketAddr,
    data_dir: &Path,
) {
    let proxy = proxy.map_or_else(|| "none".to_owned(), |addr| addr.to_string());
    tracing::info!(
        role = %role,
        proxy = %proxy,
        ops = %ops,
        data_dir = %data_dir.display(),
        routes = config.ingress.routes.len(),
        store = config.store.is_some(),
        "gateway started"
    );
    for missing in role.not_built() {
        tracing::info!(role = %role, not_built = *missing, "part of the role does not exist yet; nothing started for it");
    }
    if role.runs_proxy() != role.runs_pipeline() {
        tracing::warn!(
            role = %role,
            "the bus is in-process until the cross-node bus (P9): a proxy process and a pipeline process do not reach each other; use --role all to capture and log end to end"
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

    /// Where `/metrics`, `/healthz` and `/readyz` are served.
    pub fn ops_addr(&self) -> SocketAddr {
        self.ops_addr
    }

    /// The parent of `blobs.root`.
    pub fn data_dir(&self) -> &Path {
        &self.data_dir
    }

    /// The in-process bus, for additional consumers.
    pub fn bus(&self) -> &MpscBus {
        &self.bus
    }

    /// The blob store the capture stage writes to.
    pub fn blobs(&self) -> &FsBlobStore {
        &self.blobs
    }

    /// What `GET /healthz` would answer now.
    pub fn health(&self) -> HealthReport {
        self.ops.health()
    }

    /// What `GET /readyz` would answer now.
    pub async fn readiness(&self) -> Readiness {
        self.ops.readiness().await
    }

    /// Stop gracefully: refuse new connections, let in-flight exchanges
    /// finish (up to the drain timeout), let the capture stage and the
    /// exchange log handle everything they were given (up to the flush
    /// timeout), stop the bus, close the log, stop the ops listener.
    pub async fn shutdown(self) -> ShutdownReport {
        tracing::info!(role = %self.role, "gateway shutting down");
        // Fails only when no reader of the phase is left.
        let _ = self.phase.send(Phase::Draining);
        let mut report = ShutdownReport {
            capture_drained: true,
            log_drained: true,
            ..ShutdownReport::default()
        };
        let mut capture = None;
        if let Some(proxy) = self.proxy {
            let _ = proxy.stop.send(true);
            report.proxy = proxy.server.await.unwrap_or_else(|error| {
                tracing::error!(error = %error, "the proxy task failed");
                DrainReport::default()
            });
            capture = Some(proxy.capture);
        }
        // From here on, one deadline for the capture stage and the log.
        let deadline = Instant::now() + self.flush;
        if let Some(capture) = capture {
            // The proxy and its connections are gone; the channel closes
            // once the last per-exchange capture task has handed off.
            report.capture_drained = join_by("capture stage", capture, deadline).await;
        }
        if let Some(log) = self.log {
            report.log_drained = wait_for_group(&self.bus, deadline).await;
            self.bus.shutdown().await;
            join_by("exchange log consumer", log, deadline).await;
        } else {
            self.bus.shutdown().await;
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
            "gateway stopped"
        );
        report
    }
}

/// Wait for `task` until `deadline`; abort it after that. True when it
/// ended on its own.
async fn join_by(name: &'static str, mut task: JoinHandle<()>, deadline: Instant) -> bool {
    match tokio::time::timeout_at(deadline, &mut task).await {
        Ok(Ok(())) => true,
        Ok(Err(error)) => {
            tracing::error!(task = name, error = %error, "task failed");
            false
        }
        Err(_) => {
            tracing::warn!(
                task = name,
                "task did not finish before the flush deadline; aborting"
            );
            task.abort();
            false
        }
    }
}

/// Wait until the exchange log's group holds nothing, until `deadline`.
async fn wait_for_group(bus: &MpscBus, deadline: Instant) -> bool {
    let group = consumer::group();
    let empty = async {
        loop {
            match bus.depth(&group).await {
                Ok(Some(depth))
                    if depth.ready
                        + depth.delayed
                        + depth.held
                        + depth.exhausted
                        + depth.waiting
                        > 0 => {}
                Ok(_) => return true,
                Err(error) => {
                    tracing::warn!(error = ?error, "reading the exchange log's group depth failed");
                    return false;
                }
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    };
    match tokio::time::timeout_at(deadline, empty).await {
        Ok(drained) => drained,
        Err(_) => {
            tracing::warn!("the exchange log did not catch up before the flush deadline");
            false
        }
    }
}
