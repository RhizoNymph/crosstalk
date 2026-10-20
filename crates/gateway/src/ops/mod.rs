//! The ops listener (`ops.listen`, 9464 in the deployment):
//!
//! | Route | Answer |
//! | --- | --- |
//! | `GET /healthz` | 200 while the process serves: liveness, plus the counters as JSON ([`HealthReport`]); with a `store` section also the bus's per-group backlog (read within [`HEALTH_BUS_BOUND`], else reported unavailable), what recovery did, the spool and the statements Postgres cancelled at a pipeline bound |
//! | `GET /readyz` | 200 when ready, 503 otherwise, with the checks as JSON ([`Readiness`]); 503 while draining |
//! | `GET /metrics` | the counters in the Prometheus text format ([`metrics`]) |
//!
//! Anything else is a 404.
//!
//! **Readiness in Postgres mode** (`docs/features/postgres_stores.md`,
//! "Restart semantics end to end"). For a role that captures, ready means
//! the proxy forwards and capture is durable (in the bus, or in a spool
//! with room), so it stays 200 while the database is down, with `status:
//! degraded`, `capture: spooling` and `pipeline: waiting_for_database`;
//! recovery and draining are degraded too, and so is the way back after a
//! lost pipeline lock (`pipeline_lock: lost`, `pipeline: abandoning`, then
//! waiting for the database and the lock, held elsewhere included, then
//! recovering): capture never stopped. It is 503 when the spool is full or
//! corrupt, capture stopped, migrations are behind, the pipeline lock is
//! held elsewhere by a process this one never took it from (a standby),
//! the pipeline stopped, a task stopped, or the process drains. The `api`
//! role also needs the database reachable and migrations at head. A
//! backlog never makes a process unready; it is in `/healthz`.

pub mod metrics;

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use crosstalk_ingress::capture::{CaptureCounts, CaptureStats};
use http_body_util::Full;
use hyper::body::Incoming;
use hyper::header::{CONTENT_TYPE, HeaderValue};
use hyper::{Method, Request, Response, StatusCode};
use serde::{Deserialize, Serialize};
use tokio::sync::watch;

use crate::gateway::late::LateValue;
use crate::live::recovery::{RecoveryReport, StatusReader};
use crate::live::{LiveReport, LiveReporter};
use crate::log::consumer::{LogCounts, LogStats};
use crate::pipeline::{PipelineCounts, PipelineStats};
use crate::role::Role;
use crate::spool::LiveBus;
use crate::store::{StoreCheck, StoreProbe};
use crate::tasks::Tasks;
use crosstalk_spec::support::Clock;
use crosstalk_transport::{PgBus, SpoolState, SpoolStats};

/// Whether the gateway is serving or shutting down. The process's phase
/// is `Ok` or `Draining`; a report says `Degraded` while it serves short
/// of its steady state (the database down, recovery or the spool's drain
/// in progress).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Phase {
    Ok,
    Degraded,
    Draining,
}

/// Ingress's capture counters, as the ops listener reports them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct CaptureReport {
    pub captured: u64,
    pub unclassified: u64,
    pub decode_error: u64,
    pub channel_full: u64,
    pub channel_closed: u64,
    pub response_too_large: u64,
    pub ids_exhausted: u64,
}

impl From<CaptureCounts> for CaptureReport {
    fn from(counts: CaptureCounts) -> Self {
        Self {
            captured: counts.captured,
            unclassified: counts.unclassified,
            decode_error: counts.decode_error,
            channel_full: counts.channel_full,
            channel_closed: counts.channel_closed,
            response_too_large: counts.response_too_large,
            ids_exhausted: counts.ids_exhausted,
        }
    }
}

/// The `GET /healthz` body.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct HealthReport {
    pub status: Phase,
    pub capture: CaptureReport,
    pub pipeline: PipelineCounts,
    pub log: LogCounts,
    /// The live process: each layer stage's handled count and the L7
    /// watermark; `null` for a role without one (`analysis`). In Postgres
    /// mode the watermark is the persisted one, from the first report.
    pub live: Option<LiveReport>,
    /// Postgres mode: each pipeline group's backlog and dead letters, or
    /// that the read did not answer within [`HEALTH_BUS_BOUND`]; left out
    /// while the database refuses the read (out of reach).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bus: Option<BusSection>,
    /// Postgres mode: what recovery did at start.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub recovery: Option<RecoveryReport>,
    /// Postgres mode, a role that captures: the publish spool.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub spool: Option<SpoolReport>,
    /// Postgres mode: statements the server cancelled at a pipeline bound
    /// since the process started, by kind.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub statement_timeouts: Option<StatementTimeoutsReport>,
}

/// How long `/healthz` waits for the bus's per-group statistics. The read
/// runs on capture's pool, which has no server-side bound, and `/healthz`
/// is a request path (probes, dashboards): past this the report goes out
/// with the bus section unavailable.
pub const HEALTH_BUS_BOUND: Duration = Duration::from_secs(2);

/// `/healthz`'s `bus` section: the groups' statistics, or that the read
/// stalled. Read as `{"groups": {...}}` or `{"unavailable": {...}}`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged, deny_unknown_fields)]
pub enum BusSection {
    Read(BusReport),
    #[serde(rename_all = "snake_case")]
    Unavailable {
        unavailable: BusUnavailable,
    },
}

/// Why `/healthz` has no group statistics.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum BusUnavailable {
    /// The read did not answer within [`HEALTH_BUS_BOUND`].
    TimedOut { bound_ms: u64 },
}

/// `/healthz`'s `statement_timeouts` section (`/metrics`:
/// `crosstalk_pg_statement_timeouts_total{kind}`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct StatementTimeoutsReport {
    /// Cancelled by `statement_timeout` (SQLSTATE 57014).
    pub statement: u64,
    /// Cancelled by `lock_timeout` (SQLSTATE 55P03).
    pub lock: u64,
}

impl From<crosstalk_store::TimeoutCounts> for StatementTimeoutsReport {
    fn from(counts: crosstalk_store::TimeoutCounts) -> Self {
        Self {
            statement: counts.statement,
            lock: counts.lock,
        }
    }
}

/// `/healthz`'s `bus` section.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct BusReport {
    /// By group name.
    pub groups: BTreeMap<String, GroupReport>,
}

/// One group's backlog.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct GroupReport {
    pub pending: u64,
    pub oldest_pending_micros: Option<u64>,
    pub dead_letters: u64,
}

/// `/healthz`'s `spool` section.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct SpoolReport {
    /// `direct`, `spooling`, `draining` or `corrupt`.
    pub state: String,
    /// The corrupt segment, when `corrupt`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub corrupt_segment: Option<String>,
    pub records: u64,
    pub bytes: u64,
    pub oldest_at_micros: Option<u64>,
    /// Seconds from the oldest record's `at` to the clock's reading
    /// (`crosstalk_spool_oldest_age_seconds`); 0 with nothing spooled.
    pub oldest_age_seconds: u64,
    pub max_bytes: u64,
    pub appended: u64,
    pub drained: u64,
    pub rejected_full: u64,
    pub rejected_io: u64,
    pub truncated_bytes: u64,
    /// Exchanges refused because the spool was full (`capture.spool_full`).
    pub capture_spool_full: u64,
}

impl SpoolReport {
    fn of(stats: &SpoolStats, now_micros: u64, capture_spool_full: u64) -> Self {
        let oldest = stats.oldest_at.map(|at| at.as_micros());
        Self {
            state: stats.state.label().to_owned(),
            corrupt_segment: match &stats.state {
                SpoolState::Corrupt { segment, .. } => Some(segment.clone()),
                _ => None,
            },
            records: stats.records,
            bytes: stats.bytes,
            oldest_at_micros: oldest,
            oldest_age_seconds: oldest.map_or(0, |at| now_micros.saturating_sub(at) / 1_000_000),
            max_bytes: stats.max_bytes,
            appended: stats.appended,
            drained: stats.drained,
            rejected_full: stats.rejected_full,
            rejected_io: stats.rejected_io,
            truncated_bytes: stats.truncated_bytes,
            capture_spool_full,
        }
    }
}

/// The `GET /readyz` body.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct Readiness {
    pub ready: bool,
    pub role: String,
    pub status: Phase,
    /// `not_configured`, `reachable`, or `unreachable: <why>`.
    pub database: String,
    /// `at_head`, `behind: <layer applied < head, ...>`, or `unknown`
    /// before the database answered (always `at_head` in memory mode).
    pub migrations: String,
    /// Postgres mode, a pipeline role: `held`, `held elsewhere`, `lost`,
    /// `released` (a planned handover let it go) or `not_taken`. `lost` and, after a loss, `held elsewhere` leave the
    /// process ready, degraded: it resumes in process.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pipeline_lock: Option<String>,
    /// Postgres mode, a role that captures: `durable`, `spooling`,
    /// `draining (<n> records)`, `dropping: spool full`, `spool corrupt:
    /// <segment>` or `stopped` (the process drains, or the proxy or
    /// capture task ended).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub capture: Option<String>,
    /// Postgres mode: `running`, `abandoning` (the lock was lost),
    /// `waiting_for_database`, `waiting_for_migrations`, `waiting_for_lock`,
    /// `fencing`, `recovering`, `handing_over` and `handed_over` (a planned
    /// shutdown), `stopped: <why>` (`not_run` for the `api` role).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pipeline: Option<String>,
    /// Postgres mode: `pending`, a recovery step, `done` or `failed: <why>`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub recovery: Option<String>,
    /// Postgres mode, once a planned shutdown began handing the pipeline
    /// over (`pipeline: handing_over`, then `handed_over`): `quiescing`,
    /// `releasing_holds`, `closing_pool`, `releasing_lock` or `done`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub handover: Option<String>,
    /// Each task of the role and whether it runs.
    pub tasks: Vec<TaskState>,
}

/// One task's state in [`Readiness`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct TaskState {
    pub name: String,
    pub running: bool,
}

/// Everything the ops listener reads. Absent stages read as zeros.
#[derive(Debug, Clone)]
pub struct Ops {
    pub role: Role,
    pub phase: watch::Receiver<Phase>,
    pub capture: Option<Arc<CaptureStats>>,
    pub pipeline: Arc<PipelineStats>,
    pub log: Arc<LogStats>,
    /// The live process's counts, when the role runs one.
    pub live: Option<LiveReporter>,
    pub tasks: Tasks,
    pub store: StoreProbe,
    /// What a Postgres-mode process adds; `None` in memory mode.
    pub postgres: Option<PgOps>,
}

/// The Postgres-mode readings of the ops listener.
#[derive(Clone)]
pub struct PgOps {
    /// Where the pipeline (or the API role's surface) stands.
    pub status: StatusReader,
    /// The bus, for per-group backlogs.
    pub bus: PgBus,
    /// The spool, for a role that captures.
    pub spool: Option<LiveBus>,
    /// The running pipeline incarnation's counts, while one runs.
    pub live: Arc<LateValue<LiveReporter>>,
    /// The `/healthz` clock: the spool's oldest record's age.
    pub clock: Arc<dyn Clock>,
}

impl std::fmt::Debug for PgOps {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PgOps")
            .field("spool", &self.spool.is_some())
            .finish_non_exhaustive()
    }
}

/// `/readyz`'s `capture` for a spool's state, and whether it blocks. The
/// spool counts as full while the last capture was refused full and the
/// spool has not drained back to `direct` since. Capture that no longer
/// runs (the process drains, or the proxy's or capture's task ended)
/// reads `stopped`, whatever the spool holds.
fn capture_text(stats: &SpoolStats, refusing: bool, running: bool) -> (String, bool) {
    if !running {
        return ("stopped".to_owned(), true);
    }
    if refusing && stats.state != SpoolState::Direct {
        return ("dropping: spool full".to_owned(), true);
    }
    match &stats.state {
        SpoolState::Corrupt { segment, .. } => (format!("spool corrupt: {segment}"), true),
        SpoolState::Spooling => ("spooling".to_owned(), false),
        SpoolState::Draining => (format!("draining ({} records)", stats.records), false),
        SpoolState::Direct => ("durable".to_owned(), false),
    }
}

/// The groups' statistics from `bus`, within [`HEALTH_BUS_BOUND`]. A read
/// past the bound is dropped (its connection goes back to the pool once
/// the server answers) and reported unavailable, warned with the bound; a
/// failed read (the database out of reach) leaves the section out.
async fn read_bus(bus: &PgBus) -> Option<BusSection> {
    let bound_ms = u64::try_from(HEALTH_BUS_BOUND.as_millis()).unwrap_or(u64::MAX);
    match tokio::time::timeout(HEALTH_BUS_BOUND, bus.group_stats()).await {
        Ok(Ok(stats)) => Some(BusSection::Read(BusReport {
            groups: stats
                .into_iter()
                .map(|stats| {
                    (
                        stats.group.0,
                        GroupReport {
                            pending: stats.pending,
                            oldest_pending_micros: stats.oldest_pending.map(|at| at.as_micros()),
                            dead_letters: stats.dead_letters,
                        },
                    )
                })
                .collect(),
        })),
        Ok(Err(error)) => {
            tracing::debug!(error = ?error, "group stats unreadable; /healthz without the bus section");
            None
        }
        Err(_) => {
            tracing::warn!(
                what = "group_stats",
                bound_ms,
                "/healthz's bus read did not answer within its bound; reported unavailable"
            );
            Some(BusSection::Unavailable {
                unavailable: BusUnavailable::TimedOut { bound_ms },
            })
        }
    }
}

/// The tasks capture runs on: the proxy that hands it exchanges and the
/// capture stage.
const CAPTURE_TASKS: [&str; 2] = ["proxy", "capture"];

impl Ops {
    /// The counters, the live report, and in Postgres mode what recovery
    /// did and the spool; `bus` needs the database ([`Ops::health_full`]).
    pub fn health(&self) -> HealthReport {
        let phase = *self.phase.borrow();
        let postgres = self.postgres.as_ref();
        let spool = postgres.and_then(|pg| {
            pg.spool.as_ref().map(|spool| {
                SpoolReport::of(
                    &spool.stats(),
                    pg.clock.now().as_micros(),
                    self.pipeline.spool_full(),
                )
            })
        });
        let degraded = spool.as_ref().is_some_and(|spool| spool.state != "direct");
        HealthReport {
            status: match phase {
                Phase::Ok if degraded => Phase::Degraded,
                other => other,
            },
            capture: self
                .capture
                .as_ref()
                .map(|stats| stats.snapshot().into())
                .unwrap_or_default(),
            pipeline: self.pipeline.snapshot(),
            log: self.log.snapshot(),
            live: self.live.as_ref().map(LiveReporter::report).or_else(|| {
                postgres
                    .and_then(|pg| pg.live.get())
                    .map(|live| live.report())
            }),
            bus: None,
            recovery: postgres.map(|pg| pg.status.snapshot().report),
            spool,
            statement_timeouts: postgres.map(|_| crosstalk_store::statement_timeouts().into()),
        }
    }

    /// [`Ops::health`] with the bus's per-group backlog read from the
    /// database within [`HEALTH_BUS_BOUND`]; past it the section says so,
    /// and while the read fails (the database out of reach) it is left out.
    pub async fn health_full(&self) -> HealthReport {
        let mut report = self.health();
        if let Some(pg) = &self.postgres {
            report.bus = read_bus(&pg.bus).await;
        }
        report
    }

    pub async fn readiness(&self) -> Readiness {
        let phase = *self.phase.borrow();
        let database = self.store.check().await;
        let tasks: Vec<TaskState> = self
            .tasks
            .states()
            .into_iter()
            .map(|(name, running)| TaskState {
                name: name.to_owned(),
                running,
            })
            .collect();
        let tasks_running = tasks.iter().all(|task| task.running);
        let database_text = match &database {
            StoreCheck::NotConfigured => "not_configured".to_owned(),
            StoreCheck::Reachable => "reachable".to_owned(),
            StoreCheck::Unreachable(why) => format!("unreachable: {why}"),
        };
        let database_ok = matches!(database, StoreCheck::NotConfigured | StoreCheck::Reachable);
        let Some(pg) = &self.postgres else {
            return Readiness {
                ready: phase == Phase::Ok && database_ok && tasks_running,
                role: self.role.to_string(),
                status: phase,
                database: database_text,
                migrations: "at_head".to_owned(),
                pipeline_lock: None,
                capture: None,
                pipeline: None,
                recovery: None,
                handover: None,
                tasks,
            };
        };
        let status = pg.status.snapshot();
        let capture_running = phase != Phase::Draining
            && tasks
                .iter()
                .filter(|task| CAPTURE_TASKS.contains(&task.name.as_str()))
                .all(|task| task.running);
        let capture = pg
            .spool
            .as_ref()
            .map(|spool| capture_text(&spool.stats(), self.pipeline.refusing(), capture_running));
        let capture_blocked = capture.as_ref().is_some_and(|(_, blocked)| *blocked);
        let capture_degraded = capture
            .as_ref()
            .is_some_and(|(text, _)| text.as_str() != "durable");
        let at_head = status.migrations_text() == "at_head";
        let ready = phase == Phase::Ok
            && tasks_running
            && !capture_blocked
            && !status.blocks_readiness()
            && match self.role {
                // The API reads only the stores.
                Role::Api => database_ok && at_head,
                _ => true,
            };
        let degraded = !database_ok || status.degraded() || capture_degraded;
        Readiness {
            ready,
            role: self.role.to_string(),
            status: match phase {
                Phase::Ok if degraded => Phase::Degraded,
                other => other,
            },
            database: database_text,
            migrations: status.migrations_text(),
            pipeline_lock: match self.role {
                Role::Api => None,
                _ => Some(status.lock_text().to_owned()),
            },
            capture: capture.map(|(text, _)| text),
            pipeline: Some(status.pipeline_text()),
            recovery: Some(status.recovery_text()),
            handover: status.handover().map(|step| step.label().to_owned()),
            tasks,
        }
    }

    /// Answer one ops request.
    pub async fn handle(self, request: Request<Incoming>) -> Response<Full<Bytes>> {
        if request.method() != Method::GET {
            return json(StatusCode::NOT_FOUND, br#"{"error":"not_found"}"#.to_vec());
        }
        match request.uri().path() {
            "/healthz" => encoded(StatusCode::OK, &self.health_full().await),
            "/readyz" => {
                let readiness = self.readiness().await;
                let status = if readiness.ready {
                    StatusCode::OK
                } else {
                    StatusCode::SERVICE_UNAVAILABLE
                };
                encoded(status, &readiness)
            }
            "/metrics" => {
                let mut response = Response::new(Full::new(Bytes::from(metrics::render(
                    &self.health(),
                    &self.pipeline.normalize_failures(),
                ))));
                response.headers_mut().insert(
                    CONTENT_TYPE,
                    HeaderValue::from_static(metrics::CONTENT_TYPE),
                );
                response
            }
            _ => json(StatusCode::NOT_FOUND, br#"{"error":"not_found"}"#.to_vec()),
        }
    }
}

fn encoded<T: Serialize>(status: StatusCode, value: &T) -> Response<Full<Bytes>> {
    match serde_json::to_vec(value) {
        Ok(body) => json(status, body),
        Err(error) => {
            tracing::error!(error = %error, "encoding an ops response failed");
            json(
                StatusCode::INTERNAL_SERVER_ERROR,
                br#"{"error":"internal"}"#.to_vec(),
            )
        }
    }
}

fn json(status: StatusCode, body: Vec<u8>) -> Response<Full<Bytes>> {
    let mut response = Response::new(Full::new(Bytes::from(body)));
    *response.status_mut() = status;
    response
        .headers_mut()
        .insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    response
}

#[cfg(test)]
mod handover_tests;
#[cfg(test)]
mod health_bound_tests;
#[cfg(test)]
mod tests;
