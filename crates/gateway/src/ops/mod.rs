//! The ops listener (`ops.listen`, 9464 in the deployment):
//!
//! | Route | Answer |
//! | --- | --- |
//! | `GET /healthz` | 200 while the process serves: liveness, plus the counters as JSON ([`HealthReport`]) |
//! | `GET /readyz` | 200 when ready, 503 otherwise, with the checks as JSON ([`Readiness`]): the database reachable when `store` is configured, migrations at head, and every task of the role running; 503 while draining |
//! | `GET /metrics` | the counters in the Prometheus text format ([`metrics`]) |
//!
//! Anything else is a 404.

pub mod metrics;

use std::sync::Arc;

use bytes::Bytes;
use crosstalk_ingress::capture::{CaptureCounts, CaptureStats};
use http_body_util::Full;
use hyper::body::Incoming;
use hyper::header::{CONTENT_TYPE, HeaderValue};
use hyper::{Method, Request, Response, StatusCode};
use serde::{Deserialize, Serialize};
use tokio::sync::watch;

use crate::capture::{PipelineCounts, PipelineStats};
use crate::log::consumer::{LogCounts, LogStats};
use crate::role::Role;
use crate::store::{StoreCheck, StoreProbe};
use crate::tasks::Tasks;

/// Whether the gateway is serving or shutting down.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Phase {
    Ok,
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
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct HealthReport {
    pub status: Phase,
    pub capture: CaptureReport,
    pub pipeline: PipelineCounts,
    pub log: LogCounts,
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
    /// Always `at_head` today: no layer has migrations yet.
    pub migrations: String,
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
    pub tasks: Tasks,
    pub store: StoreProbe,
}

impl Ops {
    pub fn health(&self) -> HealthReport {
        HealthReport {
            status: *self.phase.borrow(),
            capture: self
                .capture
                .as_ref()
                .map(|stats| stats.snapshot().into())
                .unwrap_or_default(),
            pipeline: self.pipeline.snapshot(),
            log: self.log.snapshot(),
        }
    }

    pub async fn readiness(&self) -> Readiness {
        let status = *self.phase.borrow();
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
        let database_ok = matches!(database, StoreCheck::NotConfigured | StoreCheck::Reachable);
        Readiness {
            ready: status == Phase::Ok && database_ok && tasks.iter().all(|task| task.running),
            role: self.role.to_string(),
            status,
            database: match database {
                StoreCheck::NotConfigured => "not_configured".to_owned(),
                StoreCheck::Reachable => "reachable".to_owned(),
                StoreCheck::Unreachable(why) => format!("unreachable: {why}"),
            },
            migrations: "at_head".to_owned(),
            tasks,
        }
    }

    /// Answer one ops request.
    pub async fn handle(self, request: Request<Incoming>) -> Response<Full<Bytes>> {
        if request.method() != Method::GET {
            return json(StatusCode::NOT_FOUND, br#"{"error":"not_found"}"#.to_vec());
        }
        match request.uri().path() {
            "/healthz" => encoded(StatusCode::OK, &self.health()),
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
mod tests;
