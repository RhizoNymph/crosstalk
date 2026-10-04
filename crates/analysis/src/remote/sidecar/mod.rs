//! The Python topics sidecar (`sidecar/topics/`) behind the spec's
//! `TopicModel` and `LayoutFitter`: its client, configuration and typed
//! error, and the two adapters.
//!
//! The contract is `docs/features/topics_sidecar.md` ([`wire`] mirrors it).
//! Every call has a deadline ([`SidecarConfig::timeout`]); a timeout, a
//! transport failure, an unexpected status or a reply that breaks the
//! contract is a [`SidecarError`], which the adapters report as the spec's
//! backend failure (`TopicError::Backend`, `LayoutError::Backend`), never as
//! a deterministic `FitFailure`.

pub mod layout;
pub mod params;
pub mod topics;
pub mod wire;

use std::num::NonZeroU64;
use std::time::Duration;

use hyper::{Method, StatusCode};
use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio::time::Instant;

use crate::remote::http::{BaseUrl, HttpCall, HttpClient, HttpError, InvalidBaseUrl};
use crate::remote::matrix::MatrixError;
use wire::{CONTRACT, ErrorBody, HEALTH, Health};

/// Where the sidecar is and how long a call may take. In the gateway's
/// config: `"topics": {"base_url": "http://topics:8090", "timeout_ms":
/// 600000}`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SidecarConfig {
    base_url: BaseUrl,
    timeout: Duration,
}

impl SidecarConfig {
    /// A fit of 100 000 points takes minutes; ten is the default ceiling.
    pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(600);

    pub fn new(base_url: &str, timeout_ms: NonZeroU64) -> Result<Self, InvalidBaseUrl> {
        Ok(Self {
            base_url: BaseUrl::new(base_url)?,
            timeout: Duration::from_millis(timeout_ms.get()),
        })
    }

    pub fn with_default_timeout(base_url: &str) -> Result<Self, InvalidBaseUrl> {
        Ok(Self {
            base_url: BaseUrl::new(base_url)?,
            timeout: Self::DEFAULT_TIMEOUT,
        })
    }

    pub fn base_url(&self) -> &BaseUrl {
        &self.base_url
    }

    pub fn timeout(&self) -> Duration {
        self.timeout
    }
}

/// Why a sidecar call produced no usable reply.
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum SidecarError {
    #[error(transparent)]
    Http(#[from] HttpError),
    /// A status other than 200, with the decoded error body when it had
    /// one in the contract's shape.
    #[error("the sidecar answered {route} with {status}: {body}")]
    Status {
        route: &'static str,
        status: u16,
        body: StatusBody,
    },
    #[error("the sidecar's reply to {route} is not the contract's JSON: {reason}")]
    Decode { route: &'static str, reason: String },
    #[error("the sidecar's reply to {route} breaks the contract: {violation}")]
    Contract {
        route: &'static str,
        violation: ContractViolation,
    },
    #[error("encoding the request to {route}: {reason}")]
    Encode { route: &'static str, reason: String },
}

impl SidecarError {
    /// A 422 whose refusal the route does not expect (a layout refusal from
    /// the topics route, say).
    pub(crate) fn refused(route: &'static str, body: ErrorBody) -> Self {
        Self::Status {
            route,
            status: StatusCode::UNPROCESSABLE_ENTITY.as_u16(),
            body: StatusBody::Error(body),
        }
    }
}

/// An error reply's body.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StatusBody {
    Error(ErrorBody),
    /// Not an error body of the contract (a proxy's page, say), kept as text
    /// up to 512 characters.
    Other(String),
}

impl std::fmt::Display for StatusBody {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Error(body) => write!(f, "{body:?}"),
            Self::Other(text) => write!(f, "{text:?}"),
        }
    }
}

/// A well-formed reply whose content the contract forbids.
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum ContractViolation {
    #[error("the service speaks contract {got:?}, not {expected:?}")]
    ContractVersion { expected: &'static str, got: String },
    #[error("{got} labels for {expected} documents")]
    LabelCount { expected: usize, got: usize },
    #[error("label {label} of document {index} names no topic")]
    LabelOutOfRange { index: usize, label: i64 },
    #[error("topic {topic} has no member")]
    EmptyTopic { topic: usize },
    #[error("topic {topic}'s term {term:?} has weight {weight}, not a finite positive number")]
    TermWeight {
        topic: usize,
        term: String,
        weight: f64,
    },
    #[error("topic {topic}'s members' mean is the zero vector or not a unit embedding")]
    DegenerateCentroid { topic: usize },
    #[error("{got} coordinate rows for {expected} points")]
    RowCount { expected: u64, got: u32 },
    #[error("coordinates have {got} columns, not 2")]
    Columns { got: u16 },
    #[error("the coordinates: {0}")]
    Matrix(MatrixError),
}

/// A call's outcome before the route-specific mapping: the decoded reply,
/// or the error body of a 422 (a deterministic refusal the adapters map to
/// the spec's own errors).
#[derive(Debug)]
pub(crate) enum Outcome<T> {
    Reply(T),
    Refused(ErrorBody),
}

/// The HTTP client bound to one sidecar.
#[derive(Debug, Clone)]
pub struct SidecarClient {
    config: SidecarConfig,
    http: HttpClient,
}

impl SidecarClient {
    pub fn new(config: SidecarConfig, http: HttpClient) -> Self {
        Self { config, http }
    }

    pub fn config(&self) -> &SidecarConfig {
        &self.config
    }

    /// `GET /healthz`, checking the service speaks this contract version.
    pub async fn health(&self) -> Result<Health, SidecarError> {
        let call = HttpCall {
            method: Method::GET,
            uri: self.config.base_url.join(HEALTH)?,
            authorization: None,
            json: None,
        };
        let reply = self.http.send(call, self.config.timeout).await?;
        if reply.status != StatusCode::OK {
            return Err(status_error(HEALTH, reply.status, &reply.body));
        }
        let health: Health = decode(HEALTH, &reply.body)?;
        if health.contract != CONTRACT {
            return Err(SidecarError::Contract {
                route: HEALTH,
                violation: ContractViolation::ContractVersion {
                    expected: CONTRACT,
                    got: health.contract,
                },
            });
        }
        Ok(health)
    }

    /// `POST route` with `request`; a 200 decodes as `T`, a 422 as the
    /// refusal it carries, anything else is an error.
    pub(crate) async fn post<R: Serialize, T: DeserializeOwned>(
        &self,
        route: &'static str,
        request: &R,
        rows: u64,
    ) -> Result<Outcome<T>, SidecarError> {
        let json = serde_json::to_vec(request).map_err(|error| SidecarError::Encode {
            route,
            reason: error.to_string(),
        })?;
        let call = HttpCall {
            method: Method::POST,
            uri: self.config.base_url.join(route)?,
            authorization: None,
            json: Some(json),
        };
        let started = Instant::now();
        let result = self.http.send(call, self.config.timeout).await;
        let duration_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
        let reply = match result {
            Ok(reply) => reply,
            Err(error) => {
                tracing::warn!(route, rows, duration_ms, %error, "sidecar call failed");
                return Err(error.into());
            }
        };
        tracing::info!(
            route,
            rows,
            duration_ms,
            status = reply.status.as_u16(),
            "sidecar call"
        );
        match reply.status {
            StatusCode::OK => decode(route, &reply.body).map(Outcome::Reply),
            StatusCode::UNPROCESSABLE_ENTITY => match serde_json::from_slice(&reply.body) {
                Ok(body) => Ok(Outcome::Refused(body)),
                Err(_) => Err(status_error(route, reply.status, &reply.body)),
            },
            status => Err(status_error(route, status, &reply.body)),
        }
    }
}

fn decode<T: DeserializeOwned>(route: &'static str, body: &[u8]) -> Result<T, SidecarError> {
    serde_json::from_slice(body).map_err(|error| SidecarError::Decode {
        route,
        reason: error.to_string(),
    })
}

/// The error for a non-200 reply, with its body decoded when it is one of
/// the contract's.
pub(crate) fn status_error(route: &'static str, status: StatusCode, body: &[u8]) -> SidecarError {
    let body = match serde_json::from_slice::<ErrorBody>(body) {
        Ok(error) => StatusBody::Error(error),
        Err(_) => {
            let text = String::from_utf8_lossy(body);
            StatusBody::Other(text.chars().take(512).collect())
        }
    };
    SidecarError::Status {
        route,
        status: status.as_u16(),
        body,
    }
}
