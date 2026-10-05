//! What can go wrong in a call, and how it becomes the surface's own error
//! type for the trait methods.
//!
//! **The status mapping, in reverse.** The surface answers every error
//! with `ErrorStatus::status` of the error and the error's wire JSON
//! (`http_api.md`, Status mapping). The client decodes the body as the
//! route's error type and accepts it only when that error's status is the
//! status received ([`decode_error`]): a body whose error belongs to
//! another status is [`ClientError::StatusMismatch`], not trusted. A `401`
//! is the binding's [`AuthError`]. Anything without an L8 body (a proxy's
//! `502`, an HTML page) is [`ClientError::UnexpectedResponse`], and so is
//! a body holding a client-only error (`Unavailable`), which a server
//! never answers.
//!
//! **Into the trait's error.** `QueryApi` and `OperatorActions` return the
//! surface's `QueryError` and `ActionError`. The trait methods return the
//! decoded error as is, and a request the client could not encode as
//! `InvalidInput(MalformedRequest)` (what the server would answer it
//! with). A call that never reached a surface that answered is the
//! client-only `Unavailable { kind, reason }` ([`ClientError::unavailable`]):
//!
//! | `ClientError` | `UnavailableKind` | `reason` starts with |
//! | --- | --- | --- |
//! | `Unauthenticated` (a `401`) | `Unauthenticated` | `no caller: ` |
//! | `Transport(Send)` | `Transport` | `sending the request: ` |
//! | `Transport(Body)` | `Body` | `reading the body: ` |
//! | `Transport(Timeout)` | `Timeout` | `no response within ` |
//!
//! Everything else (`Transport(Build)`, `Transport(TooLarge)`,
//! `StatusMismatch`, `UnexpectedResponse`) is `Store { reason }`: a
//! response came back, but not one the binding describes. Either way the
//! reason is the `ClientError`'s text, so it names the cause.

use std::fmt::Debug;

use crosstalk_spec::interfaces::l8_surface::http::{AuthError, EncodeError, ErrorStatus, Route};
use crosstalk_spec::interfaces::l8_surface::{
    ActionError, InputError, QueryError, UnavailableKind,
};
use crosstalk_spec::wire::DecodeErrorKind;
use serde::de::DeserializeOwned;

/// An error type a route answers with: decoded from the body, with the
/// status the binding gives it.
pub trait ApiError: DeserializeOwned + ErrorStatus + Debug + Send + 'static {
    /// Whether only a client produces this error: a server never answers
    /// it, so a body holding one is not the surface's answer.
    fn is_client_only(&self) -> bool;
}

impl ApiError for QueryError {
    fn is_client_only(&self) -> bool {
        QueryError::is_client_only(self)
    }
}

impl ApiError for ActionError {
    fn is_client_only(&self) -> bool {
        ActionError::is_client_only(self)
    }
}

/// Why the HTTP exchange itself failed: nothing the surface decided.
#[derive(Debug, thiserror::Error)]
pub enum TransportError {
    /// The request could not be built (an invalid header value).
    #[error("building the request: {0}")]
    Build(#[source] hyper::http::Error),
    /// Connecting, sending, or reading the response head failed.
    #[error("sending the request: {0}")]
    Send(#[source] hyper_util::client::legacy::Error),
    /// Reading the body failed: the connection was cut or reset.
    #[error("reading the body: {0}")]
    Body(#[source] Box<dyn std::error::Error + Send + Sync>),
    /// No response, or no bytes of a stream, within the configured time.
    #[error("no response within {millis} ms")]
    Timeout { millis: u128 },
    /// A body or one line longer than the configured limit.
    #[error("a response over {limit} bytes")]
    TooLarge { limit: usize },
}

/// Why a call failed, for an error type `E` the route answers with.
#[derive(Debug, thiserror::Error)]
pub enum ClientError<E: Debug> {
    /// The surface refused the call: the error it answered with, at the
    /// status the binding gives that error.
    #[error("the surface answered {0:?}")]
    Api(E),
    /// `401`: the request had no caller.
    #[error("no caller: {0:?}")]
    Unauthenticated(AuthError),
    /// The call does not follow the route table.
    #[error("the call does not follow the route table: {0:?}")]
    Encode(EncodeError),
    #[error(transparent)]
    Transport(#[from] TransportError),
    /// An error body whose error the binding answers with another status.
    #[error(
        "{route:?} answered {status} with {error:?}, which the binding answers with {expected}"
    )]
    StatusMismatch {
        route: Route,
        status: u16,
        expected: u16,
        error: E,
    },
    /// A response the binding does not describe: a status with no L8
    /// body, a success of the wrong content type, or a body that does not
    /// decode as the route's result.
    #[error("{route:?} answered {status}: {reason}")]
    UnexpectedResponse {
        route: Route,
        status: u16,
        reason: String,
    },
}

impl<E: Debug> ClientError<E> {
    /// The kind of `Unavailable` this error becomes through the traits:
    /// the call never reached a surface that answered it. `None` for an
    /// error that is the surface's answer, a call off the route table, or
    /// a response the binding does not describe.
    pub fn unavailable(&self) -> Option<UnavailableKind> {
        match self {
            Self::Unauthenticated(_) => Some(UnavailableKind::Unauthenticated),
            Self::Transport(TransportError::Send(_)) => Some(UnavailableKind::Transport),
            Self::Transport(TransportError::Body(_)) => Some(UnavailableKind::Body),
            Self::Transport(TransportError::Timeout { .. }) => Some(UnavailableKind::Timeout),
            Self::Api(_)
            | Self::Encode(_)
            | Self::Transport(TransportError::Build(_) | TransportError::TooLarge { .. })
            | Self::StatusMismatch { .. }
            | Self::UnexpectedResponse { .. } => None,
        }
    }

    pub(crate) fn unexpected(route: Route, status: u16, reason: impl Into<String>) -> Self {
        Self::UnexpectedResponse {
            route,
            status,
            reason: reason.into(),
        }
    }
}

/// The error a non-success response carries: a 401's `AuthError`, else the
/// route's error type, accepted only at its own status.
pub(crate) fn decode_error<E: ApiError>(route: Route, status: u16, body: &[u8]) -> ClientError<E> {
    if status == 401 {
        return match serde_json::from_slice::<AuthError>(body) {
            Ok(error) => ClientError::Unauthenticated(error),
            Err(error) => {
                ClientError::unexpected(route, status, format!("not an AuthError: {error}"))
            }
        };
    }
    match serde_json::from_slice::<E>(body) {
        Ok(error) if error.is_client_only() => ClientError::unexpected(
            route,
            status,
            format!("an error only a client produces: {error:?}"),
        ),
        Ok(error) => {
            let expected = error.status().code();
            if expected == status {
                ClientError::Api(error)
            } else {
                ClientError::StatusMismatch {
                    route,
                    status,
                    expected,
                    error,
                }
            }
        }
        Err(error) => ClientError::unexpected(route, status, format!("no error body: {error}")),
    }
}

fn malformed(error: &EncodeError) -> InputError {
    InputError::MalformedRequest {
        kind: DecodeErrorKind::Data,
        reason: format!("the client could not encode the request: {error:?}"),
    }
}

impl From<ClientError<QueryError>> for QueryError {
    fn from(error: ClientError<QueryError>) -> Self {
        let kind = error.unavailable();
        match (error, kind) {
            (ClientError::Api(error), _) => error,
            (ClientError::Encode(encode), _) => Self::InvalidInput(malformed(&encode)),
            (other, Some(kind)) => Self::Unavailable {
                kind,
                reason: other.to_string(),
            },
            (other, None) => Self::Store {
                reason: other.to_string(),
            },
        }
    }
}

impl From<ClientError<ActionError>> for ActionError {
    fn from(error: ClientError<ActionError>) -> Self {
        let kind = error.unavailable();
        match (error, kind) {
            (ClientError::Api(error), _) => error,
            (ClientError::Encode(encode), _) => Self::InvalidInput(malformed(&encode)),
            (other, Some(kind)) => Self::Unavailable {
                kind,
                reason: other.to_string(),
            },
            (other, None) => Self::Store {
                reason: other.to_string(),
            },
        }
    }
}
